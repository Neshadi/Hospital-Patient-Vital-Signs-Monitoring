"""
Kappa-architecture stream processor.

Consumes TWO Kafka topics with the SAME Structured Streaming engine:
  - vitals-stream : high-frequency bedside monitor events
  - lab-results   : low-frequency ("daily") pathology results

Pipeline stages (three independent streaming queries run side by side):
  1. Parse + clean both streams
       - from_json with an explicit schema (VITALS_SCHEMA / LAB_SCHEMA)
       - vitals get an event_time column and a 2-minute watermark for late data
       - implausible sensor readings (HR outside 20-250, SpO2 outside 50-100)
         are dropped before alerting
  2. Windowed aggregation of vitals per patient (trend detection)
       - 1-minute tumbling window per patient: avg HR / SpO2 / systolic BP,
         max temperature, spike count
       - trend_flag = 'worsening' if the window contains any simulated spike,
         otherwise 'stable'
       - UPSERTed into vitals_windowed_agg every 30 s (key: patient_id + window_start)
  3. Persist to PostgreSQL (10 s micro-batches via foreachBatch + JDBC append)
       - vitals-stream -> vitals_raw
       - lab-results   -> lab_results_raw
  4. Threshold alerting (per vitals micro-batch)
       - HR > HR_HIGH or < HR_LOW, SpO2 < SPO2_LOW, systolic BP > SBP_HIGH,
         temperature > TEMP_HIGH -> 'patient_critical' row in pipeline_alerts
       - a failed micro-batch writes an 'error_rate' alert instead of crashing
  5. Health metrics
       - one heartbeat row per vitals micro-batch in pipeline_health
         (events processed, last event time)

Run (inside the spark-processor container):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
        org.postgresql:postgresql:42.7.3 processing/jobs/streaming_job.py
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from observability.logging.logging_config import get_logger

log = get_logger("spark-processor")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://localhost:5432/hospital_monitoring")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "hospital")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "hospital_pw")

# --- Alert thresholds (kept simple/explicit for the report's observability section) ---
HR_HIGH, HR_LOW = 130, 45
SPO2_LOW = 90
SBP_HIGH = 170
TEMP_HIGH = 38.5
NO_DATA_ALERT_MINUTES = 2  # if a patient has no reading in this window -> health alert

VITALS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("heart_rate", DoubleType()),
    StructField("spo2", DoubleType()),
    StructField("systolic_bp", DoubleType()),
    StructField("diastolic_bp", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("is_simulated_spike", BooleanType()),
    StructField("spike_type", StringType()),
])

LAB_SCHEMA = StructType([
    StructField("record_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("test_type", StringType()),
    StructField("result_value", DoubleType()),
    StructField("unit", StringType()),
    StructField("reference_range", StringType()),
    StructField("collected_at", StringType()),
    StructField("simulated_day", IntegerType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HospitalVitalsKappaPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_jdbc(df, table_name: str):
    """Common JDBC sink helper used inside foreachBatch."""
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_vitals_windowed_agg_upsert(df, batch_id: int):
    """UPSERT the 1-minute patient window aggregate using patient_id + window_start as the key."""
    if df.rdd.isEmpty():
        return

    import psycopg2

    rows = df.select(
        "patient_id",
        "window_start",
        "window_end",
        "avg_heart_rate",
        "avg_spo2",
        "avg_systolic_bp",
        "max_temperature",
        "spike_count",
        "trend_flag",
    ).collect()

    if not rows:
        return

    pg_url = POSTGRES_URL.replace("jdbc:postgresql://", "postgresql://")
    conn = psycopg2.connect(pg_url, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO vitals_windowed_agg (
                        patient_id, window_start, window_end,
                        avg_heart_rate, avg_spo2, avg_systolic_bp,
                        max_temperature, spike_count, trend_flag
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (patient_id, window_start)
                    DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        avg_heart_rate = EXCLUDED.avg_heart_rate,
                        avg_spo2 = EXCLUDED.avg_spo2,
                        avg_systolic_bp = EXCLUDED.avg_systolic_bp,
                        max_temperature = EXCLUDED.max_temperature,
                        spike_count = EXCLUDED.spike_count,
                        trend_flag = EXCLUDED.trend_flag,
                        computed_at = now()
                    """,
                    (
                        row.patient_id,
                        row.window_start,
                        row.window_end,
                        row.avg_heart_rate,
                        row.avg_spo2,
                        row.avg_systolic_bp,
                        row.max_temperature,
                        row.spike_count,
                        row.trend_flag,
                    ),
                )
        conn.commit()
        log.info("Windowed aggregate micro-batch %d wrote %d rows with UPSERT", batch_id, len(rows))
    except Exception as exc:
        conn.rollback()
        log.error("Upsert failed for vitals_windowed_agg batch %d: %s", batch_id, exc)
        raise
    finally:
        conn.close()


def process_vitals_batch(batch_df, batch_id: int):
    """foreachBatch sink for the raw + windowed vitals stream."""
    count = batch_df.count()
    log.info("Vitals micro-batch %d: %d rows", batch_id, count)
    if count == 0:
        return
    try:
        raw = batch_df.select(
            "event_id", "patient_id", "heart_rate", "spo2",
            "systolic_bp", "diastolic_bp", "temperature",
            F.col("timestamp").cast("timestamp").alias("event_timestamp"),
            "is_simulated_spike",
        )
        write_jdbc(raw, "vitals_raw")

        # Threshold-based alerting (data-quality cleaned: drop obviously bad sensor rows first)
        clean = raw.filter(
            (F.col("heart_rate").between(20, 250)) &
            (F.col("spo2").between(50, 100))
        )
        breaches = clean.filter(
            (F.col("heart_rate") > HR_HIGH) | (F.col("heart_rate") < HR_LOW) |
            (F.col("spo2") < SPO2_LOW) |
            (F.col("systolic_bp") > SBP_HIGH) |
            (F.col("temperature") > TEMP_HIGH)
        )
        breach_count = breaches.count()
        if breach_count > 0:
            alerts = breaches.select(
                F.lit("patient_critical").alias("alert_type"),
                F.lit("critical").alias("severity"),
                F.lit("processing").alias("source_stage"),
                F.concat(F.lit("Vital sign breach for patient "), F.col("patient_id")).alias("message"),
                F.col("patient_id"),
            )
            write_jdbc(alerts, "pipeline_alerts")
            log.warning("Vitals micro-batch %d: %d threshold breaches -> alerts written", batch_id, breach_count)

        # Heartbeat / health metric update
        health = batch_df.sparkSession.createDataFrame(
            [("spark-processor-vitals", count, 0)],
            ["stage", "events_processed", "errors_count"],
        ).withColumn("last_event_at", F.current_timestamp()) \
         .withColumn("updated_at", F.current_timestamp())
        # Upsert-style: append is fine for a mini-project; note in report as a
        # simplification (production would use MERGE/UPSERT).
        write_jdbc(health, "pipeline_health")

    except Exception as exc:
        log.error("Error processing vitals micro-batch %d: %s", batch_id, exc)
        error_schema = StructType([
            StructField("alert_type", StringType()),
            StructField("severity", StringType()),
            StructField("source_stage", StringType()),
            StructField("message", StringType()),
            StructField("patient_id", StringType()),
        ])
        error_row = batch_df.sparkSession.createDataFrame(
            [("error_rate", "critical", "processing", f"Vitals batch {batch_id} failed: {exc}", None)],
            schema=error_schema,
        )
        try:
            write_jdbc(error_row, "pipeline_alerts")
        except Exception:
            log.error("Also failed to write the error alert itself - check Postgres connectivity")


def process_lab_batch(batch_df, batch_id: int):
    """foreachBatch sink for the raw lab-results stream."""
    count = batch_df.count()
    log.info("Lab-results micro-batch %d: %d rows", batch_id, count)
    if count == 0:
        return
    try:
        raw = batch_df.select(
            "record_id", "patient_id", "test_type", "result_value", "unit",
            "reference_range",
            F.col("collected_at").cast("timestamp").alias("collected_at"),
            "simulated_day",
        )
        write_jdbc(raw, "lab_results_raw")
    except Exception as exc:
        log.error("Error processing lab micro-batch %d: %s", batch_id, exc)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---- Vitals stream: parse -> windowed trend aggregation ----
    vitals_raw_stream = read_kafka_stream(spark, "vitals-stream")
    vitals_parsed = (
        vitals_raw_stream
        .select(F.from_json(F.col("value").cast("string"), VITALS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withWatermark("event_time", "2 minutes")
    )

    vitals_query = (
        vitals_parsed.writeStream
        .foreachBatch(process_vitals_batch)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/vitals")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 1-minute tumbling window trend (separate query -> vitals_windowed_agg)
    windowed = (
        vitals_parsed
        .groupBy(F.col("patient_id"), F.window(F.col("event_time"), "1 minute"))
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.avg("spo2").alias("avg_spo2"),
            F.avg("systolic_bp").alias("avg_systolic_bp"),
            F.max("temperature").alias("max_temperature"),
            F.sum(F.col("is_simulated_spike").cast("int")).alias("spike_count"),
        )
        .select(
            "patient_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_heart_rate", "avg_spo2", "avg_systolic_bp", "max_temperature", "spike_count",
            F.when(F.col("spike_count") > 0, F.lit("worsening")).otherwise(F.lit("stable")).alias("trend_flag"),
        )
    )
    windowed_query = (
        windowed.writeStream
        .foreachBatch(write_vitals_windowed_agg_upsert)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/vitals_windowed")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ---- Lab-results stream: parse -> raw sink ----
    lab_raw_stream = read_kafka_stream(spark, "lab-results")
    lab_parsed = (
        lab_raw_stream
        .select(F.from_json(F.col("value").cast("string"), LAB_SCHEMA).alias("data"))
        .select("data.*")
    )
    lab_query = (
        lab_parsed.writeStream
        .foreachBatch(process_lab_batch)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/labs")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("All streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
