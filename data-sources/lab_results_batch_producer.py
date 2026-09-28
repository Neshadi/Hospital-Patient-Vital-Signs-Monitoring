"""
Simulated daily-batch source: pathology lab uploads one results file per
simulated day, per patient.

Even though this is conceptually a "batch drop", in the Kappa architecture
we publish each record onto the Kafka topic `lab-results` -- it is simply
a much lower-frequency stream than vitals. This means Spark Structured
Streaming can consume both topics with ONE consistent programming model
(the core justification for choosing Kappa over Lambda for this project).

Simulated clock: 1 "day" = SIMULATED_DAY_SECONDS (default 300s = 5 min).
State this clearly in the report as required by the assignment.

Run standalone:
    python lab_results_batch_producer.py
"""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | lab-producer | %(levelname)s | %(message)s",
)
log = logging.getLogger("lab-producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "lab-results"
SIMULATED_DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))
NUM_PATIENTS = int(os.environ.get("NUM_PATIENTS", "10"))
PATIENT_IDS = [f"P{str(i).zfill(4)}" for i in range(1, NUM_PATIENTS + 1)]

# test_type -> (normal_low, normal_high, unit)
LAB_TESTS = {
    "WBC": (4.0, 11.0, "10^9/L"),
    "Creatinine": (0.6, 1.3, "mg/dL"),
    "Potassium": (3.5, 5.1, "mmol/L"),
    "CRP": (0.0, 5.0, "mg/L"),
    "Hemoglobin": (12.0, 17.5, "g/dL"),
}


def generate_daily_file(day_index: int) -> list[dict]:
    records = []
    collected_at = datetime.now(timezone.utc).isoformat()
    for patient_id in PATIENT_IDS:
        # each patient gets 1-3 tests this simulated day
        for test_type in random.sample(list(LAB_TESTS.keys()), k=random.randint(1, 3)):
            low, high, unit = LAB_TESTS[test_type]
            # 15% chance of an abnormal result to feed the risk-reconciliation logic
            if random.random() < 0.15:
                value = round(random.uniform(high * 1.2, high * 1.8), 2)
            else:
                value = round(random.uniform(low, high), 2)
            records.append({
                "record_id": str(uuid.uuid4()),
                "patient_id": patient_id,
                "test_type": test_type,
                "result_value": value,
                "unit": unit,
                "reference_range": f"{low}-{high}",
                "collected_at": collected_at,
                "simulated_day": day_index,
            })
    return records


def build_producer(max_retries: int = 10, retry_delay_seconds: float = 5.0) -> KafkaProducer:
    """Connects to Kafka with retries, since the broker may still be
    finishing startup even after depends_on/health-check conditions pass."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
            )
        except Exception as exc:
            last_exc = exc
            log.warning(
                "Kafka not ready yet (attempt %d/%d): %s. Retrying in %ss...",
                attempt, max_retries, exc, retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts") from last_exc


def main():
    producer = build_producer()
    log.info(
        "Starting daily lab-results batch drop every %ss (simulated day) -> topic '%s'",
        SIMULATED_DAY_SECONDS, TOPIC,
    )
    day_index = 0
    try:
        while True:
            day_index += 1
            records = generate_daily_file(day_index)
            log.info("Simulated day %d: generated %d lab records", day_index, len(records))
            for record in records:
                try:
                    producer.send(TOPIC, key=record["patient_id"], value=record).get(timeout=10)
                except Exception as exc:
                    log.error("Failed to publish lab record for %s: %s", record["patient_id"], exc)
            producer.flush()
            log.info("Day %d batch drop complete. Sleeping %ss until next day.", day_index, SIMULATED_DAY_SECONDS)
            time.sleep(SIMULATED_DAY_SECONDS)
    except KeyboardInterrupt:
        log.info("Shutting down lab-results producer at day %d", day_index)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
