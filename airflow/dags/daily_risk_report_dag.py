"""
Airflow DAG - Daily Patient Risk Reconciliation Report.

Even under a Kappa architecture, ORCHESTRATION of periodic reporting still
belongs in Airflow: Kafka/Spark handle continuous processing, but "run once
per simulated day, join the last 24h of vitals trend with the latest lab
results, compute a consolidated risk score" is a scheduled batch job over
the serving store (Postgres) -- not a new streaming query. This keeps the
Kappa boundary clean: Spark Streaming = continuous processing engine,
Airflow = scheduling/orchestration of periodic reporting & housekeeping.

Schedule: every 5 minutes to match the simulated day (SIMULATED_DAY_SECONDS=300).
Adjust `schedule_interval` to match whatever simulated-day length you use.

Task flow (check_pipeline_freshness >> compute_and_write_risk_report):
  1. check_pipeline_freshness -- reads MAX(event_timestamp) from vitals_raw and
     inserts a 'no_data' warning into pipeline_alerts if the newest reading is
     more than 2 minutes old. It only raises an alert; it does not stop the DAG.
  2. compute_and_write_risk_report -- for every patient with a windowed vitals
     trend, combines:
       - the latest trend_flag from vitals_windowed_agg
       - the number of 'patient_critical' alerts in the last simulated day
       - lab results from the latest simulated day that fall outside their
         reference_range (lab_results_raw)
     into a risk score and UPSERTs it into patient_risk_report
     (key: patient_id + report_day).

Risk score (capped at 100), weights in RISK_WEIGHTS:
  score = 30 if the vitals trend is 'worsening'
        + 5  per abnormal-vitals alert in the last simulated day
        + 15 per abnormal lab result in the latest lab day
  Level: >= 70 critical, >= 40 high, >= 15 medium, otherwise low.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

SIMULATED_DAY_SECONDS = 300

RISK_WEIGHTS = {
    "vitals_worsening": 30,
    "abnormal_vitals_24h_per_event": 5,
    "abnormal_lab_per_result": 15,
}


def compute_and_write_risk_report(**context):
    hook = PostgresHook(postgres_conn_id="hospital_postgres")

    # 1. Pull latest vitals trend per patient (last completed windows)
    vitals_trend = hook.get_records("""
        SELECT DISTINCT ON (patient_id)
            patient_id, trend_flag, spike_count
        FROM vitals_windowed_agg
        ORDER BY patient_id, window_start DESC
    """)

    # 2. Pull abnormal vitals count in last 24h (breach alerts raised)
    abnormal_counts = dict(hook.get_records(f"""
        SELECT patient_id, COUNT(*) FROM pipeline_alerts
        WHERE alert_type = 'patient_critical'
          AND triggered_at > now() - interval '{SIMULATED_DAY_SECONDS} seconds'
        GROUP BY patient_id
    """))

    # 3. Pull most recent day's abnormal lab results per patient
    lab_rows = hook.get_records("""
        SELECT patient_id, test_type, result_value, reference_range
        FROM lab_results_raw
        WHERE simulated_day = (SELECT MAX(simulated_day) FROM lab_results_raw)
    """)

    labs_by_patient = {}
    for patient_id, test_type, value, ref_range in lab_rows:
        try:
            low, high = [float(x) for x in ref_range.split("-")]
            is_abnormal = not (low <= value <= high)
        except Exception:
            is_abnormal = False
        if is_abnormal:
            labs_by_patient.setdefault(patient_id, []).append(
                {"test_type": test_type, "value": value, "reference_range": ref_range}
            )

    max_day_row = hook.get_first("SELECT COALESCE(MAX(simulated_day), 0) FROM lab_results_raw")
    report_day = max_day_row[0] if max_day_row else 0

    rows_written = 0
    for patient_id, trend_flag, spike_count in vitals_trend:
        abnormal_24h = abnormal_counts.get(patient_id, 0)
        abnormal_labs = labs_by_patient.get(patient_id, [])

        score = 0
        if trend_flag == "worsening":
            score += RISK_WEIGHTS["vitals_worsening"]
        score += abnormal_24h * RISK_WEIGHTS["abnormal_vitals_24h_per_event"]
        score += len(abnormal_labs) * RISK_WEIGHTS["abnormal_lab_per_result"]
        score = min(score, 100)

        if score >= 70:
            risk_level = "critical"
        elif score >= 40:
            risk_level = "high"
        elif score >= 15:
            risk_level = "medium"
        else:
            risk_level = "low"

        import json
        hook.run(
            """
            INSERT INTO patient_risk_report
                (patient_id, report_day, vitals_trend_flag, abnormal_vitals_24h,
                 latest_abnormal_labs, risk_score, risk_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (patient_id, report_day) DO UPDATE SET
                vitals_trend_flag = EXCLUDED.vitals_trend_flag,
                abnormal_vitals_24h = EXCLUDED.abnormal_vitals_24h,
                latest_abnormal_labs = EXCLUDED.latest_abnormal_labs,
                risk_score = EXCLUDED.risk_score,
                risk_level = EXCLUDED.risk_level,
                generated_at = now()
            """,
            parameters=(
                patient_id, report_day, trend_flag, abnormal_24h,
                json.dumps(abnormal_labs), score, risk_level,
            ),
        )
        rows_written += 1

    context["ti"].log.info(f"Daily risk report: wrote {rows_written} patient rows for day {report_day}")


def check_pipeline_freshness(**context):
    """Basic health check: alert if no vitals data received in N minutes."""
    hook = PostgresHook(postgres_conn_id="hospital_postgres")
    last_event = hook.get_first("SELECT MAX(event_timestamp) FROM vitals_raw")[0]
    if last_event is None:
        return
    from datetime import timezone
    staleness_minutes = (datetime.now(timezone.utc) - last_event.replace(tzinfo=timezone.utc)).total_seconds() / 60
    if staleness_minutes > 2:
        hook.run(
            """
            INSERT INTO pipeline_alerts (alert_type, severity, source_stage, message)
            VALUES ('no_data', 'warning', 'ingestion', %s)
            """,
            parameters=(f"No vitals data received in {staleness_minutes:.1f} minutes",),
        )
        context["ti"].log.warning(f"Freshness check FAILED: {staleness_minutes:.1f} min since last event")
    else:
        context["ti"].log.info(f"Freshness check OK: last event {staleness_minutes:.1f} min ago")


with DAG(
    dag_id="daily_patient_risk_report",
    description="Kappa orchestration: consolidated daily patient risk report + pipeline health check",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(seconds=SIMULATED_DAY_SECONDS),
    catchup=False,
    tags=["hospital", "risk-report", "kappa"],
) as dag:

    freshness_check = PythonOperator(
        task_id="check_pipeline_freshness",
        python_callable=check_pipeline_freshness,
    )

    build_risk_report = PythonOperator(
        task_id="compute_and_write_risk_report",
        python_callable=compute_and_write_risk_report,
    )

    freshness_check >> build_risk_report
