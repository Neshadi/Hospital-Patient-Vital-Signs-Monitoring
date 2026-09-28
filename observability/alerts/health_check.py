"""
Standalone health-check / alerting daemon.

Implements the two minimum observability rules required by the assignment:
  1. No data received in N minutes (per source: vitals, labs)
  2. Error rate above threshold (based on pipeline_alerts volume)

This can run as its own container/cron, independent of the Airflow DAG's
freshness check, to demonstrate a second, simpler alerting mechanism
(e.g. for the live demo / viva). In production you'd pick ONE of these,
not both -- documented here for completeness of the observability story.

Run:
    python observability/alerts/health_check.py
"""
import os
import time
from datetime import datetime, timezone

import psycopg2

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://hospital:hospital_pw@localhost:5432/hospital_monitoring"
)
CHECK_INTERVAL_SECONDS = int(os.environ.get("HEALTH_CHECK_INTERVAL", "30"))
NO_DATA_THRESHOLD_MINUTES = float(os.environ.get("NO_DATA_THRESHOLD_MINUTES", "2"))
ERROR_RATE_THRESHOLD = int(os.environ.get("ERROR_RATE_THRESHOLD", "5"))  # alerts per 5-min window


def check_no_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(event_timestamp) FROM vitals_raw")
        last_event = cur.fetchone()[0]
        if last_event is None:
            return
        staleness = (datetime.now(timezone.utc) - last_event.replace(tzinfo=timezone.utc)).total_seconds() / 60
        if staleness > NO_DATA_THRESHOLD_MINUTES:
            cur.execute(
                """
                INSERT INTO pipeline_alerts (alert_type, severity, source_stage, message)
                VALUES ('no_data', 'critical', 'ingestion', %s)
                """,
                (f"No vitals ingested for {staleness:.1f} min (threshold {NO_DATA_THRESHOLD_MINUTES})",),
            )
            conn.commit()
            print(f"[ALERT] no_data: {staleness:.1f} min since last vitals event")


def check_error_rate(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline_alerts
            WHERE alert_type = 'patient_critical'
              AND triggered_at > now() - interval '5 minutes'
        """)
        count = cur.fetchone()[0]
        if count > ERROR_RATE_THRESHOLD:
            cur.execute(
                """
                INSERT INTO pipeline_alerts (alert_type, severity, source_stage, message)
                VALUES ('high_error_rate', 'warning', 'processing', %s)
                """,
                (f"{count} critical breaches in last 5 min (threshold {ERROR_RATE_THRESHOLD})",),
            )
            conn.commit()
            print(f"[ALERT] high_error_rate: {count} breaches in last 5 min")


def main():
    print(f"Health-check daemon started. Interval={CHECK_INTERVAL_SECONDS}s")
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            check_no_data(conn)
            check_error_rate(conn)
            conn.close()
        except Exception as exc:
            print(f"[health-check] error: {exc}")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
