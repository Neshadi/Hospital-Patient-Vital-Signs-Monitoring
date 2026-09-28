# Simulated Clock

Real hospital operations run on a 24-hour cycle for daily lab batches. For demo purposes
within a single session, we compress:

    1 simulated "day" = 300 seconds (5 minutes)

This is controlled by `SIMULATED_DAY_SECONDS` (env var, default 300) in
`data-sources/lab_results_batch_producer.py` and must match the Airflow DAG's
`schedule_interval` in `airflow/dags/daily_risk_report_dag.py`.

Each lab-results batch drop increments `simulated_day` by 1. The Airflow DAG's
"daily" risk report similarly runs once per `SIMULATED_DAY_SECONDS` and tags its
output rows with the corresponding `report_day`.

Vitals events are NOT time-compressed — they emit continuously every
`EVENT_INTERVAL_SECONDS` (default 3s), representing genuine real-time monitoring.

State this configuration explicitly in the report per the assignment's requirement to
"state your simulated clock clearly."
