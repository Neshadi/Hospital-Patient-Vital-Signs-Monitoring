-- ============================================================
-- Hospital Vitals Monitoring - Serving Layer Schema
-- ============================================================
-- Tables and the pipeline stage that owns each:
--   vitals_raw           <- Spark streaming job (vitals-stream topic)
--   lab_results_raw      <- Spark streaming job (lab-results topic)
--   vitals_windowed_agg  <- Spark streaming job (1-minute window aggregate)
--   patient_risk_report  <- Airflow DAG daily_patient_risk_report
--   pipeline_alerts      <- Spark (threshold breaches) + Airflow (no-data check)
--   pipeline_health      <- Spark (heartbeat per micro-batch)
-- The API in serving/ reads vitals_raw, vitals_windowed_agg,
-- patient_risk_report and pipeline_alerts.
-- ============================================================

-- Raw vitals events from bedside monitors, one row per event (event_id is the
-- dedup key). Source for the freshness check and per-patient history.
CREATE TABLE IF NOT EXISTS vitals_raw (
    event_id        TEXT PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    heart_rate      NUMERIC,
    spo2            NUMERIC,
    systolic_bp     NUMERIC,
    diastolic_bp    NUMERIC,
    temperature     NUMERIC,
    event_timestamp TIMESTAMPTZ NOT NULL,
    is_simulated_spike BOOLEAN DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vitals_patient_time ON vitals_raw (patient_id, event_timestamp DESC);

-- Raw lab results (low-frequency, one batch per simulated day). The Airflow DAG
-- compares result_value against reference_range to find abnormal results.
CREATE TABLE IF NOT EXISTS lab_results_raw (
    record_id       TEXT PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    test_type       TEXT NOT NULL,
    result_value    NUMERIC,
    unit            TEXT,
    reference_range TEXT,
    collected_at    TIMESTAMPTZ,
    simulated_day   INT,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lab_patient_day ON lab_results_raw (patient_id, simulated_day DESC);

-- Windowed aggregation output from Spark (e.g. 1-minute vitals trend per patient).
-- UPSERTed on (patient_id, window_start) as windows update; the DAG reads the
-- latest row per patient as the current trend_flag.
CREATE TABLE IF NOT EXISTS vitals_windowed_agg (
    patient_id      TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    avg_heart_rate  NUMERIC,
    avg_spo2        NUMERIC,
    avg_systolic_bp NUMERIC,
    max_temperature NUMERIC,
    spike_count     INT,
    trend_flag      TEXT,  -- 'stable' | 'worsening' | 'improving'
    computed_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);

-- Consolidated daily risk report: vitals trend + latest lab results joined.
-- Written only by the Airflow DAG, one row per patient per report_day.
CREATE TABLE IF NOT EXISTS patient_risk_report (
    patient_id          TEXT NOT NULL,
    report_day          INT NOT NULL,
    vitals_trend_flag   TEXT,
    abnormal_vitals_24h INT,
    latest_abnormal_labs JSONB,
    risk_score          NUMERIC,
    risk_level          TEXT, -- 'low' | 'medium' | 'high' | 'critical'
    generated_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (patient_id, report_day)
);

-- Observability: alert log. Patient-level alerts carry a patient_id; pipeline-level
-- alerts (e.g. 'no_data') leave it NULL. The DAG counts 'patient_critical' rows
-- when scoring risk.
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    alert_id     SERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,     -- 'no_data', 'high_error_rate', 'patient_critical'
    severity     TEXT NOT NULL,     -- 'warning' | 'critical'
    source_stage TEXT NOT NULL,     -- 'ingestion' | 'processing' | 'storage'
    message      TEXT NOT NULL,
    patient_id   TEXT,
    triggered_at TIMESTAMPTZ DEFAULT now(),
    resolved_at  TIMESTAMPTZ
);

-- Observability: pipeline health / heartbeat metrics.
-- NOTE: no primary key here on purpose. Spark's JDBC sink uses append mode
-- once per micro-batch (see streaming_job.py), so this is a time-series log
-- of heartbeats rather than a single upserted row per stage. Query the
-- latest row per stage with `SELECT DISTINCT ON (stage) ... ORDER BY stage, updated_at DESC`.
CREATE TABLE IF NOT EXISTS pipeline_health (
    stage           TEXT NOT NULL,
    last_event_at   TIMESTAMPTZ,
    events_processed BIGINT DEFAULT 0,
    errors_count    BIGINT DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
