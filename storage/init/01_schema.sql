-- ============================================================
-- Hospital Vitals Monitoring - Serving Layer Schema
-- ============================================================

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

-- Windowed aggregation output from Spark (e.g. 1-minute vitals trend per patient)
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

-- Consolidated daily risk report: vitals trend + latest lab results joined
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

-- Observability: alert log
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
