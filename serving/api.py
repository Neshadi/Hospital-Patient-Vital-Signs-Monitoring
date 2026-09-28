"""
Serving layer - FastAPI application.

Endpoints:
  GET /health                        -> API + DB liveness
  GET /ward/live                     -> real-time ward monitoring snapshot
  GET /patients/{patient_id}/trend   -> recent windowed vitals trend
  GET /reports/daily/{report_day}    -> consolidated daily risk report
  GET /alerts/active                 -> unresolved pipeline/patient alerts

Run:
    uvicorn serving.api:app --host 0.0.0.0 --port 8000 --reload
"""
import logging
import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | serving-api | %(levelname)s | %(message)s",
)
log = logging.getLogger("serving-api")

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://hospital:hospital_pw@localhost:5432/hospital_monitoring"
)

app = FastAPI(title="Hospital Ward Monitoring API", version="0.1.0")

# Live dashboard: http://localhost:8000/dashboard/
app.mount("/dashboard", StaticFiles(directory="serving/static", html=True), name="dashboard")


def get_conn():
    return psycopg2.connect(POSTGRES_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as exc:
        log.error("DB health check failed: %s", exc)
        raise HTTPException(status_code=503, detail="database unreachable")


@app.get("/ward/live")
def ward_live():
    """Real-time snapshot: latest reading + trend flag per patient."""
    query = """
        SELECT DISTINCT ON (v.patient_id)
            v.patient_id, v.heart_rate, v.spo2, v.systolic_bp,
            v.diastolic_bp, v.temperature, v.event_timestamp,
            w.trend_flag
        FROM vitals_raw v
        LEFT JOIN LATERAL (
            SELECT trend_flag FROM vitals_windowed_agg wa
            WHERE wa.patient_id = v.patient_id
            ORDER BY window_start DESC LIMIT 1
        ) w ON true
        ORDER BY v.patient_id, v.event_timestamp DESC
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {"patients": rows, "count": len(rows)}


@app.get("/patients/{patient_id}/trend")
def patient_trend(patient_id: str, limit: int = 20):
    query = """
        SELECT window_start, window_end, avg_heart_rate, avg_spo2,
               avg_systolic_bp, max_temperature, spike_count, trend_flag
        FROM vitals_windowed_agg
        WHERE patient_id = %s
        ORDER BY window_start DESC
        LIMIT %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (patient_id, limit))
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No trend data for {patient_id}")
    return {"patient_id": patient_id, "trend": rows}


@app.get("/reports/daily/{report_day}")
def daily_report(report_day: int):
    query = """
        SELECT patient_id, vitals_trend_flag, abnormal_vitals_24h,
               latest_abnormal_labs, risk_score, risk_level, generated_at
        FROM patient_risk_report
        WHERE report_day = %s
        ORDER BY risk_score DESC
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (report_day,))
        rows = cur.fetchall()
    return {"report_day": report_day, "patients": rows, "count": len(rows)}


@app.get("/alerts/active")
def active_alerts():
    query = """
        SELECT alert_id, alert_type, severity, source_stage, message,
               patient_id, triggered_at
        FROM pipeline_alerts
        WHERE resolved_at IS NULL
        ORDER BY triggered_at DESC
        LIMIT 100
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {"alerts": rows, "count": len(rows)}
