# Hospital Patient Vital Signs Monitoring — Kappa Architecture Pipeline

Mini-project for Applied Big Data Engineering (EC8203). Use Case 2.

## Architecture Summary

**Chosen architecture: Kappa** (see `docs/architecture_decision.md` for full justification
and the rejected Lambda alternative — this belongs in the report's 20-mark section).

```
                 ┌────────────────────┐
 Bedside          │  Kafka             │
 Monitors  ─────▶ │  topic:            │
 (producer)       │  vitals-stream     │──┐
                 └────────────────────┘  │
                                          │      ┌─────────────────────────┐      ┌────────────┐
                 ┌────────────────────┐  ├────▶ │ Spark Structured        │────▶ │ PostgreSQL │
 Pathology Lab    │  Kafka             │  │      │ Streaming               │      │ (serving   │
 (daily batch     │  topic:            │──┘      │ - clean/validate        │      │  store)    │
  producer)       │  lab-results       │          │ - windowed vitals trend │      └─────┬──────┘
                 └────────────────────┘          │ - threshold alerts      │            │
                                                  │ - stream-stream join    │            │
                                                  └─────────────────────────┘            │
                                                                                          ▼
                                          ┌───────────────────────┐            ┌──────────────────┐
                                          │ Airflow DAG (5-min)   │───────────▶│ FastAPI serving   │
                                          │ - freshness check     │            │ /ward/live        │
                                          │ - daily risk report   │            │ /reports/daily/N  │
                                          └───────────────────────┘            │ /alerts/active    │
                                                                                └──────────────────┘
```

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Apache Kafka | Decouples producers/consumers; partitioned `vitals-stream` scales per-patient throughput; retained topics enable Kappa-style replay |
| Stream processing | Spark Structured Streaming | Native Kafka source, windowing + watermarking for trend detection, `foreachBatch` for JDBC sink + alerting logic |
| Orchestration | Apache Airflow | Schedules the periodic (simulated-daily) risk-reconciliation job and pipeline freshness check over the serving store |
| Storage/Serving | PostgreSQL | Relational joins (trend + labs + alerts) needed for the risk report; simple to query from FastAPI |
| Serving API | FastAPI | Lightweight REST layer for the "live dashboard" requirement |

## Simulated Clock

1 simulated **day** = **300 seconds (5 minutes)**, configurable via `SIMULATED_DAY_SECONDS`.
The Airflow DAG's `schedule_interval` must be kept in sync with this value. See
`docs/simulated_clock.md`.

## Repository Layout

```
hospital-vitals-pipeline/
├── docker-compose.yml          # Kafka, Postgres, Spark, Airflow, API, producers
├── data-sources/               # Simulated streaming + daily-batch sources
├── processing/jobs/            # Spark Structured Streaming job
├── storage/init/               # Postgres schema (DDL)
├── serving/                    # FastAPI serving layer
├── airflow/dags/               # Daily risk report + freshness-check DAG
├── observability/
│   ├── logging/                # Shared JSON structured logging
│   └── alerts/                 # Standalone no-data / error-rate health-check daemon
├── docker/                     # Per-service Dockerfiles
├── tests/                      # Unit tests
└── docs/                       # Architecture decision, simulated clock notes
```

## Running the Pipeline

Prerequisites: Docker Desktop installed and **running** (check the whale icon / app status
before running any command below — most first-time failures are simply Docker Desktop not
having finished starting).

### First-time setup (or after `down -v`)

```bash
docker compose down -v        # only needed if re-running; safe to skip on a truly first run
docker compose up -d --build
```

Run it **detached** (`-d`) rather than in the foreground: if the foreground process is
interrupted (accidentally closing the terminal, `Ctrl+C`, laptop sleep), Docker stops every
container with it, which tends to leave slower-starting containers behind in a `Created`
(never-started) state. `-d` avoids that class of problem entirely.

First run downloads several large images (Kafka, Airflow, Spark) and Spark's Kafka/Postgres
JDBC packages via Maven on first job start — allow **5–10 minutes** before assuming something
is wrong.

### Verify it came up correctly

```bash
docker compose ps -a
```
Every service should show `Up` **except** `kafka-init`, which should show `Exited (0)` — it is
a one-shot job that creates the Kafka topics and is expected to exit once done. If anything
else shows `Created` (never started) or `Exited` with a non-zero code, see Troubleshooting
below.

```bash
docker compose logs --tail=30 spark-processor
```
Look for `All streaming queries started. Awaiting termination...` with no Python traceback
after it. The first few `Vitals micro-batch N: 0 rows` lines are normal while the producers'
first events are still arriving.

```bash
docker compose logs --tail=10 vitals-producer
```
Should show a new event roughly every 3 seconds, with no `Traceback` / `NoBrokersAvailable`.

```bash
docker exec -it hospital-vitals-pipeline-postgres-1 psql -U hospital -d hospital_monitoring -c "SELECT count(*) FROM vitals_raw;"
```
Count should be > 0 and climbing on repeated runs once the stack has been up for a minute or two.

### Open the UIs

- **Live dashboard**: `http://localhost:8000/dashboard/` — patient monitor grid, active alerts
  feed, daily risk report table (polls the API every 5–15s, no build step required)
- Ward live dashboard data (raw JSON): `curl http://localhost:8000/ward/live`
- Active alerts: `curl http://localhost:8000/alerts/active`
- Daily risk report: `curl http://localhost:8000/reports/daily/0` (see the note on
  `report_day` below — the very first report is written under day `0`, not `1`)
- **Airflow UI**: `http://localhost:8081` (note: **8081**, not Airflow's usual 8080 — this is
  remapped in `docker-compose.yml` to avoid colliding with software many machines already run
  on port 8080). Username is always `admin`; the password is regenerated on every
  `down -v` — retrieve it with:
```bash
  docker exec -it hospital-vitals-pipeline-airflow-webserver-1 cat /opt/airflow/simple_auth_manager_passwords.json
```
  or set a known one directly:
```bash
  docker exec -it hospital-vitals-pipeline-airflow-webserver-1 airflow users create --username admin --password admin123 --firstname Admin --lastname User --role Admin --email admin@example.com
```
  The DAG (`daily_patient_risk_report`) must be toggled **on** (blue) the first time — new
  DAGs load paused by default — and can be run immediately with the ▶ **Trigger DAG** button
  rather than waiting for its 5-minute schedule.

### Stopping

```bash
docker compose stop      # stop containers, keep all data — resume later with `docker compose start`
docker compose down      # stop and remove containers, keep the Postgres volume (data survives)
docker compose down -v   # stop, remove containers AND the Postgres volume (full reset, including Airflow's own metadata/admin user)
```

### Daily use (no code changes since last build)

```bash
docker compose up -d     # skip --build; much faster once images already exist
docker compose ps -a
```

## Troubleshooting

Issues actually encountered while building and running this project, in case they recur:

| Symptom | Cause | Fix |
|---|---|---|
| `error during connect ... dockerDesktopLinuxEngine` | Docker Desktop app isn't running yet | Open Docker Desktop, wait for "Running" status, retry |
| Several services stuck in `Created`, never `Up` | `docker compose up --build` (foreground) was interrupted before dependent containers started | Re-run with `docker compose up -d` (detached) so an interrupted terminal can't take containers down with it |
| `bitnami/spark:3.5.1: not found` during build | Bitnami moved versioned images behind a paid tier in 2025 | Already fixed in this repo's `docker/spark.Dockerfile`, which uses the official `apache/spark:3.5.1-python3` image instead |
| `Ports are not available: ... 0.0.0.0:8080` | Another process (or a previous container) already holds port 8080 | Already fixed — Airflow is mapped to host port **8081** in `docker-compose.yml` |
| `vitals-producer` / `lab-results-producer` exit with `kafka.errors.NoBrokersAvailable` shortly after `Created` state clears | Race condition: the producer started before Kafka had *finished* becoming ready, only after `kafka-init`'s container had *started* (not completed) | Already fixed — producers now `depends_on: kafka-init: condition: service_completed_successfully`, have `restart: unless-stopped`, and retry the Kafka connection internally with backoff |
| Spark job crashes with `column "event_id" is of type uuid but expression is of type character varying` | Spark's JDBC writer sends plain strings; Postgres columns were declared `UUID` and don't auto-cast | Already fixed — `event_id`/`record_id` columns are `TEXT` in `storage/init/01_schema.sql` |
| Spark job crashes on the **second** micro-batch with a duplicate-key error on `pipeline_health` | That table had a `PRIMARY KEY (stage)` but the sink appends a new heartbeat row every batch | Already fixed — the primary key was removed; `pipeline_health` is a time-series log, queried with `SELECT DISTINCT ON (stage) ... ORDER BY stage, updated_at DESC` for the latest row |
| Airflow task `check_pipeline_freshness` / `compute_and_write_risk_report` fails with `AirflowNotFoundException: The conn_id 'hospital_postgres' isn't defined` | Airflow's own metadata database (where UI-created connections live) is wiped by `docker compose down -v` along with everything else | Already fixed — the connection is defined as the environment variable `AIRFLOW_CONN_HOSPITAL_POSTGRES` on the `airflow-webserver` service, so it exists automatically on every startup regardless of volume state |
| `/reports/daily/1` returns no rows even though the DAG succeeded | The very first DAG run typically executes before the lab-results producer has emitted its first simulated-day batch, so `MAX(simulated_day)` is `NULL` and the code's `COALESCE(..., 0)` writes the report under `report_day = 0`, not `1` | Not a bug — query `/reports/daily/0` for the first (vitals-only) report; day `1` appears once a full lab batch has landed |
| Airflow "Invalid login" after it previously worked | Same as the connection issue above — `down -v` regenerates the admin password | Re-fetch the password with the command above, or recreate the admin user with a known password |

## Reproducing Results for the Report

1. Let the stack run for at least 2–3 simulated days (≈15 min) so the Airflow DAG has
   produced multiple `patient_risk_report` rows.
2. Screenshot `/dashboard/`'s Live Vitals, Active Alerts, and Daily Risk Report sections
   for the report's Results section.
3. Kill the `vitals-producer` container mid-run (`docker compose stop vitals-producer`) to
   demonstrate the no-data alert firing within ~2 minutes — capture this for the
   Observability section, then `docker compose start vitals-producer` to resume it.

## Assumptions & Simplifications

- Fixed roster of 10 simulated patients (`NUM_PATIENTS`), shared between both producers.
- JDBC sink writes use `append` mode rather than a true upsert for windowed aggregates;
  a production system would use `MERGE` semantics.
- Risk-scoring weights in the Airflow DAG are illustrative, not clinically validated.