# Architecture Decision Record: Kappa over Lambda

## Context
Two data sources: a high-frequency vitals stream and a low-frequency (simulated daily)
lab-results feed. The system must support real-time ward monitoring alerts AND a daily
consolidated patient risk report that joins both sources.

## Decision
Adopt a **Kappa architecture**: both sources are modeled as Kafka topics, consumed by a
single Spark Structured Streaming application. Periodic reporting is handled by an
Airflow-scheduled job that queries the serving store (Postgres), not by a separate
batch-processing engine over raw data.

## Rationale
1. **Unified processing model** — both ingestion paths fit the "event stream" abstraction;
   the lab feed is simply lower-frequency, not fundamentally different in kind.
2. **Replay substitutes for a batch layer** — Kafka retention (7 days configured) allows
   reprocessing vitals/labs from raw events if risk-scoring logic changes, without
   maintaining a second batch codebase.
3. **Reduced engineering overhead within a 2-week timeline** — a single pipeline can be
   made robust and observable; a Lambda pipeline would split effort across two codebases
   that must stay logically consistent.
4. **Stream-stream joins are natively supported** in Spark Structured Streaming with
   watermarking, satisfying the "join between sources" processing requirement without a
   separate batch join step.

## Rejected Alternative: Lambda Architecture
A Lambda design (separate batch layer recomputing from raw historical data + a speed
layer for real-time views, merged at serving time) was considered and rejected because:

- **Dual-code problem**: transformation logic (risk scoring, threshold checks) would need
  to be implemented and kept consistent in both a batch job (e.g. Spark batch/Airflow) and
  a streaming job — doubling implementation and testing effort for a 2-week project.
- **Marginal benefit for this scenario**: Lambda's main advantage is a fully reprocessable,
  audit-grade "batch view" independent of streaming bugs. In this use case, Kafka retention
  + a fault-tolerant streaming job's checkpointing already give an acceptable
  reprocessing guarantee for a mini-project (not a production clinical system).
- **Merge complexity**: reconciling batch and speed-layer views at serving time (handling
  overlapping time ranges, late data) adds complexity the project's 2-week scope does not
  justify relative to the learning objectives being assessed.

## Trade-offs Accepted
- If truly bit-for-bit reproducible historical recomputation were a hard requirement
  (e.g. real clinical/regulatory audit), Lambda's independent batch layer would be the
  safer choice. We accept the weaker (but practically sufficient) guarantee that Kafka
  replay + streaming checkpoints provide for this project's scope.
- Kappa places more responsibility on getting the streaming job's logic right the first
  time, since there's no independently-computed batch view to cross-check against. This is
  mitigated by structured logging and the health-check alerts implemented in
  `observability/`.
