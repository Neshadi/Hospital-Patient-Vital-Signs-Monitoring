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
1. **Unified processing model** — both feeds fit the event-stream abstraction; the lab
   feed is just lower-frequency, not a different kind of data.
2. **Replay replaces a batch layer** — Kafka retention (7 days configured) lets us
   reprocess vitals and labs from raw events if the risk-scoring logic changes, with no
   second batch codebase to maintain.
3. **Lower engineering overhead for a 2-week timeline** — one pipeline can be made robust
   and observable; Lambda would split the effort across two codebases that must stay
   logically consistent.
4. **Stream-stream joins are natively supported** in Spark Structured Streaming with
   watermarking, satisfying the "join between sources" processing requirement without a
   separate batch join step.

## Rejected Alternative: Lambda Architecture
A Lambda design (a batch layer recomputing from raw historical data plus a speed layer
for real-time views, merged at serving time) was considered and rejected because:

- **Dual-code problem**: the transformation logic (risk scoring, threshold checks) would
  have to be written and kept consistent in both a batch job (Spark batch or Airflow) and
  a streaming job, doubling implementation and testing effort.
- **Marginal benefit here**: Lambda's main advantage is an audit-grade batch view that is
  independent of streaming bugs. For this project, Kafka retention plus the streaming
  job's checkpointing already give an acceptable reprocessing guarantee; this is a
  mini-project, not a production clinical system.
- **Merge complexity**: reconciling batch and speed-layer views at serving time
  (overlapping time ranges, late data) is more than the 2-week scope justifies.

## Trade-offs Accepted
- If bit-for-bit reproducible historical recomputation were a hard requirement (for
  example a clinical or regulatory audit), Lambda's independent batch layer would be the
  safer choice. We accept the weaker but practically sufficient guarantee that Kafka
  replay and streaming checkpoints provide at this scope.
- Kappa depends on the streaming job's logic being right the first time, because there is
  no independently computed batch view to cross-check against. Structured logging and the
  health-check alerts in `observability/` mitigate this.
