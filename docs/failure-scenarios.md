# Failure scenarios and operator response

| Failure | Expected behavior | Recovery / replay rule |
|---|---|---|
| Missing `_SUCCESS` marker | Sensor reschedules until its bounded timeout; no read or publication begins. | Correct the upstream delivery; clear from the sensor after the immutable batch is complete. |
| Manifest path, count, schema, or checksum mismatch | Permanent DQ failure with zero retries; Spark and Vertex tasks remain blocked. | Never edit a published batch in place. Land a new batch ID and rerun the logical date. |
| Duplicate `loan_id` or borrower key | Batch-level permanent failure before a many-to-many join can corrupt the grain. | Correct upstream and publish a new immutable batch. |
| Invalid or unmatched application | Row is written once to reason-coded quarantine and included in reconciliation. | Correct upstream under a new batch ID; retain the original quarantine evidence. |
| Spark/Vertex transient service failure | Operator uses bounded retries and a retry-stable cloud job identity. | Retry/clear the failed task; do not create a new source batch for an infrastructure-only failure. |
| Feature reconciliation mismatch | Non-retrying assertion prevents downstream prediction or candidate training. | Investigate accepted/quarantine counts and staging data; replay only after correction. |
| Partial curated publication | Transactional same-key delete/insert or merge is replay-safe. Staging tables expire after seven days. | Clear the publication task with the same Airflow run/source identity. |
| No champion effective for score date | Model resolution fails before batch prediction. | Perform a reviewed effective-dated assignment or provide the documented override for an approved replay. |
| Candidate misses quality gates | Artifacts retain evaluation evidence, trainer exits unsuccessfully, and registration is blocked. | Review data/feature/model changes; never lower a gate silently or auto-promote. |
| Vertex prediction count/probability failure | Non-retrying score DQ blocks drift calculation and batch success. | Inspect Vertex output and normalized score table, then replay with the same business keys. |
| Drift threshold exceeded | An `ALERT` metric is persisted; no automatic retraining, promotion, or credit action occurs. | Human review decides whether investigation or a future candidate run is justified. |
| Composer DAG import failure | DagBag CI fails before merge. | Correct provider/API compatibility; do not deploy the broken DAG. |
| Accidental Terraform gate closure on managed state | Terraform would propose removals, while `prevent_destroy` blocks protected resources. | Do not use gates as temporary toggles. Follow a reviewed decommission change if destruction is intended. |

Failure callbacks are best-effort audit writes. The original task failure remains authoritative if
the audit write itself fails. This repository has no automatic cloud deployment or recovery path.
