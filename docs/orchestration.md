# Orchestration contract

The project defines two Cloud Composer / Apache Airflow batch workflows:

- `credit_risk_daily_scoring` runs at 04:00 UTC for each logical business date.
- `credit_risk_periodic_retraining` runs at 05:00 UTC on the first day of each month.

Both DAGs use the Airflow logical date—not wall-clock time—to derive the immutable source
partition, feature date, training snapshot and publication predicates. `catchup=True` supports
ordered historical runs; `max_active_runs=1` avoids concurrent publication within a DAG. A manual
replay can select an existing immutable `batch_id`, while its Airflow run ID produces a distinct
audit/job identity. Task retries keep the same identity.

The completion marker is only a readiness signal. A non-retrying manifest task then validates the
dataset type, partition key/date, batch ID, exact entity set, exact object paths, schema version,
row counts and SHA-256 checksums. Deterministic contract or reconciliation failures use `retries=0`.
Transient sensors and cloud submissions retain bounded retries and execution timeouts.

Managed Spark runtime `2.3` builds an execution-specific staging table. BigQuery publication uses a
transactional delete-and-insert for the same business identity, so retrying or deliberately
replaying does not append duplicate features. Quarantine records are merged at their natural grain,
and both dynamic staging-table families receive a seven-day expiry before reconciliation. Training
selects the exact accepted `(feature_date, batch_id, feature_version)` snapshot. Scoring resolves the ACTIVE model whose effective
interval covers the historical score date. Vertex output is discovered from the job response and
merged into the curated score grain `(loan_id, score_date, model_version)`.

Retraining requires an existing fully qualified parent model and registers only a candidate version.
This guard keeps scheduled retraining from creating an uncontrolled first/default model. Bootstrap
registration is a separate reviewed operation. The DAG never promotes or marks a version as default.
The trainer exits unsuccessfully after retaining evaluation artifacts when required quality gates
fail, which prevents candidate registration. Human-controlled promotion and rollback update the
effective-dated model assignment separately, as described in `model-lifecycle.md`.

These DAGs are deployable definitions, not proof of a running Composer, Managed Spark, BigQuery or
Vertex environment. Local tests exercise pure identity and manifest functions and statically verify
the orchestration contract without importing a cloud client.
