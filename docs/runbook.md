# Local and conceptual operations runbook

## Safety boundary

The supported verification path is local and cloud-free. Do not add credentials to this repository.
Do not run Terraform plan/apply, upload data, publish images, or submit cloud jobs as part of normal
portfolio validation. The checked-in CI cannot authenticate or deploy.

## Local environment

Use Python 3.11. Spark tests additionally require Java 17.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows PowerShell, activate with `.\.venv\Scripts\Activate.ps1`.

## Generate and verify an immutable batch

```bash
python -m data.simulator.generate_loans \
  --dataset-type training \
  --partition-date 2026-09-03 \
  --batch-id training-20260903 \
  --records 200 --seed 17
```

Output is local under `data/output/`. Reusing the same path fails to preserve immutability. Use a
new batch ID for corrected input. Scenarios are `normal`, `duplicates`, `missing-profile`,
`invalid-values`, and `distribution-drift`.

## Run cloud-free validation

```bash
ruff check .
ruff format --check .
python -m compileall -q credit_risk data dags monitoring spark vertex tests scripts
pytest -q tests/test_simulator_contract.py tests/test_model_lifecycle.py tests/test_orchestration_contract.py
pytest -q tests/test_spark_features.py
python scripts/check_repository_safety.py
python scripts/check_cloud_free_ci.py
python -m pip check
```

DagBag validation uses the pinned packages and Apache constraints in CI. Terraform validation is
limited to `fmt -check`, `init -backend=false`, and `validate`; deliberately do not add `plan` or
`apply` to this repository's CI.

## Local model and predictor checks

The unit suite trains a deterministic synthetic candidate, writes `model.joblib`, `metadata.json`,
and `metrics.json`, and exercises `/health` and `/predict` with Flask's local test client. It never
constructs a Vertex, GCS, or BigQuery client because cloud paths fail before import without their
explicit confirmations.

Candidate evaluation never performs promotion. Promotion requires a separate call to the pure
governance function with `PROMOTE`, actor, timestamp, eligible candidate, and versioned model
resource. A real implementation would then persist that reviewed assignment transactionally.

## Logical-date replay and backfill

- Prefer Airflow's backfill/clear mechanisms; do not change DAG wall-clock code.
- Keep the original logical date so source prefix, feature date, model resolution, and publication
  predicates stay historically correct.
- A retry of one Airflow run preserves `pipeline_run_id` and cloud job ID.
- A deliberate replay receives a new audit/run identity but reuses the immutable source batch and
  curated business keys.
- Process historical dates in order. `max_active_runs=1` serializes each DAG, but cross-DAG source
  readiness and model-assignment dependencies still require operator coordination.
- An approved `model_version` override is for controlled reproducibility, not silent use of today's
  champion.

## Incident triage

1. Identify the logical date, source batch ID, Airflow run ID, feature version, and model version.
2. Separate permanent contract/DQ failures from retryable infrastructure failures.
3. Verify manifest counts/checksums and the accepted + quarantine reconciliation.
4. For scores, verify count reconciliation and probability bounds before marking success.
5. Preserve failed staging, audit, quarantine, and model artifacts during investigation.
6. Use the recovery rule in [failure scenarios](failure-scenarios.md).

## Infrastructure review and decommission

Fresh Terraform state has zero resources unless `deployment_enabled=true` and the exact `DEPLOY`
confirmation are both persistent. Composer requires two additional persistent controls. Never
temporarily close a gate after Terraform manages resources: that represents desired deletion.

For a real decommission, create a reviewed change that names every target, addresses retained data,
and deliberately changes lifecycle protections. This repository intentionally does not contain a
one-command deployment or destruction workflow.
