# Validation evidence and claim boundary

## Automated checks

The `Cloud-free CI` workflow runs on pull requests and pushes to `main` with `contents: read` only.
Every external action is pinned to a full commit SHA.

| Job | Evidence |
|---|---|
| `quality` | Ruff lint/format, Python compile, 24 non-Spark/non-Airflow tests, exact dependency pins, `pip check`, credential-pattern scan, zero-resource defaults, workflow safety scan |
| `spark-tests` | Six local PySpark 3.5.3 tests on Python 3.11 and Java 17: schemas, calculations, joins, labels, DQ, quarantine, reconciliation, no Python UDF, deterministic staging and fail-closed gates |
| `dagbag` | Both DAGs import under Airflow 2.11.1 and Google provider 19.5.0; dependency, backfill, permanent-DQ, and no-auto-promotion assertions |
| `terraform` | Terraform 1.14.5 format, backend-disabled initialization, and validation only |
| `container-build` | Local Docker build and metadata inspection for trainer and predictor images; no login or push |

Exact test counts may change as focused assertions are added; GitHub Actions is the authoritative
per-commit result. Local validation uses the same commands where the host provides Python 3.11,
Java 17, Docker, Airflow, and Terraform.

The Windows workspace path contains spaces and the available local PySpark worker launcher does not
quote that path correctly. The Linux Python 3.11 Spark job is therefore the supported Spark execution
evidence; the Windows failure is not represented as a product-code pass or failure.

## Implementation evidence

- Source generation, local Spark transformations, local model training, local HTTP prediction,
  governance guards, and drift functions execute without GCP.
- Airflow, BigQuery SQL/schema, monitoring, and Terraform are implementation/static evidence.
- Docker builds prove packaging, not Vertex runtime execution.
- CI deliberately clears common Google credential environment variables and rejects authentication,
  cloud CLI, Terraform plan/apply/destroy, and container-push commands in workflow files.

## Not evidenced

No authenticated GCP operation was performed. Specifically, there is no evidence of:

- GCS upload or object-trigger behavior;
- Managed Spark batch submission or scale/performance;
- authenticated BigQuery DDL, DML, connector, transaction, or cost behavior;
- Artifact Registry image publication;
- Vertex custom training, Model Registry upload, batch prediction, or endpoint behavior;
- Composer environment creation or DAG execution;
- active Cloud Monitoring alerts, dashboards, SLA, availability, RPO/RTO, or measured cost.

These are explicit limits on portfolio and CV claims, not hidden future accomplishments.
