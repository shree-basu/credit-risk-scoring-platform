from __future__ import annotations

import hashlib
import importlib
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from dags.credit_risk_runtime import (
    VERTEX_INSTANCE_FIELDS,
    PermanentDataQualityError,
    _merge_vertex_prediction_output,
    _persist_failed_run,
    _query_dataframe,
    _resolve_active_model_with_hook,
    _submit_object_batch_prediction_with_hook,
    execution_identity,
    require_parent_model,
    source_batch_id,
    source_prefix,
    staging_table,
    validate_manifest_payload,
    vertex_batch_prediction_request,
)

ROOT = Path(__file__).resolve().parents[1]


class _FakeRows(list):
    def __init__(self, rows: list[tuple[object, ...]], frame: object | None = None) -> None:
        super().__init__(rows)
        self.frame = frame
        self.to_dataframe_kwargs: dict[str, object] | None = None

    def to_dataframe(self, **kwargs: object) -> object:
        self.to_dataframe_kwargs = kwargs
        return self.frame


class _FakeJob:
    def __init__(self, rows: _FakeRows) -> None:
        self.rows = rows

    def result(self) -> _FakeRows:
        return self.rows


class _FakeBigQueryHook:
    def __init__(self, rows: _FakeRows | None = None) -> None:
        self.rows = rows if rows is not None else _FakeRows([])
        self.calls: list[dict[str, object]] = []

    def insert_job(self, **kwargs: object) -> _FakeJob:
        self.calls.append(kwargs)
        return _FakeJob(self.rows)


def _query_parameters(call: dict[str, object]) -> list[dict[str, object]]:
    configuration = call["configuration"]
    assert isinstance(configuration, dict)
    query = configuration["query"]
    assert isinstance(query, dict)
    assert query["useLegacySql"] is False
    assert query["parameterMode"] == "NAMED"
    parameters = query["queryParameters"]
    assert isinstance(parameters, list)
    return parameters


def _valid_manifest() -> tuple[bytes, dict[str, bytes], str]:
    logical_date = date(2026, 9, 2)
    batch_id = "scoring-20260902"
    prefix = source_prefix("scoring", logical_date, batch_id)
    objects = {
        f"{prefix}/applications.csv": b"loan_id\nloan-1\n",
        f"{prefix}/borrower_profiles.csv": b"borrower_id\nborrower-1\n",
    }
    entities = []
    for entity in ("applications", "borrower_profiles"):
        object_path = f"{prefix}/{entity}.csv"
        payload = objects[object_path]
        entities.append(
            {
                "entity": entity,
                "object_path": object_path,
                "expected_row_count": 1,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "schema_version": "1.0.0",
            }
        )
    manifest = {
        "dataset_type": "scoring",
        "partition_key": "business_date",
        "partition_date": logical_date.isoformat(),
        "batch_id": batch_id,
        "generated_at": "2026-09-02T00:00:00Z",
        "entities": entities,
    }
    return json.dumps(manifest).encode(), objects, prefix


def test_execution_identity_is_retry_stable_and_replay_specific() -> None:
    logical_date = datetime(2026, 9, 2, tzinfo=UTC)
    first = execution_identity(
        dag_id="credit_risk_daily_scoring",
        logical_date=logical_date,
        airflow_run_id="scheduled__2026-09-02",
        purpose="daily-score",
    )
    retry = execution_identity(
        dag_id="credit_risk_daily_scoring",
        logical_date=logical_date,
        airflow_run_id="scheduled__2026-09-02",
        purpose="daily-score",
    )
    replay = execution_identity(
        dag_id="credit_risk_daily_scoring",
        logical_date=logical_date,
        airflow_run_id="manual__replay-1",
        purpose="daily-score",
    )
    assert first == retry
    assert first != replay
    assert len(first["cloud_job_id"]) <= 63


def test_source_and_staging_identity_follow_logical_date() -> None:
    logical_date = date(2026, 9, 2)
    batch_id = source_batch_id("scoring", logical_date, None)
    assert batch_id == "scoring-20260902"
    assert source_prefix("scoring", logical_date, batch_id) == (
        "raw/scoring/business_date=2026-09-02/batch_id=scoring-20260902"
    )
    assert staging_table("scoring", batch_id) == staging_table("scoring", batch_id)
    assert staging_table("training", batch_id) != staging_table("scoring", batch_id)


def test_retraining_requires_an_existing_parent_model() -> None:
    resource = "projects/example/locations/us-central1/models/123456"
    assert require_parent_model(resource) == resource
    with pytest.raises(PermanentDataQualityError, match="vertex_parent_model"):
        require_parent_model("")


def test_manifest_validator_enforces_header_paths_counts_checksums_and_schema() -> None:
    payload, objects, prefix = _valid_manifest()
    result = validate_manifest_payload(
        manifest_payload=payload,
        objects=objects,
        expected_prefix=prefix,
        dataset_type="scoring",
        logical_date=date(2026, 9, 2),
        batch_id="scoring-20260902",
    )
    assert result == {"applications": 1, "borrower_profiles": 1}

    redirected = json.loads(payload)
    redirected["entities"][0]["object_path"] = "raw/redirected.csv"
    with pytest.raises(PermanentDataQualityError, match="redirected"):
        validate_manifest_payload(
            manifest_payload=json.dumps(redirected).encode(),
            objects=objects,
            expected_prefix=prefix,
            dataset_type="scoring",
            logical_date=date(2026, 9, 2),
            batch_id="scoring-20260902",
        )

    wrong_date = json.loads(payload)
    wrong_date["partition_date"] = "2026-09-01"
    with pytest.raises(PermanentDataQualityError, match="partition_date"):
        validate_manifest_payload(
            manifest_payload=json.dumps(wrong_date).encode(),
            objects=objects,
            expected_prefix=prefix,
            dataset_type="scoring",
            logical_date=date(2026, 9, 2),
            batch_id="scoring-20260902",
        )


def test_runtime_import_does_not_import_cloud_sdks() -> None:
    before = set(sys.modules)
    importlib.reload(sys.modules["dags.credit_risk_runtime"])
    imported = set(sys.modules) - before
    assert not any(name.startswith("google.cloud") for name in imported)
    assert not any(name.startswith("airflow.providers") for name in imported)


def test_active_model_resolution_submits_bound_named_parameters() -> None:
    hook = _FakeBigQueryHook(_FakeRows([("projects/p/locations/r/models/1", "2", "v1")]))
    model = _resolve_active_model_with_hook(
        hook=hook,
        project_id="project-id",
        region="us-central1",
        score_date="2026-09-02",
        model_version_override="2",
    )

    assert model == {
        "model_resource": "projects/p/locations/r/models/1",
        "model_version": "2",
        "feature_version": "v1",
    }
    call = hook.calls[0]
    assert call["project_id"] == "project-id"
    assert call["location"] == "us-central1"
    assert call["nowait"] is False
    assert {item["name"] for item in _query_parameters(call)} == {
        "score_date",
        "model_version",
    }


def test_score_merge_uses_insert_job_and_preserves_replay_grain() -> None:
    hook = _FakeBigQueryHook()
    _merge_vertex_prediction_output(
        hook=hook,
        project_id="project-id",
        region="us-central1",
        source_dataset="project-id.vertex_output",
        prediction_table="predictions_1",
        error_table="errors_1",
        score_date="2026-09-02",
        pipeline_run_id="run-1",
        model={
            "model_resource": "projects/p/locations/r/models/1",
            "model_version": "2",
            "feature_version": "v1",
        },
    )

    call = hook.calls[0]
    query = call["configuration"]["query"]["query"]
    assert "MERGE `project-id.credit_risk_scoring.risk_scores`" in query
    assert "target.loan_id = source.loan_id" in query
    assert "target.score_date = source.score_date" in query
    assert "target.model_version = source.model_version" in query
    assert {item["name"] for item in _query_parameters(call)} == {
        "score_date",
        "model_resource",
        "model_version",
        "feature_version",
        "pipeline_run_id",
    }


def test_drift_query_converts_bound_job_results_to_pandas() -> None:
    marker = object()
    rows = _FakeRows([], frame=marker)
    hook = _FakeBigQueryHook(rows)
    frame = _query_dataframe(
        hook=hook,
        project_id="project-id",
        region="us-central1",
        sql="SELECT * FROM t WHERE feature_date = @current_date",
        parameters=[
            {
                "name": "current_date",
                "parameterType": {"type": "DATE"},
                "parameterValue": {"value": "2026-09-02"},
            }
        ],
    )

    assert frame is marker
    assert rows.to_dataframe_kwargs == {"create_bqstorage_client": False}
    assert [item["name"] for item in _query_parameters(hook.calls[0])] == ["current_date"]


def test_failure_audit_uses_bound_insert_job_update() -> None:
    hook = _FakeBigQueryHook()
    _persist_failed_run(
        hook=hook,
        project_id="project-id",
        region="us-central1",
        run_id="run-1",
        logical_date="2026-09-02",
    )

    call = hook.calls[0]
    assert "SET status = 'FAILED'" in call["configuration"]["query"]["query"]
    assert {item["name"] for item in _query_parameters(call)} == {
        "run_id",
        "logical_date",
    }


def test_vertex_request_explicitly_submits_named_object_instances() -> None:
    request = vertex_batch_prediction_request(
        project_id="project-id",
        region="us-central1",
        job_display_name="daily-20260902",
        model_resource="projects/project-id/locations/us-central1/models/1@2",
        bigquery_source="bq://project-id.credit_risk_staging.vertex_input_1",
        bigquery_destination_prefix="bq://project-id",
        service_account="vertex@project-id.iam.gserviceaccount.com",
        labels={"pipeline": "credit-risk"},
    )
    job = request["batch_prediction_job"]
    assert job["instance_config"]["instance_type"] == "object"
    assert job["instance_config"]["included_fields"] == list(VERTEX_INSTANCE_FIELDS)
    assert "defaulted" not in VERTEX_INSTANCE_FIELDS
    assert "age" not in VERTEX_INSTANCE_FIELDS
    assert "loan_id" in VERTEX_INSTANCE_FIELDS
    assert "borrower_id" in VERTEX_INSTANCE_FIELDS

    class FakeOperation:
        def result(self, timeout: int) -> object:
            assert timeout == 4 * 60 * 60
            return type("Job", (), {"name": "projects/p/locations/r/batchPredictionJobs/7"})()

    class FakeClient:
        submitted: dict[str, object] | None = None

        def create_batch_prediction_job(self, *, request: dict[str, object]) -> FakeOperation:
            self.submitted = request
            return FakeOperation()

    class FakeVertexHook:
        client = FakeClient()

        def get_job_service_client(self, *, region: str) -> FakeClient:
            assert region == "us-central1"
            return self.client

    hook = FakeVertexHook()
    job_name = _submit_object_batch_prediction_with_hook(hook=hook, request=request)
    assert job_name.endswith("/7")
    assert hook.client.submitted is request


def test_dags_encode_logical_date_backfill_and_permanent_dq_contracts() -> None:
    daily = (ROOT / "dags" / "daily_scoring.py").read_text(encoding="utf-8")
    retraining = (ROOT / "dags" / "periodic_retraining.py").read_text(encoding="utf-8")
    for source in (daily, retraining):
        assert "catchup=True" in source
        assert "max_active_runs=1" in source
        assert '"{{ ds }}"' in source
        assert "DataprocCreateBatchOperator" in source
        assert 'runtime_config": {"version": "2.3"}' in source
        assert "retries=0" in source
        assert "publish_quarantine" in source
        assert "expiration_timestamp" in source
    assert "resolve_active_model" in daily
    assert "submit_object_batch_prediction_job" in daily
    assert "CreateCustomContainerTrainingJobOperator" in retraining
    assert "is_default_version=False" in retraining
    assert 'key="model_id"' in retraining


def test_terraform_defaults_are_zero_resource_and_deletion_safe() -> None:
    terraform_dir = ROOT / "infra" / "terraform"
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(terraform_dir.glob("*.tf"))
    )
    variables = (terraform_dir / "variables.tf").read_text(encoding="utf-8")
    example = (terraform_dir / "dev.auto.tfvars.example").read_text(encoding="utf-8")

    assert 'variable "deployment_enabled"' in variables
    assert 'variable "enable_composer"' in variables
    assert variables.count("default     = false") >= 2
    assert 'deployment_confirmation == "DEPLOY"' in terraform
    assert 'composer_confirmation == "COMPOSER"' in terraform
    assert "deployment_enabled      = false" in example
    assert "enable_composer         = false" in example
    assert "google_dataproc_cluster" not in terraform
    assert "force_destroy               = false" in terraform
    assert re.search(r"delete_contents_on_destroy\s*=\s*false", terraform)
    assert terraform.count("prevent_destroy = true") >= 5
    assert "roles/owner" not in terraform.lower()
    assert 'roles/editor"' not in terraform.lower()
