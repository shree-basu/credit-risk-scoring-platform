"""Pure identity helpers and lazily imported cloud task callables."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

from credit_risk.feature_contract import MODEL_FEATURE_ALLOWLIST

EXPECTED_ENTITIES = {
    "training": ("applications", "borrower_profiles", "loan_outcomes"),
    "scoring": ("applications", "borrower_profiles"),
}
SCHEMA_VERSION = "1.0.0"
VERTEX_INSTANCE_FIELDS = (
    "loan_id",
    "borrower_id",
    "feature_date",
    "feature_version",
    *MODEL_FEATURE_ALLOWLIST,
)


class PermanentDataQualityError(RuntimeError):
    """A deterministic contract violation that should not be blindly retried."""


def execution_identity(
    *, dag_id: str, logical_date: datetime, airflow_run_id: str, purpose: str
) -> dict[str, str]:
    """Build retry-stable IDs that differ across deliberate replay DAG runs."""

    material = f"{dag_id}|{logical_date.isoformat()}|{airflow_run_id}|{purpose}"
    digest = hashlib.sha256(material.encode()).hexdigest()[:16]
    safe_purpose = re.sub(r"[^a-z0-9-]", "-", purpose.lower()).strip("-")[:20]
    return {
        "pipeline_run_id": f"{dag_id}:{logical_date.isoformat()}:{digest}",
        "cloud_job_id": f"cr-{safe_purpose}-{logical_date:%Y%m%d}-{digest}"[:63],
        "table_suffix": f"{safe_purpose}_{logical_date:%Y%m%d}_{digest}".replace("-", "_"),
    }


def source_batch_id(dataset_type: str, logical_date: date, override: str | None) -> str:
    if dataset_type not in EXPECTED_ENTITIES:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")
    value = override or f"{dataset_type}-{logical_date:%Y%m%d}"
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise PermanentDataQualityError("batch_id contains unsupported characters")
    return value


def source_prefix(dataset_type: str, logical_date: date, batch_id: str) -> str:
    partition_key = "snapshot_date" if dataset_type == "training" else "business_date"
    return f"raw/{dataset_type}/{partition_key}={logical_date.isoformat()}/batch_id={batch_id}"


def staging_table(dataset_type: str, batch_id: str, feature_version: str = "v1") -> str:
    safe_prefix = re.sub(r"[^a-z0-9_]", "_", batch_id.lower()).strip("_")[:32] or "batch"
    digest = hashlib.sha256(f"{dataset_type}|{batch_id}|{feature_version}".encode()).hexdigest()[
        :12
    ]
    return f"{dataset_type}_features_stage_{safe_prefix}_{digest}"


def require_parent_model(parent_model: str | None) -> str:
    """Refuse to create an uncontrolled first/default model from the scheduled DAG."""

    value = (parent_model or "").strip()
    if not re.fullmatch(r"projects/[^/]+/locations/[^/]+/models/[^/]+", value):
        raise PermanentDataQualityError(
            "vertex_parent_model must be an existing fully qualified Vertex model resource"
        )
    return value


def quarantine_merge_sql() -> str:
    """Return the shared replay-safe quarantine publication statement."""

    return """
        ALTER TABLE `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_stage_{{
          ti.xcom_pull(task_ids="source_identity")["staging_table"]
        }}` SET OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        );

        MERGE `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_records` target
        USING `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_stage_{{
          ti.xcom_pull(task_ids="source_identity")["staging_table"]
        }}` source
        ON target.run_id = source.run_id
          AND target.batch_id = source.batch_id
          AND target.business_date = source.business_date
          AND COALESCE(target.loan_id, '') = COALESCE(source.loan_id, '')
          AND target.reason_code = source.reason_code
        WHEN MATCHED THEN UPDATE SET
          entity = source.entity,
          reason_text = source.reason_text,
          rejected_at = source.rejected_at
        WHEN NOT MATCHED THEN INSERT
          (run_id, batch_id, business_date, entity, loan_id,
           reason_code, reason_text, rejected_at)
        VALUES
          (source.run_id, source.batch_id, source.business_date, source.entity,
           source.loan_id, source.reason_code, source.reason_text, source.rejected_at)
    """


def airflow_execution_identity(*, purpose: str, **context: Any) -> dict[str, str]:
    return execution_identity(
        dag_id=context["dag"].dag_id,
        logical_date=context["logical_date"],
        airflow_run_id=context["dag_run"].run_id,
        purpose=purpose,
    )


def airflow_source_identity(*, dataset_type: str, **context: Any) -> dict[str, str]:
    logical_date = context["logical_date"].date()
    configured = context["dag_run"].conf.get("batch_id") if context.get("dag_run") else None
    batch_id = source_batch_id(dataset_type, logical_date, configured)
    return {
        "batch_id": batch_id,
        "prefix": source_prefix(dataset_type, logical_date, batch_id),
        "staging_table": staging_table(dataset_type, batch_id),
    }


def _csv_count(payload: bytes) -> int:
    rows = csv.reader(io.StringIO(payload.decode("utf-8")))
    next(rows, None)
    return sum(1 for _ in rows)


def validate_manifest_payload(
    *,
    manifest_payload: bytes,
    objects: dict[str, bytes],
    expected_prefix: str,
    dataset_type: str,
    logical_date: date,
    batch_id: str,
) -> dict[str, int]:
    """Validate exact paths, counts and checksums without constructing a cloud client."""

    manifest = json.loads(manifest_payload)
    partition_key = "snapshot_date" if dataset_type == "training" else "business_date"
    expected_header = {
        "dataset_type": dataset_type,
        "partition_key": partition_key,
        "partition_date": logical_date.isoformat(),
        "batch_id": batch_id,
    }
    for field, expected_value in expected_header.items():
        if manifest.get(field) != expected_value:
            raise PermanentDataQualityError(f"manifest {field} does not match the DAG run")
    entries = manifest.get("entities")
    if not isinstance(entries, list):
        raise PermanentDataQualityError("manifest entities must be a list")
    expected = set(EXPECTED_ENTITIES[dataset_type])
    actual = {str(item.get("entity")) for item in entries if isinstance(item, dict)}
    if actual != expected or len(entries) != len(expected):
        raise PermanentDataQualityError("manifest entity set is incorrect")

    counts: dict[str, int] = {}
    for item in entries:
        entity = str(item["entity"])
        if item.get("schema_version") != SCHEMA_VERSION:
            raise PermanentDataQualityError(f"schema-version mismatch for {entity}")
        object_path = f"{expected_prefix}/{entity}.csv"
        if item.get("object_path") != object_path:
            raise PermanentDataQualityError(f"manifest redirected {entity} from its exact path")
        payload = objects.get(object_path)
        if payload is None:
            raise PermanentDataQualityError(f"missing source object: {object_path}")
        observed_count = _csv_count(payload)
        if item.get("expected_row_count") != observed_count:
            raise PermanentDataQualityError(f"row-count mismatch for {entity}")
        if item.get("sha256") != hashlib.sha256(payload).hexdigest():
            raise PermanentDataQualityError(f"checksum mismatch for {entity}")
        counts[entity] = observed_count
    return counts


def validate_gcs_manifest(
    *, bucket_name: str, dataset_type: str, logical_date: str, batch_id: str, **_: Any
) -> dict[str, int]:
    """Airflow callable: import the GCS hook only when the task actually executes."""

    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    prefix = source_prefix(dataset_type, date.fromisoformat(logical_date), batch_id)
    hook = GCSHook()
    manifest_payload = hook.download(bucket_name=bucket_name, object_name=f"{prefix}/manifest.json")
    objects = {
        f"{prefix}/{entity}.csv": hook.download(
            bucket_name=bucket_name, object_name=f"{prefix}/{entity}.csv"
        )
        for entity in EXPECTED_ENTITIES[dataset_type]
    }
    return validate_manifest_payload(
        manifest_payload=manifest_payload,
        objects=objects,
        expected_prefix=prefix,
        dataset_type=dataset_type,
        logical_date=date.fromisoformat(logical_date),
        batch_id=batch_id,
    )


def resolve_active_model(
    *,
    project_id: str,
    region: str,
    score_date: str,
    model_version_override: str | None = None,
    **_: Any,
) -> dict[str, str]:
    """Airflow callable: resolve the model effective on the historical score date."""

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(use_legacy_sql=False, location=region)
    return _resolve_active_model_with_hook(
        hook=hook,
        project_id=project_id,
        region=region,
        score_date=score_date,
        model_version_override=model_version_override,
    )


def _resolve_active_model_with_hook(
    *,
    hook: Any,
    project_id: str,
    region: str,
    score_date: str,
    model_version_override: str | None,
) -> dict[str, str]:
    override_filter = "AND model_version = @model_version" if model_version_override else ""
    query = f"""
        SELECT model_resource, model_version, feature_version
        FROM `{project_id}.credit_risk_audit.model_assignments`
        WHERE status = 'ACTIVE'
          AND effective_from <= TIMESTAMP(@score_date)
          AND (effective_to IS NULL OR effective_to > TIMESTAMP(@score_date))
          {override_filter}
        ORDER BY effective_from DESC
        LIMIT 1
    """
    parameters: list[dict[str, object]] = [
        {
            "name": "score_date",
            "parameterType": {"type": "STRING"},
            "parameterValue": {"value": score_date},
        }
    ]
    if model_version_override:
        parameters.append(
            {
                "name": "model_version",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": model_version_override},
            }
        )
    records = list(
        _submit_query_job(
            hook=hook,
            project_id=project_id,
            region=region,
            sql=query,
            parameters=parameters,
        ).result()
    )
    if len(records) != 1:
        raise PermanentDataQualityError(
            "No unique ACTIVE model assignment covers the requested logical score date"
        )
    resource, version, feature_version = tuple(records[0])
    return {
        "model_resource": str(resource),
        "model_version": str(version),
        "feature_version": str(feature_version),
    }


def normalize_vertex_predictions(
    *,
    project_id: str,
    region: str,
    batch_prediction_job_id: str,
    score_date: str,
    pipeline_run_id: str,
    model: dict[str, str],
    **_: Any,
) -> None:
    """Discover Vertex's generated BigQuery dataset and merge scores idempotently."""

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from google.api_core.client_options import ClientOptions
    from google.cloud import aiplatform_v1

    job_name = batch_prediction_job_id
    if not job_name.startswith("projects/"):
        job_name = (
            f"projects/{project_id}/locations/{region}/batchPredictionJobs/"
            f"{batch_prediction_job_id}"
        )
    client = aiplatform_v1.JobServiceClient(
        client_options=ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com")
    )
    job = client.get_batch_prediction_job(name=job_name)
    dataset_uri = job.output_info.bigquery_output_dataset.removeprefix("bq://")
    source_dataset = dataset_uri.replace(":", ".")
    hook = BigQueryHook(use_legacy_sql=False, location=region)
    bq_client = hook.get_client(project_id=project_id, location=region)
    table_ids = {table.table_id for table in bq_client.list_tables(source_dataset)}
    prediction_tables = sorted(name for name in table_ids if name.startswith("predictions_"))
    error_tables = sorted(name for name in table_ids if name.startswith("errors_"))
    if len(prediction_tables) != 1 or len(error_tables) > 1:
        raise PermanentDataQualityError("Vertex output does not contain one prediction table")
    prediction_table = prediction_tables[0]
    _merge_vertex_prediction_output(
        hook=hook,
        project_id=project_id,
        region=region,
        source_dataset=source_dataset,
        prediction_table=prediction_table,
        error_table=error_tables[0] if error_tables else None,
        score_date=score_date,
        pipeline_run_id=pipeline_run_id,
        model=model,
    )


def _merge_vertex_prediction_output(
    *,
    hook: Any,
    project_id: str,
    region: str,
    source_dataset: str,
    prediction_table: str,
    error_table: str | None,
    score_date: str,
    pipeline_run_id: str,
    model: dict[str, str],
) -> None:
    errors_assertion = ""
    if error_table:
        errors_assertion = f"""
            ASSERT NOT EXISTS (
              SELECT 1 FROM `{source_dataset}.{error_table}`
            ) AS 'VERTEX_PREDICTION_ERRORS_PRESENT';
        """
    _submit_query_job(
        hook=hook,
        project_id=project_id,
        region=region,
        sql=f"""
            {errors_assertion}

            MERGE `{project_id}.credit_risk_scoring.risk_scores` target
            USING (
              SELECT
                loan_id,
                borrower_id,
                DATE(@score_date) AS score_date,
                CAST(probability_of_default AS NUMERIC) AS probability_of_default,
                risk_band,
                @model_resource AS model_resource,
                @model_version AS model_version,
                @feature_version AS feature_version,
                @pipeline_run_id AS pipeline_run_id,
                CURRENT_TIMESTAMP() AS scored_at
              FROM `{source_dataset}.{prediction_table}`
            ) source
            ON target.loan_id = source.loan_id
              AND target.score_date = source.score_date
              AND target.model_version = source.model_version
            WHEN MATCHED THEN UPDATE SET
              borrower_id = source.borrower_id,
              probability_of_default = source.probability_of_default,
              risk_band = source.risk_band,
              model_resource = source.model_resource,
              feature_version = source.feature_version,
              pipeline_run_id = source.pipeline_run_id,
              scored_at = source.scored_at
            WHEN NOT MATCHED THEN INSERT ROW;
        """,
        parameters=[
            _scalar_parameter("score_date", "STRING", score_date),
            _scalar_parameter("model_resource", "STRING", model["model_resource"]),
            _scalar_parameter("model_version", "STRING", model["model_version"]),
            _scalar_parameter("feature_version", "STRING", model["feature_version"]),
            _scalar_parameter("pipeline_run_id", "STRING", pipeline_run_id),
        ],
    )


def _scalar_parameter(name: str, parameter_type: str, value: object) -> dict[str, object]:
    return {
        "name": name,
        "parameterType": {"type": parameter_type},
        "parameterValue": {"value": value},
    }


def _query_configuration(
    sql: str, parameters: list[dict[str, object]] | None = None
) -> dict[str, object]:
    query: dict[str, object] = {"query": sql, "useLegacySql": False}
    if parameters:
        query["parameterMode"] = "NAMED"
        query["queryParameters"] = parameters
    return {"query": query}


def _submit_query_job(
    *,
    hook: Any,
    project_id: str,
    region: str,
    sql: str,
    parameters: list[dict[str, object]] | None = None,
) -> Any:
    """Submit a synchronous parameterized query through provider 19.5.0."""

    return hook.insert_job(
        configuration=_query_configuration(sql, parameters),
        project_id=project_id,
        location=region,
        nowait=False,
    )


def _query_dataframe(
    *,
    hook: Any,
    project_id: str,
    region: str,
    sql: str,
    parameters: list[dict[str, object]],
) -> Any:
    rows = _submit_query_job(
        hook=hook,
        project_id=project_id,
        region=region,
        sql=sql,
        parameters=parameters,
    ).result()
    return rows.to_dataframe(create_bqstorage_client=False)


def vertex_batch_prediction_request(
    *,
    project_id: str,
    region: str,
    job_display_name: str,
    model_resource: str,
    bigquery_source: str,
    bigquery_destination_prefix: str,
    service_account: str,
    labels: dict[str, str] | None = None,
) -> dict[str, object]:
    """Build a Vertex request that converts BigQuery rows to named objects."""

    return {
        "parent": f"projects/{project_id}/locations/{region}",
        "batch_prediction_job": {
            "display_name": job_display_name,
            "model": model_resource,
            "input_config": {
                "instances_format": "bigquery",
                "bigquery_source": {"input_uri": bigquery_source},
            },
            "instance_config": {
                "instance_type": "object",
                "included_fields": list(VERTEX_INSTANCE_FIELDS),
            },
            "output_config": {
                "predictions_format": "bigquery",
                "bigquery_destination": {"output_uri": bigquery_destination_prefix},
            },
            "service_account": service_account,
            "labels": labels or {},
        },
    }


def _submit_object_batch_prediction_with_hook(
    *, hook: Any, request: dict[str, object], timeout_seconds: int = 4 * 60 * 60
) -> str:
    region = str(request["parent"]).rsplit("/", 1)[-1]
    client = hook.get_job_service_client(region=region)
    operation = client.create_batch_prediction_job(request=request)
    job = operation.result(timeout=timeout_seconds)
    return str(job.name)


def submit_object_batch_prediction_job(
    *,
    project_id: str,
    region: str,
    job_display_name: str,
    model: dict[str, str],
    bigquery_source: str,
    bigquery_destination_prefix: str,
    service_account: str,
    labels: dict[str, str] | None = None,
    **_: Any,
) -> str:
    """Submit one BigQuery-backed Vertex job with object-shaped instances."""

    from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import (
        BatchPredictionJobHook,
    )

    request = vertex_batch_prediction_request(
        project_id=project_id,
        region=region,
        job_display_name=job_display_name,
        model_resource=model["model_resource"],
        bigquery_source=bigquery_source,
        bigquery_destination_prefix=bigquery_destination_prefix,
        service_account=service_account,
        labels=labels,
    )
    return _submit_object_batch_prediction_with_hook(hook=BatchPredictionJobHook(), request=request)


def compute_and_persist_drift(
    *, project_id: str, region: str, score_date: str, run_id: str, model_version: str, **_: Any
) -> dict[str, object]:
    """Compute transparent reference-window drift and persist auditable metrics."""

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    from monitoring.drift import build_drift_metric

    current_date = date.fromisoformat(score_date)
    reference_end = current_date - timedelta(days=1)
    reference_start = current_date - timedelta(days=30)
    hook = BigQueryHook(use_legacy_sql=False, location=region)
    feature_frame = _query_dataframe(
        hook=hook,
        project_id=project_id,
        region=region,
        sql=f"""
            SELECT feature_date, annual_income, credit_score, home_ownership
            FROM `{project_id}.credit_risk_features.scoring_features`
            WHERE feature_date BETWEEN @reference_start AND @current_date
        """,
        parameters=[
            _scalar_parameter("reference_start", "DATE", reference_start.isoformat()),
            _scalar_parameter("current_date", "DATE", current_date.isoformat()),
        ],
    )
    score_frame = _query_dataframe(
        hook=hook,
        project_id=project_id,
        region=region,
        sql=f"""
            SELECT score_date, probability_of_default
            FROM `{project_id}.credit_risk_scoring.risk_scores`
            WHERE score_date BETWEEN @reference_start AND @current_date
              AND model_version = @model_version
        """,
        parameters=[
            _scalar_parameter("reference_start", "DATE", reference_start.isoformat()),
            _scalar_parameter("current_date", "DATE", current_date.isoformat()),
            _scalar_parameter("model_version", "STRING", model_version),
        ],
    )
    reference_features = feature_frame[feature_frame["feature_date"] < current_date]
    current_features = feature_frame[feature_frame["feature_date"] == current_date]
    reference_scores = score_frame[score_frame["score_date"] < current_date]
    current_scores = score_frame[score_frame["score_date"] == current_date]
    if any(
        frame.empty
        for frame in (reference_features, current_features, reference_scores, current_scores)
    ):
        return {"status": "INSUFFICIENT_REFERENCE", "metrics_written": 0}

    measured_at = datetime.now(UTC)
    specifications = [
        ("annual_income", reference_features, current_features, "PSI", 0.2),
        (
            "credit_score",
            reference_features,
            current_features,
            "STANDARDIZED_MEAN_SHIFT",
            0.5,
        ),
        (
            "home_ownership",
            reference_features,
            current_features,
            "TOTAL_VARIATION",
            0.2,
        ),
        (
            "probability_of_default",
            reference_scores,
            current_scores,
            "PSI",
            0.2,
        ),
    ]
    metrics = [
        build_drift_metric(
            feature_name=name,
            reference=reference[name],
            current=current[name],
            reference_period=reference_end,
            current_period=current_date,
            measured_at=measured_at,
            metric=metric,
            threshold=threshold,
        )
        for name, reference, current, metric, threshold in specifications
    ]
    errors = hook.get_client(project_id=project_id, location=region).insert_rows_json(
        f"{project_id}.credit_risk_audit.drift_metrics",
        [
            {
                "run_id": run_id,
                **metric.as_record(),
                "reference_period": metric.reference_period.isoformat(),
                "current_period": metric.current_period.isoformat(),
                "measured_at": metric.measured_at.isoformat(),
            }
            for metric in metrics
        ],
        row_ids=[f"{run_id}:{metric.feature_name}:{metric.metric}" for metric in metrics],
    )
    if errors:
        raise RuntimeError(f"Failed to persist drift metrics: {errors}")
    return {
        "status": "ALERT" if any(metric.status == "ALERT" for metric in metrics) else "OK",
        "metrics_written": len(metrics),
    }


def audit_failure_callback(context: dict[str, Any]) -> None:
    """Best-effort audit update that never hides the original task failure."""

    try:
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        identity = context["ti"].xcom_pull(task_ids="execution_identity")
        if not identity:
            return
        run_id = identity["pipeline_run_id"]
        project_id = Variable.get("gcp_project_id")
        region = Variable.get("gcp_region")
        _persist_failed_run(
            hook=BigQueryHook(use_legacy_sql=False, location=region),
            project_id=project_id,
            region=region,
            run_id=run_id,
            logical_date=context["logical_date"].date().isoformat(),
        )
    except Exception:
        return


def _persist_failed_run(
    *, hook: Any, project_id: str, region: str, run_id: str, logical_date: str
) -> None:
    _submit_query_job(
        hook=hook,
        project_id=project_id,
        region=region,
        sql=f"""
            UPDATE `{project_id}.credit_risk_audit.pipeline_runs`
            SET status = 'FAILED', completed_at = CURRENT_TIMESTAMP()
            WHERE run_id = @run_id
              AND DATE(logical_date) = @logical_date
        """,
        parameters=[
            _scalar_parameter("run_id", "STRING", run_id),
            _scalar_parameter("logical_date", "DATE", logical_date),
        ],
    )
