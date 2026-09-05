"""Periodic training-feature build and governed Vertex candidate workflow."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomContainerTrainingJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from credit_risk_runtime import (
    airflow_execution_identity,
    airflow_source_identity,
    audit_failure_callback,
    quarantine_merge_sql,
    require_parent_model,
    validate_gcs_manifest,
)

PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region }}"
RAW_BUCKET = "{{ var.value.raw_bucket }}"
ARTIFACT_BUCKET = "{{ var.value.artifact_bucket }}"
SPARK_SERVICE_ACCOUNT = "{{ var.value.spark_service_account }}"
VERTEX_SERVICE_ACCOUNT = "{{ var.value.vertex_service_account }}"
TRAINER_IMAGE = "{{ var.value.vertex_trainer_image }}"
PREDICTOR_IMAGE = "{{ var.value.vertex_predictor_image }}"


def query_task(task_id: str, query: str, *, retries: int = 1) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        project_id=PROJECT_ID,
        location=REGION,
        configuration={"query": {"query": query, "useLegacySql": False}},
        retries=retries,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=30),
    )


with DAG(
    dag_id="credit_risk_periodic_retraining",
    description="Monthly matured-loan feature build and governed candidate registration",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 5 1 * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": audit_failure_callback,
    },
    render_template_as_native_obj=True,
    tags=["credit-risk", "batch", "retraining", "vertex-ai"],
) as dag:
    execution_identity = PythonOperator(
        task_id="execution_identity",
        python_callable=airflow_execution_identity,
        op_kwargs={"purpose": "retraining"},
        retries=0,
    )
    source_identity = PythonOperator(
        task_id="source_identity",
        python_callable=airflow_source_identity,
        op_kwargs={"dataset_type": "training"},
        retries=0,
    )
    parent_model = PythonOperator(
        task_id="require_parent_model",
        python_callable=require_parent_model,
        op_kwargs={"parent_model": "{{ var.value.vertex_parent_model }}"},
        retries=0,
    )
    wait_for_batch = GCSObjectExistenceSensor(
        task_id="wait_for_batch",
        bucket=RAW_BUCKET,
        object="{{ ti.xcom_pull(task_ids='source_identity')['prefix'] }}/_SUCCESS",
        mode="reschedule",
        deferrable=True,
        poke_interval=60,
        timeout=2 * 60 * 60,
    )
    validate_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=validate_gcs_manifest,
        op_kwargs={
            "bucket_name": RAW_BUCKET,
            "dataset_type": "training",
            "logical_date": "{{ ds }}",
            "batch_id": "{{ ti.xcom_pull(task_ids='source_identity')['batch_id'] }}",
        },
        retries=0,
    )
    start_audit_run = query_task(
        "start_audit_run",
        """
        MERGE `{{ var.value.gcp_project_id }}.credit_risk_audit.pipeline_runs` target
        USING (SELECT
          '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}' AS run_id,
          '{{ dag.dag_id }}' AS dag_id,
          TIMESTAMP('{{ logical_date.isoformat() }}') AS logical_date,
          '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}' AS batch_id
        ) source
        ON target.run_id = source.run_id
          AND target.logical_date = source.logical_date
        WHEN MATCHED THEN UPDATE SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
          (run_id, dag_id, logical_date, batch_id, status, started_at, feature_version)
        VALUES
          (source.run_id, source.dag_id, source.logical_date, source.batch_id,
           'RUNNING', CURRENT_TIMESTAMP(), 'v1')
        """,
    )
    submit_training_features = DataprocCreateBatchOperator(
        task_id="submit_training_features",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="{{ ti.xcom_pull(task_ids='execution_identity')['cloud_job_id'] }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{ARTIFACT_BUCKET}/code/spark/credit_risk/job.py",
                "args": [
                    "--mode",
                    "training",
                    "--batch-uri",
                    f"gs://{RAW_BUCKET}/{{{{ ti.xcom_pull(task_ids='source_identity')['prefix'] }}}}",
                    "--feature-date",
                    "{{ ds }}",
                    "--batch-id",
                    "{{ ti.xcom_pull(task_ids='source_identity')['batch_id'] }}",
                    "--run-id",
                    "{{ ti.xcom_pull(task_ids='execution_identity')['pipeline_run_id'] }}",
                    "--input-mode",
                    "gcs",
                    "--confirm-cloud-read",
                    "GCS",
                    "--output-mode",
                    "bigquery",
                    "--project-id",
                    PROJECT_ID,
                    "--confirm-cloud-write",
                    "BIGQUERY",
                ],
                "python_file_uris": [f"gs://{ARTIFACT_BUCKET}/code/credit_risk_runtime.zip"],
            },
            "runtime_config": {"version": "2.3"},
            "environment_config": {"execution_config": {"service_account": SPARK_SERVICE_ACCOUNT}},
            "labels": {"pipeline": "credit-risk", "workload": "retraining-features"},
        },
        deferrable=True,
        polling_interval_seconds=30,
        num_retries_if_resource_is_not_ready=3,
        execution_timeout=timedelta(hours=3),
    )
    publish_training_features = query_task(
        "publish_training_features",
        """
        ALTER TABLE
          `{{ var.value.gcp_project_id }}.credit_risk_features.{{ ti.xcom_pull(task_ids="source_identity")["staging_table"] }}`
        SET OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        );
        BEGIN TRANSACTION;
        DELETE FROM `{{ var.value.gcp_project_id }}.credit_risk_features.training_features`
        WHERE feature_date = DATE('{{ ds }}')
          AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
          AND feature_version = 'v1';
        INSERT INTO `{{ var.value.gcp_project_id }}.credit_risk_features.training_features`
        SELECT source.*, CURRENT_TIMESTAMP()
        FROM `{{ var.value.gcp_project_id }}.credit_risk_features.{{ ti.xcom_pull(task_ids="source_identity")["staging_table"] }}` source;
        COMMIT TRANSACTION;
        """,
    )
    publish_quarantine = query_task("publish_quarantine", quarantine_merge_sql())
    training_dq = query_task(
        "training_dq",
        """
        DECLARE feature_count INT64;
        DECLARE quarantine_count INT64;
        SET feature_count = (
          SELECT COUNT(*)
          FROM `{{ var.value.gcp_project_id }}.credit_risk_features.training_features`
          WHERE feature_date = DATE('{{ ds }}')
            AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
            AND feature_version = 'v1'
        );
        SET quarantine_count = (
          SELECT COUNT(*)
          FROM `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_records`
          WHERE run_id = '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
            AND business_date = DATE('{{ ds }}')
        );
        ASSERT feature_count + quarantine_count =
          {{ ti.xcom_pull(task_ids='validate_manifest')['applications'] }}
          AS 'TRAINING_FEATURE_RECONCILIATION_FAILED';
        ASSERT NOT EXISTS (
          SELECT 1
          FROM `{{ var.value.gcp_project_id }}.credit_risk_features.training_features`
          WHERE feature_date = DATE('{{ ds }}') AND defaulted NOT IN (0, 1)
        ) AS 'INVALID_TRAINING_LABEL';
        """,
        retries=0,
    )
    train_register_candidate = CreateCustomContainerTrainingJobOperator(
        task_id="train_register_candidate",
        project_id=PROJECT_ID,
        region=REGION,
        display_name="{{ ti.xcom_pull(task_ids='execution_identity')['cloud_job_id'] }}",
        container_uri=TRAINER_IMAGE,
        command=["python", "-m", "vertex.trainer.train"],
        args=[
            "--input",
            "{{ var.value.gcp_project_id }}.credit_risk_features.training_features",
            "--input-mode",
            "bigquery",
            "--project-id",
            PROJECT_ID,
            "--batch-id",
            "{{ ti.xcom_pull(task_ids='source_identity')['batch_id'] }}",
            "--confirm-cloud-read",
            "BIGQUERY",
            "--model-version",
            "candidate-{{ ds_nodash }}-{{ ti.xcom_pull(task_ids='execution_identity')['table_suffix'] }}",
            "--training-snapshot",
            "{{ ds }}",
            "--confirm-cloud-write",
            "GCS",
            "--fail-on-rejected",
        ],
        staging_bucket=f"gs://{ARTIFACT_BUCKET}",
        base_output_dir=(
            f"gs://{ARTIFACT_BUCKET}/models/training_snapshot={{{{ ds }}}}/"
            "run_id={{ ti.xcom_pull(task_ids='execution_identity')['table_suffix'] }}"
        ),
        service_account=VERTEX_SERVICE_ACCOUNT,
        machine_type="n1-standard-4",
        replica_count=1,
        model_display_name="credit-risk-logistic-regression",
        model_serving_container_image_uri=PREDICTOR_IMAGE,
        model_serving_container_predict_route="/predict",
        model_serving_container_health_route="/health",
        model_serving_container_ports=[8080],
        model_instance_schema_uri=(f"gs://{ARTIFACT_BUCKET}/code/vertex/schemas/instance.yaml"),
        model_prediction_schema_uri=(f"gs://{ARTIFACT_BUCKET}/code/vertex/schemas/prediction.yaml"),
        parent_model="{{ ti.xcom_pull(task_ids='require_parent_model') }}",
        is_default_version=False,
        model_version_aliases=["candidate"],
        model_version_description="Synthetic evaluation; feature_version=v1; snapshot={{ ds }}",
        model_labels={"feature_version": "v1", "lifecycle": "candidate"},
        deferrable=True,
        poll_interval=60,
        execution_timeout=timedelta(hours=4),
    )
    record_candidate = query_task(
        "record_candidate",
        """
        MERGE `{{ var.value.gcp_project_id }}.credit_risk_audit.training_runs` target
        USING (SELECT
          '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}' AS run_id,
          DATE('{{ ds }}') AS training_snapshot,
          CONCAT(
            'projects/{{ var.value.gcp_project_id }}/locations/{{ var.value.gcp_region }}/models/',
            '{{ ti.xcom_pull(task_ids="train_register_candidate", key="model_id") }}'
          ) AS model_resource,
          'candidate-{{ ds_nodash }}-{{ ti.xcom_pull(task_ids="execution_identity")["table_suffix"] }}' AS model_version,
          JSON_OBJECT('artifact_uri', CONCAT(
            'gs://{{ var.value.artifact_bucket }}/models/training_snapshot={{ ds }}/run_id=',
            '{{ ti.xcom_pull(task_ids="execution_identity")["table_suffix"] }}/metrics.json'
          )) AS metrics,
          JSON_OBJECT('status', 'PASSED', 'source', 'trainer exit gate') AS quality_gates
        ) source
        ON target.run_id = source.run_id
          AND target.training_snapshot = source.training_snapshot
        WHEN MATCHED THEN UPDATE SET
          model_resource = source.model_resource, model_version = source.model_version,
          status = 'CANDIDATE', metrics = source.metrics, quality_gates = source.quality_gates,
          completed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
          (run_id, training_snapshot, model_resource, model_version, feature_version,
           status, metrics, quality_gates, started_at, completed_at)
        VALUES
          (source.run_id, source.training_snapshot, source.model_resource, source.model_version,
           'v1', 'CANDIDATE', source.metrics, source.quality_gates,
           CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """,
        retries=0,
    )
    mark_success = query_task(
        "mark_success",
        """
        UPDATE `{{ var.value.gcp_project_id }}.credit_risk_audit.pipeline_runs`
        SET status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP(),
            source_count = {{ ti.xcom_pull(task_ids='validate_manifest')['applications'] }},
            accepted_count = (
              SELECT COUNT(*)
              FROM `{{ var.value.gcp_project_id }}.credit_risk_features.training_features`
              WHERE feature_date = DATE('{{ ds }}')
                AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
            ),
            quarantined_count = (
              SELECT COUNT(*)
              FROM `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_records`
              WHERE run_id = '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
                AND business_date = DATE('{{ ds }}')
            ),
            model_version = 'candidate-{{ ds_nodash }}-{{ ti.xcom_pull(task_ids="execution_identity")["table_suffix"] }}'
        WHERE run_id = '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
          AND DATE(logical_date) = DATE('{{ ds }}')
        """,
        retries=0,
    )

    [execution_identity, source_identity, parent_model] >> wait_for_batch
    wait_for_batch >> validate_manifest >> start_audit_run >> submit_training_features
    submit_training_features >> [publish_training_features, publish_quarantine] >> training_dq
    training_dq >> train_register_candidate >> record_candidate >> mark_success
