"""Daily logical-date-aware credit-risk batch scoring workflow."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from credit_risk_runtime import (
    airflow_execution_identity,
    airflow_source_identity,
    audit_failure_callback,
    compute_and_persist_drift,
    normalize_vertex_predictions,
    quarantine_merge_sql,
    resolve_active_model,
    submit_object_batch_prediction_job,
    validate_gcs_manifest,
)

PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region }}"
RAW_BUCKET = "{{ var.value.raw_bucket }}"
ARTIFACT_BUCKET = "{{ var.value.artifact_bucket }}"
SPARK_SERVICE_ACCOUNT = "{{ var.value.spark_service_account }}"
VERTEX_SERVICE_ACCOUNT = "{{ var.value.vertex_service_account }}"

SOURCE_ID = "{{ ti.xcom_pull(task_ids='source_identity') }}"
EXECUTION_ID = "{{ ti.xcom_pull(task_ids='execution_identity') }}"
MODEL = "{{ ti.xcom_pull(task_ids='resolve_active_model') }}"


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
    dag_id="credit_risk_daily_scoring",
    description="Immutable daily feature engineering and governed Vertex batch scoring",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 4 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": audit_failure_callback,
    },
    render_template_as_native_obj=True,
    tags=["credit-risk", "batch", "managed-spark", "vertex-ai"],
) as dag:
    execution_identity = PythonOperator(
        task_id="execution_identity",
        python_callable=airflow_execution_identity,
        op_kwargs={"purpose": "daily-score"},
        retries=0,
    )
    source_identity = PythonOperator(
        task_id="source_identity",
        python_callable=airflow_source_identity,
        op_kwargs={"dataset_type": "scoring"},
        retries=0,
    )
    wait_for_batch = GCSObjectExistenceSensor(
        task_id="wait_for_batch",
        bucket=RAW_BUCKET,
        object="{{ ti.xcom_pull(task_ids='source_identity')['prefix'] }}/_SUCCESS",
        mode="reschedule",
        deferrable=True,
        poke_interval=60,
        timeout=60 * 60,
    )
    validate_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=validate_gcs_manifest,
        op_kwargs={
            "bucket_name": RAW_BUCKET,
            "dataset_type": "scoring",
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
    submit_feature_batch = DataprocCreateBatchOperator(
        task_id="submit_feature_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="{{ ti.xcom_pull(task_ids='execution_identity')['cloud_job_id'] }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{ARTIFACT_BUCKET}/code/spark/credit_risk/job.py",
                "args": [
                    "--mode",
                    "scoring",
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
            "labels": {"pipeline": "credit-risk", "workload": "daily-scoring"},
        },
        deferrable=True,
        polling_interval_seconds=30,
        num_retries_if_resource_is_not_ready=3,
        execution_timeout=timedelta(hours=2),
    )
    publish_features = query_task(
        "publish_features",
        """
        ALTER TABLE
          `{{ var.value.gcp_project_id }}.credit_risk_features.{{ ti.xcom_pull(task_ids="source_identity")["staging_table"] }}`
        SET OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        );
        BEGIN TRANSACTION;
        DELETE FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
        WHERE feature_date = DATE('{{ ds }}')
          AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
          AND feature_version = 'v1';
        INSERT INTO `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
        SELECT source.*, CURRENT_TIMESTAMP()
        FROM `{{ var.value.gcp_project_id }}.credit_risk_features.{{ ti.xcom_pull(task_ids="source_identity")["staging_table"] }}` source;
        COMMIT TRANSACTION;
        """,
    )
    publish_quarantine = query_task("publish_quarantine", quarantine_merge_sql())
    feature_dq = query_task(
        "feature_dq",
        """
        DECLARE feature_count INT64;
        DECLARE quarantine_count INT64;
        SET feature_count = (
          SELECT COUNT(*)
          FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
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
          AS 'FEATURE_RECONCILIATION_FAILED';
        ASSERT NOT EXISTS (
          SELECT loan_id
          FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
          WHERE feature_date = DATE('{{ ds }}')
            AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
          GROUP BY loan_id HAVING COUNT(*) > 1
        ) AS 'DUPLICATE_SCORING_FEATURE';
        """,
        retries=0,
    )
    prepare_prediction_input = query_task(
        "prepare_prediction_input",
        """
        CREATE OR REPLACE TABLE
          `{{ var.value.gcp_project_id }}.credit_risk_staging.vertex_input_{{ ti.xcom_pull(task_ids="execution_identity")["table_suffix"] }}`
        OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS
        SELECT * EXCEPT(batch_id, published_at)
        FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
        WHERE feature_date = DATE('{{ ds }}')
          AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
          AND feature_version = 'v1';
        """,
    )
    resolve_model = PythonOperator(
        task_id="resolve_active_model",
        python_callable=resolve_active_model,
        op_kwargs={
            "project_id": PROJECT_ID,
            "region": REGION,
            "score_date": "{{ ds }}",
            "model_version_override": "{{ dag_run.conf.get('model_version') if dag_run else none }}",
        },
        retries=0,
    )
    vertex_batch_prediction = PythonOperator(
        task_id="vertex_batch_prediction",
        python_callable=submit_object_batch_prediction_job,
        op_kwargs={
            "project_id": PROJECT_ID,
            "region": REGION,
            "job_display_name": (
                "{{ ti.xcom_pull(task_ids='execution_identity')['cloud_job_id'] }}"
            ),
            "model": MODEL,
            "bigquery_source": (
                "bq://{{ var.value.gcp_project_id }}.credit_risk_staging."
                "vertex_input_{{ ti.xcom_pull(task_ids='execution_identity')['table_suffix'] }}"
            ),
            "bigquery_destination_prefix": "bq://{{ var.value.gcp_project_id }}",
            "service_account": VERTEX_SERVICE_ACCOUNT,
            "labels": {"pipeline": "credit-risk", "workload": "daily-scoring"},
        },
        execution_timeout=timedelta(hours=4),
    )
    normalize_scores = PythonOperator(
        task_id="normalize_merge_scores",
        python_callable=normalize_vertex_predictions,
        op_kwargs={
            "project_id": PROJECT_ID,
            "region": REGION,
            "batch_prediction_job_id": "{{ ti.xcom_pull(task_ids='vertex_batch_prediction') }}",
            "score_date": "{{ ds }}",
            "pipeline_run_id": (
                "{{ ti.xcom_pull(task_ids='execution_identity')['pipeline_run_id'] }}"
            ),
            "model": MODEL,
        },
    )
    score_dq = query_task(
        "score_dq",
        """
        ASSERT NOT EXISTS (
          SELECT 1 FROM `{{ var.value.gcp_project_id }}.credit_risk_scoring.risk_scores`
          WHERE score_date = DATE('{{ ds }}')
            AND model_version = '{{ ti.xcom_pull(task_ids="resolve_active_model")["model_version"] }}'
            AND (probability_of_default < 0 OR probability_of_default > 1)
        ) AS 'INVALID_PROBABILITY';
        ASSERT (
          SELECT COUNT(*) FROM `{{ var.value.gcp_project_id }}.credit_risk_scoring.risk_scores`
          WHERE score_date = DATE('{{ ds }}')
            AND model_version = '{{ ti.xcom_pull(task_ids="resolve_active_model")["model_version"] }}'
            AND pipeline_run_id =
              '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
        ) = (
          SELECT COUNT(*)
          FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
          WHERE feature_date = DATE('{{ ds }}')
            AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
            AND feature_version = 'v1'
        )
          AS 'SCORE_RECONCILIATION_FAILED';
        """,
        retries=0,
    )
    drift_metrics = PythonOperator(
        task_id="drift_metrics",
        python_callable=compute_and_persist_drift,
        op_kwargs={
            "project_id": PROJECT_ID,
            "region": REGION,
            "score_date": "{{ ds }}",
            "run_id": "{{ ti.xcom_pull(task_ids='execution_identity')['pipeline_run_id'] }}",
            "model_version": "{{ ti.xcom_pull(task_ids='resolve_active_model')['model_version'] }}",
        },
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
              FROM `{{ var.value.gcp_project_id }}.credit_risk_features.scoring_features`
              WHERE feature_date = DATE('{{ ds }}')
                AND batch_id = '{{ ti.xcom_pull(task_ids="source_identity")["batch_id"] }}'
            ),
            quarantined_count = (
              SELECT COUNT(*)
              FROM `{{ var.value.gcp_project_id }}.credit_risk_audit.quarantine_records`
              WHERE run_id = '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
                AND business_date = DATE('{{ ds }}')
            ),
            score_count = (
              SELECT COUNT(*)
              FROM `{{ var.value.gcp_project_id }}.credit_risk_scoring.risk_scores`
              WHERE score_date = DATE('{{ ds }}')
                AND pipeline_run_id =
                  '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
            ),
            model_version = '{{ ti.xcom_pull(task_ids="resolve_active_model")["model_version"] }}'
        WHERE run_id = '{{ ti.xcom_pull(task_ids="execution_identity")["pipeline_run_id"] }}'
          AND DATE(logical_date) = DATE('{{ ds }}')
        """,
        retries=0,
    )

    [execution_identity, source_identity] >> wait_for_batch
    wait_for_batch >> validate_manifest >> start_audit_run >> submit_feature_batch
    submit_feature_batch >> [publish_features, publish_quarantine] >> feature_dq
    feature_dq >> prepare_prediction_input
    prepare_prediction_input >> resolve_model >> vertex_batch_prediction >> normalize_scores
    normalize_scores >> score_dq >> drift_metrics >> mark_success
