from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.aiplatform_v1.types import BatchPredictionJob

from dags.credit_risk_runtime import vertex_batch_prediction_request

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False, safe_mode=False)
    assert bag.import_errors == {}
    return bag


def test_expected_dags_import(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == {
        "credit_risk_daily_scoring",
        "credit_risk_periodic_retraining",
    }


@pytest.mark.parametrize(
    ("dag_id", "terminal_task"),
    [
        ("credit_risk_daily_scoring", "mark_success"),
        ("credit_risk_periodic_retraining", "mark_success"),
    ],
)
def test_backfill_and_success_gates(dagbag: DagBag, dag_id: str, terminal_task: str) -> None:
    dag = dagbag.dags[dag_id]
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.get_task(terminal_task).downstream_task_ids == set()

    permanent_dq_tasks = {
        task.task_id: task.retries
        for task in dag.tasks
        if task.task_id in {"validate_manifest", "feature_dq", "training_dq", "score_dq"}
    }
    assert permanent_dq_tasks
    assert set(permanent_dq_tasks.values()) == {0}


def test_daily_scoring_dependency_contract(dagbag: DagBag) -> None:
    dag = dagbag.dags["credit_risk_daily_scoring"]
    prediction_task = dag.get_task("vertex_batch_prediction")
    assert prediction_task.python_callable.__name__ == "submit_object_batch_prediction_job"
    assert dag.get_task("score_dq").downstream_task_ids == {"drift_metrics"}
    assert dag.get_task("drift_metrics").downstream_task_ids == {"mark_success"}


def test_pinned_provider_contracts_accept_query_jobs_and_object_instances() -> None:
    assert callable(BigQueryHook.insert_job)
    assert not hasattr(BigQueryHook, "run_query")

    request = vertex_batch_prediction_request(
        project_id="project-id",
        region="us-central1",
        job_display_name="daily-20260902",
        model_resource="projects/project-id/locations/us-central1/models/1@2",
        bigquery_source="bq://project-id.credit_risk_staging.vertex_input_1",
        bigquery_destination_prefix="bq://project-id",
        service_account="vertex@project-id.iam.gserviceaccount.com",
    )
    job = BatchPredictionJob(request["batch_prediction_job"])
    assert job.instance_config.instance_type == "object"
    assert (
        list(job.instance_config.included_fields)
        == request["batch_prediction_job"]["instance_config"]["included_fields"]
    )


def test_retraining_only_registers_candidate(dagbag: DagBag) -> None:
    dag = dagbag.dags["credit_risk_periodic_retraining"]
    task = dag.get_task("train_register_candidate")
    assert task.is_default_version is False
    assert task.model_version_aliases == ["candidate"]
    assert "record_candidate" in task.downstream_task_ids
