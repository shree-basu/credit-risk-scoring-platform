from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag

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
    assert dag.get_task("score_dq").downstream_task_ids == {"drift_metrics"}
    assert dag.get_task("drift_metrics").downstream_task_ids == {"mark_success"}


def test_retraining_only_registers_candidate(dagbag: DagBag) -> None:
    dag = dagbag.dags["credit_risk_periodic_retraining"]
    task = dag.get_task("train_register_candidate")
    assert task.is_default_version is False
    assert task.model_version_aliases == ["candidate"]
    assert "record_candidate" in task.downstream_task_ids
