import pytest
import os
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle


@pytest.fixture
def dagbag() -> DagBag:
    return DagBag(dag_folder="airflow/dags", include_examples=False)


def test_dag_loading(dagbag):
    """Test that all DAGs load without errors"""
    dag_bag = dagbag
    assert (
        len(dag_bag.import_errors) == 0
    ), f"DAG import errors: {dag_bag.import_errors}"


def test_dag_cycles(dagbag):
    """Test that DAGs don't have cycles"""
    dag_bag = dagbag
    for dag_id, dag in dag_bag.dags.items():
        check_cycle(dag)


def test_dag_default_args(dagbag):
    """Test that DAGs have required default args"""
    dag_bag = dagbag
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("owner") is not None
        assert dag.default_args.get("retries") is not None


def test_dag_task_count(dagbag):
    """Test that DAGs have minimum expected tasks"""
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"


def test_dag_schedule_interval(dagbag):
    """Test that DAGs have valid schedule intervals"""
    for dag_id, dag in dagbag.dags.items():
        assert dag.schedule_interval is not None
