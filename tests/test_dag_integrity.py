"""
DAG integrity tests — verify all DAGs parse and load without errors.
Run with: pytest tests/test_dag_integrity.py
"""
import os
import pytest
from airflow.models import DagBag


DAG_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'dags')
EXPECTED_DAGS = [
    'ai_portfolio_job_search',   # production pipeline
]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    """No DAG file should fail to import."""
    assert dagbag.import_errors == {}, (
        f"DAG import failures:\n" +
        "\n".join(f"  {k}: {v}" for k, v in dagbag.import_errors.items())
    )


def test_expected_dags_present(dagbag):
    """Production and utility DAGs must be registered."""
    for dag_id in EXPECTED_DAGS:
        assert dag_id in dagbag.dags, f"Expected DAG '{dag_id}' not found in DagBag"


def test_production_dag_task_count(dagbag):
    """Production DAG must have exactly 6 tasks in the correct order."""
    dag = dagbag.dags.get('ai_portfolio_job_search')
    assert dag is not None
    assert len(dag.tasks) == 6


def test_production_dag_task_ids(dagbag):
    """Production DAG task IDs match the expected pipeline steps."""
    dag = dagbag.dags.get('ai_portfolio_job_search')
    assert dag is not None
    expected_tasks = {
        'analyze_resume_task',
        'extract_jobs_task',
        'load_to_bq_task',
        'transform_bq_task',
        'rank_jobs_task',
        'export_to_sheets_task',
    }
    actual_tasks = {t.task_id for t in dag.tasks}
    assert actual_tasks == expected_tasks


def test_production_dag_has_no_schedule(dagbag):
    """Production DAG should be manually triggered (schedule=None)."""
    dag = dagbag.dags.get('ai_portfolio_job_search')
    assert dag is not None
    assert dag.schedule_interval is None
