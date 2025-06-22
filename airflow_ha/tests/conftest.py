from airflow.models import DAG
from pytest import fixture

from airflow_ha import Action, HighAvailabilityOperator, Result


def _choose(**kwargs):
    """
    Example callable for HighAvailabilityOperator.
    Returns a tuple of Result and Action.
    """
    return (Result.PASS, Action.CONTINUE)  # noqa: E731


@fixture(autouse=True)
def operator():
    dag = DAG(dag_id="test_dag", default_args={}, schedule=None, params={})
    operator = HighAvailabilityOperator(task_id="test_task", python_callable=_choose, dag=dag, pool="test-pool")
    return operator
