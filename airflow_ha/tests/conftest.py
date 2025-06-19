from airflow.models import DAG
from pytest import fixture

from airflow_ha import Action, HighAvailabilityOperator, Result


@fixture(autouse=True)
def operator():
    callable = lambda **kwargs: (Result.PASS, Action.CONTINUE)  # noqa: E731
    dag = DAG(dag_id="test_dag", default_args={}, schedule=None, params={})
    operator = HighAvailabilityOperator(task_id="test_task", python_callable=callable, dag=dag, pool="test-pool")
    return operator
