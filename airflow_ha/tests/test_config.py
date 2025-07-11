from airflow_config import DAG, load_config
from airflow_pydantic import Dag


class TestConfig:
    def test_cleanup_task(self):
        conf = load_config("config", "config")
        d = DAG(dag_id="test_ha", config=conf)
        assert len(d.tasks) == 7
        d = Dag(dag_id="test_ha").instantiate(config=conf)
        assert len(d.tasks) == 7

    def test_cleanup_render(self):
        conf = load_config("config", "config")
        assert (
            conf.dags["test_ha"].render()
            == """# Generated by airflow-config
from airflow.models import DAG
from airflow.models.param import Param
from pendulum import Timezone, datetime as pdatetime

from airflow_ha.operator import HighAvailabilitySensor
from airflow_ha.tests.conftest import _choose

with DAG(
    schedule="0 0 * * *",
    start_date=pdatetime(year=2025, month=1, day=1, tz=Timezone("America/New_York")),
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    params={
        "poke_interval": Param(None, title="Poke Interval", description=None, type=["null", "number"]),
        "timeout": Param(None, title="Timeout", description=None, type=["null", "number"]),
        "soft_fail": Param(None, title="Soft Fail", description=None, type=["null", "boolean"]),
        "mode": Param(None, title="Mode", description=None, type=["null", "string"]),
        "exponential_backoff": Param(None, title="Exponential Backoff", description=None, type=["null", "boolean"]),
        "max_wait": Param(None, title="Max Wait", description=None, type=["null", "number"]),
        "silent_fail": Param(None, title="Silent Fail", description=None, type=["null", "boolean"]),
        "never_fail": Param(None, title="Never Fail", description=None, type=["null", "boolean"]),
        "op_args": Param(
            None,
            title="Op Args",
            description="a list of positional arguments that will get unpacked when calling your callable",
            type=["null", "array"],
        ),
        "op_kwargs": Param(
            None, title="Op Kwargs", description="a dictionary of keyword arguments that will get unpacked in your function", type=["null", "object"]
        ),
        "templates_dict": Param(
            None,
            title="Templates Dict",
            description="a dictionary where the values are templates that will get templated by the Airflow engine sometime between __init__ and execute takes place and are made available in your callable’s context after the template has been applied. (templated)",
            type=["null", "object"],
        ),
        "pass_trigger_kwargs": Param(None, title="Pass Trigger Kwargs", description=None, type=["null", "object"]),
        "fail_trigger_kwargs": Param(None, title="Fail Trigger Kwargs", description=None, type=["null", "object"]),
        "runtime": Param(120, title="Runtime", description=None, type="integer"),
        "endtime": Param(None, title="Endtime", description=None, type=["null", "string"]),
        "maxretrigger": Param(2, title="Maxretrigger", description=None, type="integer"),
        "reference_date": Param("data_interval_end", title="Reference Date", description=None, type="string"),
    },
    dag_id="test_ha",
    default_args={},
) as dag:
    test_ha = HighAvailabilitySensor(python_callable=_choose, task_id="test_ha", dag=dag)
"""
        )
