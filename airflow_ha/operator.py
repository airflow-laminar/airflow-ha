from typing import Literal

from airflow.models.operator import Operator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

__all__ = ("HighAvailabilityOperator",)


CheckResult = Literal[
    "done",
    "running",
    "failed",
]


def skip_():
    raise AirflowSkipException


def fail_():
    raise AirflowFailException


def pass_():
    pass


class HighAvailabilityOperator(PythonSensor):
    _decide_task: BranchPythonOperator

    _end_fail: Operator
    _end_pass: Operator

    _loop_pass: Operator
    _loop_fail: Operator

    _done_task: Operator
    _end_task: Operator
    _running_task: Operator
    _failed_task: Operator
    _kill_task: Operator

    _cleanup_task: Operator
    _loop_task: Operator
    _restart_task: Operator

    def __init__(self, **kwargs) -> None:
        """The HighAvailabilityOperator is an Airflow Meta-Operator for long-running or "always-on" tasks.

        It resembles a BranchPythonOperator with the following predefined set of outcomes:

                       /-> "done"    -> Done -> EndPass
        check -> decide -> "running" -> Loop -> EndPass
                       \-> "failed"  -> Loop -> EndFail
           \-------------> failed    -> Loop -> EndPass

        Given a check, there are four outcomes:
        - The tasks finished/exited cleanly, and thus the DAG should terminate cleanly
        - The tasks finished/exited uncleanly, in which case the DAG should restart
        - The tasks did not finish, but the end time has been reached anyway, so the DAG should terminate cleanly
        - The tasks did not finish, but we've reached an interval and should loop and rerun the DAG

        The last case is particularly important when DAGs have a max run time, e.g. on AWS MWAA where DAGs
        cannot run for longer than 12 hours at a time and so must be "restarted".

        Additionally, there is a "KillTask" to force kill the DAG.

        Any setup should be state-aware (e.g. don't just start a process, check if it is currently started first).
        """
        python_callable = kwargs.pop("python_callable")

        def _callable_wrapper(**kwargs):
            task_instance = kwargs["task_instance"]
            ret: CheckResult = python_callable(**kwargs)
            if ret == "done":
                task_instance.xcom_push(key="return_value", value="done")
                # finish
                return True
            elif ret == "failed":
                task_instance.xcom_push(key="return_value", value="failed")
                # finish
                return True
            elif ret == "running":
                task_instance.xcom_push(key="return_value", value="running")
                # finish
                return True
            task_instance.xcom_push(key="return_value", value="")
            return False

        super().__init__(python_callable=_callable_wrapper, **kwargs)

        self._end_fail = PythonOperator(task_id=f"{self.task_id}-dag-fail", python_callable=fail_, trigger_rule="all_success")
        self._end_pass = PythonOperator(task_id=f"{self.task_id}-dag-pass", python_callable=pass_, trigger_rule="all_success")

        self._loop_fail = TriggerDagRunOperator(task_id=f"{self.task_id}-loop-fail", trigger_dag_id=self.dag_id, trigger_rule="all_success")
        self._loop_pass = TriggerDagRunOperator(task_id=f"{self.task_id}-loop-pass", trigger_dag_id=self.dag_id, trigger_rule="one_success")

        self._done_task = PythonOperator(task_id=f"{self.task_id}-done", python_callable=pass_, trigger_rule="all_success")
        self._running_task = PythonOperator(task_id=f"{self.task_id}-running", python_callable=pass_, trigger_rule="all_success")
        self._failed_task = PythonOperator(task_id=f"{self.task_id}-failed", python_callable=pass_, trigger_rule="all_success")
        self._sensor_failed_task = PythonOperator(task_id=f"{self.task_id}-sensor-timeout", python_callable=pass_, trigger_rule="all_failed")

        branch_choices = {
            "done": self._done_task.task_id,
            "running": self._running_task.task_id,
            "failed": self._failed_task.task_id,
            "": self._sensor_failed_task.task_id,
        }

        def _choose_branch(branch_choices=branch_choices, **kwargs):
            task_instance = kwargs["task_instance"]
            check_program_result = task_instance.xcom_pull(key="return_value", task_ids=self.task_id)
            ret = branch_choices.get(check_program_result, None)
            if ret is None:
                raise AirflowSkipException
            return ret

        self._decide_task = BranchPythonOperator(
            task_id=f"{self.task_id}-decide",
            python_callable=_choose_branch,
            provide_context=True,
            trigger_rule="all_success",
        )

        self >> self._sensor_failed_task >> self._loop_pass >> self._end_pass
        self >> self._decide_task >> self._done_task
        self >> self._decide_task >> self._running_task >> self._loop_pass >> self._end_pass
        self >> self._decide_task >> self._failed_task >> self._loop_fail >> self._end_fail

    @property
    def check(self) -> Operator:
        return self

    @property
    def failed(self) -> Operator:
        # NOTE: use loop_fail as this will pass, but self._end_fail will fail to mark the DAG failed
        return self._loop_fail

    @property
    def passed(self) -> Operator:
        # NOTE: use loop_pass here to match failed()
        return self._loop_pass

    @property
    def done(self) -> Operator:
        return self._done_task
