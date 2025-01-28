from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from airflow.sensors.python import PythonSensor

from .common import Action, CheckResult, Result
from .utils import fail_, pass_

if TYPE_CHECKING:
    from airflow.models.operator import Operator
    from airflow.operators.python import BranchPythonOperator

__all__ = ("HighAvailabilityOperator",)


class HighAvailabilityOperator(PythonSensor):
    _decide_task: "BranchPythonOperator"
    _fail: "Operator"
    _retrigger_fail: "Operator"
    _retrigger_pass: "Operator"
    _stop_pass: "Operator"
    _stop_fail: "Operator"
    _sensor_failed_task: "Operator"

    def __init__(
        self,
        python_callable: Callable[..., CheckResult],
        pass_trigger_kwargs: Optional[Dict[str, Any]] = None,
        fail_trigger_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """The HighAvailabilityOperator is an Airflow Meta-Operator for long-running or "always-on" tasks.

        It resembles a BranchPythonOperator with the following predefined set of outcomes:

        check -> decide -> PASS/RETRIGGER
                        -> PASS/STOP
                        -> FAIL/RETRIGGER
                        -> FAIL/STOP
                        -> */CONTINUE

        Any setup should be state-aware (e.g. don't just start a process, check if it is currently started first).
        """
        pass_trigger_kwargs = pass_trigger_kwargs or {}
        fail_trigger_kwargs = fail_trigger_kwargs or {}

        def _callable_wrapper(**kwargs):
            task_instance = kwargs["task_instance"]
            ret: CheckResult = python_callable(**kwargs)

            if not isinstance(ret, tuple) or not len(ret) == 2 or not isinstance(ret[0], Result) or not isinstance(ret[1], Action):
                # malformed
                task_instance.xcom_push(key="return_value", value=(Result.FAIL, Action.STOP))
                return True

            # push to xcom
            task_instance.xcom_push(key="return_value", value=ret)

            if ret[1] == Action.CONTINUE:
                # keep checking
                return False
            return True

        super().__init__(python_callable=_callable_wrapper, **kwargs)

        # Deferred imports
        from airflow.operators.python import BranchPythonOperator, PythonOperator
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        # this is needed to ensure the dag fails, since the
        # retrigger_fail step will pass (to ensure dag retriggers!)
        self._fail = PythonOperator(task_id=f"{self.task_id}-force-dag-fail", python_callable=fail_, trigger_rule="all_success")

        self._retrigger_fail = TriggerDagRunOperator(
            task_id=f"{self.task_id}-retrigger-fail", **{"trigger_dag_id": self.dag_id, "trigger_rule": "all_success", **fail_trigger_kwargs}
        )
        self._retrigger_pass = TriggerDagRunOperator(
            task_id=f"{self.task_id}-retrigger-pass", **{"trigger_dag_id": self.dag_id, "trigger_rule": "one_success", **pass_trigger_kwargs}
        )

        self._stop_pass = PythonOperator(task_id=f"{self.task_id}-stop-pass", python_callable=pass_, trigger_rule="all_success")
        self._stop_fail = PythonOperator(task_id=f"{self.task_id}-stop-fail", python_callable=fail_, trigger_rule="all_success")

        self._sensor_failed_task = PythonOperator(task_id=f"{self.task_id}-sensor-timeout", python_callable=pass_, trigger_rule="all_failed")

        branch_choices = {
            (Result.PASS, Action.RETRIGGER): self._retrigger_pass.task_id,
            (Result.PASS, Action.STOP): self._stop_pass.task_id,
            (Result.FAIL, Action.RETRIGGER): self._retrigger_fail.task_id,
            (Result.FAIL, Action.STOP): self._stop_fail.task_id,
        }

        def _choose_branch(branch_choices=branch_choices, **kwargs):
            from airflow.exceptions import AirflowSkipException

            task_instance = kwargs["task_instance"]
            check_program_result = task_instance.xcom_pull(key="return_value", task_ids=self.task_id)
            try:
                result = Result(check_program_result[0])
                action = Action(check_program_result[1])
                ret = branch_choices.get((result, action), None)
            except (ValueError, IndexError, TypeError):
                ret = None
            if ret is None:
                # skip result
                raise AirflowSkipException
            return ret

        self._decide_task = BranchPythonOperator(
            task_id=f"{self.task_id}-decide",
            python_callable=_choose_branch,
            provide_context=True,
            trigger_rule="all_success",
        )

        self >> self._decide_task >> self._stop_pass
        self >> self._decide_task >> self._stop_fail
        self >> self._decide_task >> self._retrigger_pass
        self >> self._decide_task >> self._retrigger_fail >> self._fail
        self >> self._sensor_failed_task >> self._retrigger_pass

    @property
    def stop_fail(self) -> "Operator":
        return self._stop_fail

    @property
    def stop_pass(self) -> "Operator":
        return self._stop_pass

    @property
    def retrigger_fail(self) -> "Operator":
        return self._retrigger_fail

    @property
    def retrigger_pass(self) -> "Operator":
        return self._retrigger_pass
