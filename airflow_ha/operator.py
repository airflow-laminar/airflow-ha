from datetime import UTC, datetime, time, timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

from airflow.models.param import Param
from airflow.sensors.python import PythonSensor

from .common import Action, CheckResult, Result
from .utils import fail_, pass_

if TYPE_CHECKING:
    from airflow.models.operator import Operator
    from airflow.operators.python import BranchPythonOperator, PythonOperator

__all__ = ("HighAvailabilityOperator",)
_log = getLogger(__name__)


class HighAvailabilityOperator(PythonSensor):
    _decide_task: "BranchPythonOperator"
    _count_task: "PythonOperator"
    _fail: "Operator"
    _retrigger_fail: "Operator"
    _retrigger_pass: "Operator"
    _stop_pass: "Operator"
    _stop_fail: "Operator"
    _pass_trigger_kwargs: Optional[Dict[str, Any]] = None
    _pass_trigger_kwargs_conf: str = "{}"
    _fail_trigger_kwargs: Optional[Dict[str, Any]] = None
    _fail_trigger_kwargs_conf: str = "{}"

    _runtime: Optional[timedelta] = None
    _endtime: Optional[time] = None
    _maxretrigger: Optional[int] = None

    def __init__(
        self,
        python_callable: Callable[..., CheckResult],
        pass_trigger_kwargs: Optional[Dict[str, Any]] = None,
        fail_trigger_kwargs: Optional[Dict[str, Any]] = None,
        runtime: Optional[Union[int, timedelta]] = None,
        endtime: Optional[Union[str, time]] = None,
        maxretrigger: Optional[int] = None,
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
        # These options control the behavior of the sensor
        self._runtime = timedelta(seconds=runtime) if isinstance(runtime, int) else runtime
        self._endtime = time.fromisoformat(endtime) if isinstance(endtime, str) else endtime
        self._maxretrigger = maxretrigger or None

        # These are kwarsg to pass to the trigger operators
        self._pass_trigger_kwargs = pass_trigger_kwargs or {}
        self._fail_trigger_kwargs = fail_trigger_kwargs or {}
        self._pass_trigger_kwargs_conf = self._pass_trigger_kwargs.pop("conf", {})
        self._fail_trigger_kwargs_conf = self._fail_trigger_kwargs.pop("conf", {})

        # Function to control the sensor
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

        if not kwargs.get("trigger_rule"):
            kwargs["trigger_rule"] = "none_failed"

        # Initialize the sensor
        super().__init__(python_callable=_callable_wrapper, **kwargs)

        # Add params to dag to control overrides
        self.dag.params.update(
            {
                f"{self.task_id}-force-run": Param(False, "Ignore runtime/endtime/maxretrigger and force run the task", type="boolean"),
                f"{self.task_id}-force-runtime": Param(
                    None, "Override `runtime` in seconds, incompatible with `force-run`", type=["null", "integer"]
                ),
                f"{self.task_id}-force-endtime": Param(
                    None, "Override `endtime`, incompatible with `force-run`", type=["null", "string"], format="time"
                ),
                f"{self.task_id}-force-maxretrigger": Param(None, "Override `maxretrigger`, incompatible with `force-run`", type=["null", "integer"]),
            }
        )

        # Deferred imports
        from airflow.operators.python import BranchPythonOperator, PythonOperator
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        # this is needed to ensure the dag fails, since the
        # retrigger_fail step will pass (to ensure dag retriggers!)
        self._fail = PythonOperator(task_id=f"{self.task_id}-force-dag-fail", python_callable=fail_)

        self._stop_pass = PythonOperator(task_id=f"{self.task_id}-stop-pass", python_callable=pass_)
        self._stop_fail = PythonOperator(task_id=f"{self.task_id}-stop-fail", python_callable=fail_)

        self._count_task = PythonOperator(
            task_id=f"{self.task_id}-get-retrigger-count",
            python_callable=lambda **kwargs: int(kwargs["dag_run"].conf.get(f"{self.task_id}-retrigger", -1)) + 1,
            provide_context=True,
            trigger_rule="none_skipped",
        )

        # Update the retrigger counts in trigger kwargs
        if isinstance(self._pass_trigger_kwargs_conf, dict):
            self._pass_trigger_kwargs_conf[f"{self.task_id}-retrigger"] = (
                f'''{{{{ ti.xcom_pull(key="return_value", task_ids="{self._count_task.task_id}") }}}}'''
            )
        else:
            if not isinstance(self._pass_trigger_kwargs_conf, str) or not self._pass_trigger_kwargs_conf.strip().endswith("}"):
                raise ValueError("pass_trigger_kwargs must be a dict or a JSON string")
            self._pass_trigger_kwargs_conf = (
                self._pass_trigger_kwargs_conf.strip()[:-1]
                + f', "{self.task_id}-retrigger": {{{{ ti.xcom_pull(key="return_value", task_ids="{self._count_task.task_id}") }}}} }}'
            )

        if isinstance(self._fail_trigger_kwargs_conf, dict):
            self._fail_trigger_kwargs_conf[f"{self.task_id}-retrigger"] = (
                f'''{{{{ ti.xcom_pull(key="return_value", task_ids="{self._count_task.task_id}") }}}}'''
            )
        else:
            if not isinstance(self._fail_trigger_kwargs_conf, str) or not self._fail_trigger_kwargs_conf.strip().endswith("}"):
                raise ValueError("fail_trigger_kwargs must be a dict or a JSON string")
            self._fail_trigger_kwargs_conf = (
                self._fail_trigger_kwargs_conf.strip()[:-1]
                + f', "{self.task_id}-retrigger": {{{{ ti.xcom_pull(key="return_value", task_ids="{self._count_task.task_id}") }}}} }}'
            )

        self._retrigger_fail = TriggerDagRunOperator(
            task_id=f"{self.task_id}-retrigger-fail",
            conf=self._fail_trigger_kwargs_conf,
            **{"trigger_dag_id": self.dag_id, "trigger_rule": "one_success", **self._fail_trigger_kwargs},
        )
        self._retrigger_pass = TriggerDagRunOperator(
            task_id=f"{self.task_id}-retrigger-pass",
            conf=self._pass_trigger_kwargs_conf,
            **{"trigger_dag_id": self.dag_id, "trigger_rule": "one_success", **self._pass_trigger_kwargs},
        )
        branch_choices = {
            (Result.PASS, Action.RETRIGGER): self._retrigger_pass.task_id,
            (Result.PASS, Action.STOP): self._stop_pass.task_id,
            (Result.FAIL, Action.RETRIGGER): self._retrigger_fail.task_id,
            (Result.FAIL, Action.STOP): self._stop_fail.task_id,
        }

        def _choose_branch(branch_choices=branch_choices, runtime=self._runtime, endtime=self._endtime, maxretrigger=self._maxretrigger, **kwargs):
            # Grab the task instance
            task_instance = kwargs["task_instance"]

            # Grab the dag start date
            dag_start_date = kwargs["dag_run"].start_date

            # Check if force run in dag run kwargs
            force_run_conf = kwargs["dag_run"].conf.get("airflow_ha_force_run", False)
            force_run_param = kwargs["params"].get(f"{self.task_id}-force-run", False)
            _log.info(f"airflow-ha configuration -- force_run (conf): {force_run_conf}, force_run (param): {force_run_param}")
            if not force_run_conf and not force_run_param:
                runtime = kwargs["params"].get(f"{self.task_id}-force-runtime", None) or runtime
                endtime = kwargs["params"].get(f"{self.task_id}-force-endtime", None) or endtime
                _log.info(f"airflow-ha configuration -- runtime: {runtime}, endtime: {endtime}")

                # Handle retrigger couts
                maxretrigger = kwargs["params"].get(f"{self.task_id}-force-maxretrigger", None) or maxretrigger or -1
                current_retrigger = int(kwargs["dag_run"].conf.get(f"{self.task_id}-retrigger", 0))
                _log.info(f"airflow-ha configuration -- current_retrigger: {current_retrigger}, maxretrigger: {maxretrigger}")

                # Check if runtime has exceeded
                if runtime is not None and (datetime.now(tz=UTC) - dag_start_date).total_seconds() > runtime.total_seconds():
                    # Runtime has exceeded, end
                    _log.info(f"Runtime exceeded for {self.task_id}, stopping")
                    return branch_choices[(Result.PASS, Action.STOP)]

                # Check if endtime has passed
                if endtime is not None and datetime.now().time() > endtime:  # NOTE: don't use utc here, assume `endtime` matches airflow timezone
                    # Endtime has passed, end
                    _log.info(f"Endtime passed for {self.task_id}, stopping")
                    return branch_choices[(Result.PASS, Action.STOP)]

                # Check if maxretrigger has exceeded
                if maxretrigger > -1 and current_retrigger >= maxretrigger:
                    # maxretrigger has exceeded, end
                    _log.info(f"maxretrigger exceeded for {self.task_id}: {maxretrigger} / {current_retrigger}, stopping")
                    retrigger_exceeded = True
                else:
                    retrigger_exceeded = False

            # Otherwise, continue to evaluate
            check_program_result = task_instance.xcom_pull(key="return_value", task_ids=self.task_id)
            try:
                result = Result(check_program_result[0])
                action = Action(check_program_result[1])
                ret = branch_choices.get((result, action), branch_choices[(Result.PASS, Action.RETRIGGER)])
                if retrigger_exceeded:
                    ret[1] = Action.STOP
                _log.info(f"Sensor returned {result.name}, {action.name}, branching to {ret}")
            except (ValueError, IndexError, TypeError):
                # Sensor has failed, retrigger
                _log.warning("Sensor failed, pass/retrigger")
                ret = branch_choices[(Result.PASS, Action.RETRIGGER if not retrigger_exceeded else Action.STOP)]
            return ret

        self._decide_task = BranchPythonOperator(
            task_id=f"{self.task_id}-decide",
            python_callable=_choose_branch,
            provide_context=True,
        )

        self >> self._count_task >> self._decide_task
        self._decide_task >> self._stop_pass
        self._decide_task >> self._stop_fail
        self._decide_task >> self._retrigger_pass
        self._decide_task >> self._retrigger_fail >> self._fail

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
