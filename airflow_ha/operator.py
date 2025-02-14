from datetime import UTC, datetime, time, timedelta
from logging import getLogger
from typing import Any, Callable, Dict, Literal, Optional, Union

from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from .common import Action, CheckResult, Result
from .utils import fail_, pass_

__all__ = ("HighAvailabilityOperator",)
_log = getLogger(__name__)


class HighAvailabilityOperator(PythonSensor):
    _decide_task: BranchPythonOperator
    _fail: PythonOperator
    _retrigger_fail: TriggerDagRunOperator
    _retrigger_pass: TriggerDagRunOperator
    _stop_pass: PythonOperator
    _stop_fail: PythonOperator
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
        *,
        runtime: Optional[Union[int, timedelta]] = None,
        endtime: Optional[Union[str, time]] = None,
        maxretrigger: Optional[int] = None,
        start_date_or_logical_date: Literal["start", "logical"] = "logical",
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
        self._start_date_or_logical_date = (
            start_date_or_logical_date if start_date_or_logical_date.endswith("_date") else f"{start_date_or_logical_date}_date"
        )

        # These are kwarsg to pass to the trigger operators
        self._pass_trigger_kwargs = pass_trigger_kwargs or {}
        self._fail_trigger_kwargs = fail_trigger_kwargs or {}
        self._pass_trigger_kwargs_conf = self._pass_trigger_kwargs.pop("conf", {})
        self._fail_trigger_kwargs_conf = self._fail_trigger_kwargs.pop("conf", {})

        # Function to check end conditions
        check_end_conditions = (  # noqa: E731
            lambda task_id=kwargs.get("task_id"),
            runtime=self._runtime,
            endtime=self._endtime,
            maxretrigger=self._maxretrigger,
            start_date_or_logical_date=self._start_date_or_logical_date,
            **kwargs: _check_end_conditions(
                task_id=task_id,
                runtime=runtime,
                endtime=endtime,
                maxretrigger=maxretrigger,
                start_date_or_logical_date=start_date_or_logical_date,
                **kwargs,
            )
        )

        # Function to control the sensor
        callable_wrapper = lambda python_callable=python_callable, check_end_conditions=check_end_conditions, **kwargs: _callable_wrapper(  # noqa: E731
            python_callable=python_callable, check_end_conditions=check_end_conditions, **kwargs
        )

        if not kwargs.get("trigger_rule"):
            kwargs["trigger_rule"] = "none_failed"

        # Initialize the sensor
        super().__init__(python_callable=callable_wrapper, **kwargs)

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

        # this is needed to ensure the dag fails, since the
        # retrigger_fail step will pass (to ensure dag retriggers!)
        self._fail = PythonOperator(task_id=f"{self.task_id}-force-dag-fail", python_callable=fail_)

        self._stop_pass = PythonOperator(task_id=f"{self.task_id}-stop-pass", python_callable=pass_)
        self._stop_fail = PythonOperator(task_id=f"{self.task_id}-stop-fail", python_callable=fail_)

        # Update the retrigger counts in trigger kwargs
        retrigger_count_conf = f'''{{{{ (ti.dag_run.conf.get("{self.task_id}-retrigger", 0)|int) + 1 }}}}'''
        startdate_conf = f'''{{{{ ti.dag_run.conf.get("{self.task_id}-startdate", ti.dag_run.start_date.isoformat()) }}}}'''
        if isinstance(self._pass_trigger_kwargs_conf, dict):
            self._pass_trigger_kwargs_conf[f"{self.task_id}-retrigger"] = retrigger_count_conf
            self._pass_trigger_kwargs_conf[f"{self.task_id}-startdate"] = startdate_conf
        else:
            if not isinstance(self._pass_trigger_kwargs_conf, str) or not self._pass_trigger_kwargs_conf.strip().endswith("}"):
                raise ValueError("pass_trigger_kwargs must be a dict or a JSON string")
            self._pass_trigger_kwargs_conf = self._pass_trigger_kwargs_conf.strip()[:-1] + f', "{self.task_id}-retrigger": {retrigger_count_conf} }}'
            self._pass_trigger_kwargs_conf = self._pass_trigger_kwargs_conf.strip()[:-1] + f', "{self.task_id}-startdate": "{startdate_conf}" }}'
        if isinstance(self._fail_trigger_kwargs_conf, dict):
            self._fail_trigger_kwargs_conf[f"{self.task_id}-retrigger"] = retrigger_count_conf
            self._fail_trigger_kwargs_conf[f"{self.task_id}-startdate"] = startdate_conf
        else:
            if not isinstance(self._fail_trigger_kwargs_conf, str) or not self._fail_trigger_kwargs_conf.strip().endswith("}"):
                raise ValueError("fail_trigger_kwargs must be a dict or a JSON string")
            self._fail_trigger_kwargs_conf = self._fail_trigger_kwargs_conf.strip()[:-1] + f', "{self.task_id}-retrigger": {retrigger_count_conf} }}'
            self._fail_trigger_kwargs_conf = self._fail_trigger_kwargs_conf.strip()[:-1] + f', "{self.task_id}-startdate": "{startdate_conf}" }}'

        # Create the retrigger pass/fail operators
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

        # Create the branch operator
        branch_choices = {
            (Result.PASS, Action.RETRIGGER): self._retrigger_pass.task_id,
            (Result.PASS, Action.STOP): self._stop_pass.task_id,
            (Result.FAIL, Action.RETRIGGER): self._retrigger_fail.task_id,
            (Result.FAIL, Action.STOP): self._stop_fail.task_id,
        }

        choose_branch = lambda task_id=self.task_id, branch_choices=branch_choices, **kwargs: _choose_branch(  # noqa: E731
            task_id=task_id, branch_choices=branch_choices, **kwargs
        )

        self._decide_task = BranchPythonOperator(
            task_id=f"{self.task_id}-decide",
            python_callable=choose_branch,
            provide_context=True,
        )

        self >> self._decide_task
        self._decide_task >> self._stop_pass
        self._decide_task >> self._stop_fail
        self._decide_task >> self._retrigger_pass
        self._decide_task >> self._retrigger_fail >> self._fail

    @property
    def stop_fail(self) -> PythonOperator:
        return self._stop_fail

    @property
    def stop_pass(self) -> PythonOperator:
        return self._stop_pass

    @property
    def retrigger_fail(self) -> TriggerDagRunOperator:
        return self._retrigger_fail

    @property
    def retrigger_pass(self) -> TriggerDagRunOperator:
        return self._retrigger_pass


# Function to check end conditions
def _check_end_conditions(task_id, runtime, endtime, maxretrigger, start_date_or_logical_date, **kwargs):
    # Check if force run in dag run kwargs
    force_run_conf = kwargs["dag_run"].conf.get("airflow_ha_force_run", False)
    force_run_param = kwargs["params"].get(f"{task_id}-force-run", False)
    _log.info(f"airflow-ha configuration -- force_run (conf): {force_run_conf}, force_run (param): {force_run_param}")

    # Grab the dag start date
    dag_original_date = kwargs["dag_run"].conf.get(f"{task_id}-startdate")
    if dag_original_date is not None:
        dag_original_date = datetime.fromisoformat(dag_original_date)
    else:
        dag_original_date = getattr(kwargs["dag_run"], start_date_or_logical_date)

    if not force_run_conf and not force_run_param:
        runtime = kwargs["params"].get(f"{task_id}-force-runtime", None) or runtime
        endtime = kwargs["params"].get(f"{task_id}-force-endtime", None) or endtime

        # Check if runtime has exceeded
        # NOTE: start date will always be normalize to UTC by airflow
        elapsed_time = (datetime.now(tz=UTC) - dag_original_date).total_seconds()
        _log.info(
            f"airflow-ha configuration -- runtime: {runtime}, dag_original_date({start_date_or_logical_date}): {dag_original_date}, elapsed_time: {elapsed_time}"
        )
        if runtime is not None and elapsed_time > runtime.total_seconds():
            # Runtime has exceeded, end
            _log.info(f"runtime exceeded for {task_id}, stopping")
            return Result.PASS, Action.STOP

        # Check if endtime has passed
        if endtime is not None:
            # NOTE: we normalize to the DAG's timezone
            # and use the provided endtime for convenience
            dag_timezone = kwargs.get("dag").timezone
            endtime_as_datetime = datetime.combine(
                date=dag_original_date.astimezone(dag_timezone).date(),
                time=endtime,
                tzinfo=dag_timezone,
            ).astimezone(UTC)
            _log.info(
                f"airflow-ha configuration -- endtime: {endtime}, endtime_as_datetime: {endtime_as_datetime}, datetime.now(tz=UTC): {datetime.now(tz=UTC)}"
            )
            if endtime_as_datetime < datetime.now(tz=UTC):
                # Endtime has passed, end
                _log.info(f"endtime passed for {task_id}, stopping")
                return Result.PASS, Action.STOP

        # Handle retrigger couts
        maxretrigger = kwargs["params"].get(f"{task_id}-force-maxretrigger", None) or maxretrigger or -1
        current_retrigger = int(kwargs["dag_run"].conf.get(f"{task_id}-retrigger", 0))
        _log.info(f"airflow-ha configuration -- current_retrigger: {current_retrigger}, maxretrigger: {maxretrigger}")

        # Check if maxretrigger has exceeded
        if maxretrigger > -1 and current_retrigger >= maxretrigger:
            # maxretrigger has exceeded, end
            _log.info(f"maxretrigger exceeded for {task_id}: {maxretrigger} / {current_retrigger}, stopping")
            return None, Action.STOP


# Function to control the sensor
def _callable_wrapper(python_callable, check_end_conditions, **kwargs):
    task_instance = kwargs["task_instance"]
    ret: CheckResult = python_callable(**kwargs)

    if not isinstance(ret, tuple) or not len(ret) == 2 or not isinstance(ret[0], Result) or not isinstance(ret[1], Action):
        # malformed
        task_instance.xcom_push(key="return_value", value=(Result.FAIL, Action.STOP))
        return True

    # push to xcom
    task_instance.xcom_push(key="return_value", value=ret)

    end_conditions = check_end_conditions(**kwargs)
    if end_conditions is not None and end_conditions[1] == Action.STOP:
        # end conditions met
        _log.info(f"End conditions met, stopping: {end_conditions}")
        return True
    if ret[1] == Action.CONTINUE:
        # keep checking
        return False
    return True


def _choose_branch(branch_choices, task_id, **kwargs):
    # Grab the task instance
    task_instance = kwargs["task_instance"]

    # Check if force run in dag run kwargs
    end_conditions = _check_end_conditions(**kwargs)
    if end_conditions is not None and end_conditions[0] is not None:
        return branch_choices[end_conditions]

    if end_conditions is not None:
        retrigger_exceeded = True
    else:
        retrigger_exceeded = False

    # Otherwise, continue to evaluate
    check_program_result = task_instance.xcom_pull(key="return_value", task_ids=task_id)
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
