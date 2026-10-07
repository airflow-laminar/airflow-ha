---
myst:
  heading_anchors: 3
---

# API reference

The public API is exported from `airflow_ha`. `HighAvailabilityOperator` is an
alias of `HighAvailabilitySensor`, which extends Airflow's PythonSensor.

## Health results and actions

`CheckResult` is `tuple[Result, Action]`. A check callable returns a tuple
containing members of both enums.

| Result | Value    | Meaning            |
| ------ | -------- | ------------------ |
| `PASS` | `"pass"` | Healthy outcome.   |
| `FAIL` | `"fail"` | Unhealthy outcome. |

| Action      | Value         | Meaning                         |
| ----------- | ------------- | ------------------------------- |
| `CONTINUE`  | `"continue"`  | Continue sensor polling.        |
| `RETRIGGER` | `"retrigger"` | Trigger another run of the DAG. |
| `STOP`      | `"stop"`      | Finish this monitoring run.     |

The callable receives Airflow's Python callable arguments and task context.
A malformed return value becomes `FAIL/STOP`. A callable exception stores
`FAIL/RETRIGGER` in the sensor's `return_value` XCom and propagates the exception.

## HighAvailabilityOperator

```text
HighAvailabilityOperator(
    python_callable,
    pass_trigger_kwargs=None,
    fail_trigger_kwargs=None,
    *,
    runtime=None,
    endtime=None,
    maxretrigger=None,
    reference_date="data_interval_end",
    **kwargs,
)
```

`kwargs` contains Airflow PythonSensor arguments, including `task_id`, `dag`,
`poke_interval`, `timeout`, `mode`, `op_args`, `op_kwargs`, retries, and callbacks.
The sensor's trigger rule defaults to `none_failed`.

### Limits and reference dates

| Argument         | Default             | Accepted values and behavior                                                             |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------- |
| `runtime`        | `None`              | Integer seconds or `timedelta`; elapsed time greater than the limit selects `PASS/STOP`. |
| `endtime`        | `None`              | ISO time string or `datetime.time`; reaching the cutoff selects `PASS/STOP`.             |
| `maxretrigger`   | `None`              | Positive integer; reaching the count changes the action to `STOP`, preserving health.    |
| `reference_date` | `data_interval_end` | DagRun attribute: `start_date`, `logical_date`, or `data_interval_end`.                  |

`None`, zero, and negative `maxretrigger` values leave the retrigger count
unbounded. Limits are evaluated in order: runtime, endtime, then retrigger
count. A reached time limit takes precedence over a failed health result.

The first check uses the selected DagRun reference attribute. A retrigger writes
the original run's `start_date` to `<task_id>-referencedate` in the next run's
configuration; subsequent retriggers preserve that timestamp. A supplied
`<task_id>-referencedate` ISO datetime overrides the selected attribute.
`endtime` is combined with the reference date's calendar day in the DAG's
timezone. It is not a rolling cutoff on the current day.

### Generated tasks and branching

An operator named `health` creates seven tasks:

| Task                    | Behavior                                                |
| ----------------------- | ------------------------------------------------------- |
| `health`                | Runs the check callable as a sensor.                    |
| `health-decide`         | Selects an outcome branch; trigger rule `none_skipped`. |
| `health-retrigger-pass` | Creates another DAG run after a healthy result.         |
| `health-retrigger-fail` | Creates another DAG run after an unhealthy result.      |
| `health-force-dag-fail` | Raises a failure after the failed retrigger succeeds.   |
| `health-stop-pass`      | Completes successfully without retriggering.            |
| `health-stop-fail`      | Raises a failure without retriggering.                  |

| Stored result and action | Selected branch  |
| ------------------------ | ---------------- |
| `PASS/RETRIGGER`         | `retrigger_pass` |
| `PASS/STOP`              | `stop_pass`      |
| `FAIL/RETRIGGER`         | `retrigger_fail` |
| `FAIL/STOP`              | `stop_fail`      |

`CONTINUE` keeps the sensor polling. If the sensor then times out, the decide
task changes `CONTINUE` to `RETRIGGER`, preserving its last health result.
Missing or malformed stored XCom results become `FAIL/RETRIGGER`. Exhausting
`maxretrigger` selects the stop branch for that result. Time limits instead
select `stop_pass`.

### Branch properties

`decide_task`, `stop_fail`, `stop_pass`, `retrigger_fail`, and `retrigger_pass`
return the corresponding generated operators. `check_end_conditions` returns
the configured limit-check callable. `get_retrigger_count(**context)` reads the
integer count from DagRun configuration. `is_initial_run(**context)` returns
whether that count is zero.

### Retrigger configuration

`pass_trigger_kwargs` and `fail_trigger_kwargs` supply keyword arguments to
their TriggerDagRunOperator. `trigger_dag_id` defaults to the current DAG ID;
`trigger_rule` defaults to `one_success`. Each mapping's `conf` accepts a
dictionary or a templated JSON-object string.

The integration adds `<task_id>-retrigger` with the incremented count and
`<task_id>-referencedate` with the retained timestamp. These keys override
matching user values. The constructor removes `conf` from supplied kwargs
mappings and modifies dictionary configuration values in place.

### Callbacks

Explicit `on_failure_callback`, `on_retry_callback`, `on_execute_callback`,
`on_success_callback`, and `on_skipped_callback` arguments apply to the sensor
and generated branch tasks. Omitted arguments use each task's inherited DAG
`default_args`. Callbacks supplied in `pass_trigger_kwargs` or
`fail_trigger_kwargs` override the corresponding retrigger task's callback.

Callbacks receive the executing task's context. A failure result can complete
the sensor successfully before a generated failure task raises its exception.
Several tasks can invoke a callback in one run; a success callback does not
imply success of the complete DAG. Callback support also depends on the
installed Airflow version and its task-state handling.

### DAG parameters and run configuration

| Parameter                      | Default | Current behavior                                     |
| ------------------------------ | ------- | ---------------------------------------------------- |
| `<task_id>-force-run`          | `False` | Bypasses runtime, endtime, and count limits.         |
| `<task_id>-force-runtime`      | `None`  | Overrides runtime with integer seconds.              |
| `<task_id>-force-endtime`      | `None`  | Overrides endtime with an ISO time string.           |
| `<task_id>-force-maxretrigger` | `None`  | Overrides the count limit when positive.             |
| `<task_id>-force-retrigger`    | `None`  | Registered but not read by current branch selection. |

`dag_run.conf["airflow_ha_force_run"]` also bypasses all three limits.
Parameter values are read from task context `params`; Airflow's configuration
controls whether run configuration overrides those values.

Runtime override integers are normalized to `timedelta`; endtime override strings
are normalized to `datetime.time`. A null override leaves the configured limit
in effect. A runtime override of zero applies a zero-second budget. Both sensor
polling and branch selection use the sensor task ID to look up these parameters.

## Declarative task models

`HighAvailabilityTaskArgs` extends `airflow_pydantic.PythonSensorArgs`.
`HighAvailabilityTask` adds the declarative task interface and defaults its
operator to `airflow_ha.HighAvailabilitySensor`.
`HighAvailabilitySensorTaskArgs` and `HighAvailabilitySensorTask` are aliases.

The model schema defaults `runtime` to `120` seconds, `maxretrigger` to `2`,
and `reference_date` to `data_interval_end`. Both trigger kwargs and `endtime`
default to `None`. Only explicitly set fields are forwarded when tasks are
instantiated or rendered; omitted limits use the native operator's defaults.

See the [how-to guides](how-to.md) for Python and `airflow-config` examples.

## Generated API

```{eval-rst}
.. currentmodule:: airflow_ha

.. autosummary::
   :toctree: _build

   Result
   Action
   CheckResult
   HighAvailabilitySensor
   HighAvailabilityOperator
   HighAvailabilityTaskArgs
   HighAvailabilityTask
   HighAvailabilitySensorTaskArgs
   HighAvailabilitySensorTask
```
