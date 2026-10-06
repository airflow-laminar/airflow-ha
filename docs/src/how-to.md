---
myst:
  heading_anchors: 3
---

# How-to guides

Use these guides to monitor a service, configure recovery and alerts, or define
the same workflow through `airflow-config`. For a first exercise, use the
[countdown tutorial](tutorial.md).

## How to monitor a service and alert on failure

Install `airflow-ha[airflow]` for Airflow 2 or `airflow-ha[airflow3]` for Airflow 3.
Save `availability_checks.py` in the DAG folder, where both the parser and
workers can import it:

```python
from urllib.error import URLError
from urllib.request import urlopen

from airflow_ha import Action, Result


def check_service(url, **context):
    try:
        with urlopen(url, timeout=10) as response:
            if response.status == 200:
                return Result.PASS, Action.CONTINUE
    except URLError as error:
        context["task"].log.warning("Health request failed: %s", error)
    return Result.FAIL, Action.RETRIGGER


def report_failure(context):
    context["task"].log.error("Availability task failed: %s", context.get("exception"))
```

Replace the callback body with your existing alert transport. Configure the
health endpoint and save this DAG beside the module:

```python
from datetime import datetime, timezone

from airflow import DAG
from airflow_ha import HighAvailabilityOperator

from availability_checks import check_service, report_failure

with DAG(
    dag_id="service-health",
    schedule="@daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
) as dag:
    health = HighAvailabilityOperator(
        task_id="health",
        python_callable=check_service,
        op_kwargs={"url": "https://service.example.com/health"},
        poke_interval=30,
        timeout=300,
        runtime=7200,
        reference_date="start_date",
        maxretrigger=3,
        retries=0,
        on_failure_callback=report_failure,
    )
```

Grant the workers network access to the endpoint. Inspect `health` for request
diagnostics, and the selected failure branch for its task exception. Use
`maxretrigger` to bound the recovery chain. Configure task `retries` separately
if a failed individual check should be retried before branching.

Callbacks apply to the generated tasks as well as the sensor. Make the alert
handler tolerate several task failures from one DAG run. See the
[callback reference](API.md#callbacks) for overrides and context.

## How to define the same check with airflow-config

Install `airflow-config` alongside the integration. Use the same
`availability_checks.py` module from the Python guide. Create
`config/service_health.yaml` beside the DAG loader:

```yaml
# @package _global_
_target_: airflow_config.Configuration
_convert_: all

dags:
  service-health:
    schedule: "@daily"
    start_date: "2025-01-01"
    catchup: false
    max_active_runs: 1
    tasks:
      health:
        _target_: airflow_ha.HighAvailabilityTask
        python_callable: availability_checks.check_service
        op_kwargs:
          url: https://service.example.com/health
        poke_interval: 30
        timeout: 300
        runtime: 7200
        reference_date: start_date
        maxretrigger: 3
        retries: 0
        on_failure_callback: availability_checks.report_failure
```

Save `service_health.py` in the DAG folder:

```python
"""Generate Airflow DAGs for service health checks."""

from airflow_config import load_config

load_config("config", "service_health").generate_in_mem()
```

Deploy either the Python DAG or this loader for the `service-health` DAG ID.
Run `airflow tasks list service-health` to confirm the seven generated tasks.

## How to stop monitoring after a time budget

Set `runtime=7200` or `runtime=timedelta(hours=2)` and
`reference_date="start_date"` on the operator. In YAML, use `runtime: 7200`
and `reference_date: start_date`.

For a wall-clock cutoff, set `endtime="18:00:00"` in Python or
`endtime: "18:00:00"` in YAML. The cutoff uses the DAG's timezone and the
reference date's day. Use a schedule that starts within that day's intended
monitoring window.

Both limits select the successful stop branch. Use the callable's
`Result.FAIL, Action.STOP` result when the final outcome must be failure.
See the [reference](API.md#limits-and-reference-dates) for precedence and
the reference retained across retriggers.

## How to run work only after monitoring stops

Attach a cleanup task to the successful stop boundary:

```python
health.stop_pass >> cleanup
```

For a task that must follow a failed stop, set its Airflow trigger rule to
`all_failed` and attach it to `health.stop_fail`. Attach preparation tasks with
`prepare >> health`. A retrigger branch schedules another DAG run; tasks
attached to that branch execute in the current run.

Use [airflow-supervisor](https://airflow-laminar.github.io/airflow-supervisor/)
or [airflow-nomad](https://airflow-laminar.github.io/airflow-nomad/) when recovery
must also restart an external workload. Their observability guides cover log
forwarding, diagnostics, and separate watchdog DAGs.
