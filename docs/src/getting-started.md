# Getting Started

## Overview

`airflow-ha` provides a **High Availability (HA) DAG utility** for Apache Airflow that enables long-running or "always-on" tasks. It's designed to work around Airflow's traditional task execution model, which assumes tasks complete within a reasonable timeframe.

**Key Benefits:**

- **Always-On DAGs**: Build DAGs that continuously run and monitor themselves
- **Self-Retriggering**: Automatically retrigger DAGs when tasks complete or fail
- **Graceful Shutdown**: Configure runtime limits, end times, and max retrigger counts
- **Flexible Outcomes**: Control DAG behavior based on check results (PASS/FAIL, CONTINUE/RETRIGGER/STOP)
- **AWS MWAA Compatible**: Perfect for AWS Managed Workflows for Apache Airflow (MWAA) where DAGs have a maximum runtime of 12 hours

> [!NOTE]
> This library is used by [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) to build DAGs that manage supervisor processes with fault tolerance and automatic recovery.

> [!IMPORTANT]
> **AWS MWAA Users**: AWS Managed Workflows for Apache Airflow imposes a 12-hour maximum DAG runtime limit. `airflow-ha` provides a clean solution by automatically retriggering DAGs before they hit this limit, allowing you to run continuous workloads on MWAA.

## Installation

Install airflow-ha from PyPI:

```bash
pip install airflow-ha
```

For use with Apache Airflow 2.x:

```bash
pip install airflow-ha[airflow]
```

For use with Apache Airflow 3.x:

```bash
pip install airflow-ha[airflow3]
```

## Basic Concepts

### The HighAvailabilityOperator

The core component is `HighAvailabilityOperator`, which inherits from `PythonSensor`. It runs a user-provided `python_callable` and takes action based on the return value:

| Return              | Result                                       | Current DAGrun End State |
| :------------------ | :------------------------------------------- | :----------------------- |
| `(PASS, RETRIGGER)` | Retrigger the same DAG to run again          | `pass`                   |
| `(PASS, STOP)`      | Finish the DAG, until its next scheduled run | `pass`                   |
| `(FAIL, RETRIGGER)` | Retrigger the same DAG to run again          | `fail`                   |
| `(FAIL, STOP)`      | Finish the DAG, until its next scheduled run | `fail`                   |
| `(*, CONTINUE)`     | Continue to run the Sensor                   | N/A                      |

> [!NOTE]
> If the sensor times out, the behavior matches `(Result.PASS, Action.RETRIGGER)`.

### Result and Action Enums

```python
from airflow_ha import Result, Action

# Result indicates success or failure
Result.PASS  # Task succeeded
Result.FAIL  # Task failed

# Action controls what happens next
Action.CONTINUE   # Keep running the sensor
Action.RETRIGGER  # Trigger a new DAG run
Action.STOP       # Stop and wait for next scheduled run
```

## Basic Usage

### Simple Always-On DAG

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action

def check_service_health(**kwargs):
    """Check if the service is healthy."""
    # Your health check logic here
    is_healthy = True  # Replace with actual check
    
    if is_healthy:
        return (Result.PASS, Action.CONTINUE)
    else:
        return (Result.FAIL, Action.RETRIGGER)

with DAG(
    dag_id="always-on-service",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="health-check",
        timeout=3600,  # 1 hour timeout
        poke_interval=60,  # Check every minute
        python_callable=check_service_health,
    )
```

### Using Limiters

Configure automatic shutdown with limiters:

```python
from datetime import datetime, timedelta, time
from airflow import DAG
from airflow_ha import HighAvailabilityOperator, Result, Action

def my_check(**kwargs):
    return (Result.PASS, Action.CONTINUE)

with DAG(
    dag_id="limited-ha-dag",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="ha-task",
        python_callable=my_check,
        poke_interval=60,
        # Limiters
        runtime=timedelta(hours=8),  # Max 8 hours runtime
        endtime=time(18, 0),  # Stop at 6 PM
        maxretrigger=10,  # Max 10 retriggers
    )
```

### Connecting to Downstream Tasks

The `HighAvailabilityOperator` exposes branches for different outcomes:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action

with DAG(
    dag_id="ha-with-branches",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="ha",
        timeout=300,
        poke_interval=30,
        python_callable=lambda **kwargs: (Result.PASS, Action.STOP),
    )
    
    # Connect to different outcome branches
    on_pass_retrigger = PythonOperator(
        task_id="on-pass-retrigger",
        python_callable=lambda: print("Retriggering after success"),
    )
    ha.retrigger_pass >> on_pass_retrigger
    
    on_fail_retrigger = PythonOperator(
        task_id="on-fail-retrigger", 
        python_callable=lambda: print("Retriggering after failure"),
    )
    ha.retrigger_fail >> on_fail_retrigger
    
    on_pass_stop = PythonOperator(
        task_id="on-pass-stop",
        python_callable=lambda: print("Stopping after success"),
    )
    ha.stop_pass >> on_pass_stop
    
    on_fail_stop = PythonOperator(
        task_id="on-fail-stop",
        python_callable=lambda: print("Stopping after failure"),
        trigger_rule="all_failed",
    )
    ha.stop_fail >> on_fail_stop
```

## Configuration Options

### HighAvailabilityOperator Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `python_callable` | Callable | Function that returns `(Result, Action)` tuple |
| `timeout` | int | Sensor timeout in seconds |
| `poke_interval` | int | Seconds between check calls |
| `runtime` | timedelta/int | Maximum runtime before auto-stop |
| `endtime` | time/str | Time of day to stop |
| `maxretrigger` | int | Maximum number of retriggers |
| `reference_date` | str | Date reference: `start_date`, `logical_date`, or `data_interval_end` |
| `pass_trigger_kwargs` | dict | Kwargs passed to retrigger on PASS |
| `fail_trigger_kwargs` | dict | Kwargs passed to retrigger on FAIL |

### DAG Params (Runtime Overrides)

The operator automatically adds DAG params for runtime overrides:

- `{task_id}-force-run`: Ignore all limiters
- `{task_id}-force-runtime`: Override runtime limit
- `{task_id}-force-endtime`: Override end time
- `{task_id}-force-maxretrigger`: Override max retrigger count

## Next Steps

- See [Examples](examples.md) for more detailed use cases
- Consult the [API Reference](API.md) for complete API documentation
- Check out [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) which uses this library for supervisor process management
