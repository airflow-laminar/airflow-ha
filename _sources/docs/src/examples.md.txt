# Examples

This page provides comprehensive examples of using `airflow-ha` for various use cases.

## Always-On Service Monitoring

Monitor a service continuously and handle failures:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action
import requests

def check_api_health(**kwargs):
    """Check if the API is responding."""
    try:
        response = requests.get("http://my-api.example.com/health", timeout=10)
        if response.status_code == 200:
            return (Result.PASS, Action.CONTINUE)
        else:
            return (Result.FAIL, Action.RETRIGGER)
    except requests.RequestException:
        return (Result.FAIL, Action.RETRIGGER)

with DAG(
    dag_id="api-health-monitor",
    description="Monitor API health continuously",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    pre = PythonOperator(
        task_id="setup",
        python_callable=lambda: print("Starting health monitoring"),
    )
    
    ha = HighAvailabilityOperator(
        task_id="health-check",
        timeout=3600,  # 1 hour timeout per check cycle
        poke_interval=30,  # Check every 30 seconds
        python_callable=check_api_health,
    )
    
    # Handle successful stop
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=lambda: print("Monitoring stopped"),
    )
    
    pre >> ha
    ha.stop_pass >> cleanup
```

## AWS MWAA Compatible DAG

AWS Managed Workflows for Apache Airflow (MWAA) has a 12-hour maximum DAG runtime. Use `runtime` limiter to automatically retrigger before hitting this limit:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow_ha import HighAvailabilityOperator, Result, Action

def long_running_check(**kwargs):
    """Check for a long-running process."""
    # Your check logic here
    process_complete = False  # Replace with actual check
    
    if process_complete:
        return (Result.PASS, Action.STOP)
    return (Result.PASS, Action.CONTINUE)

with DAG(
    dag_id="mwaa-compatible-dag",
    description="Long-running DAG compatible with MWAA 12-hour limit",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="long-running-task",
        timeout=3600,
        poke_interval=60,
        python_callable=long_running_check,
        # Retrigger before hitting MWAA's 12-hour limit
        runtime=timedelta(hours=11),  # Stop after 11 hours
    )
```

## Time-Based Scheduling

Run a process during business hours only:

```python
from datetime import datetime, timedelta, time
from airflow import DAG
from airflow_ha import HighAvailabilityOperator, Result, Action

def business_hours_process(**kwargs):
    """Process that should only run during business hours."""
    # Your processing logic
    return (Result.PASS, Action.CONTINUE)

with DAG(
    dag_id="business-hours-dag",
    description="Run only during business hours",
    schedule="0 9 * * 1-5",  # Start at 9 AM on weekdays
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="business-process",
        timeout=3600,
        poke_interval=300,  # Check every 5 minutes
        python_callable=business_hours_process,
        # Stop at 6 PM
        endtime=time(18, 0),
    )
```

## Recursive/Countdown DAG

Build DAGs that trigger themselves with state:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action

with DAG(
    dag_id="countdown-dag",
    description="Recursive countdown DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    def _get_count(**kwargs):
        """Get current count from DAG config, default to 5."""
        return kwargs['dag_run'].conf.get('counter', 5) - 1

    get_count = PythonOperator(
        task_id="get-count",
        python_callable=_get_count,
    )

    def _keep_counting(**kwargs):
        """Decide whether to continue counting."""
        count = kwargs["task_instance"].xcom_pull(
            key="return_value", 
            task_ids="get-count"
        )
        if count > 0:
            return (Result.PASS, Action.RETRIGGER)
        elif count == 0:
            return (Result.PASS, Action.STOP)
        else:
            return (Result.FAIL, Action.STOP)

    keep_counting = HighAvailabilityOperator(
        task_id="countdown",
        timeout=30,
        poke_interval=5,
        python_callable=_keep_counting,
        # Pass the new counter value to the retriggered DAG
        pass_trigger_kwargs={
            "conf": '''{"counter": {{ ti.xcom_pull(key="return_value", task_ids="get-count") }}}'''
        },
    )

    get_count >> keep_counting
```

## Max Retrigger Limit

Limit the number of times a DAG can retrigger:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow_ha import HighAvailabilityOperator, Result, Action

def check_with_retries(**kwargs):
    """A check that might need multiple retries."""
    # Your logic here
    success = False  # Replace with actual check
    
    if success:
        return (Result.PASS, Action.STOP)
    return (Result.FAIL, Action.RETRIGGER)

with DAG(
    dag_id="limited-retries-dag",
    description="DAG with max retrigger limit",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="retry-limited-task",
        timeout=300,
        poke_interval=30,
        python_callable=check_with_retries,
        # Give up after 5 retriggers
        maxretrigger=5,
    )
```

## Full Branching Example

Handle all possible outcomes with custom tasks:

```python
from datetime import datetime, timedelta
from random import choice
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action, fail

def random_outcome(**kwargs):
    """Randomly choose an outcome for demonstration."""
    return choice([
        (Result.PASS, Action.CONTINUE),
        (Result.PASS, Action.RETRIGGER),
        (Result.PASS, Action.STOP),
        (Result.FAIL, Action.CONTINUE),
        (Result.FAIL, Action.RETRIGGER),
        (Result.FAIL, Action.STOP),
    ])

with DAG(
    dag_id="full-branching-dag",
    description="Demonstrate all HA branches",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # Pre-task
    pre = PythonOperator(
        task_id="pre",
        python_callable=lambda: print("Starting"),
    )
    
    # HA Operator
    ha = HighAvailabilityOperator(
        task_id="ha",
        timeout=30,
        poke_interval=5,
        python_callable=random_outcome,
    )
    
    pre >> ha
    
    # Handle retrigger after pass
    retrigger_pass = PythonOperator(
        task_id="retrigger-pass",
        python_callable=lambda: print("Retriggering after success"),
    )
    ha.retrigger_pass >> retrigger_pass
    
    # Handle retrigger after fail
    retrigger_fail = PythonOperator(
        task_id="retrigger-fail",
        python_callable=lambda: print("Retriggering after failure"),
    )
    ha.retrigger_fail >> retrigger_fail
    
    # Handle stop after pass
    stop_pass = PythonOperator(
        task_id="stop-pass",
        python_callable=lambda: print("Stopping after success"),
    )
    ha.stop_pass >> stop_pass
    
    # Handle stop after fail
    stop_fail = PythonOperator(
        task_id="stop-fail",
        python_callable=fail,
        trigger_rule="all_failed",
    )
    ha.stop_fail >> stop_fail
```

## Integration with airflow-supervisor

`airflow-ha` is used by [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) to monitor long-running supervisor processes:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow_supervisor import (
    SupervisorAirflowConfiguration,
    Supervisor,
    ProgramConfiguration,
)

# The Supervisor internally uses HighAvailabilityOperator
# to monitor and restart supervised programs
cfg = SupervisorAirflowConfiguration(
    port=9001,
    working_dir="/data/supervisor",
    # These are passed to HighAvailabilityOperator
    check_interval=timedelta(seconds=30),
    check_timeout=timedelta(hours=8),
    runtime=timedelta(hours=11),  # MWAA compatible
    program={
        "my-service": ProgramConfiguration(
            command="python my_service.py",
        ),
    },
)

with DAG(
    dag_id="supervisor-with-ha",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    supervisor = Supervisor(dag=dag, cfg=cfg)
```

## Passing State Between Retriggers

Pass configuration data to retriggered DAG runs:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator, Result, Action

def process_batch(**kwargs):
    """Process a batch and track progress."""
    conf = kwargs['dag_run'].conf
    current_batch = conf.get('batch_id', 0)
    total_batches = conf.get('total_batches', 10)
    
    # Process the batch
    print(f"Processing batch {current_batch + 1} of {total_batches}")
    
    if current_batch + 1 >= total_batches:
        return (Result.PASS, Action.STOP)
    return (Result.PASS, Action.RETRIGGER)

with DAG(
    dag_id="batch-processing-dag",
    description="Process batches across retriggers",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    def _get_next_batch(**kwargs):
        conf = kwargs['dag_run'].conf
        return conf.get('batch_id', 0) + 1

    get_batch = PythonOperator(
        task_id="get-next-batch",
        python_callable=_get_next_batch,
    )

    ha = HighAvailabilityOperator(
        task_id="process-batch",
        timeout=300,
        poke_interval=10,
        python_callable=process_batch,
        pass_trigger_kwargs={
            "conf": '''{"batch_id": {{ ti.xcom_pull(key="return_value", task_ids="get-next-batch") }}, "total_batches": 10}'''
        },
    )
    
    get_batch >> ha
```

## Using with airflow-config (YAML-Driven)

Define HA DAGs declaratively with [airflow-config](https://github.com/airflow-laminar/airflow-config):

```yaml
# config/ha/monitoring.yaml
_target_: airflow_config.Configuration

default_args:
  _target_: airflow_config.DefaultArgs
  retries: 0
  depends_on_past: false

all_dags:
  _target_: airflow_config.DagArgs
  start_date: "2024-01-01"
  catchup: false
  schedule: "0 0 * * *"

dags:
  monitoring-dag:
    tasks:
      health-check:
        _target_: airflow_ha.HighAvailabilityTask
        task_id: health-check
        timeout: 3600
        poke_interval: 60
        runtime: 43200  # 12 hours in seconds
        python_callable: my_module.check_health
```

```python
# dags/monitoring.py
from airflow_config import load_config, DAG

config = load_config("config/ha", "monitoring")

with DAG(dag_id="monitoring-dag", config=config) as dag:
    # Tasks are automatically created from config
    pass
```

## Combining Multiple Limiters

Use multiple limiters together for fine-grained control:

```python
from datetime import datetime, timedelta, time
from airflow import DAG
from airflow_ha import HighAvailabilityOperator, Result, Action

def my_check(**kwargs):
    return (Result.PASS, Action.CONTINUE)

with DAG(
    dag_id="multi-limiter-dag",
    description="DAG with multiple limiters",
    schedule="0 6 * * *",  # Start at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ha = HighAvailabilityOperator(
        task_id="multi-limited",
        timeout=3600,
        poke_interval=60,
        python_callable=my_check,
        # Multiple limiters - first one triggered wins
        runtime=timedelta(hours=11),  # Max 11 hours
        endtime=time(20, 0),  # Stop by 8 PM
        maxretrigger=24,  # Max 24 retriggers
    )
```

## Integration Notes

### airflow-supervisor

`airflow-ha` is a core dependency of [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor), which uses it to:

- Monitor supervisor process health
- Automatically restart failed processes
- Handle graceful shutdown and cleanup

### AWS MWAA

For AWS Managed Workflows for Apache Airflow:

- Use `runtime=timedelta(hours=11)` to stay under the 12-hour limit
- DAGs will automatically retrigger before timing out
- State is preserved across retriggers via `pass_trigger_kwargs`

### airflow-pydantic

`airflow-ha` is built on [airflow-pydantic](https://github.com/airflow-laminar/airflow-pydantic), which provides:

- Pydantic models for Airflow constructs
- Operator wrappers for serialization
- Integration with airflow-config for YAML-driven DAGs
