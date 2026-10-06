# Tutorial: run a DAG that retriggers itself

Create a countdown DAG that starts at two, retriggers twice, and then stops.
Use a working local Airflow 3 development environment with its scheduler and
worker running. Activate that environment before installing the integration.

## Install the package

```bash
pip install 'airflow-ha[airflow3]'
```

## Create the countdown DAG

Save `ha_countdown.py` in your Airflow DAG folder:

```python
from datetime import datetime, timezone

from airflow import DAG
from airflow_ha import Action, HighAvailabilityOperator, Result


def count_down(**context):
    remaining = int(context["dag_run"].conf.get("remaining", 2))
    context["task"].log.info("Remaining: %s", remaining)
    if remaining > 0:
        return Result.PASS, Action.RETRIGGER
    return Result.PASS, Action.STOP


with DAG(
    dag_id="ha-countdown",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
) as dag:
    countdown = HighAvailabilityOperator(
        task_id="countdown",
        python_callable=count_down,
        pass_trigger_kwargs={
            "conf": {"remaining": "{{ (dag_run.conf.get('remaining', 2)|int) - 1 }}"},
        },
        reference_date="start_date",
        maxretrigger=2,
        poke_interval=1,
        timeout=30,
        retries=0,
    )
```

Wait for the scheduler to discover the DAG, then inspect its tasks:

```bash
airflow tasks list ha-countdown
```

The output includes `countdown`, `countdown-decide`, and five branch tasks.

## Trigger the countdown

Unpause the DAG and start one run:

```bash
airflow dags unpause ha-countdown
airflow dags trigger ha-countdown
```

Open the DAG in Airflow's UI. Its first `countdown` log contains `Remaining: 2`.
The `countdown-retrigger-pass` task creates another run. The next two logs
contain `Remaining: 1` and `Remaining: 0`.

In the final run, `countdown-stop-pass` succeeds and both retrigger branches
are skipped. There are three successful runs, with no fourth run scheduled.
Trigger the DAG again to repeat the countdown from two.

For service monitoring and failure callbacks, continue with the
[how-to guides](how-to.md).
