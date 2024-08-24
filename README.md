# airflow-ha

High Availability (HA) DAG Utility

[![Build Status](https://github.com/airflow-laminar/airflow-ha/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-ha/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-ha/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-ha)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-ha)](https://github.com/airflow-laminar/airflow-ha)
[![PyPI](https://img.shields.io/pypi/v/airflow-ha.svg)](https://pypi.python.org/pypi/airflow-ha)

## Overview

This library provides an operator called `HighAvailabilityOperator`, which inherits from `PythonSensor` and does the following:

- runs a user-provided `python_callable` as a sensor
  - if this returns `"done"`, mark the DAG as passed and finish
  - if this returns `"running"`, keep checking
  - if this returns `"failed"`, mark the DAG as failed and re-run
- if the sensor times out, mark the DAG as passed and re-run

Consider the following DAG:

```python
from datetime import datetime, timedelta
from random import choice

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_ha import HighAvailabilityOperator


with DAG(
    dag_id="test-high-availability",
    description="Test HA Operator",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    ha = HighAvailabilityOperator(
        task_id="ha",
        timeout=30,
        poke_interval=5,
        python_callable=lambda **kwargs: choice(("done", "failed", "running", ""))
    )
    
    pre = PythonOperator(task_id="pre", python_callable=lambda **kwargs: "test")
    pre >> ha
    
    fail = PythonOperator(task_id="fail", python_callable=lambda **kwargs: "test")
    ha.failed >> fail
    
    passed = PythonOperator(task_id="passed", python_callable=lambda **kwargs: "test")
    ha.passed >> passed

    done = PythonOperator(task_id="done", python_callable=lambda **kwargs: "test")
    ha.done >> done
```

This produces a DAG with the following topology:

<img src="https://raw.githubusercontent.com/airflow-laminar/airflow-ha/main/docs/src/top.png" />

This DAG exhibits cool behavior.
If a check fails or the interval elapses, the DAG will re-trigger itself.
If the check passes, the DAG will finish.
This allows the one to build "always-on" DAGs without having individual long blocking tasks.

This library is used to build [airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor), which uses [supervisor](http://supervisord.org) as a process-monitor while checking and restarting jobs via `airflow-ha`.

## License

This software is licensed under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
