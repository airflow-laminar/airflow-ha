# Why health results and recovery actions are separate

A workload can be unhealthy while recovery should continue. It can also be
healthy when a monitoring window has ended. `airflow-ha` represents those
decisions separately: `Result` describes health and `Action` controls the next
step. This lets a failed DAG run start a recovery run while still reporting
its failure to Airflow.

## Task retries and DAG retriggers have different scopes

An Airflow task retry repeats the sensor task in the same DAG run. A retrigger
creates another run of the DAG, so preparation and recovery tasks can execute
again. `maxretrigger` counts the latter. Workload-manager restart policies,
such as Supervisor autorestart or Nomad rescheduling, are a third recovery
mechanism with their own limits.

The failed retrigger task must succeed to schedule the next run. A separate
failure task then marks the failure path. When the retrigger budget is
exhausted, the stop branch preserves the last health result, so exhaustion
does not turn an unhealthy workload into a successful outcome.

## A monitoring deadline is a successful stop condition

`runtime` and `endtime` mean that the configured monitoring window is over.
They select `PASS/STOP`, including when the latest health result was failure.
They do not restart the monitoring DAG. Sensor timeout has different behavior:
the decide task uses the last health result and selects a retrigger unless the
retrigger budget has been exhausted.

Retriggers carry a reference timestamp forward. Consequently, a runtime
budget can cover the whole chain rather than restarting its clock on each
new DAG run. The [reference](API.md) defines the timestamp selection and
limiter precedence.

## Monitoring and workload ownership are separate

`airflow-ha` runs a callable and coordinates Airflow branches. It does not own
an external process, restart it, or collect its output. Supervisor and Nomad
integrations add those operations around the health sensor. A standalone
watchdog can instead check an existing service without changing its lifecycle.

Callbacks observe Airflow task state transitions. A check returning a failure
result can still complete its sensor successfully so branching can occur;
the generated failure task supplies the corresponding failure event. This
also means callbacks receive the context of the task that is executing,
rather than one shared context for the entire lifecycle.

Cron conversion assigns schedule and process ownership to ordinary Airflow
tasks. Their BashOperator logs and failure callbacks provide observability
without a separate external-manager polling loop.
