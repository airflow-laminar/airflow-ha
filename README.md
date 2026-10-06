# airflow-ha

Monitor health with Apache Airflow and branch to stop or retrigger a DAG.

[![Build Status](https://github.com/airflow-laminar/airflow-ha/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/airflow-laminar/airflow-ha/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/airflow-laminar/airflow-ha/branch/main/graph/badge.svg)](https://codecov.io/gh/airflow-laminar/airflow-ha)
[![License](https://img.shields.io/github/license/airflow-laminar/airflow-ha)](https://github.com/airflow-laminar/airflow-ha)
[![PyPI](https://img.shields.io/pypi/v/airflow-ha.svg)](https://pypi.python.org/pypi/airflow-ha)

`HighAvailabilityOperator` runs a Python check returning `(Result, Action)`.
Health and recovery are separate: a failed result can retrigger the DAG while
its current run follows a failure branch. Time limits stop monitoring, and
`maxretrigger` bounds recovery attempts while preserving the health result.

## Documentation

- [Tutorial: run a DAG that retriggers itself](docs/src/tutorial.md) builds a countdown with three runs.
- [How-to guides](docs/src/how-to.md) cover service monitoring, alerts, Python and airflow-config, and stop boundaries.
- [API reference](docs/src/API.md) defines results, branches, limits, callbacks, task models, and run configuration.
- [Why health results and recovery actions are separate](docs/src/explanation.md) explains retries, retriggers, and workload ownership.

Published documentation is available at
[airflow-laminar.github.io/airflow-ha](https://airflow-laminar.github.io/airflow-ha/).

## Related integrations

[airflow-supervisor](https://github.com/airflow-laminar/airflow-supervisor) and
[airflow-nomad](https://github.com/airflow-laminar/airflow-nomad) use this sensor
inside external workload lifecycles and add log forwarding and diagnostics.
[airflow-pydantic](https://github.com/airflow-laminar/airflow-pydantic) provides
declarative task models; [airflow-config](https://github.com/airflow-laminar/airflow-config)
loads those models from YAML.

## License

Apache 2.0. See [LICENSE](LICENSE).
