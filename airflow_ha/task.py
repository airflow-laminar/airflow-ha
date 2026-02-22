from airflow_pydantic import ImportPath, PythonSensorArgs, Task
from pydantic import Field, field_validator

from .common import (
    Endtime,
    FailTriggerKwargs,
    MaxRetrigger,
    PassTriggerKwargs,
    ReferenceDate,
    Runtime,
)
from .operator import HighAvailabilityOperator

__all__ = (
    "HighAvailabilitySensorTask",
    "HighAvailabilitySensorTaskArgs",
    "HighAvailabilityTask",
    "HighAvailabilityTaskArgs",
)


class HighAvailabilityTaskArgs(PythonSensorArgs):
    """
    Args for the HighAvailabilityTask.
    Inherits from PythonSensorArgs to allow for custom Python callable.
    """

    # python_callable inherited from PythonSensorArgs
    pass_trigger_kwargs: PassTriggerKwargs | None = None
    fail_trigger_kwargs: FailTriggerKwargs | None = None

    runtime: Runtime | None = 120
    endtime: Endtime | None = None
    maxretrigger: MaxRetrigger | None = 2
    reference_date: ReferenceDate = "data_interval_end"


# Alias
HighAvailabilitySensorTaskArgs = HighAvailabilityTaskArgs


class HighAvailabilityTask(Task, HighAvailabilityTaskArgs):
    operator: ImportPath = Field(default="airflow_ha.HighAvailabilitySensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: type) -> type:
        if not isinstance(v, type) and issubclass(v, HighAvailabilityOperator):
            raise TypeError(f"operator must be 'airflow_ha.HighAvailabilitySensor', got: {v}")
        return v


# Alias
HighAvailabilitySensorTask = HighAvailabilityTask
