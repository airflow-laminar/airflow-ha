from typing import Optional, Type

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
    "HighAvailabilityTaskArgs",
    "HighAvailabilitySensorTaskArgs",
    "HighAvailabilityTask",
    "HighAvailabilitySensorTask",
)


class HighAvailabilityTaskArgs(PythonSensorArgs):
    """
    Args for the HighAvailabilityTask.
    Inherits from PythonSensorArgs to allow for custom Python callable.
    """

    # python_callable inherited from PythonSensorArgs
    pass_trigger_kwargs: Optional[PassTriggerKwargs] = None
    fail_trigger_kwargs: Optional[FailTriggerKwargs] = None

    runtime: Optional[Runtime] = 120
    endtime: Optional[Endtime] = None
    maxretrigger: Optional[MaxRetrigger] = 2
    reference_date: ReferenceDate = "data_interval_end"


# Alias
HighAvailabilitySensorTaskArgs = HighAvailabilityTaskArgs


class HighAvailabilityTask(Task, HighAvailabilityTaskArgs):
    operator: ImportPath = Field(default="airflow_ha.HighAvailabilitySensor", description="airflow sensor path", validate_default=True)

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: Type) -> Type:
        if not isinstance(v, Type) and issubclass(v, HighAvailabilityOperator):
            raise ValueError(f"operator must be 'airflow_ha.HighAvailabilitySensor', got: {v}")
        return v


# Alias
HighAvailabilitySensorTask = HighAvailabilityTask
