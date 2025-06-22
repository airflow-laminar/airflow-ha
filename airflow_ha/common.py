from datetime import time, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Literal, Tuple, Union

__all__ = (
    "Result",
    "Action",
    "CheckResult",
    "PythonCallable",
    "PassTriggerKwargs",
    "FailTriggerKwargs",
    "Runtime",
    "Endtime",
    "MaxRetrigger",
    "ReferenceDate",
)


class Result(str, Enum):
    PASS = "pass"
    FAIL = "fail"


class Action(str, Enum):
    CONTINUE = "continue"
    RETRIGGER = "retrigger"
    STOP = "stop"


CheckResult = Tuple[Result, Action]

PythonCallable = Callable[..., CheckResult]
PassTriggerKwargs = Dict[str, Any]
FailTriggerKwargs = Dict[str, Any]
Runtime = Union[int, timedelta]
Endtime = Union[str, time]
MaxRetrigger = int
ReferenceDate = Literal["start_date", "logical_date", "data_interval_end"]
