from collections.abc import Callable
from datetime import time, timedelta
from enum import Enum
from typing import Any, Literal

__all__ = (
    "Action",
    "CheckResult",
    "Endtime",
    "FailTriggerKwargs",
    "MaxRetrigger",
    "PassTriggerKwargs",
    "PythonCallable",
    "ReferenceDate",
    "Result",
    "Runtime",
)


class Result(str, Enum):
    PASS = "pass"
    FAIL = "fail"


class Action(str, Enum):
    CONTINUE = "continue"
    RETRIGGER = "retrigger"
    STOP = "stop"


CheckResult = tuple[Result, Action]

PythonCallable = Callable[..., CheckResult]
PassTriggerKwargs = dict[str, Any]
FailTriggerKwargs = dict[str, Any]
Runtime = int | timedelta
Endtime = str | time
MaxRetrigger = int
ReferenceDate = Literal["start_date", "logical_date", "data_interval_end"]
