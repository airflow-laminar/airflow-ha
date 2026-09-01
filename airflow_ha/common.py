from collections.abc import Callable
from datetime import time, timedelta
from enum import StrEnum
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


class Result(StrEnum):
    PASS = "pass"
    FAIL = "fail"


class Action(StrEnum):
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
