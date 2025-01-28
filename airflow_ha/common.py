from enum import Enum
from typing import Tuple

__all__ = (
    "Result",
    "Action",
    "CheckResult",
)


class Result(str, Enum):
    PASS = "pass"
    FAIL = "fail"


class Action(str, Enum):
    CONTINUE = "continue"
    RETRIGGER = "retrigger"
    STOP = "stop"


CheckResult = Tuple[Result, Action]
