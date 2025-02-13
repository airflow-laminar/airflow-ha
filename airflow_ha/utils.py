from airflow.exceptions import AirflowFailException, AirflowSkipException

__all__ = (
    "skip_",
    "fail_",
    "pass_",
)


def skip_():
    raise AirflowSkipException


def fail_():
    raise AirflowFailException


def pass_():
    pass
