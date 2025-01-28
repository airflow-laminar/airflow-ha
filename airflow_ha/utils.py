__all__ = (
    "skip_",
    "fail_",
    "pass_",
)


def skip_():
    from airflow.exceptions import AirflowSkipException

    raise AirflowSkipException


def fail_():
    from airflow.exceptions import AirflowFailException

    raise AirflowFailException


def pass_():
    pass
