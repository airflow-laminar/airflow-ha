from airflow.models.dag import DAG  # noqa: F401
from airflow.models.operator import Operator  # noqa: F401
from airflow.operators.python import BranchPythonOperator, PythonOperator  # noqa: F401
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # noqa: F401


class HighAvailabilityOperator(PythonOperator):
    def __init__(self, **kwargs) -> None:
        """The HighAvailabilityOperator is an Airflow Meta-Operator for long-running or "always-on" tasks.

        It resembles a BranchPythonOperator with the following predefined set of outcomes:

        HA-Task (the instance of HighAvailabilityOperator itself)
           |-> CheckTask (run a user-provided check or task)
                   | DoneTask (tasks finished cleanly)------|
                   | EndTask (end time reached)-------------|
                                                            |
                                                            |--> CleanupTask (Finish DAG, success)
                   | RunningTask (tasks are still running)--|
                                                            |--> LoopTask (Re-trigger DAG, success)
                   | FailedTask (tasks finished uncleanly)--|
                                                            |--> RestartTask (Re-trigger DAG, failure)
                   | KillTask-------------------------------|
                                                            |--> CleanupTask (Finish DAG, failure)

        Given a check, there are four outcomes:
        - The tasks finished/exited cleanly, and thus the DAG should terminate cleanly
        - The tasks finished/exited uncleanly, in which case the DAG should restart
        - The tasks did not finish, but the end time has been reached anyway, so the DAG should terminate cleanly
        - The tasks did not finish, but we've reached an interval and should loop and rerun the DAG

        The last case is particularly important when DAGs have a max run time, e.g. on AWS MWAA where DAGs
        cannot run for longer than 12 hours at a time and so must be "restarted".

        Additionally, there is a "KillTask" to force kill the DAG.

        Any setup should be state-aware (e.g. don't just start a process, check if it is currently started first).
        """
        ...

        kwargs.pop("python_callable", None)
        kwargs.pop("op_args", None)
        kwargs.pop("op_kwargs", None)
        kwargs.pop("templates_dict", None)
        kwargs.pop("templates_exts", None)
        kwargs.pop("show_return_value_in_logs", None)
