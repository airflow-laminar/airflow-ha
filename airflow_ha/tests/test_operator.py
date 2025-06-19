from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from airflow_ha import Action, HighAvailabilityOperator, Result
from airflow_ha.operator import _callable_wrapper, _choose_branch


class TestHighAvailabilityOperator:
    def test_instantiation(self, operator: HighAvailabilityOperator):
        assert operator.upstream_list == []
        assert operator.downstream_list == [operator.decide_task]
        assert operator.retrigger_fail in operator.decide_task.downstream_list
        assert operator.retrigger_pass in operator.decide_task.downstream_list
        assert operator.stop_pass in operator.decide_task.downstream_list
        assert operator.stop_fail in operator.decide_task.downstream_list
        assert operator.trigger_rule == "none_failed"

    def test_pool_passthrough(self, operator: HighAvailabilityOperator):
        assert operator.pool == "test-pool"
        assert operator.decide_task.pool == "test-pool"
        assert operator.retrigger_fail.pool == "test-pool"
        assert operator.retrigger_pass.pool == "test-pool"
        assert operator.stop_pass.pool == "test-pool"
        assert operator.stop_fail.pool == "test-pool"

    def test_check_end_conditions_default(self, operator: HighAvailabilityOperator):
        dag_run_mock = MagicMock()
        params_mock = MagicMock()
        dag_run_mock.conf.get.side_effect = [
            # airflow_ha_force_run
            False,
            # startdate
            datetime(2025, 1, 1, tzinfo=UTC).isoformat(),
            # retrigger
            0,
        ]
        params_mock.get.side_effect = [
            # force-run
            False,
            # force-runtime
            None,
            # force-endtime
            None,
            # maxretrigger
            None,
        ]
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) is None

    def test_check_end_conditions_force(self, operator: HighAvailabilityOperator):
        dag_run_mock = MagicMock()
        params_mock = MagicMock()
        dag_run_mock.conf.get.side_effect = [
            # airflow_ha_force_run
            True,
            # startdate
            datetime(2025, 1, 1, tzinfo=UTC).isoformat(),
            # retrigger
            0,
        ]
        params_mock.get.side_effect = [
            # force-run
            False,
            # force-runtime
            None,
            # force-endtime
            None,
            # maxretrigger
            None,
        ]
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) is None

    def test_check_end_conditions_runtime(self, operator: HighAvailabilityOperator):
        now = datetime.now(tz=UTC)
        yesterday_now = now - timedelta(days=1)
        yesterday_now_almost = now - timedelta(hours=23, minutes=59, seconds=59)

        dag_run_mock = MagicMock()
        params_mock = MagicMock()
        dag_run_mock.conf.get.side_effect = [
            # airflow_ha_force_run
            False,
            # startdate
            yesterday_now_almost.isoformat(),
            # retrigger
            0,
            # airflow_ha_force_run
            False,
            # startdate
            yesterday_now.isoformat(),
            # retrigger
            0,
        ]
        params_mock.get.side_effect = [
            # force-run
            False,
            # force-runtime
            timedelta(days=1),
            # force-endtime
            None,
            # maxretrigger
            None,
            # force-run
            False,
            # force-runtime
            timedelta(days=1),
            # force-endtime
            None,
            # maxretrigger
            None,
        ]
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) is None
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) == (Result.PASS, Action.STOP)

    def test_check_end_conditions_endtime(self, operator: HighAvailabilityOperator):
        with patch("airflow_ha.operator.datetime") as mock_datetime:
            now = datetime.now(tz=UTC)
            one_second_ago = now - timedelta(seconds=1)
            mock_datetime.combine = datetime.combine
            mock_datetime.fromisoformat = datetime.fromisoformat
            mock_datetime.now.return_value = one_second_ago

            dag_run_mock = MagicMock()
            params_mock = MagicMock()
            dag_run_mock.conf.get.side_effect = [
                # airflow_ha_force_run
                False,
                # startdate
                one_second_ago.isoformat(),
                # retrigger
                0,
                # airflow_ha_force_run
                False,
                # startdate
                one_second_ago.isoformat(),
                # retrigger
                0,
            ]
            params_mock.get.side_effect = [
                # force-run
                False,
                # force-runtime
                None,
                # force-endtime
                now.time(),
                # maxretrigger
                None,
                # force-run
                False,
                # force-runtime
                None,
                # force-endtime
                now.time(),
                # maxretrigger
                None,
            ]
            assert operator.check_end_conditions(dag=operator.dag, dag_run=dag_run_mock, params=params_mock) is None
            mock_datetime.now.return_value = now
            assert operator.check_end_conditions(dag=operator.dag, dag_run=dag_run_mock, params=params_mock) == (Result.PASS, Action.STOP)

    def test_check_end_conditions_maxretrigger(self, operator: HighAvailabilityOperator):
        dag_run_mock = MagicMock()
        params_mock = MagicMock()
        dag_run_mock.conf.get.side_effect = [
            # airflow_ha_force_run
            False,
            # startdate
            None,
            # retrigger
            0,
            # airflow_ha_force_run
            False,
            # startdate
            None,
            # retrigger
            1,
        ]
        params_mock.get.side_effect = [
            # force-run
            False,
            # force-runtime
            None,
            # force-endtime
            None,
            # maxretrigger
            1,
            # force-run
            False,
            # force-runtime
            None,
            # force-endtime
            None,
            # maxretrigger
            1,
        ]
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) is None
        assert operator.check_end_conditions(dag_run=dag_run_mock, params=params_mock) == (None, Action.STOP)

    def test_choose_branch(self):
        task_instance_mock = MagicMock()
        branch_choices = {
            (Result.PASS, Action.RETRIGGER): "a",
            (Result.PASS, Action.STOP): "b",
            (Result.FAIL, Action.RETRIGGER): "c",
            (Result.FAIL, Action.STOP): "d",
        }

        # default
        res = _choose_branch(
            task_instance=task_instance_mock, branch_choices=branch_choices, task_id="test_task", check_end_conditions=lambda **kwargs: None
        )
        assert res == "a"

        # pass, stop
        task_instance_mock.xcom_pull.return_value = (Result.PASS, Action.STOP)
        res = _choose_branch(
            task_instance=task_instance_mock, branch_choices=branch_choices, task_id="test_task", check_end_conditions=lambda **kwargs: None
        )
        assert res == "b"
        task_instance_mock.xcom_pull.return_value = (Result.FAIL, Action.RETRIGGER)
        res = _choose_branch(
            task_instance=task_instance_mock, branch_choices=branch_choices, task_id="test_task", check_end_conditions=lambda **kwargs: None
        )
        assert res == "c"
        task_instance_mock.xcom_pull.return_value = (Result.FAIL, Action.STOP)
        res = _choose_branch(
            task_instance=task_instance_mock, branch_choices=branch_choices, task_id="test_task", check_end_conditions=lambda **kwargs: None
        )
        assert res == "d"

        # end conditions met
        res = _choose_branch(
            task_instance=task_instance_mock,
            branch_choices=branch_choices,
            task_id="test_task",
            check_end_conditions=lambda **kwargs: (Result.PASS, Action.STOP),
        )
        assert res == "b"

        # retrigger exceeded
        res = _choose_branch(
            task_instance=task_instance_mock,
            branch_choices=branch_choices,
            task_id="test_task",
            check_end_conditions=lambda **kwargs: (None, Action.STOP),
        )
        assert res == "b"

    def test_callable_wrapper(self):
        task_instance_mock = MagicMock()

        # malformed
        res = _callable_wrapper(
            task_instance=task_instance_mock, python_callable=lambda **kwargs: (None, Action.CONTINUE), check_end_conditions=lambda **kwargs: None
        )
        assert res is True

        # pass and continue
        res = _callable_wrapper(
            task_instance=task_instance_mock,
            python_callable=lambda **kwargs: (Result.PASS, Action.CONTINUE),
            check_end_conditions=lambda **kwargs: None,
        )
        assert res is False

        # pass and retrigger (keep testing)
        res = _callable_wrapper(
            task_instance=task_instance_mock,
            python_callable=lambda **kwargs: (Result.PASS, Action.RETRIGGER),
            check_end_conditions=lambda **kwargs: None,
        )
        assert res is True

        # pass and stop
        res = _callable_wrapper(
            task_instance=task_instance_mock, python_callable=lambda **kwargs: (Result.PASS, Action.STOP), check_end_conditions=lambda **kwargs: None
        )
        assert res is True

        # end conditions met
        res = _callable_wrapper(
            task_instance=task_instance_mock,
            python_callable=lambda **kwargs: (None, Action.CONTINUE),
            check_end_conditions=lambda **kwargs: (None, Action.STOP),
        )
        assert res is True
