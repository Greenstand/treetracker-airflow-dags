import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from uuid import UUID


class DatabaseError(Exception):
    pass


class FakeDAG:
    active_dag = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.schedule_interval = kwargs.get('schedule_interval')
        self.catchup = kwargs.get('catchup')
        self.tags = kwargs.get('tags')
        self.task_dict = {}

    def __enter__(self):
        FakeDAG.active_dag = self
        return self

    def __exit__(self, exception_type, exception, traceback):
        FakeDAG.active_dag = None

    def get_task(self, task_id):
        return self.task_dict[task_id]


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.dag = FakeDAG.active_dag
        self.dag.task_dict[task_id] = self


def load_dag_module():
    airflow = types.ModuleType('airflow')
    airflow.DAG = FakeDAG

    airflow_operators = types.ModuleType('airflow.operators')
    airflow_python = types.ModuleType('airflow.operators.python')
    airflow_python.PythonOperator = FakePythonOperator

    airflow_providers = types.ModuleType('airflow.providers')
    airflow_postgres = types.ModuleType('airflow.providers.postgres')
    airflow_postgres_hooks = types.ModuleType('airflow.providers.postgres.hooks')
    airflow_postgres_hook = types.ModuleType(
        'airflow.providers.postgres.hooks.postgres'
    )
    airflow_postgres_hook.PostgresHook = MagicMock

    airflow_models = types.ModuleType('airflow.models')
    airflow_models.Variable = MagicMock

    fake_modules = {
        'airflow': airflow,
        'airflow.operators': airflow_operators,
        'airflow.operators.python': airflow_python,
        'airflow.providers': airflow_providers,
        'airflow.providers.postgres': airflow_postgres,
        'airflow.providers.postgres.hooks': airflow_postgres_hooks,
        'airflow.providers.postgres.hooks.postgres': airflow_postgres_hook,
        'airflow.models': airflow_models,
    }

    sys.modules.pop('intra_session_duplicate_dag', None)
    sys.modules.pop('lib.utils', None)
    with patch.dict(sys.modules, fake_modules):
        return importlib.import_module('intra_session_duplicate_dag')


class Test_IntraSessionDuplicateDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_module = load_dag_module()

    def setUp(self):
        self.conn = MagicMock()
        self.cursor = self.conn.cursor.return_value
        self.hook = MagicMock()
        self.hook.return_value.get_conn.return_value = self.conn
        self.hook_patch = patch.object(
            self.dag_module,
            'PostgresHook',
            self.hook,
        )
        self.hook_patch.start()
        self.addCleanup(self.hook_patch.stop)

    def test_dag_structure(self):
        dag = self.dag_module.dag

        self.assertEqual(dag.dag_id, 'intra_session_duplicate_detection')
        self.assertEqual(dag.schedule_interval, '@hourly')
        self.assertFalse(dag.catchup)
        task = dag.get_task('detect_and_flag_duplicates')
        self.assertIs(
            task.python_callable,
            self.dag_module.run_intra_session_duplicate_check,
        )

    def test_no_duplicates_does_not_run_update(self):
        self.cursor.fetchall.return_value = []

        result = self.dag_module.run_intra_session_duplicate_check()

        self.hook.assert_called_once_with(postgres_conn_id='postgres_default')
        self.assertEqual(self.cursor.execute.call_count, 1)
        self.assertIn(
            'session_id IS NOT NULL',
            self.cursor.execute.call_args.args[0],
        )
        self.assertNotIn('UPDATE raw_capture', self.cursor.execute.call_args.args[0])
        self.conn.commit.assert_called_once_with()
        self.conn.rollback.assert_not_called()
        self.conn.close.assert_called_once_with()
        self.assertEqual(result['updated_captures'], 0)

    def test_duplicates_are_updated_and_committed(self):
        duplicate_1 = UUID('11111111-1111-1111-1111-111111111111')
        duplicate_2 = UUID('22222222-2222-2222-2222-222222222222')
        self.cursor.fetchall.return_value = [
            ('primary-1', duplicate_1, 'session-1', 1.0, 5.0),
            ('primary-2', duplicate_2, 'session-1', 2.0, 10.0),
        ]
        self.cursor.rowcount = 2

        result = self.dag_module.run_intra_session_duplicate_check()

        self.assertEqual(self.cursor.execute.call_count, 2)
        update_call = self.cursor.execute.call_args_list[1]
        self.assertIn('UPDATE raw_capture', update_call.args[0])
        self.assertIn("status = 'rejected'", update_call.args[0])
        self.assertIn("rejection_reason = 'duplicate_intra_session'", update_call.args[0])
        self.assertIn("status = 'unprocessed'", update_call.args[0])
        self.assertEqual(update_call.args[1], ([duplicate_1, duplicate_2],))
        self.conn.commit.assert_called_once_with()
        self.conn.rollback.assert_not_called()
        self.conn.close.assert_called_once_with()
        self.assertEqual(result['updated_captures'], 2)

    def test_update_error_rolls_back(self):
        duplicate_id = UUID('33333333-3333-3333-3333-333333333333')
        self.cursor.fetchall.return_value = [
            ('primary-1', duplicate_id, 'session-1', 1.0, 5.0),
        ]

        def execute(sql, params=None):
            if 'UPDATE raw_capture' in sql:
                raise DatabaseError('database update failed')

        self.cursor.execute.side_effect = execute

        with self.assertRaises(DatabaseError):
            self.dag_module.run_intra_session_duplicate_check()

        self.conn.commit.assert_not_called()
        self.conn.rollback.assert_called_once_with()
        self.cursor.close.assert_called_once_with()
        self.conn.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
