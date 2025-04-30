import unittest
from unittest import mock
from collections import namedtuple
from airflow.providers.teradata.operators.bteq import BteqOperator
from airflow.providers.teradata.hooks.bteq import BteqHook


class TestBteqOperator(unittest.TestCase):
    def setUp(self):
        self.mock_context = {}
        self.task_id = "test_bteq_operator"
        self.bteq = "SELECT * FROM my_table;"
        self.ttu_conn_id = "teradata_default"

    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_with_xcom_push(self, mock_hook_init, mock_execute_bteq):
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
            xcom_push_flag=True,
            ttu_conn_id=self.ttu_conn_id,
        )

        # When
        result = operator.execute(self.mock_context)

        # Then
        mock_hook_init.assert_called_once_with(ttu_conn_id=self.ttu_conn_id)
        mock_execute_bteq.assert_called_once_with(self.bteq, True)
        self.assertEqual(result, expected_result)

    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_without_xcom_push(self, mock_hook_init, mock_execute_bteq):
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
            xcom_push_flag=False,
            ttu_conn_id=self.ttu_conn_id,
        )

        # When
        result = operator.execute(self.mock_context)

        # Then
        mock_hook_init.assert_called_once_with(ttu_conn_id=self.ttu_conn_id)
        mock_execute_bteq.assert_called_once_with(self.bteq, False)
        self.assertIsNone(result)

    @mock.patch.object(BteqHook, "on_kill")
    def test_on_kill(self, mock_on_kill):
        # Given
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
        )
        operator._hook = BteqHook()

        # When
        operator.on_kill()

        # Then
        mock_on_kill.assert_called_once()

    def test_on_kill_not_initialized(self):
        # Given
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
        )
        operator._hook = None

        # When/Then (no exception should be raised)
        operator.on_kill()

    def test_template_fields(self):
        # Verify template fields are defined correctly
        self.assertEqual(BteqOperator.template_fields, ("bteq"))

    def test_template_ext(self):
        # Verify template extensions are defined correctly
        self.assertEqual(BteqOperator.template_ext, (".sql", ".bteq"))


if __name__ == "__main__":
    unittest.main()
