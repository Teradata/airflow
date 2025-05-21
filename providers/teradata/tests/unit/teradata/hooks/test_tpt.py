#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
import shutil
import tempfile
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.teradata.hooks.tpt import TptHook


# Fixtures
@pytest.fixture
def source_conn_dict():
    return {
        "host": "localhost",
        "login": "user",
        "password": "pass",
        "ttu_log_folder": tempfile.gettempdir(),
        "console_output_encoding": "utf-8",
    }


@pytest.fixture
def target_conn_dict():
    return {
        "host": "remotehost",
        "login": "target_user",
        "password": "target_pass",
    }


@pytest.fixture
def ddl_sql():
    return "CREATE TABLE foo (id INT);"


@pytest.fixture
def error_list():
    return "[3807]"


@pytest.fixture
def context():
    return {}


@pytest.fixture
def tdload_cmd():
    return ["tdload", "-j", "jobfile"]


@pytest.fixture
def job_variable_file(tmp_path):
    file = tmp_path / "jobvars.txt"
    file.write_text("dummy content")
    return str(file)


@pytest.fixture
def ssh_conn_id():
    return "ssh_conn_id"


# Helper for SSH mocks
def make_ssh_client_mock(stdout_data=b"output\n", stderr_data=b"", exit_status=0):
    ssh_client = mock.Mock()
    sftp_client = mock.Mock()
    ssh_client.open_sftp.return_value = sftp_client
    sftp_client.put = mock.Mock()
    sftp_client.remove = mock.Mock()
    sftp_client.close = mock.Mock()
    stdout = mock.Mock()
    stdout.read.return_value = stdout_data
    stdout.channel.recv_exit_status.return_value = exit_status
    stderr = mock.Mock()
    stderr.read.return_value = stderr_data
    ssh_client.exec_command.return_value = (mock.Mock(), stdout, stderr)
    return ssh_client, sftp_client


def make_ssh_client_mock_tbuild(stdout_data=b"output\n", stderr_data=b"", exit_status=0):
    ssh_client = mock.Mock()
    sftp_client = mock.Mock()
    ssh_client.open_sftp.return_value = sftp_client
    sftp_client.put = mock.Mock()
    sftp_client.remove = mock.Mock()
    sftp_client.close = mock.Mock()
    stdout = mock.Mock()
    stdout.read.return_value = stdout_data
    stdout.channel.recv_exit_status.return_value = exit_status
    stderr = mock.Mock()
    stderr.read.return_value = stderr_data
    ssh_client.exec_command.return_value = (mock.Mock(), stdout, stderr)
    return ssh_client, sftp_client


# --------------------------
# Tests for _execute_tbuild_locally
# --------------------------
@mock.patch("airflow.providers.teradata.hooks.tpt.subprocess")
def test__execute_tbuild_locally_success(mock_subprocess, ddl_sql, error_list, source_conn_dict):
    hook = TptHook()
    process_mock = mock.Mock()
    process_mock.stdout.readline.side_effect = [b"DDL output\n", b""]
    process_mock.wait.return_value = 0
    process_mock.returncode = 0
    mock_subprocess.Popen.return_value = process_mock

    result = hook._execute_tbuild_locally(
        tbuild_cmd=["tbuild", "-f", "script.sql"],
        conn=source_conn_dict,
        xcom_push_flag=True,
    )
    assert "DDL output" in result


@mock.patch("airflow.providers.teradata.hooks.tpt.subprocess")
def test__execute_tbuild_locally_error(mock_subprocess, source_conn_dict):
    hook = TptHook()
    process_mock = mock.Mock()
    process_mock.stdout.readline.side_effect = [b"Error output\n", b""]
    process_mock.wait.return_value = 1
    process_mock.returncode = 1
    mock_subprocess.Popen.return_value = process_mock

    with pytest.raises(AirflowException, match="TPT command exited with return code 1 due to: Error output"):
        hook._execute_tbuild_locally(
            tbuild_cmd=["tbuild", "-f", "script.sql"],
            conn=source_conn_dict,
            xcom_push_flag=False,
        )


# --------------------------
# Tests for _execute_tbuild_via_ssh
# --------------------------
@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test__execute_tbuild_via_ssh_success_xcom_push(mock_ssh_hook, tmp_path):
    hook = TptHook()
    tbuild_cmd = ["tbuild", "-f", "script.sql"]
    script_file = str(tmp_path / "script.sql")
    with open(script_file, "w") as f:
        f.write("TPT SCRIPT")
    ssh_conn_id = "ssh_conn_id"
    ssh_client, sftp_client = make_ssh_client_mock_tbuild(
        stdout_data=b"line1\nline2\n", stderr_data=b"", exit_status=0
    )
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    result = hook._execute_tbuild_via_ssh(
        tbuild_cmd=tbuild_cmd.copy(),
        script_file=script_file,
        ssh_conn_id=ssh_conn_id,
        xcom_push_flag=True,
    )
    assert result == "line2"
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()
    remote_script_file = f"/tmp/airflow_tpt_{os.path.basename(script_file)}"
    called_args = ssh_client.exec_command.call_args[0][0].split()
    assert remote_script_file in called_args


@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test__execute_tbuild_via_ssh_success_no_xcom_push(mock_ssh_hook, tmp_path):
    hook = TptHook()
    tbuild_cmd = ["tbuild", "-f", "script.sql"]
    script_file = str(tmp_path / "script.sql")
    with open(script_file, "w") as f:
        f.write("TPT SCRIPT")
    ssh_conn_id = "ssh_conn_id"
    ssh_client, sftp_client = make_ssh_client_mock_tbuild(
        stdout_data=b"output\n", stderr_data=b"", exit_status=0
    )
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    result = hook._execute_tbuild_via_ssh(
        tbuild_cmd=tbuild_cmd.copy(),
        script_file=script_file,
        ssh_conn_id=ssh_conn_id,
        xcom_push_flag=False,
    )
    assert result is None
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()


@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test__execute_tbuild_via_ssh_nonzero_exit_status(mock_ssh_hook, tmp_path):
    hook = TptHook()
    tbuild_cmd = ["tbuild", "-f", "script.sql"]
    script_file = str(tmp_path / "script.sql")
    with open(script_file, "w") as f:
        f.write("TPT SCRIPT")
    ssh_conn_id = "ssh_conn_id"
    ssh_client, sftp_client = make_ssh_client_mock_tbuild(
        stdout_data=b"output\n", stderr_data=b"err\n", exit_status=2
    )
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    with pytest.raises(AirflowException, match="tbuild command failed with exit code 2: err"):
        hook._execute_tbuild_via_ssh(
            tbuild_cmd=tbuild_cmd.copy(),
            script_file=script_file,
            ssh_conn_id=ssh_conn_id,
            xcom_push_flag=True,
        )
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()


@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test__execute_tbuild_via_ssh_cleanup_error_warning(mock_ssh_hook, tmp_path):
    hook = TptHook()
    tbuild_cmd = ["tbuild", "-f", "script.sql"]
    script_file = str(tmp_path / "script.sql")
    with open(script_file, "w") as f:
        f.write("TPT SCRIPT")
    ssh_conn_id = "ssh_conn_id"
    ssh_client, sftp_client = make_ssh_client_mock_tbuild(
        stdout_data=b"output\n", stderr_data=b"", exit_status=0
    )
    # Simulate error on remove
    sftp_client.remove.side_effect = Exception("remove failed")
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    # Should not raise, but log a warning
    result = hook._execute_tbuild_via_ssh(
        tbuild_cmd=tbuild_cmd.copy(),
        script_file=script_file,
        ssh_conn_id=ssh_conn_id,
        xcom_push_flag=True,
    )
    assert result == "output"
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()


# --------------------------
# Tests for _execute_tdload_locally
# --------------------------
@mock.patch("airflow.providers.teradata.hooks.tpt.subprocess")
def test_execute_tdload_locally_success(mock_subprocess, source_conn_dict):
    hook = TptHook()
    process_mock = mock.Mock()
    process_mock.stdout.readline.side_effect = [b"Line1\n", b"Line2\n", b""]
    process_mock.wait.return_value = 0
    process_mock.returncode = 0
    mock_subprocess.Popen.return_value = process_mock

    result = hook._execute_tdload_locally(["tdload", "-j", "jobfile"], source_conn_dict, xcom_push_flag=True)
    assert result == "Line2"


# --------------------------
# Tests for _execute_tdload_via_ssh
# --------------------------
@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test_execute_tdload_via_ssh_success_xcom_push(mock_ssh_hook, tdload_cmd, job_variable_file, ssh_conn_id):
    hook = TptHook()
    ssh_client, sftp_client = make_ssh_client_mock(
        stdout_data=b"line1\nline2\n", stderr_data=b"", exit_status=0
    )
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    result = hook._execute_tdload_via_ssh(
        tdload_cmd=tdload_cmd.copy(),
        job_variable_file=job_variable_file,
        ssh_conn_id=ssh_conn_id,
        xcom_push_flag=True,
    )
    assert result == "line2"
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()
    remote_job_file = f"/tmp/airflow_tdload_{os.path.basename(job_variable_file)}"
    called_args = ssh_client.exec_command.call_args[0][0].split()
    assert remote_job_file in called_args


@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test_execute_tdload_via_ssh_success_no_xcom_push(
    mock_ssh_hook, tdload_cmd, job_variable_file, ssh_conn_id
):
    hook = TptHook()
    ssh_client, sftp_client = make_ssh_client_mock(stdout_data=b"output\n", stderr_data=b"", exit_status=0)
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    result = hook._execute_tdload_via_ssh(
        tdload_cmd=tdload_cmd.copy(),
        job_variable_file=job_variable_file,
        ssh_conn_id=ssh_conn_id,
        xcom_push_flag=False,
    )
    assert result is None
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()


@mock.patch("airflow.providers.teradata.hooks.tpt.SSHHook")
def test_execute_tdload_via_ssh_nonzero_exit_status(
    mock_ssh_hook, tdload_cmd, job_variable_file, ssh_conn_id
):
    hook = TptHook()
    ssh_client, sftp_client = make_ssh_client_mock(
        stdout_data=b"output\n", stderr_data=b"err\n", exit_status=2
    )
    mock_ssh_hook.return_value.get_conn.return_value.__enter__.return_value = ssh_client

    with pytest.raises(AirflowException, match="tdload command failed with exit code 2: err"):
        hook._execute_tdload_via_ssh(
            tdload_cmd=tdload_cmd.copy(),
            job_variable_file=job_variable_file,
            ssh_conn_id=ssh_conn_id,
            xcom_push_flag=True,
        )
    sftp_client.put.assert_called_once()
    sftp_client.remove.assert_called_once()
    sftp_client.close.assert_called()
    ssh_client.exec_command.assert_called_once()


# --------------------------
# Tests for _prepare_tdload_script
# --------------------------
def test_prepare_tdload_script_file_to_table(tmp_path, source_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="file_to_table",
        source_table=None,
        select_stmt=None,
        target_table="my_table",
        source_file_name="data.csv",
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "TargetTable='my_table'" in content
    assert "SourceFileName='data.csv'" in content
    os.remove(file_path)


def test_prepare_tdload_script_table_to_file(tmp_path, source_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="table_to_file",
        source_table="src_table",
        select_stmt=None,
        target_table=None,
        source_file_name=None,
        target_file_name="out.csv",
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter="|",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceTable='src_table'" in content
    assert "TargetFileName='out.csv'" in content
    assert "TargetTextDelimiter='|'" in content
    os.remove(file_path)


def test_prepare_tdload_script_table_to_table(tmp_path, source_conn_dict, target_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="table_to_table",
        source_table="src_table",
        select_stmt=None,
        target_table="dest_table",
        source_file_name=None,
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table="stage_table",
        tdload_options={"CustomOpt": "val"},
        source_conn=source_conn_dict,
        target_conn=target_conn_dict,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceTable='src_table'" in content
    assert "TargetTable='dest_table'" in content
    assert "StagingTable='stage_table'" in content
    assert "CustomOpt='val'" in content
    os.remove(file_path)


def test_prepare_tdload_script_table_to_table_missing_target_conn(source_conn_dict):
    hook = TptHook()
    with pytest.raises(ValueError, match="Target connection details required for table_to_table mode"):
        hook._prepare_tdload_script(
            mode="table_to_table",
            source_table="src_table",
            select_stmt=None,
            target_table="dest_table",
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            source_conn=source_conn_dict,
            target_conn=None,
        )


def test_prepare_tdload_script_select_stmt_file_to_table(tmp_path, source_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="file_to_table",
        source_table=None,
        select_stmt="SELECT * FROM src",
        target_table="my_table",
        source_file_name="data.csv",
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=";",
        target_text_delimiter=";",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "TargetTable='my_table'" in content
    assert "SourceFileName='data.csv'" in content
    assert "SELECT * FROM src" not in content
    os.remove(file_path)


def test_prepare_tdload_script_with_tdload_options(tmp_path, source_conn_dict):
    hook = TptHook()
    tdload_options = {"ErrorLimit": "100", "LogLevel": "DEBUG"}
    file_path = hook._prepare_tdload_script(
        mode="file_to_table",
        source_table=None,
        select_stmt=None,
        target_table="my_table",
        source_file_name="data.csv",
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=tdload_options,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "ErrorLimit='100'" in content
    assert "LogLevel='DEBUG'" in content
    os.remove(file_path)


def test_prepare_tdload_script_file_to_table_with_nondefault_format_and_delimiter(tmp_path, source_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="file_to_table",
        source_table=None,
        select_stmt=None,
        target_table="my_table",
        source_file_name="data.csv",
        target_file_name=None,
        source_format="CSV",
        target_format="delimited",
        source_text_delimiter="|",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceFormat='CSV'" in content
    assert "SourceTextDelimiter='|'" in content
    os.remove(file_path)


def test_prepare_tdload_script_table_to_file_with_select_stmt_and_nondefault_target_format(
    tmp_path, source_conn_dict
):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="table_to_file",
        source_table=None,
        select_stmt="SELECT * FROM foo",
        target_table=None,
        source_file_name=None,
        target_file_name="output.txt",
        source_format="delimited",
        target_format="TEXT",
        source_text_delimiter=",",
        target_text_delimiter=";",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceSelectStmt='SELECT * FROM foo'" in content
    assert "TargetFileName='output.txt'" in content
    assert "TargetFormat='TEXT'" in content
    assert "TargetTextDelimiter=';'" in content
    os.remove(file_path)


def test_prepare_tdload_script_table_to_table_with_select_stmt_and_custom_options(
    tmp_path, source_conn_dict, target_conn_dict
):
    hook = TptHook()
    tdload_options = {"CustomOpt1": "val1", "CustomOpt2": "val2"}
    file_path = hook._prepare_tdload_script(
        mode="table_to_table",
        source_table=None,
        select_stmt="SELECT * FROM bar",
        target_table="dest_table",
        source_file_name=None,
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=tdload_options,
        source_conn=source_conn_dict,
        target_conn=target_conn_dict,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceSelectStmt='SELECT * FROM bar'" in content
    assert "TargetTable='dest_table'" in content
    assert "CustomOpt1='val1'" in content
    assert "CustomOpt2='val2'" in content
    os.remove(file_path)


def test_prepare_tdload_script_with_staging_table(tmp_path, source_conn_dict, target_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="table_to_table",
        source_table="src_table",
        select_stmt=None,
        target_table="dest_table",
        source_file_name=None,
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table="stage_table",
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=target_conn_dict,
    )
    with open(file_path) as f:
        content = f.read()
    assert "StagingTable='stage_table'" in content
    os.remove(file_path)


def test_prepare_tdload_script_tdload_options_overrides(tmp_path, source_conn_dict):
    hook = TptHook()
    tdload_options = {"TargetTable": "should_not_override"}
    file_path = hook._prepare_tdload_script(
        mode="file_to_table",
        source_table=None,
        select_stmt=None,
        target_table="real_table",
        source_file_name="file.csv",
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=tdload_options,
        source_conn=source_conn_dict,
        target_conn=None,
    )
    with open(file_path) as f:
        content = f.read()
    assert "TargetTable='real_table'" in content
    assert "TargetTable='should_not_override'" not in content
    os.remove(file_path)


# Test for _prepare_tdload_script  mode == "table_to_table" and select_stmt
def test_prepare_tdload_script_table_to_table_with_select_stmt(tmp_path, source_conn_dict, target_conn_dict):
    hook = TptHook()
    file_path = hook._prepare_tdload_script(
        mode="table_to_table",
        source_table=None,
        select_stmt="SELECT * FROM src_table",
        target_table="dest_table",
        source_file_name=None,
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=None,
        source_conn=source_conn_dict,
        target_conn=target_conn_dict,
    )
    with open(file_path) as f:
        content = f.read()
    assert "SourceSelectStmt='SELECT * FROM src_table'" in content
    assert "TargetTable='dest_table'" in content
    os.remove(file_path)


# --------------------------
# Tests for _prepare_tpt_ddl_script
# --------------------------
def test_prepare_tpt_ddl_script(ddl_sql, source_conn_dict):
    hook = TptHook()
    script_content = hook._prepare_tpt_ddl_script(
        sql=ddl_sql,
        error_list="[3807]",
        host=source_conn_dict["host"],
        login=source_conn_dict["login"],
        password=source_conn_dict["password"],
    )
    assert "CREATE TABLE foo (id INT);" in script_content
    assert "ErrorList = ['[3807]']" in script_content


# --------------------------
# Tests for execute_ddl
# --------------------------
@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook.get_conn")
@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tbuild_locally")
def test_execute_ddl_success_local(mock_tbuild, mock_get_conn, ddl_sql, error_list):
    hook = TptHook()
    mock_tbuild.return_value = "DDL OK"
    result = hook.execute_ddl(
        sql=ddl_sql,
        error_list=error_list,
        xcom_push_flag=True,
        ssh_conn_id=None,
    )
    assert result == "DDL OK"
    mock_tbuild.assert_called_once()
    mock_get_conn.assert_called_once()


@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook.get_conn")
@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tbuild_via_ssh")
def test_execute_ddl_success_ssh(mock_tbuild_ssh, mock_get_conn, ddl_sql, error_list):
    hook = TptHook()
    mock_tbuild_ssh.return_value = "DDL SSH OK"
    result = hook.execute_ddl(
        sql=ddl_sql,
        error_list=error_list,
        xcom_push_flag=False,
        ssh_conn_id="ssh_conn_id",
    )
    assert result == "DDL SSH OK"
    mock_tbuild_ssh.assert_called_once()
    mock_get_conn.assert_called_once()


# add one more test for execute_ddl with empty sql
def test_execute_ddl_empty_sql(source_conn_dict):
    hook = TptHook()
    with pytest.raises(ValueError, match="SQL statement cannot be empty"):
        hook.execute_ddl(
            sql="",
            error_list="",
            xcom_push_flag=False,
            ssh_conn_id=None,
        )


# --------------------------
# Tests for execute_tdload
# --------------------------
def test_execute_tdload_invalid_mode(source_conn_dict):
    hook = TptHook()
    with pytest.raises(ValueError, match="Invalid mode: invalid_mode"):
        hook.execute_tdload(
            mode="invalid_mode",
            ssh_conn_id=None,
            source_table=None,
            select_stmt=None,
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_file_to_table_missing_params(source_conn_dict):
    hook = TptHook()
    with pytest.raises(ValueError, match="file_to_table mode requires source_file_name and target_table"):
        hook.execute_tdload(
            mode="file_to_table",
            ssh_conn_id=None,
            source_table=None,
            select_stmt=None,
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_table_to_file_missing_params(source_conn_dict):
    hook = TptHook()
    with pytest.raises(
        ValueError,
        match="table_to_file mode requires target_file_name and either source_table or select_stmt",
    ):
        hook.execute_tdload(
            mode="table_to_file",
            ssh_conn_id=None,
            source_table=None,
            select_stmt=None,
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_table_to_table_missing_params(source_conn_dict):
    hook = TptHook()
    with pytest.raises(
        ValueError, match="table_to_table mode requires target_table and either source_table or select_stmt"
    ):
        hook.execute_tdload(
            mode="table_to_table",
            ssh_conn_id=None,
            source_table=None,
            select_stmt=None,
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id="target_conn_id",
            xcom_push_flag=False,
        )


def test_execute_tdload_table_to_table_missing_target_conn_id(source_conn_dict):
    hook = TptHook()
    with pytest.raises(ValueError, match="table_to_table mode requires target_teradata_conn_id"):
        hook.execute_tdload(
            mode="table_to_table",
            ssh_conn_id=None,
            source_table="src_table",
            select_stmt=None,
            target_table="dest_table",
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_with_target_teradata_conn_id_and_tdload_options():
    hook = TptHook()
    tdload_options = {"ErrorLimit": "10"}
    # Provide an invalid target_teradata_conn_id to trigger the ValueError
    with pytest.raises(ValueError, match="table_to_table mode requires target_teradata_conn_id"):
        hook.execute_tdload(
            mode="table_to_table",
            ssh_conn_id=None,
            source_table="src_table",
            select_stmt=None,
            target_table="dest_table",
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=tdload_options,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_table_to_file_with_select_stmt(source_conn_dict):
    hook = TptHook()
    with pytest.raises(
        ValueError,
        match="table_to_file mode requires target_file_name and either source_table or select_stmt",
    ):
        hook.execute_tdload(
            mode="table_to_file",
            ssh_conn_id=None,
            source_table=None,
            select_stmt="SELECT * FROM foo",
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id=None,
            xcom_push_flag=False,
        )


def test_execute_tdload_table_to_table_with_select_stmt(source_conn_dict):
    hook = TptHook()
    with pytest.raises(
        ValueError, match="table_to_table mode requires target_table and either source_table or select_stmt"
    ):
        hook.execute_tdload(
            mode="table_to_table",
            ssh_conn_id=None,
            source_table=None,
            select_stmt="SELECT * FROM foo",
            target_table=None,
            source_file_name=None,
            target_file_name=None,
            source_format="delimited",
            target_format="delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            staging_table=None,
            tdload_options=None,
            target_teradata_conn_id="target_conn_id",
            xcom_push_flag=False,
        )


@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook.get_conn")
@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook._prepare_tdload_script")
@mock.patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tdload_locally")
def test_execute_tdload_runs_code_after_try_block(
    mock_execute_tdload_locally, mock_prepare_tdload_script, mock_get_conn, tmp_path, source_conn_dict
):
    """
    Test that code after the try block in execute_tdload is executed,
    including log directory creation, job name generation, and command construction.
    """
    hook = TptHook()
    # Setup mocks
    mock_get_conn.return_value = dict(source_conn_dict, ttu_log_folder=str(tmp_path))
    tdload_job_variable_file = str(tmp_path / "jobvars.txt")
    mock_prepare_tdload_script.return_value = tdload_job_variable_file
    mock_execute_tdload_locally.return_value = "TDLOAD OK"

    # Remove logs dir if exists to check creation
    logs_dir = os.path.join(str(tmp_path), "tdload", "logs")
    if os.path.exists(logs_dir):
        shutil.rmtree(os.path.join(str(tmp_path), "tdload"))

    tdload_options = {"ErrorLimit": "100", "LogLevel": "DEBUG"}

    result = hook.execute_tdload(
        mode="file_to_table",
        ssh_conn_id=None,
        source_table=None,
        select_stmt=None,
        target_table="my_table",
        source_file_name="file.csv",
        target_file_name=None,
        source_format="delimited",
        target_format="delimited",
        source_text_delimiter=",",
        target_text_delimiter=",",
        staging_table=None,
        tdload_options=tdload_options,
        target_teradata_conn_id=None,
        xcom_push_flag=True,
    )

    # Assert that the logs directory was created
    assert os.path.isdir(logs_dir)
    # Assert that the tdload command was constructed and executed
    mock_execute_tdload_locally.assert_called_once()
    tdload_cmd = mock_execute_tdload_locally.call_args[0][0]
    # The command should include the job variable file and a job name starting with 'airflow_tdload_'
    assert tdload_job_variable_file in tdload_cmd
    assert any(arg.startswith("airflow_tdload_") for arg in tdload_cmd)
    # The command should include the tdload_options as command-line args
    assert "-ErrorLimit" in tdload_cmd, "ErrorLimit option not found in tdload command"
    assert "100" in tdload_cmd, "ErrorLimit value '100' not found in tdload command"
    # Check that LogLevel option is included in the command
    assert "-LogLevel" in tdload_cmd, "LogLevel option not found in tdload command"
    # Check that the DEBUG value is included in the command
    assert "DEBUG" in tdload_cmd, "LogLevel value 'DEBUG' not found in tdload command"
    # The result should be returned
    assert result == "TDLOAD OK"
