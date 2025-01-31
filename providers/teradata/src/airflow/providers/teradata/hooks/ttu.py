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

import os
import uuid
import subprocess
from typing import Any, Dict, Iterator, List, Optional, Union
from tempfile import gettempdir, NamedTemporaryFile, TemporaryDirectory

from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook

class TtuHook(BaseHook, LoggingMixin):
    """
    Interact with Teradata using Teradata Tools and Utilities (TTU) binaries.
    Note: it is required that TTU previously installed and configured propertly.

    extras example: ``{"bteq_quit_zero":true, "bteq_session_encoding";"UTF8"}``
    """
    conn_name_attr = 'ttu_conn_id'
    default_conn_name = 'ttu_default'
    conn_type = 'ttu'
    hook_name = 'TTU'

    def __init__(self, ttu_conn_id: str = 'ttu_default') -> None:
        super().__init__()
        self.ttu_conn_id = ttu_conn_id
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn is not None:
            self.close_conn()

    def get_conn(self) -> dict:
        if not self.conn:
            connection = self.get_connection(self.ttu_conn_id)
            extras = connection.extra_dejson
            self.conn = dict(
                login=connection.login,
                password=connection.password,
                host=connection.host,
                ttu_log_folder=extras.get('ttu_log_folder', '/tmp'),
                console_output_encoding=extras.get('console_output_encoding', 'utf-8'),
                bteq_session_encoding=extras.get('bteq_session_encoding', 'ASCII'),
                bteq_output_width=extras.get('bteq_output_width', 65531),
                bteq_quit_zero=extras.get('bteq_quit_zero', False),
                sp = None
                )
        return self.conn

    def close_conn(self):
        """
        Closes the connection. An error will occur if the
        connection wasn't ever opened.
        """
        self.conn = None

    def execute_bteq(self, bteq, xcom_push_flag=False):
        """
        Executes BTEQ sentences using BTEQ binary.
        :param bteq: string of BTEQ sentences
        :param xcom_push_flag: Flag for pushing last line of BTEQ Log to XCom
        """
        conn = self.get_conn()
        self.log.info("Executing BTEQ sentences...")
        with TemporaryDirectory(prefix='airflowtmp_ttu_bteq_') as tmpdir:
            with NamedTemporaryFile(dir=tmpdir, mode='wb') as tmpfile:
                bteq_file = self._prepare_bteq_script(bteq,
                                                       conn['host'],
                                                       conn['login'],
                                                       conn['password'],
                                                       conn['bteq_output_width'],
                                                       conn['bteq_session_encoding'],
                                                       conn['bteq_quit_zero']
                                                       )
                self.log.debug(bteq_file)
                tmpfile.write(bytes(bteq_file,'UTF8'))
                tmpfile.flush()
                tmpfile.seek(0)

                conn['sp'] = subprocess.Popen(['bteq'],
                    stdin=tmpfile,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmpdir,
                    preexec_fn=os.setsid)

                line = ''
                failure_line = 'unknown reasons. Please see full BTEQ Output for more details.'
                self.log.info("Output:")
                for line in iter(conn['sp'].stdout.readline, b''):
                    line = line.decode(conn['console_output_encoding']).strip()
                    self.log.info(line)
                    if "Failure" in line:
                        #Just save the last failure
                        failure_line = line
                conn['sp'].wait()

                self.log.info("BTEQ command exited with return code {0}".format(conn['sp'].returncode))

                if conn['sp'].returncode:
                    raise AirflowException("BTEQ command exited with return code " + str(conn['sp'].returncode) + ' because of ' +
                                           failure_line)
                if xcom_push_flag:
                    return line

    def on_kill(self):
        self.log.debug('Killing child process...')
        conn = self.get_conn()
        conn['sp'].kill()

    @staticmethod
    def _prepare_bteq_script(bteq_string, host, login, password, bteq_output_width, bteq_session_encoding, bteq_quit_zero) -> str:
        """
        Prepare a BTEQ file with connection parameters for executing SQL Sentences with BTEQ syntax.
        :param bteq_string : bteq sentences to execute
        :param host : Teradata Host
        :param login : username for login
        :param password : password for login
        :param bteq_output_width : width of BTEQ output in console
        :param bteq_session_encoding : session encoding. See offical teradata docs for possible values
        :param bteq_quit_zero : if True, force a .QUIT 0 sentence at the end of the sentences (forcing return code = 0)
        """
        bteq_list = [".LOGON {}/{},{};".format(host, login, password)]
        bteq_list += [".SET WIDTH " + str(bteq_output_width) + ";"]
        bteq_list += [".SET SESSION CHARSET '" + bteq_session_encoding + "';"]
        bteq_list += [bteq_string]
        if bteq_quit_zero:
            bteq_list += [".QUIT 0;"]
        bteq_list += [".LOGOFF;"]
        bteq_list += [".EXIT;"]
        return "\n".join(bteq_list)




