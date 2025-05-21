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

from collections.abc import Mapping

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.bteq import BteqHook
from airflow.providers.teradata.hooks.teradata import TeradataHook


class BteqOperator(BaseOperator):
    """
    Teradata Operator to execute BTEQ (Basic Teradata Query) scripts using Teradata BTEQ utility.

    This supports execution of BTEQ scripts either locally or remotely via SSH.

    The BTEQ scripts are used to interact with Teradata databases, allowing users to perform
    operations such as querying, data manipulation, and administrative tasks.

    Features:
    - Supports both local and remote execution of BTEQ scripts.
    - Handles connection details, script preparation, and execution.
    - Provides robust error handling and logging for debugging.
    - Allows configuration of session parameters like output width and encoding.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataOperator`


    :param bteq: The BTEQ script to be executed. (templated) :param xcom_push_flag: Whether to push the
    result of the BTEQ execution to XCom. Defaults to True. :param teradata_conn_id: Reference to a
    specific Teradata connection. :param ssh_conn_id: Optional SSH connection ID for remote execution. This
    parameter is used only for remote execution.
    """

    template_fields = "bteq"
    template_ext = (
        ".sql",
        ".bteq",
    )
    ui_color = "#ff976d"

    def __init__(
        self,
        *,
        bteq: str,
        xcom_push_flag: bool = True,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        ssh_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bteq = bteq
        self.xcom_push_flag = xcom_push_flag
        self.teradata_conn_id = teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self._hook: BteqHook | None = None

    def execute(self, context: Mapping) -> str | None:
        """Execute BTEQ code using the BteqHook."""
        self._hook = BteqHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)
        result = self._hook.execute_bteq(self.bteq, self.xcom_push_flag)
        return result if self.xcom_push_flag else None

    def on_kill(self) -> None:
        """Handle task termination by invoking the on_kill method of BteqHook."""
        if self._hook:
            self._hook.on_kill()
        else:
            self.log.warning("BteqHook was not initialized. Nothing to terminate.")
