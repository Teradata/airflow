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
from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.ttu import TtuHook

class BteqOperator(BaseOperator):
    """
    Executes BTEQ code in a specific Teradata database.

    :param bteq: The BTEQ code to be executed. (templated)
    :type bteq: str
    :param xcom_push: Whether to push the result of the BTEQ execution to XCom. Defaults to True.
    :type xcom_push: bool
    :param ttu_conn_id: Reference to a specific Teradata TTU connection. Defaults to TtuHook.default_conn_name.
    :type ttu_conn_id: str
    """

    template_fields = ('bteq',)
    template_ext = ('.sql', '.bteq',)
    ui_color = '#ff976d'

    def __init__(
        self,
        *,
        bteq: str,
        xcom_push: bool = True,
        ttu_conn_id: str = TtuHook.default_conn_name,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.bteq = bteq
        self.xcom_push = xcom_push
        self.ttu_conn_id = ttu_conn_id
        self._hook: Optional[TtuHook] = None

    def execute(self, context: Mapping) -> None:
        """
        Executes the provided BTEQ string using the TtuHook.
        """
        self.log.info("Initializing TtuHook with connection ID: %s", self.ttu_conn_id)
        self._hook = TtuHook(ttu_conn_id=self.ttu_conn_id)
        self.log.info("Executing BTEQ script...")
        self._hook.execute_bteq(self.bteq, self.xcom_push)
        self.log.info("BTEQ script execution completed.")

    def on_kill(self) -> None:
        """
        Handles task termination by invoking the on_kill method of TtuHook.
        """
        if self._hook:
            self.log.info("Terminating BTEQ execution...")
            self._hook.on_kill()
        else:
            self.log.warning("TtuHook was not initialized. Nothing to terminate.")


