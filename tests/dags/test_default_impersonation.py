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

import textwrap
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    "owner": "airflow",
    "start_date": DEFAULT_DATE,
}

dag = DAG(dag_id="test_default_impersonation", schedule=None, default_args=args)

deelevated_user = "airflow_test_user"

test_command = textwrap.dedent(
    f"""\
    if [ '{deelevated_user}' != "$(whoami)" ]; then
        echo current user $(whoami) is not {deelevated_user}!
        exit 1
    fi
    """
)

task = BashOperator(
    task_id="test_deelevated_user",
    bash_command=test_command,
    dag=dag,
)
