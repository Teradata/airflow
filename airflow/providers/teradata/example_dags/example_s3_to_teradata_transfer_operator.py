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
"""
Example Airflow DAG to show usage of S3 to teradata transfer operator

The transfer operator connects to source teradata server, runs query to fetch data from source
and inserts that data into destination teradata database server. It assumes tables already exists.
The example DAG below assumes Airflow Connection with connection id `teradata_default` already exists.
It creates sample my_users table at source and destination, sets up sample data at source and then
runs transfer operator to copy data to corresponding table on destination server.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from airflow import DAG
from airflow.models.baseoperator import chain

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.s3_to_teradata import S3ToTeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

CONN_ID = "teradata_default"

with DAG(
    dag_id="example_s3_to_teradata_transfer_operator",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    # [START howto_transfer_operator_s3_to_teradata]

    drop_table_ifexists = TeradataOperator(
        task_id="drop_table_ifexists",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata;
            """,
    )

    transfer_data = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/CSVDATA/",
        teradata_table="example_s3_teradata",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default"
    )

    read_data_dest = TeradataOperator(
        task_id="read_data_dest",
        conn_id=CONN_ID,
        sql="""
            SELECT TOP 10 * from example_s3_teradata;
        """,
    )

    drop_table = TeradataOperator(
        task_id="drop_table",
        conn_id=CONN_ID,
        sql="""
            DROP TABLE example_s3_teradata;
        """,
    )

    chain(
        drop_table_ifexists,
        transfer_data,
        read_data_dest,
        drop_table
    )

    # Make sure create was done before deleting table
    drop_table_ifexists >> transfer_data >> drop_table
    # [END howto_transfer_operator_s3_to_teradata]
