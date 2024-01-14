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

    drop_table_csv_exists = TeradataOperator(
        task_id="drop_table_csv_exists",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_csv;
            """,
    )

    drop_table_json_exists = TeradataOperator(
        task_id="drop_table_json_exists",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_json;
            """,
        trigger_rule="all_done",
    )

    drop_table_parquet_exists = TeradataOperator(
        task_id="drop_table_parquet_exists",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_parquet;
            """,
        trigger_rule="all_done",
    )

    drop_table_access_exists = TeradataOperator(
        task_id="drop_table_access_exists",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_access;
            """,
        trigger_rule="all_done",
    )

    transfer_data_csv = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_csv",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/CSVDATA/",
        teradata_table="example_s3_teradata_csv",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )

    transfer_data_json = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_json",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/JSONDATA/",
        teradata_table="example_s3_teradata_json",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )

    transfer_data_parquet = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_parquet",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/PARQUETDATA/",
        teradata_table="example_s3_teradata_parquet",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )

    transfer_data_access = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_access",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/CSVDATA/",
        teradata_table="example_s3_teradata_access",
        aws_access_key="",
        aws_access_secret="",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )

    read_data_table_csv = TeradataOperator(
        task_id="read_data_table_csv",
        conn_id=CONN_ID,
        sql="""
                SELECT * from example_s3_teradata_csv;
            """,
    )

    read_data_table_json = TeradataOperator(
        task_id="read_data_table_json",
        conn_id=CONN_ID,
        sql="""
                SELECT * from example_s3_teradata_json;
            """,
    )

    read_data_table_parquet = TeradataOperator(
        task_id="read_data_table_parquet",
        conn_id=CONN_ID,
        sql="""
                SELECT * from example_s3_teradata_parquet;
            """,
    )

    read_data_table_access = TeradataOperator(
        task_id="read_data_table_access",
        conn_id=CONN_ID,
        sql="""
            SELECT * from example_s3_teradata_access;
            """,
    )

    drop_table_csv = TeradataOperator(
        task_id="drop_table_csv",
        conn_id=CONN_ID,
        sql="""
            DROP TABLE example_s3_teradata_csv;
        """,
    )

    drop_table_json = TeradataOperator(
        task_id="drop_table_json",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_json;
            """,
    )

    drop_table_parquet = TeradataOperator(
        task_id="drop_table_parquet",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_parquet;
            """,
    )

    drop_table_access = TeradataOperator(
        task_id="drop_table_access",
        conn_id=CONN_ID,
        sql="""
               DROP TABLE example_s3_teradata_access;
           """,
    )

    chain(
        drop_table_csv_exists,
        drop_table_json_exists,
        drop_table_parquet_exists,
        drop_table_access_exists,
        transfer_data_csv,
        transfer_data_json,
        transfer_data_parquet,
        transfer_data_access,
        read_data_table_csv,
        read_data_table_json,
        read_data_table_parquet,
        read_data_table_access,
        drop_table_csv,
        drop_table_json,
        drop_table_parquet,
        drop_table_access
    )

    # Make sure create was done before deleting table
    drop_table_csv_exists >> transfer_data_csv >> drop_table_csv
    drop_table_json_exists >> transfer_data_json >> drop_table_json
    drop_table_parquet_exists >> transfer_data_parquet >> drop_table_parquet
    drop_table_access_exists >> transfer_data_access >> drop_table_access
    # [END howto_transfer_operator_s3_to_teradata]
