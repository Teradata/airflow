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
from datetime import datetime
from typing import List, Optional, Tuple

import boto3
from botocore.client import BaseClient

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, purge_logs

DAG_ID = 'example_glue'

# Externally fetched variables:
# Role needs S3 putobject/getobject access as well as the glue service role,
# see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
ROLE_ARN_KEY = 'ROLE_ARN'

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# Example csv data used as input to the example AWS Glue Job.
EXAMPLE_CSV = '''
apple,0.5
milk,2.5
bread,4.0
'''

# Example Spark script to operate on the above sample csv data.
EXAMPLE_SCRIPT = '''
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
datasource = glueContext.create_dynamic_frame.from_catalog(
             database='{db_name}', table_name='input')
print('There are %s items in the table' % datasource.count())

datasource.toDF().write.format('csv').mode("append").save('s3://{bucket_name}/output')
'''


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(job_id: str, glue_crawler_name: str) -> None:
    """
    Glue generates four Cloudwatch log groups and multiple log streams and leaves them.
    """
    generated_log_groups: List[Tuple[str, Optional[str]]] = [
        # Format: ('log group name', 'log stream prefix')
        ('/aws-glue/crawlers', glue_crawler_name),
        ('/aws-glue/jobs/logs-v2', job_id),
        ('/aws-glue/jobs/error', job_id),
        ('/aws-glue/jobs/output', job_id),
    ]

    purge_logs(generated_log_groups)


@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(glue_crawler_name: str, glue_job_name: str, glue_db_name: str) -> None:
    client: BaseClient = boto3.client('glue')

    client.delete_crawler(Name=glue_crawler_name)
    client.delete_job(JobName=glue_job_name)
    client.delete_database(Name=glue_db_name)


@task
def set_up(env_id, role_arn):
    glue_crawler_name = f'{env_id}_crawler'
    glue_db_name = f'{env_id}_glue_db'
    glue_job_name = f'{env_id}_glue_job'
    bucket_name = f'{env_id}-bucket'

    role_name = role_arn.split('/')[-1]

    glue_crawler_config = {
        'Name': glue_crawler_name,
        'Role': role_arn,
        'DatabaseName': glue_db_name,
        'Targets': {'S3Targets': [{'Path': f'{bucket_name}/input'}]},
    }

    ti = get_current_context()['ti']
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='glue_db_name', value=glue_db_name)
    ti.xcom_push(key='glue_crawler_config', value=glue_crawler_config)
    ti.xcom_push(key='glue_crawler_name', value=glue_crawler_name)
    ti.xcom_push(key='glue_job_name', value=glue_job_name)
    ti.xcom_push(key='role_name', value=role_name)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_setup = set_up(
        env_id=test_context[ENV_ID_KEY],
        role_arn=test_context[ROLE_ARN_KEY],
    )

    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name=test_setup['bucket_name'],
    )

    upload_csv = S3CreateObjectOperator(
        task_id='upload_csv',
        s3_bucket=test_setup['bucket_name'],
        s3_key='input/input.csv',
        data=EXAMPLE_CSV,
        replace=True,
    )

    upload_script = S3CreateObjectOperator(
        task_id='upload_script',
        s3_bucket=test_setup['bucket_name'],
        s3_key='etl_script.py',
        data=EXAMPLE_SCRIPT.format(db_name=test_setup['glue_db_name'], bucket_name=test_setup['bucket_name']),
        replace=True,
    )

    # [START howto_operator_glue_crawler]
    crawl_s3 = GlueCrawlerOperator(
        task_id='crawl_s3',
        config=test_setup['glue_crawler_config'],
        # Waits by default, set False to test the Sensor below
        wait_for_completion=False,
    )
    # [END howto_operator_glue_crawler]

    # [START howto_sensor_glue_crawler]
    wait_for_crawl = GlueCrawlerSensor(
        task_id='wait_for_crawl',
        crawler_name=test_setup['glue_crawler_name'],
    )
    # [END howto_sensor_glue_crawler]

    # [START howto_operator_glue]
    submit_glue_job = GlueJobOperator(
        task_id='submit_glue_job',
        job_name=test_setup['glue_job_name'],
        script_location=f's3://{test_setup["bucket_name"]}/etl_script.py',
        s3_bucket=test_setup['bucket_name'],
        iam_role_name=test_setup['role_name'],
        create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 2, 'WorkerType': 'G.1X'},
        # Waits by default, set False to test the Sensor below
        wait_for_completion=False,
    )
    # [END howto_operator_glue]

    # [START howto_sensor_glue]
    wait_for_job = GlueJobSensor(
        task_id='wait_for_job',
        job_name=test_setup['glue_job_name'],
        # Job ID extracted from previous Glue Job Operator task
        run_id=submit_glue_job.output,
    )
    # [END howto_sensor_glue]

    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=test_setup['bucket_name'],
        force_delete=True,
    )

    clean_up = glue_cleanup(
        test_setup['glue_crawler_name'],
        test_setup['glue_job_name'],
        test_setup['glue_db_name'],
    )

    chain(
        # TEST SETUP
        test_context,
        test_setup,
        create_bucket,
        upload_csv,
        upload_script,
        # TEST BODY
        crawl_s3,
        wait_for_crawl,
        submit_glue_job,
        wait_for_job,
        # TEST TEARDOWN
        clean_up,
        delete_bucket,
        delete_logs(submit_glue_job.output, test_setup['glue_crawler_name']),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
