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
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.suite.transfers.gcs_to_gdrive import GCSToGoogleDriveOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gdrive_to_gcs_with_gdrive_sensor"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

OBJECT = "abc123xyz"
FOLDER_ID = ""
FILE_NAME = "example_upload.txt"
LOCAL_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        source_bucket=BUCKET_NAME,
        source_object=FILE_NAME,
        destination_object=FILE_NAME,
    )

    # [START detect_file]
    detect_file = GoogleDriveFileExistenceSensor(
        task_id="detect_file", folder_id=FOLDER_ID, file_name=FILE_NAME
    )
    # [END detect_file]

    # [START upload_gdrive_to_gcs]
    upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
        task_id="upload_gdrive_object_to_gcs",
        folder_id=FOLDER_ID,
        file_name=FILE_NAME,
        bucket_name=BUCKET_NAME,
        object_name=OBJECT,
    )
    # [END upload_gdrive_to_gcs]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        >> copy_single_file
        # TEST BODY
        >> detect_file
        >> upload_gdrive_to_gcs
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
