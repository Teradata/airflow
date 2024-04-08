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

import fnmatch
import os
import re
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Sequence, cast

from deprecated import deprecated

from airflow.configuration import conf
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.triggers.s3 import S3KeysUnchangedTrigger, S3KeyTrigger
from airflow.sensors.base import BaseSensorOperator, poke_mode_only


class TeradataComputeClusterSensor(BaseSensorOperator):
    """
    Waits for one or multiple keys (a file-like instance on S3) to be present in a S3 bucket.

    The path is just a key/value pointer to a resource for the given S3 path.
    Note: S3 does not support folders directly, and only provides key/value pairs.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:S3KeySensor`

    :param bucket_key: The key(s) being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full ``s3://`` url. When specified, all the keys passed to ``bucket_key``
        refers to this bucket
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param check_fn: Function that receives the list of the S3 objects,
        and returns a boolean:
        - ``True``: the criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(files: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in files)
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether to verify SSL certificates for S3 connection.
        By default, SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param deferrable: Run operator in the deferrable mode
    :param use_regex: whether to use regex to check bucket
    """

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        *,
        conn_id: str,
        end_time: float,
        compute_profile_name: str,
        computer_group_name: str | None = None,
        type: str | None = None,
        poll_interval: float | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.end_time = end_time
        self.compute_profile_name = compute_profile_name
        self.computer_group_name = computer_group_name
        self.type = type
        self.poll_interval = poll_interval

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self._defer()

    def _defer(self) -> None:
        """Check for a keys in s3 and defers using the triggerer."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=TeradataComputeClusterSyncTrigger(
                conn_id=cast(str, self.conn_id),
                end_time=self.end_time,
                compute_profile_name=self.compute_profile_name,
                computer_group_name=self.computer_group_name,
                type=self.type,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "running":
            found_keys = self.check_fn(event["files"])  # type: ignore[misc]
            if not found_keys:
                self._defer()
        elif event["status"] == "error":
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(event["message"])
            raise AirflowException(event["message"])
