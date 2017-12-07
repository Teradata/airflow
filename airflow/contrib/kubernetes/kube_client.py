# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def load_kube_config(in_cluster=True):
    from kubernetes import config, client
    if in_cluster:
        config.load_incluster_config()
    else:
        try:
            config.load_kube_config()
            return client.CoreV1Api()
        except NotImplementedError:
            NotImplementedError(
                "requires incluster config or defined configuration in airflow.cfg")


def get_kube_client(in_cluster=True):
    # TODO: This should also allow people to point to a cluster.

    from kubernetes import client
    load_kube_config(in_cluster)
    return client.CoreV1Api()
