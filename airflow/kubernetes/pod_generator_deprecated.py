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
This module provides an interface between the previous Pod
API and outputs a kubernetes.client.models.V1Pod.
The advantage being that the full Kubernetes API
is supported and no serialization need be written.
"""
import copy
import hashlib
import re
import uuid
from typing import Dict, List, Optional, Union

from kubernetes.client import models as k8s

MAX_POD_ID_LEN = 253

MAX_LABEL_LEN = 63


class PodDefaults:
    """Static defaults for Pods"""

    XCOM_MOUNT_PATH = '/airflow/xcom'
    SIDECAR_CONTAINER_NAME = 'airflow-xcom-sidecar'
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 30; done;'
    VOLUME_MOUNT = k8s.V1VolumeMount(name='xcom', mount_path=XCOM_MOUNT_PATH)
    VOLUME = k8s.V1Volume(name='xcom', empty_dir=k8s.V1EmptyDirVolumeSource())
    SIDECAR_CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=['sh', '-c', XCOM_CMD],
        image='alpine',
        volume_mounts=[VOLUME_MOUNT],
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
            }
        ),
    )


def make_safe_label_value(string):
    """
    Valid label values must be 63 characters or less and must be empty or begin and
    end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
    dots (.), and alphanumerics between.

    If the label value is greater than 63 chars once made safe, or differs in any
    way from the original value sent to this function, then we need to truncate to
    53 chars, and append it with a unique hash.
    """
    safe_label = re.sub(r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$", "", string)

    if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
        safe_hash = hashlib.md5(string.encode()).hexdigest()[:9]
        safe_label = safe_label[: MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

    return safe_label


class PodGenerator:
    """
    Contains Kubernetes Airflow Worker configuration logic

    Represents a kubernetes pod and manages execution of a single pod.
    Any configuration that is container specific gets applied to
    the first container in the list of containers.

    :param image: The docker image
    :type image: Optional[str]
    :param name: name in the metadata section (not the container name)
    :type name: Optional[str]
    :param namespace: pod namespace
    :type namespace: Optional[str]
    :param volume_mounts: list of kubernetes volumes mounts
    :type volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]]
    :param envs: A dict containing the environment variables
    :type envs: Optional[Dict[str, str]]
    :param cmds: The command to be run on the first container
    :type cmds: Optional[List[str]]
    :param args: The arguments to be run on the pod
    :type args: Optional[List[str]]
    :param labels: labels for the pod metadata
    :type labels: Optional[Dict[str, str]]
    :param node_selectors: node selectors for the pod
    :type node_selectors: Optional[Dict[str, str]]
    :param ports: list of ports. Applies to the first container.
    :type ports: Optional[List[Union[k8s.V1ContainerPort, dict]]]
    :param volumes: Volumes to be attached to the first container
    :type volumes: Optional[List[Union[k8s.V1Volume, dict]]]
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param restart_policy: The restart policy of the pod
    :type restart_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a comma separated list:
        secret_a,secret_b
    :type image_pull_secrets: str
    :param init_containers: A list of init containers
    :type init_containers: Optional[List[k8s.V1Container]]
    :param service_account_name: Identity for processes that run in a Pod
    :type service_account_name: Optional[str]
    :param resources: Resource requirements for the first containers
    :type resources: Optional[Union[k8s.V1ResourceRequirements, dict]]
    :param annotations: annotations for the pod
    :type annotations: Optional[Dict[str, str]]
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: Optional[dict]
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: Optional[list]
    :param security_context: A dict containing the security context for the pod
    :type security_context: Optional[Union[k8s.V1PodSecurityContext, dict]]
    :param configmaps: Any configmap refs to envfrom.
        If more than one configmap is required, provide a comma separated list
        configmap_a,configmap_b
    :type configmaps: List[str]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: Optional[str]
    :param schedulername: Specify a schedulername for the pod
    :type schedulername: Optional[str]
    :param pod: The fully specified pod. Mutually exclusive with `path_or_string`
    :type pod: Optional[kubernetes.client.models.V1Pod]
    :param extract_xcom: Whether to bring up a container for xcom
    :type extract_xcom: bool
    :param priority_class_name: priority class name for the launched Pod
    :type priority_class_name: str
    """

    def __init__(
        self,
        image: Optional[str] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]] = None,
        envs: Optional[Dict[str, str]] = None,
        cmds: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
        labels: Optional[Dict[str, str]] = None,
        node_selectors: Optional[Dict[str, str]] = None,
        ports: Optional[List[Union[k8s.V1ContainerPort, dict]]] = None,
        volumes: Optional[List[Union[k8s.V1Volume, dict]]] = None,
        image_pull_policy: Optional[str] = None,
        restart_policy: Optional[str] = None,
        image_pull_secrets: Optional[str] = None,
        init_containers: Optional[List[k8s.V1Container]] = None,
        service_account_name: Optional[str] = None,
        resources: Optional[Union[k8s.V1ResourceRequirements, dict]] = None,
        annotations: Optional[Dict[str, str]] = None,
        affinity: Optional[dict] = None,
        hostnetwork: bool = False,
        tolerations: Optional[list] = None,
        security_context: Optional[Union[k8s.V1PodSecurityContext, dict]] = None,
        configmaps: Optional[List[str]] = None,
        dnspolicy: Optional[str] = None,
        schedulername: Optional[str] = None,
        extract_xcom: bool = False,
        priority_class_name: Optional[str] = None,
    ):

        self.pod = k8s.V1Pod()
        self.pod.api_version = 'v1'
        self.pod.kind = 'Pod'

        # Pod Metadata
        self.metadata = k8s.V1ObjectMeta()
        self.metadata.labels = labels
        self.metadata.name = name
        self.metadata.namespace = namespace
        self.metadata.annotations = annotations

        # Pod Container
        self.container = k8s.V1Container(name='base')
        self.container.image = image
        self.container.env = []

        if envs:
            if isinstance(envs, dict):
                for key, val in envs.items():
                    self.container.env.append(k8s.V1EnvVar(name=key, value=val))
            elif isinstance(envs, list):
                self.container.env.extend(envs)

        configmaps = configmaps or []
        self.container.env_from = []
        for configmap in configmaps:
            self.container.env_from.append(
                k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap))
            )

        self.container.command = cmds or []
        self.container.args = args or []
        if image_pull_policy:
            self.container.image_pull_policy = image_pull_policy
        self.container.ports = ports or []
        self.container.resources = resources
        self.container.volume_mounts = volume_mounts or []

        # Pod Spec
        self.spec = k8s.V1PodSpec(containers=[])
        self.spec.security_context = security_context
        self.spec.tolerations = tolerations
        if dnspolicy:
            self.spec.dns_policy = dnspolicy
        self.spec.scheduler_name = schedulername
        self.spec.host_network = hostnetwork
        self.spec.affinity = affinity
        self.spec.service_account_name = service_account_name
        self.spec.init_containers = init_containers
        self.spec.volumes = volumes or []
        self.spec.node_selector = node_selectors
        if restart_policy:
            self.spec.restart_policy = restart_policy
        self.spec.priority_class_name = priority_class_name

        self.spec.image_pull_secrets = []

        if image_pull_secrets:
            for image_pull_secret in image_pull_secrets.split(','):
                self.spec.image_pull_secrets.append(k8s.V1LocalObjectReference(name=image_pull_secret))

        # Attach sidecar
        self.extract_xcom = extract_xcom

    def gen_pod(self) -> k8s.V1Pod:
        """Generates pod"""
        result = None

        if result is None:
            result = self.pod
            result.spec = self.spec
            result.metadata = self.metadata
            result.spec.containers = [self.container]

        result.metadata.name = self.make_unique_pod_id(result.metadata.name)

        if self.extract_xcom:
            result = self.add_sidecar(result)

        return result

    @staticmethod
    def add_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
        """Adds sidecar"""
        pod_cp = copy.deepcopy(pod)
        pod_cp.spec.volumes = pod.spec.volumes or []
        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_obj(obj) -> Optional[k8s.V1Pod]:
        """Converts to pod from obj"""
        if obj is None:
            return None

        if isinstance(obj, PodGenerator):
            return obj.gen_pod()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary or non-PodGenerator '
                'object into a KubernetesExecutorConfig'
            )

        # We do not want to extract constant here from ExecutorLoader because it is just
        # A name in dictionary rather than executor selection mechanism and it causes cyclic import
        namespaced = obj.get("KubernetesExecutor", {})

        if not namespaced:
            return None

        resources = namespaced.get('resources')

        if resources is None:
            requests = {
                'cpu': namespaced.get('request_cpu'),
                'memory': namespaced.get('request_memory'),
                'ephemeral-storage': namespaced.get('ephemeral-storage'),
            }
            limits = {
                'cpu': namespaced.get('limit_cpu'),
                'memory': namespaced.get('limit_memory'),
                'ephemeral-storage': namespaced.get('ephemeral-storage'),
            }
            all_resources = list(requests.values()) + list(limits.values())
            if all(r is None for r in all_resources):
                resources = None
            else:
                resources = k8s.V1ResourceRequirements(requests=requests, limits=limits)
        namespaced['resources'] = resources
        return PodGenerator(**namespaced).gen_pod()

    @staticmethod
    def make_unique_pod_id(dag_id):
        r"""
        Kubernetes pod names must be <= 253 chars and must pass the following regex for
        validation
        ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

        :param dag_id: a dag_id with only alphanumeric characters
        :return: ``str`` valid Pod name of appropriate length
        """
        if not dag_id:
            return None

        safe_uuid = uuid.uuid4().hex
        safe_pod_id = dag_id[: MAX_POD_ID_LEN - len(safe_uuid) - 1] + "-" + safe_uuid

        return safe_pod_id
