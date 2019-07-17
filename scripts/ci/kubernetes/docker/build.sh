#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

IMAGE=${IMAGE:-airflow}
TAG=${TAG:-latest}
DIRNAME=$(cd "$(dirname "$0")" && pwd)
AIRFLOW_ROOT="${DIRNAME}/../../../.."
PYTHON_DOCKER_IMAGE=python:3.6-slim

set -e

# Don't rebuild the image more than once on travis
if [[ -n "${TRAVIS}" || -z "${AIRFLOW_CI_REUSE_K8S_IMAGE}" ]] && \
    docker image inspect "${IMAGE}:${TAG}" > /dev/null 2>/dev/null; then
  echo "Re-using existing image"
  exit 0
fi

if [[ "${VM_DRIVER:-none}" != "none" ]]; then
    if ENVCONFIG=$(minikube docker-env); then
      eval "${ENVCONFIG}"
    fi
fi

echo "Airflow directory ${AIRFLOW_ROOT}"
echo "Airflow Docker directory ${DIRNAME}"

cd "${AIRFLOW_ROOT}"
docker run -ti --rm -v "${AIRFLOW_ROOT}:/airflow" \
    -w /airflow "${PYTHON_DOCKER_IMAGE}" ./scripts/ci/kubernetes/docker/compile.sh

sudo rm -rf "${AIRFLOW_ROOT}/airflow/www/node_modules"

echo "Copy distro ${AIRFLOW_ROOT}/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz"
cp ${AIRFLOW_ROOT}/dist/*.tar.gz "${DIRNAME}/airflow.tar.gz"
cd "${DIRNAME}" && docker build --pull "${DIRNAME}" --tag="${IMAGE}:${TAG}"
rm "${DIRNAME}/airflow.tar.gz"
