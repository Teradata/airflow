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

from abc import abstractmethod

from airflow.utils.log.logging_mixin import LoggingMixin


class BaseAuthManager(LoggingMixin):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...
    """

    @abstractmethod
    def get_user_name(self) -> str:
        """Return the username associated to the user in session."""
        ...

    @abstractmethod
    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        ...

    def get_security_manager_override_class(self) -> type:
        """
        Return the security manager override class.

        The security manager override class is responsible for overriding the default security manager
        class airflow.www.security.AirflowSecurityManager with a custom implementation. This class is
        essentially inherited from airflow.www.security.AirflowSecurityManager.

        By default, return an empty class.
        """
        return object
