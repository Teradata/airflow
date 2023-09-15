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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from flask import Flask

    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.cli.cli_config import CLICommand
    from airflow.www.security_manager import AirflowSecurityManagerV2


class BaseAuthManager(LoggingMixin):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...
    """

    def __init__(self, app: Flask) -> None:
        self._security_manager: AirflowSecurityManagerV2 | None = None
        self.app = app

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI.

        Override this method to expose commands via Airflow CLI to manage this auth manager.
        """
        return []

    @abstractmethod
    def get_user_name(self) -> str:
        """Return the username associated to the user in session."""

    @abstractmethod
    def get_user_display_name(self) -> str:
        """Return the user's display name associated to the user in session."""

    @abstractmethod
    def get_user(self) -> BaseUser:
        """Return the user associated to the user in session."""

    @abstractmethod
    def get_user_id(self) -> str:
        """Return the user ID associated to the user in session."""

    @abstractmethod
    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""

    @abstractmethod
    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""

    @abstractmethod
    def get_url_logout(self) -> str:
        """Return the logout page url."""

    @abstractmethod
    def get_url_user_profile(self) -> str | None:
        """Return the url to a page displaying info about the current user."""

    def get_security_manager_override_class(self) -> type:
        """
        Return the security manager override class.

        The security manager override class is responsible for overriding the default security manager
        class airflow.www.security_manager.AirflowSecurityManagerV2 with a custom implementation.
        This class is essentially inherited from airflow.www.security_manager.AirflowSecurityManagerV2.

        By default, return the generic AirflowSecurityManagerV2.
        """
        from airflow.www.security_manager import AirflowSecurityManagerV2

        return AirflowSecurityManagerV2

    @property
    def security_manager(self) -> AirflowSecurityManagerV2:
        """Get the security manager."""
        if not self._security_manager:
            raise AirflowException("Security manager not defined.")
        return self._security_manager

    @security_manager.setter
    def security_manager(self, security_manager: AirflowSecurityManagerV2):
        """
        Set the security manager.

        :param security_manager: the security manager
        """
        self._security_manager = security_manager
