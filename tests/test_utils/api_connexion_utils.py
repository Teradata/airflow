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
from contextlib import contextmanager

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.www.security import EXISTING_ROLES


@contextmanager
def create_user_scope(app, username, **kwargs):
    """
    Helper function designed to be used with pytest fixture mainly.
    It will create a user and provide it for the fixture via YIELD (generator)
    then will tidy up once test is complete
    """
    test_user = create_user(app, username, **kwargs)

    try:
        yield test_user
    finally:
        delete_user(app, username)


def create_user(app, username, role_name=None, email=None, permissions=None):
    appbuilder = app.appbuilder

    # Removes user and role so each test has isolated test data.
    delete_user(app, username)
    role = None
    if role_name:
        delete_role(app, role_name)
        role = create_role(app, role_name, permissions)

    return appbuilder.sm.add_user(
        username=username,
        first_name=username,
        last_name=username,
        email=email or f"{username}@example.org",
        role=role,
        password=username,
    )


def create_role(app, name, permissions=None):
    appbuilder = app.appbuilder
    role = appbuilder.sm.find_role(name)
    if not role:
        role = appbuilder.sm.add_role(name)
    if not permissions:
        permissions = []
    for permission in permissions:
        perm_object = appbuilder.sm.get_permission(*permission)
        appbuilder.sm.add_permission_role(role, perm_object)
    return role


def delete_role(app, name):
    if app.appbuilder.sm.find_role(name):
        app.appbuilder.sm.delete_role(name)


def delete_roles(app):
    for role in app.appbuilder.sm.get_all_roles():
        if role.name not in EXISTING_ROLES:
            app.appbuilder.sm.delete_role(role.name)


def delete_user(app, username):
    appbuilder = app.appbuilder
    for user in appbuilder.sm.get_all_users():
        if user.username == username:
            _ = [
                delete_role(app, role.name) for role in user.roles if role and role.name not in EXISTING_ROLES
            ]
            appbuilder.sm.del_register_user(user)
            break


def assert_401(response):
    assert response.status_code == 401, f"Current code: {response.status_code}"
    assert response.json == {
        'detail': None,
        'status': 401,
        'title': 'Unauthorized',
        'type': EXCEPTIONS_LINK_MAP[401],
    }
