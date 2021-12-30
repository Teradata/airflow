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

"""adding index for dag_id in job

Revision ID: 587bdf053233
Revises: f9da662e7089
Create Date: 2021-12-14 10:20:12.482940

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '587bdf053233'
down_revision = 'f9da662e7089'
branch_labels = None
depends_on = None


def upgrade():
    """Apply adding index for dag_id in job"""
    op.create_index('idx_job_dag_id', 'job', ['dag_id'], unique=False)


def downgrade():
    """Unapply adding index for dag_id in job"""
    op.drop_index('idx_job_dag_id', table_name='job')
