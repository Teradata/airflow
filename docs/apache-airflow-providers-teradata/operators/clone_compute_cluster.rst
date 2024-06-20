 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. _howto/operator:TeradataCloneComputeClusterGroupOperator:


========================================
TeradataCloneComputeClusterGroupOperator
========================================

The purpose of ``TeradataCloneComputeClusterGroupOperator`` is to clone a compute cluster group from other
compute cluster group either with profiles or without profiles.
Use the :class:`TeradataCloneComputeClusterGroupOperator <airflow.providers.teradata.operators.teradata_clone_compute_cluster>`
to clone the compute cluster group in a Teradata Vantage Cloud Lake.



An example usage of the TeradataCloneComputeClusterGroupOperator to clone a compute cluster group from other
compute cluster group with profiles in a Teradata Vantage Cloud Lake is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata_compute_cluster_group_clone.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_copy_profiles_from_a_group_to_new_group]
    :end-before: [END teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_copy_profiles_from_a_group_to_new_group]

An example usage of the TeradataCloneComputeClusterGroupOperator to clone a compute cluster group from other
compute cluster group without profiles in a Teradata Vantage Cloud Lake is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata_compute_cluster_group_clone.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_create_new_group_from_a_group_without_profiles]
    :end-before: [END teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_create_new_group_from_a_group_without_profiles]


.. _howto/operator:TeradataCloneComputeClusterProfileOperator:


==========================================
TeradataCloneComputeClusterProfileOperator
==========================================

The purpose of ``TeradataCloneComputeClusterProfileOperator`` is to clone a compute cluster profile from another compute cluster profile.
Use the :class:`TeradataCloneComputeClusterProfileOperator <airflow.providers.teradata.operators.teradata_clone_compute_cluster>`
to clone a compute cluster profile in a Teradata Vantage Cloud Lake.



An example usage of the TeradataCloneComputeClusterProfileOperator to clone a compute cluster profile from another compute cluster profile
in same compute group is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata_compute_cluster_profile_clone.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_from_other_profile_in_same_group]
    :end-before: [END teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_from_other_profile_in_same_group]

An example usage of the TeradataCloneComputeClusterProfileOperator to clone a compute cluster profile from another compute cluster profile
in different compute group is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata_compute_cluster_profile_clone.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_in_another_group_from_a_profile_in_a_group]
    :end-before: [END teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_in_another_group_from_a_profile_in_a_group]

