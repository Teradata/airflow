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

.. _howto/operator:TdLoadOperator:

TdLoadOperator
==============

The ``TdLoadOperator`` is an Airflow operator that interfaces with Teradata PT Easy Loader (tdload) to perform data operations on Teradata databases. This operator leverages TPT (Teradata Parallel Transporter), eliminating the need to write manual TPT scripts.

**What is Teradata PT Easy Loader?**
A command-line interface extension for TPT that automatically determines appropriate load/unload operators based on user-provided parameters and the requested operation type.

**Key Capabilities:**

- **Data Loading:** Import data from flat files into Teradata tables
- **Data Exporting:** Extract data from Teradata tables to flat files
- **Table-to-Table Transfers:** Move data between Teradata database tables
- **Deployment Flexibility:** Execute on local or remote machines with TPT installed
- **Airflow Integration:** Seamlessly works with Airflow's scheduling, monitoring, and logging

The operator simplifies complex Teradata data operations while providing the robustness and reliability of Airflow's workflow management.

This operator enables the execution of tdload commands on either the local host machine or a remote machine where TPT is installed.

To execute data loading, exporting, or transferring operations in a Teradata database, use the
:class:`~airflow.providers.teradata.operators.tpt.TdLoadOperator`.

Key Operation Examples with TdLoadOperator
------------------------------------------

Loading data into a Teradata database table from a file
-------------------------------------------------------
You can use the TdLoadOperator to load data from a file into a Teradata database table. The following example demonstrates how to load data from a delimited text file into a Teradata table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_load_data_from_txt_file_to_table]
    :end-before: [END tdload_operator_howto_guide_load_data_from_txt_file_to_table]

Exporting data from a Teradata table to a file
----------------------------------------------
You can export data from a Teradata table to a file using the TdLoadOperator. The following example shows how to export data from a Teradata table to a CSV file:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_export_data_to_a_file]
    :end-before: [END tdload_operator_howto_guide_export_data_to_a_file]

Transferring data between Teradata tables
-----------------------------------------
The TdLoadOperator can also be used to transfer data between two Teradata tables, potentially across different databases:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_load_data_from_table_to_table]
    :end-before: [END tdload_operator_howto_guide_load_data_from_table_to_table]

Remote operations using SSH
---------------------------
The TdLoadOperator support remote execution via SSH, enabling you to run TPT operations on servers where Teradata Parallel Transporter is installed. This capability is particularly valuable when:

- TPT is only available on specific servers in your infrastructure
- You need to leverage the processing power of dedicated data processing nodes
- Security policies require operations to occur on specific machines

To use remote execution, simply provide SSH connection details in your task configuration. The following examples demonstrate how to perform common operations remotely:

Loading data from a file to a Teradata table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_load_data_from_txt_file_to_table_remote]
    :end-before: [END tdload_operator_howto_guide_load_data_from_txt_file_to_table_remote]

Exporting data from a remote Teradata table to a file:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_export_data_to_a_file_remote]
    :end-before: [END tdload_operator_howto_guide_export_data_to_a_file_remote]

Transferring data between remote Teradata tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START tdload_operator_howto_guide_load_data_from_table_to_table_remote]
    :end-before: [END tdload_operator_howto_guide_load_data_from_table_to_table_remote]


The complete Teradata Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :start-after: [START tdload_operator_howto_guide]
    :end-before: [END tdload_operator_howto_guide]



.. _howto/operator:DdlOperator:

DdlOperator
===========

The ``DdlOperator`` is an Airflow operator designed to execute Data Definition Language (DDL) statements on Teradata databases. It provides a robust way to create, alter, or drop database objects as part of your data pipelines.

**Key Features:**

- Executes DDL SQL statements (CREATE, ALTER, DROP, etc.)
- Works with single statements or batches of multiple DDL operations
- Integrates with Airflow's connection management for secure database access
- Provides comprehensive logging of execution results
- Supports both local and remote execution via SSH

When you need to manage database schema changes, create temporary tables, or clean up data structures as part of your workflow, the ``DdlOperator`` offers a streamlined approach that integrates seamlessly with your Airflow DAGs.

To execute DDL operations in a Teradata database, use the
:class:`~airflow.providers.teradata.operators.ddl.DdlOperator`.

Key Operation Examples with DdlOperator
---------------------------------------

Dropping tables in Teradata
---------------------------
You can use the DdlOperator to drop tables in Teradata. The following example demonstrates how to drop multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_drop_table]
    :end-before: [END ddl_operator_howto_guide_drop_table]

Creating tables in Teradata
---------------------------
You can use the DdlOperator to create tables in Teradata. The following example demonstrates how to create multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_create_table]
    :end-before: [END ddl_operator_howto_guide_create_table]

Remote operations using SSH
---------------------------
The DdlOperator support remote execution via SSH, enabling you to run TPT operations on servers where Teradata Parallel Transporter is installed. To use remote execution, simply provide SSH connection details in your task configuration.

The following examples demonstrate how to perform common operations remotely:

Dropping tables in Teradata:
You can use the DdlOperator to drop tables in Teradata. The following example demonstrates how to drop multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_drop_table_remote]
    :end-before: [END ddl_operator_howto_guide_drop_table_remote]

Creating tables in Teradata:
You can use the DdlOperator to create tables in Teradata. The following example demonstrates how to create multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_create_table_remote]
    :end-before: [END ddl_operator_howto_guide_create_table_remote]


The complete Teradata Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :start-after: [START tdload_operator_howto_guide]
    :end-before: [END tdload_operator_howto_guide]
