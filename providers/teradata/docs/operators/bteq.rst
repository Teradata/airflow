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

.. _howto/operator:BteqOperator:

BteqOperator
============

The BteqOperator allows you to execute BTEQ (Basic Teradata Query) scripts against Teradata databases.
These scripts can contain SQL queries, BTEQ commands, and data manipulation statements, providing
comprehensive interaction with your Teradata environment.

This operator leverages the TTU (Teradata Tools and Utilities) to establish connections and execute
commands efficiently. The operator handles script formatting, execution, and response parsing, making
it easy to integrate Teradata operations into your data pipelines.

Key features:

- Execute complex BTEQ scripts with full Teradata SQL support
- Support for BTEQ-specific commands and formatting
- Integration with Teradata connection configurations
- Error handling and reporting capabilities

To execute arbitrary SQL or BTEQ commands in a Teradata database, use the
:class:`~airflow.providers.teradata.operators.bteq.BteqOperator`.

Common Database Operations with BteqOperator
--------------------------------------------

Creating a Teradata database table
----------------------------------

You can use the BteqOperator to create tables in a Teradata database. The following example demonstrates how to create a simple employee table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_create_table]
    :end-before: [END bteq_operator_howto_guide_create_table]

The BTEQ script within this operator handles the table creation, including defining columns, data types, and constraints.


Inserting data into a Teradata database table
---------------------------------------------

The following example demonstrates how to populate the ``my_employees`` table with sample employee records:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_populate_table]
    :end-before: [END bteq_operator_howto_guide_populate_table]

This BTEQ script inserts multiple rows into the table in a single operation, making it efficient for batch data loading.


Exporting data from a Teradata database table to a file
-------------------------------------------------------

The BteqOperator makes it straightforward to export query results to a file. This capability is valuable for data extraction, backups, and transferring data between systems. The following example demonstrates how to query the employee table and export the results:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_export_data_to_a_file]
    :end-before: [END bteq_operator_howto_guide_export_data_to_a_file]

The BTEQ script above handles the data export with options for formatting, file location specification, and error handling during the export process.


Fetching and processing records from your Teradata database
-----------------------------------------------------------

You can use BteqOperator to query and retrieve data from your Teradata tables. The following example demonstrates
how to fetch specific records from the employee table with filtering and formatting:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_get_it_employees]
    :end-before: [END bteq_operator_howto_guide_get_it_employees]

This example shows how to:
- Execute a SELECT query with WHERE clause filtering
- Format the output for better readability
- Process the result set within the BTEQ script
- Handle empty result sets appropriately


Using Conditional Logic with BteqOperator
-----------------------------------------

The BteqOperator supports executing conditional logic within your BTEQ scripts. This powerful feature lets you create dynamic, decision-based workflows that respond to data conditions or processing results:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_conditional_logic]
    :end-before: [END bteq_operator_howto_guide_conditional_logic]

Conditional execution enables more intelligent data pipelines that can adapt to different scenarios without requiring separate DAG branches.


Error Handling in BTEQ Scripts
------------------------------

The BteqOperator allows you to implement comprehensive error handling within your BTEQ scripts:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_error_handling]
    :end-before: [END bteq_operator_howto_guide_error_handling]

This approach lets you catch and respond to errors at the BTEQ script level, providing more granular control over error conditions and enabling appropriate recovery actions.


Dropping a Teradata Database Table
----------------------------------

When your workflow completes or requires cleanup, you can use the BteqOperator to drop database objects. The following example demonstrates how to drop the ``my_employees`` table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_drop_table]
    :end-before: [END bteq_operator_howto_guide_drop_table]


The complete Teradata Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :start-after: [START bteq_operator_howto_guide]
    :end-before: [END bteq_operator_howto_guide]
