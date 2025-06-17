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

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.tpt import TptHook


class DdlOperator(BaseOperator):
    """
    Operator to execute a DDL (Data Definition Language) statement on a Teradata Database.

    This operator is designed to facilitate DDL operations such as creating or dropping
    tables, indexes, or other database objects in a scalable and efficient manner.

    It leverages the TPT utility to perform the operation and supports templating for
    SQL statements, allowing dynamic generation of SQL at runtime.

    Key Features:
    - Executes DDL statements on Teradata using TPT.
    - Supports XCom push to share execution results with downstream tasks.
    - Integrates with Airflow's templating engine for dynamic SQL generation.

    :param ddl: The DDL statement to be executed. This can include any valid SQL
                DDL command supported by Teradata.
    :param error_list: Optional list of error codes to monitor during execution.
                       If provided, the operator will handle these errors accordingly.
    :param teradata_conn_id: The connection ID for the Teradata Tools and Utilities (TTU) hook.
                        This is used to establish a connection to the Teradata database.
                        Defaults to 'teradata_default'.
    :raises AirflowException: If the DDL operation fails, an AirflowException is raised
                              with details of the failure.

    Example usage::

        # Example of creating a table using DdlOperator
        ddl = DdlOperator(
            task_id="create_table_task",
            ddl="CREATE TABLE my_table (id INT, name VARCHAR(100))",
            teradata_conn_id="my_teradata_conn",
        )

        # Example of dropping a table using DdlOperator
        ddl = DdlOperator(
            task_id="drop_table_task",
            ddl="DROP TABLE my_table",
            teradata_conn_id="my_teradata_conn",
        )


    """

    template_fields = ("ddl",)
    template_ext = (".sql",)
    ui_color = "#a8e4b1"

    def __init__(
        self,
        *,
        ddl: str,
        error_list: int | list[int] | None = None,
        teradata_conn_id: str = TptHook.default_conn_name,
        ssh_conn_id: str | None = None,
        working_dir: str = "/tmp",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ddl = ddl
        self.error_list = error_list
        self.teradata_conn_id = teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.working_dir = working_dir
        self._hook = None

    def execute(self, context):
        """Execute the DDL operation using the TptHook."""
        if not self.ddl:
            raise ValueError("DDL statement cannot be empty. Please provide a valid DDL statement.")
        # Normalize error_list to a list of ints
        if self.error_list is None:
            self.error_list = []
        elif isinstance(self.error_list, int):
            self.error_list = [self.error_list]
        elif not isinstance(self.error_list, list):
            raise ValueError(
                f"error_list must be an int or a list of ints, got {type(self.error_list).__name__}. "
                "Example: error_list=3803 or error_list=[3803, 3807]"
            )

        self._hook = TptHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)

        return self._hook.execute_ddl(
            sql=self.ddl,
            error_list=self.error_list,
            ssh_conn_id=self.ssh_conn_id,
            working_dir=self.working_dir,
        )

    def on_kill(self):
        """Handle termination signals and ensure the hook is properly cleaned up."""
        if self._hook:
            self._hook.on_kill()


class TdLoadOperator(BaseOperator):
    """
    Operator to handle data transfers using Teradata Parallel Transporter (TPT) tdload utility.

    This operator supports three main scenarios:
    1. Load data from a file to a Teradata table
    2. Export data from a Teradata table to a file
    3. Transfer data between two Teradata tables (potentially across different databases)

    For all scenarios:
        :param teradata_conn_id: Connection ID for Teradata database (source for table operations)

    For file to table loading:
        :param source_file_name: Path to the source file (required for file to table)
        :param select_stmt: SQL SELECT statement to filter data (optional)
        :param target_table: Name of the target table (required for file to table)
        :param target_teradata_conn_id: Connection ID for target Teradata database (defaults to teradata_conn_id)

    For table to file export:
        :param source_table: Name of the source table (required for table to file)
        :param target_file_name: Path to the target file (required for table to file)

    For table to table transfer:
        :param source_table: Name of the source table (required for table to table)
        :param select_stmt: SQL SELECT statement to filter data (optional)
        :param target_table: Name of the target table (required for table to table)
        :param target_teradata_conn_id: Connection ID for target Teradata database (required for table to table)

    Optional configuration parameters:
        :param source_format: Format of source data (default: 'Delimited')
        :param target_format: Format of target data (default: 'Delimited')
        :param source_text_delimiter: Source text delimiter (default: ',')
        :param target_text_delimiter: Target text delimiter (default: ',')
        :param staging_table: Name of the staging table (optional, used for intermediate storage)
        :param tdload_options: Additional options for tdload (optional)
    :param ssh_conn_id: SSH connection ID for secure file transfer (optional, used for file operations)
    :raises AirflowException: If the operation fails due to invalid parameters or execution errors.

    Example usage::

        # Example usage for file to table:
        load_file = TdLoadOperator(
            task_id="load_from_file",
            source_file_name="/path/to/data.csv",
            target_table="my_database.my_table",
            target_teradata_conn_id="teradata_target_conn",
        )

        # Example usage for table to file:
        export_data = TdLoadOperator(
            task_id="export_to_file",
            source_table="my_database.my_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_source_conn",
            ssh_conn_id="ssh_default",
        )

        # Example usage for table to table:
        transfer_data = TdLoadOperator(
            task_id="transfer_between_tables",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_source_conn",
            target_teradata_conn_id="teradata_target_conn",
        )


    """

    template_fields = ("source_table", "target_table", "source_file_name", "target_file_name")
    ui_color = "#a8e4b1"

    def __init__(
        self,
        *,
        teradata_conn_id: str = TptHook.default_conn_name,
        target_teradata_conn_id: str | None = None,
        ssh_conn_id: str | None = None,
        source_table: str | None = None,
        select_stmt: str | None = None,
        target_table: str | None = None,
        source_file_name: str | None = None,
        target_file_name: str | None = None,
        source_format: str = "Delimited",
        target_format: str = "Delimited",
        source_text_delimiter: str = ",",
        target_text_delimiter: str = ",",
        staging_table: str | None = None,
        tdload_options: str | None = None,
        working_dir: str = "/tmp",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.teradata_conn_id = teradata_conn_id
        self.target_teradata_conn_id = target_teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.source_table = source_table
        self.select_stmt = select_stmt
        self.target_table = target_table
        self.source_file_name = source_file_name
        self.target_file_name = target_file_name
        self.source_format = source_format
        self.source_text_delimiter = source_text_delimiter
        self.target_format = target_format
        self.target_text_delimiter = target_text_delimiter
        self.staging_table = staging_table
        self.tdload_options = tdload_options
        self.working_dir = working_dir
        self._src_hook = None
        self._dest_hook = None

        if source_table and select_stmt:
            raise ValueError(
                "Both source_table and select_stmt cannot be provided simultaneously. "
                "Please provide only one."
            )

        # Determine the mode of operation based on provided parameters
        if source_file_name and target_table:
            self.mode = "file_to_table"
            # For file_to_table, if target_teradata_conn_id is not provided, use teradata_conn_id
            if self.target_teradata_conn_id is None:
                self.target_teradata_conn_id = self.teradata_conn_id
        elif (source_table or select_stmt) and target_file_name:
            self.mode = "table_to_file"
        elif (source_table or select_stmt) and target_table:
            self.mode = "table_to_table"
            # For table_to_table, target_teradata_conn_id must be provided
            if self.target_teradata_conn_id is None:
                raise ValueError("For table to table transfer, target_teradata_conn_id must be provided.")
        else:
            raise ValueError(
                "Invalid parameter combination for the TdLoadOperator. Please provide one of these valid combinations:\n"
                "1. source_file_name and target_table: to load data from a file to a table\n"
                "2. source_table/select_stmt and target_file_name: to export data from a table to a file\n"
                "3. source_table/select_stmt and target_table: to transfer data between tables\n"
            )

    def execute(self, context):
        """Execute the TPT tdload operation based on the specified mode."""
        self.log.info("Executing TPT tdload operation in %s mode", self.mode)

        # Initialize hooks
        self.log.info("Initializing source connection using teradata_conn_id: %s", self.teradata_conn_id)
        self._src_hook = TptHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)

        if self.mode in ("table_to_table", "file_to_table"):
            self.log.info(
                "Initializing destination connection using target_teradata_conn_id: %s",
                self.target_teradata_conn_id,
            )
            self._dest_hook = TptHook(teradata_conn_id=self.target_teradata_conn_id)

        # Log operation details based on mode
        if self.mode == "file_to_table":
            self.log.info(
                "Loading data from file '%s' to table '%s'", self.source_file_name, self.target_table
            )
        elif self.mode == "table_to_file":
            self.log.info(
                "Exporting data from table '%s' to file '%s'", self.source_table, self.target_file_name
            )
        elif self.mode == "table_to_table":
            self.log.info(
                "Transferring data from table '%s' to table '%s'", self.source_table, self.target_table
            )

        # Execute tdload
        return self._src_hook.execute_tdload(
            mode=self.mode,
            ssh_conn_id=self.ssh_conn_id,
            source_table=self.source_table,
            select_stmt=self.select_stmt,
            target_table=self.target_table,
            source_file_name=self.source_file_name,
            target_file_name=self.target_file_name,
            source_format=self.source_format,
            target_format=self.target_format,
            source_text_delimiter=self.source_text_delimiter,
            target_text_delimiter=self.target_text_delimiter,
            staging_table=self.staging_table,
            tdload_options=self.tdload_options,
            target_teradata_conn_id=self.target_teradata_conn_id,
            working_dir=self.working_dir,
        )

    def on_kill(self):
        """Handle termination signals and ensure all hooks are properly cleaned up."""
        self.log.info("Cleaning up TPT tdload connections on task kill")

        # Clean up the source hook if it was initialized
        if self._src_hook:
            self.log.info("Cleaning up source connection")
            self._src_hook.on_kill()

        # Clean up the destination hook if it was initialized
        if self._dest_hook:
            self.log.info("Cleaning up destination connection")
            self._dest_hook.on_kill()
