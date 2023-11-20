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



.. _howto/connection:teradata:

Teradata Connection
======================
The Teradata connection type enables integrations with Teradata.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Database (optional)
    Specify the name of the database to connect to.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Teradata
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``sslmode`` - This option determines whether or with what priority a secure SSL
      TCP/IP connection will be negotiated with the server. There are six modes:
      ``disable``, ``allow``, ``prefer``, ``require``, ``verify-ca``, ``verify-full``.
    * ``sslca`` - This parameter specifies the file name of a PEM file that
      contains Certificate Authority (CA) certificates for use with sslmode values
      VERIFY-CA or VERIFY-FULL.
    * ``sslcapath`` - This parameter specifies a directory of PEM files that contain
      Certificate Authority (CA) certificates for use with sslmode values VERIFY-CA
      or VERIFY-FULL.
    * ``sslrootcert`` - This parameter specifies the name of a file containing SSL
      certificate authority (CA) certificate(s).
    * ``sslcrl`` - This parameter specifies the file name of the SSL certificate
      revocation list (CRL).
    * ``tmode`` - Specifies the transaction mode. Possible values are DEFAULT (the default),
      ANSI, or TERA.

    More details on all Teradata parameters supported can be found in
    `Teradata documentation <https://github.com/Teradata/python-driver#ConnectionParameters`_.

    Example "extras" field:

    .. code-block:: json

       {
          "sslmode": "verify-ca",
          "sslca": "/tmp/server-ca.pem",
          "tmode": "TERA"
       }


    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        export AIRFLOW_CONN_TERADATA_DEFAULT='teradata://teradata_user:XXXXXXXXXXXX@1.1.1.1:/teradatadb?sslmode=verify-ca&sslca=%2Ftmp%2Fserver-ca.pem'
