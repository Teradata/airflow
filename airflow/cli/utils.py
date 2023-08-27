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

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import io


class CliConflictError(Exception):
    """Error for when CLI commands are defined twice by different sources."""

    pass


def is_stdout(fileio: io.IOBase) -> bool:
    """Check whether a file IO is stdout.

    The intended use case for this helper is to check whether an argument parsed
    with argparse.FileType points to stdout (by setting the path to ``-``). This
    is why there is no equivalent for stderr; argparse does not allow using it.

    .. warning:: *fileio* must be open for this check to be successful.
    """
    return fileio.fileno() == sys.stdout.fileno()
