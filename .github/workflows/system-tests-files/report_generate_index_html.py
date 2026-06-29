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

import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from jinja2 import Template

tz = ZoneInfo("Asia/Kolkata")
now = datetime.now(tz)
formatted_date = now.strftime("%B %d, %Y at %I:%M:%S %p GMT%z")

# Load full run history from CSV — keyed by classname.
# Each value is a list of (result, duration) tuples in chronological order.
history: dict[str, list[tuple[str, str]]] = {}
try:
    with open("reporttest.csv", newline="") as csvfile:
        for row in csv.reader(csvfile):
            if len(row) < 3:
                continue
            classname = row[0].strip()
            result = row[1].strip()
            duration = row[2].strip()
            history.setdefault(classname, []).append((result, duration))
except FileNotFoundError:
    pass

directory = "providers/teradata/tests/system/teradata"
system_test_files = [
    f for f in os.listdir(directory) if f.endswith(".py") and f != "__init__.py"
]

items = []
for filename in system_test_files:
    test_name = filename[:-3]
    classname = f"providers.teradata.tests.system.teradata.{test_name}"
    runs = history.get(classname, [])

    successes = sum(1 for result, _ in runs if result == "S")
    failures = sum(1 for result, _ in runs if result == "F")
    last_duration = runs[-1][1] if runs else "N/A"
    last_date = formatted_date if runs else "Never run"
    status = [result for result, _ in runs[-10:]]

    items.append(
        dict(
            classname=classname,
            successre=successes,
            failurere=failures,
            time=last_duration,
            lastrundate=last_date,
            status=status,
        )
    )

with open("report_index_template.html") as f:
    template = Template(f.read())

with open("index.html", "w") as f:
    f.write(template.render(items=items))
