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
formatted_date = now.strftime("%d-%b-%Y %H:%M IST")

branch = os.environ.get("BRANCH_NAME", "main")

history: dict[str, list[tuple[str, str]]] = {}
csv_file = "reporttest.csv"

# Load and validate CSV
try:
    with open(csv_file, newline="") as csvfile:
        for row_num, row in enumerate(csv.reader(csvfile), start=1):
            try:
                # Validate row format
                if len(row) < 4:
                    print(f"WARNING: Row {row_num} has insufficient columns ({len(row)}), skipping: {row}")
                    continue

                classname = row[0].strip()
                result = row[1].strip()
                duration = row[2].strip()

                # Validate result is S or F
                if result not in ("S", "F"):
                    print(f"WARNING: Row {row_num} has invalid result '{result}', skipping: {row}")
                    continue

                # Validate classname is not empty
                if not classname:
                    print(f"WARNING: Row {row_num} has empty classname, skipping: {row}")
                    continue

                # Validate duration is numeric
                try:
                    float(duration)
                except ValueError:
                    print(f"WARNING: Row {row_num} has non-numeric duration '{duration}', skipping: {row}")
                    continue

                history.setdefault(classname, []).append((result, duration))

            except Exception as e:
                print(f"ERROR: Failed to parse row {row_num}: {e}, row: {row}")
                continue

    if history:
        print(f"INFO: Loaded {sum(len(v) for v in history.values())} test results from {len(history)} unique tests")
    else:
        print(f"WARNING: No valid test results found in {csv_file}")

except FileNotFoundError:
    print(f"WARNING: {csv_file} not found, creating report with no history")

directory = "providers/teradata/tests/system/teradata"
system_test_files = []

try:
    system_test_files = [
        f for f in os.listdir(directory) if f.endswith(".py") and f != "__init__.py"
    ]
except FileNotFoundError:
    print(f"ERROR: Test directory not found: {directory}")
    system_test_files = []

items = []
for filename in system_test_files:
    test_name = filename[:-3]
    classname = f"providers.teradata.tests.system.teradata.{test_name}"
    runs = history.get(classname, [])

    if not runs:
        continue

    successes = sum(1 for result, _ in runs if result == "S")
    failures = sum(1 for result, _ in runs if result == "F")
    last_duration = runs[-1][1]
    last_result = runs[-1][0]
    status_icons = ["✅" if r == "S" else "❌" for r, _ in runs[-10:]]

    items.append(
        dict(
            classname=classname,
            successre=successes,
            failurere=failures,
            time=last_duration,
            lastrundate=formatted_date,
            last_status="✅ Passing" if last_result == "S" else "❌ Failing",
            status=status_icons,
        )
    )

total_passing = sum(1 for item in items if item["last_status"].startswith("✅"))
total_failing = sum(1 for item in items if item["last_status"].startswith("❌"))
overall = "✅ All tests passing" if total_failing == 0 else f"❌ {total_failing} test(s) failing"

with open("report_index_template.html") as f:
    template = Template(f.read())

with open("index.html", "w") as f:
    f.write(template.render(
        items=items,
        updated_at=formatted_date,
        overall_status=overall,
        total_passing=total_passing,
        total_failing=total_failing,
        branch=branch,
    ))
