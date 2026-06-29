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
import xml.etree.ElementTree as ET
from datetime import date
from collections import defaultdict

try:
    tree = ET.parse("report_test.xml")
except (FileNotFoundError, ET.ParseError):
    print("report_test.xml not found or empty — skipping XML parse")
    raise SystemExit(0)

root = tree.getroot()
today = date.today()

# Load existing CSV history
existing_rows = []
csv_file = "reporttest.csv"

try:
    with open(csv_file, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # Validate row format: [testclass, result, duration, date]
            if len(row) >= 4:
                try:
                    # Validate result is S or F
                    if row[1].strip() in ("S", "F"):
                        existing_rows.append(row)
                    else:
                        print(f"WARNING: Skipping invalid result '{row[1]}' in row {row}")
                except Exception as e:
                    print(f"WARNING: Error validating row {row}: {e}")
            elif len(row) > 0:
                print(f"WARNING: Skipping malformed CSV row with {len(row)} columns: {row}")
except FileNotFoundError:
    print(f"INFO: {csv_file} not found, creating new file")

# Parse current test results
current_results = []
for child in root:
    for subchild in child:
        testclass = subchild.attrib.get("classname", "")
        testduration = subchild.attrib.get("time", "0")
        testresult = "F" if list(subchild) else "S"

        if not testclass:
            print("WARNING: Skipping test with empty classname")
            continue

        current_results.append([testclass, testresult, testduration, str(today)])

# Combine existing history with current results
all_rows = existing_rows + current_results

# Group by test class to keep only last 10 runs per test
test_history = defaultdict(list)
for row in all_rows:
    if len(row) >= 4:
        testclass = row[0].strip()
        test_history[testclass].append(row)

# Trim to last 10 runs per test and flatten
final_rows = []
for testclass, runs in test_history.items():
    # Keep only last 10 runs for this test
    final_rows.extend(runs[-10:])

# Write cleaned CSV
try:
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(final_rows)
    print(f"INFO: Updated {csv_file} with {len(final_rows)} total records ({len(test_history)} unique tests)")
except Exception as e:
    print(f"ERROR: Failed to write CSV file: {e}")
    raise SystemExit(1)
