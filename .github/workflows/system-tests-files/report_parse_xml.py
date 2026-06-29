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

try:
    tree = ET.parse("report_test.xml")
except (FileNotFoundError, ET.ParseError):
    print("report_test.xml not found or empty — skipping XML parse")
    raise SystemExit(0)

root = tree.getroot()
today = date.today()

for child in root:
    for subchild in child:
        testclass = subchild.attrib.get("classname", "")
        testduration = subchild.attrib.get("time", "0")
        testresult = "F" if list(subchild) else "S"
        with open("reporttest.csv", "a", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            writer.writerow([testclass, testresult, testduration, today])
