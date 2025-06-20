from __future__ import annotations

import csv
import xml.etree.ElementTree as ET
from datetime import date

tree = ET.parse("report_test.xml")
root = tree.getroot()


for child in root:
    for subchild in child:
        testclass = subchild.attrib["classname"]
        testduration = subchild.attrib["time"]
        testresult = "S"
        today = date.today()
        for _subsubchild in subchild:
            testresult = "F"
        with open("reporttest.csv", "a", newline="") as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([testclass, testresult, testduration, today])
