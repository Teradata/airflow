import os
import re
from datetime import datetime

import pandas as pd
from jinja2 import Template
import pytz


class Record:
    def __init__(self, filename, successcount, failurecount, result, time, rundate):
        self.filename = filename
        self.successcount = successcount
        self.failurecount = failurecount
        self.result = result
        self.time = time
        self.rundate = rundate


pd.options.display.max_rows = 9999
fileName = 'reporttest.csv'
# Read test report
df = pd.read_csv(fileName, delimiter=',', header=None)
"""
# the code for running locally
origdir = os.getcwd()
os.chdir("../../..")
directory = os.getcwd() + '/providers/teradata/tests/system/teradata'
os.chdir(origdir)
"""
# Read system test files under system tests directory
directory = 'providers/teradata/tests/system/teradata'
# List all file names in the directory
file_names = os.listdir(directory)

# Filter only Python files excluding __init__.py
system_test_files = [file_name for file_name in file_names if
                     file_name.endswith('.py') and file_name != '__init__.py']

liRecords = []
items = []

tz = pytz.timezone('Asia/Kolkata')
now = datetime.now(tz)
# Format the date and time
formatted_date = now.strftime('%B %d, %Y at %I:%M:%S %p GMT%z')

# Getting each system test history from report and preparing UI for github page
for system_test_file in system_test_files:
    record = Record('', 0, 0,[], '', '')
    system_test_file = system_test_file[:-3]
    system_test_file = 'providers.teradata.tests.system.teradata.' + system_test_file.strip()
    record.filename = system_test_file
    liRecords = []
    for index, row in df.iterrows():
        if row[0].strip() == system_test_file:
            record.time = row[2]
            record.rundate = formatted_date
            if row[1] == 'S':
                record.successcount += 1
                record.result.append('S')
            else:
                record.failurecount += 1
                record.result.append('F')
            liRecords.append(record)
    an_item = dict(classname=system_test_file, successre=record.successcount,
                   failurere=record.failurecount, time=liRecords[-1].time, lastrundate=liRecords[-1].rundate, status=liRecords[-1].result[-10:])
    items.append(an_item)


# Create one external form_template html page and read it
File = open('report_index_template.html', 'r')
content = File.read()
File.close()

# Render the template and pass the variables
template = Template(content)
rendered_form = template.render(items=items)

# save the txt file in the index.html
output = open('index.html', 'w')
output.write(rendered_form)
output.close()
