#!/usr/bin/env python

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
from resource_management.libraries.functions.default import default

OK_MESSAGE = "OK - Sys DB and Information Schema created"
CRITICAL_MESSAGE = "Sys DB and Information Schema not created yet"

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return ()

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label

  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  host_sys_prepped = default("/ambariLevelParams/host_sys_prepped", False)

  if not host_sys_prepped and not os.path.isfile("/etc/hive/sys.db.created"):
    result_code = 'CRITICAL'
    label = CRITICAL_MESSAGE
  else:
    result_code = 'OK'
    label = OK_MESSAGE

  return (result_code, [label])
