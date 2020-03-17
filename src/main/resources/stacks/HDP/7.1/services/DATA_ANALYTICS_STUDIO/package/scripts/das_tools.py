#!/usr/bin/env python
"""
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import glob
import os

def get_das_home():
  stack_name = "hdp"
  component_name = "data_analytics_studio"
  stack_dir = os.path.join('/usr', stack_name)
  das_home_dir = os.path.join(stack_dir, "current", component_name)
  if not os.path.isdir(das_home_dir):
    # get the latest version installed.
    # once stack select is implemented this should not be required.
    versions = os.listdir(stack_dir)
    versions.sort()
    versions.reverse()
    for v in versions:
      das_home_dir = os.path.join(stack_dir, v, component_name)
      if os.path.isdir(das_home_dir):
        return das_home_dir
  return das_home_dir

def get_das_lib(lib_glob):
  return glob.glob(os.path.join(get_das_home(), "lib", lib_glob))[0]
