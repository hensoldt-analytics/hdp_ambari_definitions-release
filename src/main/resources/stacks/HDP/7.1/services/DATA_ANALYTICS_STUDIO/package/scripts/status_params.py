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

from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script

config = Script.get_config()

data_analytics_studio_pid_dir = default("/configurations/data_analytics_studio-env/data_analytics_studio_pid_dir", "/var/run/das")
data_analytics_studio_webapp_pid_file = format("{data_analytics_studio_pid_dir}/das-webapp.pid")
data_analytics_studio_event_processor_pid_file = format("{data_analytics_studio_pid_dir}/das-event-processor.pid")

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
