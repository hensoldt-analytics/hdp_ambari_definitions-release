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

from resource_management.core.logger import Logger
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.script.script import Script

from data_analytics_studio import data_analytics_studio
from data_analytics_studio_service import data_analytics_studio_service


class DataAnalyticsStudioWebapp(Script):
  def install(self, env):
    self.install_packages(env)
    import params
    env.set_params(params)
    data_analytics_studio_service(name = "data_analytics_studio_webapp", action = "install")

  def configure(self, env, upgrade_type=None, config_dir=None):
    import params
    env.set_params(params)
    data_analytics_studio(name = "data_analytics_studio_webapp")

  def start(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    self.configure(env)

    data_analytics_studio_service("data_analytics_studio_webapp", action = "start")

  def stop(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    data_analytics_studio_service("data_analytics_studio_webapp", action = "stop" )

  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.data_analytics_studio_webapp_pid_file)

  def get_log_folder(self):
    import params
    return params.data_analytics_studio_log_dir
  
  def get_user(self):
    import params
    return params.data_analytics_studio_user

  def get_pid_files(self):
    import status_params
    return [status_params.data_analytics_studio_webapp_pid_file]

if __name__ == "__main__":
  DataAnalyticsStudioWebapp().execute()
