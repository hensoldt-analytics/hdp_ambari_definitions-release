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

from resource_management.libraries.script import Script
from resource_management.core.resources.system import Execute
from resource_management.core.shell import as_user


class TimelineReaderUpgrade(Script):

  def fix_coprocessor_jars_path(self, env):
    import params

    if params.use_external_hbase or params.is_hbase_system_service_launch:
      return

    required_path = "file:/usr/hdp/current/hadoop-yarn-timelinereader/timelineservice/" \
                    "hadoop-yarn-server-timelineservice-hbase-coprocessor.jar"
    table_to_fix_name = "prod.timelineservice.flowrun"

    fix_cmd = "echo 'alter \"{2}\", METHOD => \"table_att_unset\", NAME => \"coprocessor$1\"; " \
              "alter \"{2}\", METHOD => \"table_att\", \"coprocessor$1\"=>" \
              "\"{0}|org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunCoprocessor|1073741823|\";" \
              "enable \"{2}\"' | {1} shell".format(required_path, params.hbase_cmd, table_to_fix_name)
    kerberized_fix_cmd = "{0} {1}".format(params.yarn_hbase_kinit_cmd, fix_cmd)

    check_cmd = "export JAVA_HOME=\"{3}\";echo 'describe \"{2}\"' |  {0} shell | grep \"{1}\" >/dev/null".format(
      params.hbase_cmd, required_path, table_to_fix_name, params.java64_home)
    kerberized_check_cmd = "{0} {1}".format(params.yarn_hbase_kinit_cmd, check_cmd)

    Execute(kerberized_fix_cmd, user=params.yarn_hbase_user,
            tries=5,
            try_sleep=10,
            logoutput=True,
            not_if=as_user(kerberized_check_cmd, params.yarn_hbase_user),
            environment={'JAVA_HOME': params.java64_home})


if __name__ == "__main__":
  TimelineReaderUpgrade().execute()
