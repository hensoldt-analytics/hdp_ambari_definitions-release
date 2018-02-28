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
from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.core.shell import as_sudo
from resource_management.core.resources.system import Execute, File




def hbase_service(
  name,
  action = 'start'): # 'start' or 'stop'

    import params

    sudo = AMBARI_SUDO_BINARY
    daemon_script = format("{params.yarn_hbase_bin}/hbase-daemon.sh")
    role = name
    cmd = format("{daemon_script} --config {params.yarn_hbase_conf_dir}")
    pid_file = format("{params.yarn_hbase_pid_dir}/hbase-{params.yarn_hbase_user}-{role}.pid")
    pid_expression = as_sudo(["cat", pid_file])
    no_op_test = as_sudo(["test", "-f", pid_file]) + format(" && ps -p `{pid_expression}` >/dev/null 2>&1")

    if action == 'start':
      daemon_cmd = format("{cmd} start {role}")

      try:
        Execute ( daemon_cmd,
          not_if = no_op_test,
          user = params.yarn_hbase_user
        )
      except:
        show_logs(params.yarn_hbase_log_dir, params.yarn_hbase_user)
        raise
    elif action == 'stop':
      daemon_cmd = format("{cmd} stop {role}")

      try:
        Execute ( daemon_cmd,
          user = params.yarn_hbase_user,
          only_if = no_op_test,
          timeout = 30,
          on_timeout = format("! ( {no_op_test} ) || {sudo} -H -E kill -9 `{pid_expression}`"),
        )
      except:
        show_logs(params.yarn_hbase_log_dir, params.yarn_hbase_user)
        raise

      File(pid_file,
           action = "delete",
      )

def install_hbase(env):
    import params
    env.set_params(params)

    hbase_home = params.yarn_atsv2_hbase_versioned_home
    hbase_download_url = params.hbase_download_url
    hbase_package_name = params.hbase_package_name
    hbase_download_cmd = format("umask 0022;wget --no-cookies --no-check-certificate {hbase_download_url} && tar -xzf {hbase_package_name} && rm -rf {hbase_package_name} && rm -rf {hbase_home} && mv hbase-* {hbase_home}")
    #TODO keep co processor jar in hdfs

    create_symb_link = format("umask 0022;cd {params.hadoop_yarn_home}/timelineservice;if [ ! -f {params.coprocessor_jar_name} ]; then ln -s hadoop-yarn-server-timelineservice-hbase.jar {params.coprocessor_jar_name};fi;cd -")
    try:
      Execute(hbase_download_cmd, user="root", logoutput=True)
      Execute(create_symb_link, user="root", logoutput=True)
    except:
      raise

def configure_hbase(env):
    import params
    env.set_params(params)
    params.HdfsResource(params.yarn_hbase_hdfs_root_dir,
                            type="directory",
                            action="create_on_execute",
                            owner=params.yarn_hbase_user
                            )
    params.HdfsResource(None, action="execute")

def start_hbase():
    hbase_service('master', action = 'start')
    hbase_service('regionserver', action='start')

def stop_hbase():
    hbase_service('master', action = 'stop')
    hbase_service('regionserver', action='stop')

def createTables():
    import params
    class_name = format("org.apache.hadoop.yarn.server.timelineservice.storage.TimelineSchemaCreator -create -s")
    cmd = format("export HBASE_CLASSPATH_PREFIX={params.hadoop_yarn_home}/timelineservice/*;{params.yarn_timelineservice_kinit_cmd} {params.yarn_hbase_bin}/hbase --config {params.yarn_hbase_conf_dir} {class_name}")
    try:
        Execute(cmd, user=params.yarn_user, timeout = 60, logoutput=True)
    except:
        show_logs(params.yarn_hbase_log_dir, params.yarn_hbase_user)
        raise
