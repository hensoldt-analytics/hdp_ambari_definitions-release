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

from ambari_commons import OSCheck
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.core.source import InlineTemplate, Template
from resource_management.libraries.functions.format import format
from resource_management.libraries.resources.properties_file import PropertiesFile


def data_analytics_studio(name = None):
  import params

  if name == "data_analytics_studio_webapp":
    setup_data_analytics_studio_postgresql_server()
    setup_data_analytics_studio_configs()
    setup_data_analytics_studio_webapp()

    if params.enable_ranger_hive:
      setup_ranger_for_das()

  elif name == "data_analytics_studio_event_processor":
    setup_data_analytics_studio_configs()
    setup_data_analytics_studio_event_processor()

def setup_ranger_for_das():
  import params

  if params.xml_configurations_supported:
    api_version = None
  if params.stack_supports_ranger_kerberos:
    api_version = 'v2'

  if params.xa_audit_hdfs_is_enabled:
    try:
      params.HdfsResource("/ranger/audit/hiveCLI",
                          type="directory",
                          action="create_on_execute",
                          owner=params.data_analytics_studio_user,
                          group=params.data_analytics_studio_group,
                          mode=0700,
                          recursive_chmod=True,
                          dfs_type=params.dfs_type
      )
      params.HdfsResource(None, action="execute")
    except Exception, err:
      print("Audit directory creation in HDFS for HIVE Ranger plugin failed with error:\n{0}".format(err))

  from resource_management.libraries.functions.setup_ranger_plugin_xml import setup_ranger_plugin
  user_principal = params.data_analytics_studio_webapp_service_principal.replace('_HOST', params.hostname)
  setup_ranger_plugin('hive-server2', 'hive', None,
                      None, None,
                      None, params.java64_home,
                      params.repo_name, None,
                      params.ranger_env, None,
                      None, params.policymgr_mgr_url,
                      params.enable_ranger_hive, conf_dict=params.conf_dir,
                      component_user=params.data_analytics_studio_user, component_group=params.data_analytics_studio_group,
                      cache_service_list=['hiveServer2'],

                      plugin_audit_properties=params.config['configurations']['ranger-hive-audit'],
                      plugin_audit_attributes=params.plugin_audit_attributes,
                      plugin_security_properties=params.config['configurations']['ranger-hive-security'],
                      plugin_security_attributes=params.plugin_security_attributes,
                      plugin_policymgr_ssl_properties=params.config['configurations']['ranger-hive-policymgr-ssl'],
                      plugin_policymgr_ssl_attributes=params.plugin_policymgr_ssl_attributes,
                      component_list=['hive-client', 'hive-metastore', 'hive-server2'],
                      audit_db_is_enabled=params.xa_audit_db_is_enabled,
                      credential_file=params.credential_file, xa_audit_db_password=params.xa_audit_db_password,
                      ssl_truststore_password=params.ssl_truststore_password, ssl_keystore_password=params.ssl_keystore_password,
                      stack_version_override=params.stack_version, skip_if_rangeradmin_down=True, api_version=api_version,
                      is_security_enabled=params.security_enabled,
                      is_stack_supports_ranger_kerberos=params.stack_supports_ranger_kerberos,
                      component_user_principal=user_principal if params.security_enabled else None,
                      component_user_keytab=params.data_analytics_studio_webapp_service_keytab if params.security_enabled else None)


def setup_data_analytics_studio_webapp():
  import params

  File(params.das_webapp_json,
       content = InlineTemplate(params.das_webapp_properties_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_webapp_log4j2_yml,
       content = InlineTemplate(params.das_webapp_log4j_config_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_webapp_env_sh,
       content = InlineTemplate(params.das_webapp_env_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

def setup_data_analytics_studio_event_processor():
  import params

  File(params.das_event_processor_json,
       content = InlineTemplate(params.das_event_processor_properties_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_ep_log4j2_yml,
       content = InlineTemplate(params.das_ep_log4j_config_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_event_processor_env_sh,
       content = InlineTemplate(params.das_event_processor_env_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_sysdb_tables_hql,
       content = Template("das-sysdb-tables.hql.j2"),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

def setup_data_analytics_studio_configs():
  import params

  # TODO: revisit ownership and permission for all the files here. I think all
  # should be owned by root and readable by all. Have to find policy to store
  # password in conf file.

  # Bug in ambari mkdirs, it does not set the mode all the way.
  Directory(os.path.dirname(params.conf_dir),
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.conf_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.data_analytics_studio_pid_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.data_analytics_studio_log_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)
  
  PropertiesFile(params.das_hive_site_conf,
                 properties = params.das_hive_site_conf_dict,
                 owner = params.data_analytics_studio_user,
                 mode = 0400
  )
  
  PropertiesFile(params.das_hive_interactive_site_conf,
                 properties = params.das_hive_interactive_site_conf_dict,
                 owner = params.data_analytics_studio_user,
                 mode = 0400
  )

def setup_data_analytics_studio_postgresql_server():
  import params

  if not params.data_analytics_studio_autocreate_db:
    return

  if OSCheck.get_os_type() in ["centos", "redhat", "fedora"]:
    pgpath = "/var/lib/pgsql/9.6/data"
  elif OSCheck.get_os_type() in ["debian", "ubuntu"]:
    pgpath = "/var/lib/postgresql/9.6/main"

  File(format("{pgpath}/pg_hba.conf"),
       content = InlineTemplate(params.data_analytics_studio_postgresql_pg_hba_conf_content),
       owner = "postgres",
       group = "postgres",
       mode = 0600)

  File(format("{pgpath}/postgresql.conf"),
       content = InlineTemplate(params.data_analytics_studio_postgresql_postgresql_conf_content),
       owner = "postgres",
       group = "postgres",
       mode = 0600)
