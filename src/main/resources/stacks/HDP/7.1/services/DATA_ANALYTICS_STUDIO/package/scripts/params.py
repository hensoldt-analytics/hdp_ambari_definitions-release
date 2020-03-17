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

import functools
import os
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.is_empty import is_empty



import status_params

config = Script.get_config()

if 'hostLevelParams' in config and 'java_home' in config['hostLevelParams']:
    java64_home = config["hostLevelParams"]["java_home"]
    jdk_location = config["hostLevelParams"]["jdk_location"]
else:
    java64_home = config['ambariLevelParams']['java_home']
    jdk_location = config["ambariLevelParams"]["jdk_location"]

conf_dir = "/etc/das/conf"

# env variables
data_analytics_studio_pid_dir = status_params.data_analytics_studio_pid_dir
data_analytics_studio_webapp_pid_file = status_params.data_analytics_studio_webapp_pid_file
data_analytics_studio_event_processor_pid_file = status_params.data_analytics_studio_event_processor_pid_file
data_analytics_studio_webapp_additional_classpath = default("/configurations/data_analytics_studio-env/webapp_additional_classpath", "")
data_analytics_studio_ep_additional_classpath = default("/configurations/data_analytics_studio-env/ep_additional_classpath", "")

data_analytics_studio_log_dir = default("/configurations/data_analytics_studio-env/data_analytics_studio_log_dir", "/var/log/das")
data_analytics_studio_user = default("/configurations/hive-env/hive_user", "hive")
das_db_password_jceks_hdfs_location = format("user/{data_analytics_studio_user}/das/security/data_analytics_studio-database.jceks")
#TODO: Find of this is the right group to use as this could give access to many
data_analytics_studio_group = config['configurations']['cluster-env']['user_group']

data_analytics_studio_webapp_jvm_opts = format(default("/configurations/data_analytics_studio-env/webapp_jvm_opts", ""))
data_analytics_studio_ep_jvm_opts = format(default("/configurations/data_analytics_studio-env/ep_jvm_opts", ""))

# configuration files
das_webapp_json = format("{conf_dir}/das-webapp.json")
das_webapp_log4j2_yml = format("{conf_dir}/das-webapp-log4j2.yml")
das_webapp_env_sh = format("{conf_dir}/das-webapp-env.sh")
das_event_processor_json = format("{conf_dir}/das-event-processor.json")
das_ep_log4j2_yml = format("{conf_dir}/das-event-processor-log4j2.yml")
das_event_processor_env_sh = format("{conf_dir}/das-event-processor-env.sh")
das_hive_site_conf = format("{conf_dir}/das-hive-site.conf")
das_hive_interactive_site_conf = format("{conf_dir}/das-hive-interactive-site.conf")
das_sysdb_tables_hql = format("{conf_dir}/das-sysdb-tables.hql")

# contents for file creations
data_analytics_studio_hive_session_params = config["configurations"]["data_analytics_studio-properties"]["hive_session_params"]
das_webapp_env_content = config["configurations"]["data_analytics_studio-webapp-env"]["content"]
das_webapp_properties_content = config["configurations"]["data_analytics_studio-webapp-properties"]["content"]
das_webapp_log4j_config_content = config["configurations"]["data_analytics_studio-webapp-properties"]["das_webapp_log4j2_yaml"]
das_event_processor_env_content = config["configurations"]["data_analytics_studio-event_processor-env"]["content"]
das_event_processor_properties_content = config["configurations"]["data_analytics_studio-event_processor-properties"]["content"]
das_ep_log4j_config_content = config["configurations"]["data_analytics_studio-event_processor-properties"]["das_event_processor_log4j2_yaml"]
data_analytics_studio_postgresql_postgresql_conf_content = config["configurations"]["data_analytics_studio-database"]["postgresql_conf_content"]
data_analytics_studio_postgresql_pg_hba_conf_content = config["configurations"]["data_analytics_studio-database"]["pg_hba_conf_content"]

# properties
data_analytics_studio_ssl_enabled = default("/configurations/data_analytics_studio-security-site/ssl_enabled", False)
das_credential_provider_paths = format("jceks://file{conf_dir}/data_analytics_studio-database.jceks,jceks://file{conf_dir}/data_analytics_studio-properties.jceks,jceks://file{conf_dir}/data_analytics_studio-security-site.jceks")

data_analytics_studio_service_authentication = default("/configurations/data_analytics_studio-security-site/service_authentication", "NONE").upper()
data_analytics_studio_user_authentication = default("/configurations/data_analytics_studio-security-site/user_authentication", "NONE").upper()

data_analytics_studio_webapp_server_protocol = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_server_protocol", "http")
data_analytics_studio_webapp_server_host = default("/clusterHostInfo/data_analytics_studio_webapp_hosts", ["localhost"])[0]
data_analytics_studio_webapp_server_port = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_server_port", "30800")
data_analytics_studio_webapp_server_url = format("{data_analytics_studio_webapp_server_protocol}://{data_analytics_studio_webapp_server_host}:{data_analytics_studio_webapp_server_port}/api/")
data_analytics_studio_webapp_admin_port = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_admin_port", "30801")
data_analytics_studio_webapp_session_timeout = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_session_timeout", "86400")
data_analytics_studio_admin_users = default("/configurations/data_analytics_studio-security-site/admin_users", "")
data_analytics_studio_admin_groups = default("/configurations/data_analytics_studio-security-site/admin_groups", "")
data_analytics_studio_webapp_service_keytab = default("/configurations/data_analytics_studio-webapp-properties/service_keytab", "")
data_analytics_studio_webapp_service_principal = default("/configurations/data_analytics_studio-webapp-properties/service_principal", "")
data_analytics_studio_webapp_knox_sso_url = default("/configurations/data_analytics_studio-security-site/knox_sso_url", "")
data_analytics_studio_webapp_knox_ssout_url = default("/configurations/data_analytics_studio-security-site/knox_ssout_url", "")
data_analytics_studio_webapp_knox_useragent = default("/configurations/data_analytics_studio-security-site/knox_useragent", "Mozilla,Chrome")
data_analytics_studio_webapp_knox_publickey = default("/configurations/data_analytics_studio-security-site/knox_publickey", "")
data_analytics_studio_webapp_knox_cookiename = default("/configurations/data_analytics_studio-security-site/knox_cookiename", "hadoop-jwt")
data_analytics_studio_webapp_knox_url_query_param = default("/configurations/data_analytics_studio-security-site/knox_url_query_param", "originalUrl")
data_analytics_studio_webapp_keystore_file = default("/configurations/data_analytics_studio-security-site/webapp_keystore_file", "")
data_analytics_studio_webapp_keystore_password = default("/configurations/data_analytics_studio-security-site/webapp_keystore_password", "")
data_analytics_studio_webapp_spnego_principal = default("/configurations/data_analytics_studio-security-site/das.kerberos.spnego.principal", "")
data_analytics_studio_webapp_spnego_keytab = default("/configurations/data_analytics_studio-security-site/das.kerberos.spnego.keytab", "")
data_analytics_studio_webapp_spnego_name_rules = default("/configurations/data_analytics_studio-security-site/das_spnego_name_rules", "")
data_analytics_studio_webapp_knox_user = default("/configurations/data_analytics_studio-security-site/das_knox_user", "")
data_analytics_studio_webapp_doas_param_name = default("/configurations/data_analytics_studio-security-site/das_knox_doas_param_name", "")

das_webapp_ldap_url = default("/configurations/data_analytics_studio-security-site/ldap_url", "")
das_webapp_ldap_guid_key = default("/configurations/data_analytics_studio-security-site/ldap_guid_key", "")
das_webapp_ldap_group_class_key = default("/configurations/data_analytics_studio-security-site/ldap_group_class_key", "")
das_webapp_ldap_group_membership_key = default("/configurations/data_analytics_studio-security-site/ldap_group_membership_key", "")
das_webapp_ldap_user_membership_key = default("/configurations/data_analytics_studio-security-site/ldap_user_membership_key", "")
das_webapp_ldap_basedn = default("/configurations/data_analytics_studio-security-site/ldap_basedn", "")
das_webapp_ldap_domain = default("/configurations/data_analytics_studio-security-site/ldap_domain", "")
das_webapp_ldap_user_dn_pattern = default("/configurations/data_analytics_studio-security-site/ldap_user_dn_pattern", "")
das_webapp_ldap_group_dn_pattern = default("/configurations/data_analytics_studio-security-site/ldap_group_dn_pattern", "")
das_webapp_ldap_custom_ldap_query = default("/configurations/data_analytics_studio-security-site/custom_ldap_query", "")
das_webapp_ldap_group_filter = default("/configurations/data_analytics_studio-security-site/ldap_group_filter", "")
das_webapp_ldap_user_filter = default("/configurations/data_analytics_studio-security-site/ldap_user_filter", "")

data_analytics_studio_event_processor_server_protocol = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_server_protocol", "http")
data_analytics_studio_event_processor_server_port = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_server_port", "30900")
data_analytics_studio_event_processor_admin_server_port = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_admin_server_port", "30901")
data_analytics_studio_event_processor_keystore_file = default("/configurations/data_analytics_studio-security-site/event_processor_keystore_file", "")
data_analytics_studio_event_processor_keystore_password = default("/configurations/data_analytics_studio-security-site/event_processor_keystore_password", "")

data_analytics_studio_autocreate_db = default("/configurations/data_analytics_studio-database/das_autocreate_db", True)
if data_analytics_studio_autocreate_db:
  data_analytics_studio_database_host = data_analytics_studio_webapp_server_host
else:
  data_analytics_studio_database_host = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_host", "")

data_analytics_studio_database_port = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_port", "5432")
data_analytics_studio_database_name = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_name", "das")
data_analytics_studio_database_username = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_username", "das")
data_analytics_studio_database_password = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_password", "das")
data_analytics_studio_database_jdbc_url = format("jdbc:postgresql://{data_analytics_studio_database_host}:{data_analytics_studio_database_port}/{data_analytics_studio_database_name}")

das_credential_store_class_path = default("/configurations/data_analytics_studio-database/credentialStoreClassPath", "/var/lib/ambari-agent/cred/lib/*")

cluster_name = str(config['clusterName'])
version_for_stack_feature_checks = get_stack_feature_version(config)
hostname = config['agentLevelParams']['hostname'] if 'agentLevelParams' in config else config['hostname']
hostname = hostname.lower()
security_enabled = config['configurations']['cluster-env']['security_enabled']
user_group = config['configurations']['cluster-env']['user_group']

# ranger configs
hive_auth_security = default('/configurations/hive-env/hive_security_authorization', "none")
is_ranger_set = hive_auth_security.lower() == 'ranger'
is_authorization_set = default("/configurations/hiveserver2-site/hive.security.authorization.enabled", False)

enable_ranger_hive = is_ranger_set and is_authorization_set
if enable_ranger_hive:
  das_webapp_authorization_mode = "RANGER"
  policymgr_mgr_url = config['configurations']['admin-properties']['policymgr_external_url']
  if 'admin-properties' in config['configurations'] and 'policymgr_external_url' in config['configurations']['admin-properties'] and policymgr_mgr_url.endswith('/'):
    policymgr_mgr_url = policymgr_mgr_url.rstrip('/')

  # get the correct version to use for checking stack features
  stack_supports_ranger_audit_db = check_stack_feature(StackFeature.RANGER_AUDIT_DB_SUPPORT, version_for_stack_feature_checks)

  # ranger-env config
  ranger_env = config['configurations']['ranger-env']
  xml_configurations_supported = ranger_env['xml_configurations_supported']

  if xml_configurations_supported and stack_supports_ranger_audit_db:
    xa_audit_db_is_enabled = config['configurations']['ranger-hive-audit']['xasecure.audit.destination.db']
  else:
    xa_audit_db_is_enabled = False

  # confirm this value. it is related to upgrade I guess.
  stack_version = None

  # These are the folders to which the configs will be written to.
  ranger_hive_ssl_config_file = os.path.join(conf_dir, "ranger-policymgr-ssl.xml")

  repo_name = str(config['clusterName']) + '_hive'
  # update the repo name set in hive configs.
  # repo_name = default("/configurations/ranger-hive-security/ranger.plugin.hive.service.name", repo_name)
  stack_supports_ranger_kerberos = check_stack_feature(StackFeature.RANGER_KERBEROS_SUPPORT, version_for_stack_feature_checks)

  ssl_keystore_password = unicode(config['configurations']['ranger-hive-policymgr-ssl']['xasecure.policymgr.clientssl.keystore.password']) if xml_configurations_supported else None
  ssl_truststore_password = unicode(config['configurations']['ranger-hive-policymgr-ssl']['xasecure.policymgr.clientssl.truststore.password']) if xml_configurations_supported else None
  credential_file = format('/etc/ranger/{repo_name}/cred.jceks') if xml_configurations_supported else None

  configurationAttributesPropertyName = "configurationAttributes"
  if 'configuration_attributes' in config:
    configurationAttributesPropertyName = 'configuration_attributes'

  plugin_audit_attributes=config[configurationAttributesPropertyName]['ranger-hive-audit']
  plugin_security_attributes=config[configurationAttributesPropertyName]['ranger-hive-security']
  plugin_policymgr_ssl_attributes=config[configurationAttributesPropertyName]['ranger-hive-policymgr-ssl']

  xa_audit_db_password = ''
  if not is_empty(config['configurations']['admin-properties']['audit_db_password']) and stack_supports_ranger_audit_db:
    xa_audit_db_password = unicode(config['configurations']['admin-properties']['audit_db_password'])

  xa_audit_hdfs_is_enabled = default('/configurations/ranger-hive-audit/xasecure.audit.destination.hdfs', False) if xml_configurations_supported else False

else:
  das_webapp_authorization_mode = "NONE"


# das-hive-site.conf, das-hive-interactive-site.conf
hive_server2_support_dynamic_service_discovery = str(default("/configurations/hive-site/hive.server2.support.dynamic.service.discovery", "True")).lower()
hive_server2_zookeeper_namespace = default("/configurations/hive-site/hive.server2.zookeeper.namespace", "hiveserver2")
hive_interactive_server_zookeeper_namespace = default("/configurations/hive-interactive-site/hive.server2.zookeeper.namespace", "hiveserver2-interactive")
hive_zookeeper_quorum = default("/configurations/hive-site/hive.zookeeper.quorum", "")
hive_bind_host = default("/configurations/hive-site/hive.server2.thrift.bind.host", "")
hive_doAs = default("/configurations/hive-site/hive.server2.enable.doAs", "")

das_hive_site_conf_dict = {
  "hive.server2.zookeeper.namespace": hive_server2_zookeeper_namespace,
  "hive.server2.support.dynamic.service.discovery": hive_server2_support_dynamic_service_discovery,
  "hive.zookeeper.quorum": hive_zookeeper_quorum,
  "hive.server2.thrift.bind.host": hive_bind_host,
  "hive.server2.enable.doAs": hive_doAs
}

das_hive_interactive_site_conf_dict = {
  "hive.server2.support.dynamic.service.discovery": hive_server2_support_dynamic_service_discovery,
  "hive.server2.zookeeper.namespace": hive_interactive_server_zookeeper_namespace,
  "hive.zookeeper.quorum": hive_zookeeper_quorum
}

# das-event-processor.json
hive_metastore_warehouse_dir = config['configurations']['hive-site']["hive.metastore.warehouse.dir"]
hive_metastore_warehouse_external_dir = config['configurations']['hive-site']["hive.metastore.warehouse.external.dir"]
data_analytics_studio_event_processor_hive_base_dir = format(config["configurations"]["hive-site"]["hive.hook.proto.base-directory"])
data_analytics_studio_event_processor_tez_base_dir = format(config["configurations"]["tez-site"]["tez.history.logging.proto-base-dir"])
data_analytics_studio_event_processor_service_keytab = default("/configurations/data_analytics_studio-event_processor-properties/service_keytab", "")
data_analytics_studio_event_processor_service_principal = default("/configurations/data_analytics_studio-event_processor-properties/service_principal", "")

#create partial functions with common arguments for every HdfsResource call
#to create/delete/copyfromlocal hdfs directories/files we need to call params.HdfsResource in code
hdfs_user = status_params.hdfs_user
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hdfs_principal_name = default('/configurations/hadoop-env/hdfs_principal_name', None)
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
if 'hostLevelParams' in config and 'not_managed_hdfs_path_list' in config['hostLevelParams']:
    not_managed_resources = get_not_managed_resources()
elif 'clusterLevelParams' in config and 'not_managed_hdfs_path_list' in config['clusterLevelParams']:
    not_managed_resources = get_not_managed_resources()
else:
    not_managed_resources = ''
dfs_type = default("/clusterLevelParams/dfs_type", "")

HdfsResource = functools.partial(
  HdfsResource,
  user = hdfs_user,
  hdfs_resource_ignore_file = "/var/lib/ambari-agent/data/.hdfs_resource_ignore",
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  kinit_path_local = kinit_path_local,
  hadoop_bin_dir = hadoop_bin_dir,
  hadoop_conf_dir = hadoop_conf_dir,
  principal_name = hdfs_principal_name,
  hdfs_site = hdfs_site,
  default_fs = default_fs,
  immutable_paths = not_managed_resources,
  dfs_type = dfs_type
)
