/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.server.serveraction.upgrades;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.actionmanager.HostRoleStatus;
import org.apache.ambari.server.agent.CommandReport;
import org.apache.ambari.server.state.Cluster;
import org.apache.ambari.server.state.Config;
import org.apache.ambari.server.state.Host;
import org.apache.ambari.server.state.ServiceComponentHost;

/**
 * There was added new restriction for offsets.topic.replication.factor property value from HDP-3.*.
 * This value should be equal or less than number of Kafka Brokers.
 */
public class FixKafkaReplicationFactor extends AbstractUpgradeServerAction {
  private static final String SOURCE_CONFIG_TYPE = "kafka-broker";
  private static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";

  @Override
  public CommandReport execute(ConcurrentMap<String, Object> requestSharedDataContext)
    throws AmbariException, InterruptedException {

    String clusterName = getExecutionCommand().getClusterName();
    Cluster cluster = getClusters().getCluster(clusterName);
    Config config = cluster.getDesiredConfigByType(SOURCE_CONFIG_TYPE);

    if (config == null) {
      return createCommandReport(0, HostRoleStatus.FAILED, "{}",
                                 String.format("Source type %s not found", SOURCE_CONFIG_TYPE), "");
    }

    Map<String, String> properties = config.getProperties();
    String replicationFactor = properties.get(OFFSETS_TOPIC_REPLICATION_FACTOR);

    if (replicationFactor == null) {
      return createCommandReport(0, HostRoleStatus.COMPLETED, "{}",
                                 String.format("%s/%s property is null", SOURCE_CONFIG_TYPE, OFFSETS_TOPIC_REPLICATION_FACTOR),
                                 "");
    }
    List<ServiceComponentHost> brokers =
      cluster.getServiceComponentHosts("KAFKA", "KAFKA_BROKER");

    if (!replicationFactor.matches("^\\d+$") || Integer.parseInt(replicationFactor) > brokers.size()) {
      properties.put(OFFSETS_TOPIC_REPLICATION_FACTOR, Integer.valueOf(brokers.size()).toString());

      config.setProperties(properties);
      config.save();
      agentConfigsHolder.updateData(cluster.getClusterId(), cluster.getHosts().stream().map(Host::getHostId)
        .collect(Collectors.toList()));

      return createCommandReport(0, HostRoleStatus.COMPLETED, "{}",
                                 String.format("The %s/%s property was decreased according to number of Kafka Brokers %s. ",
                                               SOURCE_CONFIG_TYPE, OFFSETS_TOPIC_REPLICATION_FACTOR, brokers.size()), "");
    } else {
      return createCommandReport(0, HostRoleStatus.COMPLETED, "{}",
                                 String.format("The %s/%s property already has acceptable value. ",
                                               SOURCE_CONFIG_TYPE, OFFSETS_TOPIC_REPLICATION_FACTOR), "");
    }
  }
}
