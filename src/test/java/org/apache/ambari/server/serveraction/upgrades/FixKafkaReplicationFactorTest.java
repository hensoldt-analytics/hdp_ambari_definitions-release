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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.ambari.server.actionmanager.ExecutionCommandWrapper;
import org.apache.ambari.server.actionmanager.HostRoleCommand;
import org.apache.ambari.server.actionmanager.HostRoleStatus;
import org.apache.ambari.server.agent.CommandReport;
import org.apache.ambari.server.agent.ExecutionCommand;
import org.apache.ambari.server.agent.stomp.AgentConfigsHolder;
import org.apache.ambari.server.state.Cluster;
import org.apache.ambari.server.state.Clusters;
import org.apache.ambari.server.state.Config;
import org.apache.ambari.server.state.ServiceComponentHost;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;

/**
 * Tests FixKafkaReplicationFactor logic. For these test we emulate cluster with 2 Kafka Brokers.
 */
public class FixKafkaReplicationFactorTest {

  private Injector injector;
  private Clusters clusters;
  private AgentConfigsHolder agentConfigsHolder;
  private Cluster cluster;
  private Field clustersField;
  private Field agentConfigsHolderField;
  private static final String SOURCE_CONFIG_TYPE = "kafka-broker";
  private static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";

  @Before
  public void setup() throws Exception {
    injector = EasyMock.createMock(Injector.class);
    clusters = EasyMock.createMock(Clusters.class);
    cluster = EasyMock.createMock(Cluster.class);
    agentConfigsHolder = createMock(AgentConfigsHolder.class);
    clustersField = AbstractUpgradeServerAction.class.getDeclaredField("m_clusters");
    clustersField.setAccessible(true);
    agentConfigsHolderField = AbstractUpgradeServerAction.class.getDeclaredField("agentConfigsHolder");
    agentConfigsHolderField.setAccessible(true);

    expect(clusters.getCluster((String) anyObject())).andReturn(cluster).anyTimes();
    expect(cluster.getClusterId()).andReturn(1L).atLeastOnce();
    expect(cluster.getHosts()).andReturn(Collections.emptyList()).atLeastOnce();

    ServiceComponentHost serviceComponentHostMock = createMock(ServiceComponentHost.class);
    expect(cluster.getServiceComponentHosts(eq("KAFKA"), eq("KAFKA_BROKER")))
      .andReturn(Arrays.asList(serviceComponentHostMock, serviceComponentHostMock)).atLeastOnce();
    agentConfigsHolder.updateData(eq(1L), eq(Collections.emptyList()));
    expectLastCall().atLeastOnce();

    expect(injector.getInstance(Clusters.class)).andReturn(clusters).atLeastOnce();
    replay(injector, clusters, agentConfigsHolder);
  }

  @Test
  public void testInvalidFixKafkaReplicationFactor() throws Exception {
    Map<String, String> mockProperties = new HashMap<String, String>() {{
      put(OFFSETS_TOPIC_REPLICATION_FACTOR, "3");
    }};

    testFixKafkaReplicationFactor(mockProperties);
  }

  @Test
  public void testValidFixKafkaReplicationFactor() throws Exception {
    Map<String, String> mockProperties = new HashMap<String, String>() {{
      put(OFFSETS_TOPIC_REPLICATION_FACTOR, "2");
    }};

    testFixKafkaReplicationFactor(mockProperties);
  }

  @Test
  public void testNotNumberFixKafkaReplicationFactor() throws Exception {
    Map<String, String> mockProperties = new HashMap<String, String>() {{
      put(OFFSETS_TOPIC_REPLICATION_FACTOR, "string");
    }};

    testFixKafkaReplicationFactor(mockProperties);
  }

  private void testFixKafkaReplicationFactor(Map<String, String> mockProperties) throws Exception {
    Config yarnSiteConfig = EasyMock.createNiceMock(Config.class);
    expect(yarnSiteConfig.getType()).andReturn(SOURCE_CONFIG_TYPE).anyTimes();
    expect(yarnSiteConfig.getProperties()).andReturn(mockProperties).anyTimes();

    expect(cluster.getDesiredConfigByType(SOURCE_CONFIG_TYPE)).andReturn(yarnSiteConfig).atLeastOnce();

    Map<String, String> commandParams = new HashMap<>();
    commandParams.put("clusterName", "c1");

    ExecutionCommand executionCommand = new ExecutionCommand();
    executionCommand.setCommandParams(commandParams);
    executionCommand.setClusterName("c1");

    HostRoleCommand hrc = EasyMock.createMock(HostRoleCommand.class);
    expect(hrc.getRequestId()).andReturn(1L).anyTimes();
    expect(hrc.getStageId()).andReturn(2L).anyTimes();
    expect(hrc.getExecutionCommandWrapper()).andReturn(new ExecutionCommandWrapper(executionCommand)).anyTimes();
    replay(cluster, hrc,yarnSiteConfig);

    FixKafkaReplicationFactor action = new FixKafkaReplicationFactor();
    clustersField.set(action, clusters);
    agentConfigsHolderField.set(action, agentConfigsHolder);

    action.setExecutionCommand(executionCommand);
    action.setHostRoleCommand(hrc);

    CommandReport report = action.execute(null);
    assertNotNull(report);
    assertEquals(HostRoleStatus.COMPLETED.toString(), report.getStatus());
    assertEquals(0, report.getExitCode());

    Cluster c = clusters.getCluster("c1");
    Config desiredKafkaBrokerConfig = c.getDesiredConfigByType(SOURCE_CONFIG_TYPE);

    Map<String, String> kafkaBrokerConfigMap = desiredKafkaBrokerConfig.getProperties();

    assertTrue(kafkaBrokerConfigMap.containsKey(OFFSETS_TOPIC_REPLICATION_FACTOR));

    assertEquals("2", kafkaBrokerConfigMap.get(OFFSETS_TOPIC_REPLICATION_FACTOR));
  }
}
