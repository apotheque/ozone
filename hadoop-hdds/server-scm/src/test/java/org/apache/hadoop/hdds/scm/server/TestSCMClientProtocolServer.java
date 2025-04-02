/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RestoreContainerReplicaRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.replicateContainerCommand;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests to validate the SCMClientProtocolServer
 * servicing commands from the scm client.
 */
public class TestSCMClientProtocolServer {
  private OzoneConfiguration config;
  private SCMClientProtocolServer server;
  private StorageContainerManager scm;
  private StorageContainerLocationProtocolServerSideTranslatorPB service;
  private ContainerManager containerManager;
  private NodeManager nodeManager;

  @BeforeEach
  void setUp() throws Exception {
    config = SCMTestUtils.getConf();
    containerManager = Mockito.mock(ContainerManager.class);
    nodeManager = new MockNodeManager(true, 3);

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setContainerManager(containerManager);
    configurator.setScmNodeManager(nodeManager);
    config.set(OZONE_READONLY_ADMINISTRATORS, "testUser");
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();
    server = scm.getClientProtocolServer();
    service = new StorageContainerLocationProtocolServerSideTranslatorPB(server,
        scm, Mockito.mock(ProtocolMessageMetrics.class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  /**
   * Tests decommissioning of scm.
   */
  @Test
  public void testScmDecommissionRemoveScmErrors() throws Exception {
    String scmId = scm.getScmId();
    String err = "Cannot remove current leader.";

    DecommissionScmRequestProto request =
        DecommissionScmRequestProto.newBuilder()
            .setScmId(scmId)
            .build();

    DecommissionScmResponseProto resp =
        service.decommissionScm(request);

    // should have optional error message set in response
    assertTrue(resp.hasErrorMsg());
    assertEquals(err, resp.getErrorMsg());
  }

  @Test
  public void testReadOnlyAdmins() throws IOException {
    UserGroupInformation testUser = UserGroupInformation.
        createUserForTesting("testUser", new String[] {"testGroup"});

    try {
      // read operator
      server.getScm().checkAdminAccess(testUser, true);
      // write operator
      assertThrows(AccessControlException.class,
          () -> server.getScm().checkAdminAccess(testUser, false));
    } finally {
      UserGroupInformation.reset();
    }
  }

  @Test
  public void testRestoreContainerReplica() throws IOException {
    DatanodeDetails sourceNode = nodeManager.getAllNodes().get(0);
    DatanodeDetails targetNode = nodeManager.getAllNodes().get(1);
    ContainerInfo mockContainer = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, CLOSED);

    when(containerManager.getContainer(Mockito.any())).thenReturn(mockContainer);

    RestoreContainerReplicaRequestProto request =
        RestoreContainerReplicaRequestProto.newBuilder()
            .setContainerId(mockContainer.getContainerID())
            .setSourceId(sourceNode.getUuidString())
            .setTargetId(targetNode.getUuidString())
            .build();

    service.restoreContainerReplica(request);

    // if true, then the source datanode will be asked to push the container to the target datanode
    final boolean push = scm.getReplicationManager().getConfig().isPush();
    List<SCMCommand> commandQueue;
    if (push) {
      commandQueue = server.getScm().getScmNodeManager().getCommandQueue(sourceNode.getUuid());
    } else {
      commandQueue = server.getScm().getScmNodeManager().getCommandQueue(targetNode.getUuid());
    }
    assertEquals(1, commandQueue.size(), "Incorrect size of command queue for a datanode");

    SCMCommand cmd = commandQueue.get(0);
    assertEquals(replicateContainerCommand, cmd.getType(), "Wrong type of command for a datanode");

    ReplicateContainerCommand replicateCmd =  (ReplicateContainerCommand) cmd;
    assertEquals(mockContainer.getContainerID(), replicateCmd.getContainerID(), "Container id doesn't match");
    if (push) {
      assertEquals(targetNode.getUuid(), replicateCmd.getTargetDatanode().getUuid(),
          "Target datanode id doesn't match");
    } else {
      List<DatanodeDetails> sourceDatanodes = replicateCmd.getSourceDatanodes();
      assertEquals(1, sourceDatanodes.size(), "Incorrect number of source datanodes");
      assertEquals(sourceNode.getUuid(), sourceDatanodes.get(0).getUuid(), "Source datanode id doesn't match");
    }
  }

  @Test
  public void testRestoreContainerReplicaThrowsWhenTargetOutsideAllowedDatacenters() throws IOException {
    DatanodeDetails sourceNode = nodeManager.getAllNodes().get(0);
    DatanodeDetails targetNode = nodeManager.getAllNodes().get(1);
    ContainerInfo mockContainer = ReplicationTestUtil.createContainer(CLOSED,
        RatisReplicationConfig.getInstance(THREE), Collections.singleton("dc1"));

    when(containerManager.getContainer(Mockito.any())).thenReturn(mockContainer);

    RestoreContainerReplicaRequestProto request =
        RestoreContainerReplicaRequestProto.newBuilder()
            .setContainerId(mockContainer.getContainerID())
            .setSourceId(sourceNode.getUuidString())
            .setTargetId(targetNode.getUuidString())
            .build();

    assertThrows(SCMException.class, () -> service.restoreContainerReplica(request));
  }
}
