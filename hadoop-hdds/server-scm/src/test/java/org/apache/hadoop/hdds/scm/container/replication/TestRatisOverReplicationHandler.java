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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DC_DATANODE_MAPPING_KEY;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainer;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicasAcrossDcs;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicasWithSameOrigin;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RatisOverReplicationHandler}.
 */
public class TestRatisOverReplicationHandler {
  private ContainerInfo container;
  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private PlacementPolicy policy;
  private OzoneConfiguration conf;
  private ReplicationManager replicationManager;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;

  @BeforeEach
  public void setup() throws NodeNotFoundException, NotLeaderException,
      CommandTargetOverloadedException {
    container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RATIS_REPLICATION_CONFIG);
    conf = SCMTestUtils.getConf();
    policy = Mockito.mock(PlacementPolicy.class);
    when(policy.validateContainerPlacement(
        anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));

    replicationManager = Mockito.mock(ReplicationManager.class);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          return new NodeStatus(dd.getPersistedOpState(),
              HddsProtos.NodeState.HEALTHY, 0);
        });

    commandsSent = new HashSet<>();
    ReplicationTestUtil.mockRMSendThrottledDeleteCommand(replicationManager,
        commandsSent);

    GenericTestUtils.setLogLevel(RatisOverReplicationHandler.LOG, Level.DEBUG);
  }

  /**
   * Handler should create one delete command when a closed ratis container
   * has 5 replicas and 1 pending delete.
   */
  @Test
  public void testOverReplicatedClosedContainer() throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            MockDatanodeDetails.randomDatanodeDetails(), 0));

    // 1 replica is already pending delete, so only 1 new command should be
    // created
    testProcessing(replicas, pendingOps, getOverReplicatedHealthResult(),
        1);
  }

  /**
   * Container has 4 replicas and 1 stale so none should be deleted.
   */
  @Test
  public void testOverReplicatedClosedContainerWithStale() throws IOException,
      NodeNotFoundException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);

    ContainerReplica stale = replicas.stream().findFirst().get();
    when(replicationManager.getNodeStatus(stale.getDatanodeDetails()))
        .thenAnswer(invocation ->
            NodeStatus.inServiceStale());

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);
  }

  /**
   * Container has up to 6 replicas spread across 3 datacenters with replication factor 3.
   * Test different combinations of replicas.
   */
  @ParameterizedTest
  @CsvSource({
      // number of replicas to keep per datacenter
      "1, 1, 1", // no over replication, shouldn't delete any replicas
      "1, 2, 1", // over replication in 2nd dc, should create 1 delete command
      "2, 2, 2", // over replication in every dc, should create 3 delete commands
  })
  public void testOverReplicatedAcrossDcs(int keepInDc1, int keepInDc2,
                                          int keepInDc3) throws IOException {
    ReplicationFactor replicationFactor = ReplicationFactor.THREE;
    conf.set(OZONE_REPLICATION, replicationFactor.toString());
    conf.set(OZONE_SCM_DC_DATANODE_MAPPING_KEY,
        "localhost:0=dc0,localhost:1=dc0,localhost:2=dc1,localhost:3=dc1,localhost:4=dc2,localhost:5=dc2");
    Map<String, String> dcMapping = ScmUtils.getDcMapping(conf);
    container = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.CLOSED, RatisReplicationConfig.getInstance(replicationFactor.toProto()),
        new HashSet<>(dcMapping.values()));

    // generate 6 replicas spread across 3 datacenters
    Map<String, List<ContainerReplica>> replicasByDc = createReplicasAcrossDcs(container.containerID(), State.CLOSED,
        conf).stream()
        .collect(Collectors.groupingBy(r -> {
          DatanodeDetails node = r.getDatanodeDetails();
          return dcMapping.get(node.getHostName() + ":" +
              node.getPort(DatanodeDetails.Port.Name.RATIS).getValue());
        }));

    // remove the specified number of replicas
    Set<ContainerReplica> replicas = new HashSet<>();
    for (Map.Entry<String, List<ContainerReplica>> entry : replicasByDc.entrySet()) {
      switch (entry.getKey()) {
      case "dc0":
        replicas.addAll(entry.getValue().stream().limit(keepInDc1).collect(Collectors.toList()));
        break;
      case "dc1":
        replicas.addAll(entry.getValue().stream().limit(keepInDc2).collect(Collectors.toList()));
        break;
      case "dc2":
        replicas.addAll(entry.getValue().stream().limit(keepInDc3).collect(Collectors.toList()));
        break;
      default:
      }
    }

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), keepInDc1 + keepInDc2 + keepInDc3 - 3);
  }

  /**
   * The container is quasi closed. All 4 replicas are quasi closed and
   * originate from the same datanode. This container is over replicated.
   * Handler should preserve 1 replica and any 1 of the other 3 replicas can
   * be deleted.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainerWithSameOrigin()
      throws IOException {
    container = createContainer(HddsProtos.LifeCycleState.QUASI_CLOSED,
        RATIS_REPLICATION_CONFIG);
    Set<ContainerReplica> replicas =
        createReplicasWithSameOrigin(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0, 0);

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 1);
  }

  /**
   * The container is quasi closed. All replicas are quasi closed but
   * originate from different datanodes. While this container is over
   * replicated, handler should not create a delete command for any replica. It
   * tries to preserve one replica per unique origin datanode.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainerWithDifferentOrigins()
      throws IOException, NodeNotFoundException {
    container = createContainer(HddsProtos.LifeCycleState.QUASI_CLOSED,
        RATIS_REPLICATION_CONFIG);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0, 0, 0);
    /*
     Even an unhealthy replica shouldn't be deleted if it has a unique
     origin. It might be possible to close this replica in the future.
     */
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0,
            HddsProtos.NodeOperationalState.IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);

    /*
    Now, introduce two UNHEALTHY replicas that share the same origin node as
    the existing UNHEALTHY replica. They're on decommissioning and stale
    nodes, respectively. Still no replica should be deleted, because these are
    likely going away soon anyway.
     */
    replicas.add(
        createContainerReplica(container.containerID(), 0, DECOMMISSIONING,
            State.UNHEALTHY, container.getNumberOfKeys(),
            container.getUsedBytes(),
            MockDatanodeDetails.randomDatanodeDetails(),
            unhealthyReplica.getOriginDatanodeId()));
    DatanodeDetails staleNode =
        MockDatanodeDetails.randomDatanodeDetails();
    replicas.add(
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            State.UNHEALTHY, container.getNumberOfKeys(),
            container.getUsedBytes(), staleNode,
            unhealthyReplica.getOriginDatanodeId()));
    when(replicationManager.getNodeStatus(eq(staleNode)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          return new NodeStatus(dd.getPersistedOpState(),
              HddsProtos.NodeState.STALE, 0);
        });

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);
  }

  @Test
  public void testClosedOverReplicatedWithAllUnhealthyReplicas()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        State.UNHEALTHY, 0, 0, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            MockDatanodeDetails.randomDatanodeDetails(), 0));

    // 1 replica is already pending delete, so only 1 new command should be
    // created
    testProcessing(replicas, pendingOps, getOverReplicatedHealthResult(),
        1);
  }

  @Test
  public void testClosedOverReplicatedWithExcessUnhealthy() throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        State.CLOSED, 0, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            State.UNHEALTHY);
    replicas.add(unhealthyReplica);

    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands =
        testProcessing(replicas, Collections.emptyList(),
            getOverReplicatedHealthResult(),
            1);
    Pair<DatanodeDetails, SCMCommand<?>> command = commands.iterator().next();
    assertEquals(unhealthyReplica.getDatanodeDetails(),
        command.getKey());
  }

  /**
   * Handler should not create any delete commands if removing a replica
   * makes the container mis replicated.
   */
  @Test
  public void testOverReplicatedContainerBecomesMisReplicatedOnRemoving()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0, 0);

    // Ensure a mis-replicated status is returned when 4 or fewer replicas are
    // checked.
    when(policy.validateContainerPlacement(
        argThat(list -> list.size() <= 4), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 3));

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);
  }

  /**
   * In this test, the container is already mis-replicated, being on 2 racks rather than 3.
   * Removing a replica does not make it "more" mis-replicated, so the handler should remove
   * one replica.
   * @throws IOException
   */
  @Test
  public void testOverReplicatedContainerAlreadyMisReplicated()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);

    // Ensure a mis-replicated status is always returned.
    when(policy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 3, 3));

    testProcessing(replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 1);
  }

  @Test
  public void testOverReplicatedContainerBecomesOnSecondRemoval()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0, 0);

    // Ensure a mis-replicated status is returned when 3 or fewer replicas are
    // checked.
    when(policy.validateContainerPlacement(argThat(list -> list.size() <= 3), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 3));

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 1);
  }

  @Test
  public void testOverReplicatedAllUnhealthySameBCSID()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0, 0);

    ContainerReplica shouldDelete = replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .findFirst().get();

    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands =
        testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 1);
    Pair<DatanodeDetails, SCMCommand<?>> commandPair
        = commands.iterator().next();
    assertEquals(shouldDelete.getDatanodeDetails(),
        commandPair.getKey());
  }

  @Test
  public void testOverReplicatedAllUnhealthyPicksLowestBCSID()
      throws IOException {
    final long sequenceID = 20;
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerReplica lowestSequenceIDReplica = createContainerReplica(
        container.containerID(), 0, IN_SERVICE, State.UNHEALTHY, sequenceID);
    replicas.add(lowestSequenceIDReplica);
    for (int i = 1; i < 4; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0,
          IN_SERVICE, State.UNHEALTHY, sequenceID + i));
    }
    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands =
        testProcessing(replicas, Collections.emptyList(),
            getOverReplicatedHealthResult(), 1);
    Pair<DatanodeDetails, SCMCommand<?>> commandPair
        = commands.iterator().next();
    assertEquals(lowestSequenceIDReplica.getDatanodeDetails(),
        commandPair.getKey());
  }

  /**
   * Closed container with 4 closed replicas and 1 quasi closed replica. This
   * container is over replicated and the handler should create a delete
   * command for the quasi closed replica even if it violates the placement
   * policy. Once the quasi closed container is removed and we have 4
   * replicas, then the mocked placement policy considers the container mis
   * replicated. As long as the rack count does not change, another replica
   * can be removed.
   */
  @Test
  public void testOverReplicatedClosedContainerWithQuasiClosedReplica()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);
    ContainerReplica quasiClosedReplica =
        createContainerReplica(container.containerID(), 0,
            HddsProtos.NodeOperationalState.IN_SERVICE,
            ContainerReplicaProto.State.QUASI_CLOSED);
    replicas.add(quasiClosedReplica);

    // Ensure a mis-replicated status is returned when 4 or fewer replicas are
    // checked.
    when(policy.validateContainerPlacement(
            argThat(list -> list.size() <= 4), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 3));

    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands = testProcessing(
        replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 2);
    Set<DatanodeDetails> datanodes =
        commands.stream().map(Pair::getKey).collect(Collectors.toSet());
    assertTrue(datanodes.contains(quasiClosedReplica.getDatanodeDetails()));
  }

  @Test
  public void testOverReplicatedWithDecomAndMaintenanceReplicas()
      throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);
    ContainerReplica decommissioningReplica =
        createContainerReplica(container.containerID(), 0,
            HddsProtos.NodeOperationalState.DECOMMISSIONING,
            ContainerReplicaProto.State.CLOSED);
    ContainerReplica maintenanceReplica =
        createContainerReplica(container.containerID(), 0,
            HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
            ContainerReplicaProto.State.CLOSED);
    replicas.add(decommissioningReplica);
    replicas.add(maintenanceReplica);

    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands = testProcessing(
        replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 1);
    Set<DatanodeDetails> datanodes =
        commands.stream().map(Pair::getKey).collect(Collectors.toSet());
    assertFalse(
        datanodes.contains(decommissioningReplica.getDatanodeDetails()));
    assertFalse(
        datanodes.contains(maintenanceReplica.getDatanodeDetails()));
  }

  @Test
  public void testPerfectlyReplicatedContainer() throws IOException {
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0);

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);

    // now test 4 replicas and 1 pending delete
    replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
            MockDatanodeDetails.randomDatanodeDetails(), 0));

    testProcessing(replicas, pendingOps, getOverReplicatedHealthResult(), 0);
  }
  @Test
  public void testOverReplicationOfQuasiClosedReplicaWithWrongSequenceID()
      throws IOException {
    final long sequenceID = 20;
    container = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1,
        HddsProtos.LifeCycleState.CLOSED, sequenceID);

    final Set<ContainerReplica> replicas = new HashSet<>(2);
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.CLOSED, sequenceID));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.CLOSED, sequenceID));

    final ContainerReplica quasiClosedReplica =
        createContainerReplica(container.containerID(), 0,
            IN_SERVICE, State.QUASI_CLOSED, sequenceID - 1);
    replicas.add(quasiClosedReplica);
    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 0);

    // Add another CLOSED replica
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, ContainerReplicaProto.State.CLOSED, sequenceID));

    testProcessing(replicas, Collections.emptyList(),
        getOverReplicatedHealthResult(), 1);
  }

  @Test
  public void testDeleteThrottlingMisMatchedReplica() throws IOException {
    Set<ContainerReplica> closedReplicas = createReplicas(
        container.containerID(), ContainerReplicaProto.State.CLOSED,
        0, 0, 0, 0);

    ContainerReplica quasiClosedReplica = createContainerReplica(
        container.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.QUASI_CLOSED);

    // When processing the quasi closed replica, simulate an overloaded
    // exception so that it does not get deleted. Then we can ensure that only
    // one of the CLOSED replicas is removed.
    doThrow(CommandTargetOverloadedException.class)
        .when(replicationManager)
        .sendThrottledDeleteCommand(Mockito.any(ContainerInfo.class),
            anyInt(),
            eq(quasiClosedReplica.getDatanodeDetails()),
            anyBoolean());

    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(quasiClosedReplica);
    replicas.addAll(closedReplicas);

    RatisOverReplicationHandler handler = new RatisOverReplicationHandler(policy, conf, replicationManager);

    try {
      handler.processAndSendCommands(replicas, Collections.emptyList(),
          getOverReplicatedHealthResult(), 2);
      fail("Expected CommandTargetOverloadedException");
    } catch (CommandTargetOverloadedException e) {
      // Expected
    }
    assertEquals(1, commandsSent.size());
    Pair<DatanodeDetails, SCMCommand<?>> cmd = commandsSent.iterator().next();
    assertNotEquals(quasiClosedReplica.getDatanodeDetails(),
        cmd.getKey());
  }

  @Test
  public void testDeleteThrottling() throws IOException {
    Set<ContainerReplica> closedReplicas = createReplicas(
        container.containerID(), ContainerReplicaProto.State.CLOSED,
        0, 0, 0, 0, 0);

    final AtomicBoolean shouldThrow = new AtomicBoolean(true);
    // On the first call we throw, on subsequent calls we succeed.
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (shouldThrow.get()) {
        shouldThrow.set(false);
        throw new CommandTargetOverloadedException("Test exception");
      }
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      int replicaIndex = invocationOnMock.getArgument(1);
      DatanodeDetails target = invocationOnMock.getArgument(2);
      boolean forceDelete = invocationOnMock.getArgument(3);
      DeleteContainerCommand deleteCommand = new DeleteContainerCommand(
          containerInfo.getContainerID(), forceDelete);
      deleteCommand.setReplicaIndex(replicaIndex);
      commandsSent.add(Pair.of(target, deleteCommand));
      return null;
    }).when(replicationManager)
        .sendThrottledDeleteCommand(any(), anyInt(), any(), anyBoolean());

    RatisOverReplicationHandler handler =
        new RatisOverReplicationHandler(policy, conf, replicationManager);

    // Only 1 command should be sent, as the first call to sendThrottledDelete
    // throws an overloaded exception. Rather than skip to the next one, the skipped
    // one should get retried later.
    assertThrows(CommandTargetOverloadedException.class,
        () -> handler.processAndSendCommands(closedReplicas, Collections.emptyList(),
            getOverReplicatedHealthResult(), 2));
    assertEquals(1, commandsSent.size());
  }

  /**
   * Tests whether the specified expectNumCommands number of commands are
   * created by the handler.
   *
   * @param replicas          All replicas of the container
   * @param pendingOps        Collection of pending ops
   * @param healthResult      ContainerHealthResult that should be passed to the
   *                          handler
   * @param expectNumCommands number of commands expected to be created by
   *                          the handler
   * @return set of commands
   */
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> testProcessing(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult healthResult,
      int expectNumCommands) throws IOException {
    RatisOverReplicationHandler handler =
        new RatisOverReplicationHandler(policy, conf, replicationManager);

    handler.processAndSendCommands(replicas, pendingOps,
            healthResult, 2);
    assertEquals(expectNumCommands, commandsSent.size());

    return commandsSent;
  }

  private ContainerHealthResult.OverReplicatedHealthResult
      getOverReplicatedHealthResult() {
    ContainerHealthResult.OverReplicatedHealthResult healthResult =
        Mockito.mock(ContainerHealthResult.OverReplicatedHealthResult.class);
    when(healthResult.getContainerInfo()).thenReturn(container);
    return healthResult;
  }
}
