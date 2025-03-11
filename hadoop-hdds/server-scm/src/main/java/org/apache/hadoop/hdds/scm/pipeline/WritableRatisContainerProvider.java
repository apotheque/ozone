/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to obtain a writable container for Ratis and Standalone pipelines.
 */
public class WritableRatisContainerProvider
    implements WritableContainerProvider<ReplicationConfig> {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableRatisContainerProvider.class);

  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManager containerManager;
  private final Map<String, String> dcMapping;
  private final NodeManager scmNodeManager;

  public WritableRatisContainerProvider(
      PipelineManager pipelineManager,
      ContainerManager containerManager,
      PipelineChoosePolicy pipelineChoosePolicy,
      NodeManager scmNodeManager,
      Map<String, String> dcMapping) {
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.dcMapping = dcMapping;
    this.scmNodeManager = scmNodeManager;
  }


  @Override
  public ContainerInfo getContainer(final long size,
      ReplicationConfig repConfig, String owner, ExcludeList excludeList, String datacenters)
      throws IOException {
    /*
      Here is the high level logic.

      1. We try to find pipelines in open state.

      2. If there are no pipelines in OPEN state, then we try to create one.

      3. We allocate a block from the available containers in the selected
      pipeline.

      TODO : #CLUTIL Support random picking of two containers from the list.
      So we can use different kind of policies.
    */

    String failureReason = null;

    //TODO we need to continue the refactor to use repConfig everywhere
    //in downstream managers.

    PipelineRequestInformation req =
        PipelineRequestInformation.Builder.getBuilder().setSize(size).build();

    ContainerInfo containerInfo =
        getContainer(repConfig, owner, excludeList, req, datacenters);
    if (containerInfo != null) {
      return containerInfo;
    }

    try {
      // TODO: #CLUTIL Remove creation logic when all replication types
      //  and factors are handled by pipeline creator
      // exclude nodes from other dcs
      List<DatanodeDetails> excludedNodes = Collections.emptyList();
      if (datacenters != null && !datacenters.isEmpty()) {
        excludedNodes = getExcludedNodesByDc(scmNodeManager.getAllNodes(), datacenters);
      }
      // TODO: why is pipeline created without accounting for excludeList???
      Pipeline pipeline = pipelineManager.createPipeline(repConfig, excludedNodes, Collections.emptyList());

      // wait until pipeline is ready
      pipelineManager.waitPipelineReady(pipeline.getId(), 0);

    } catch (SCMException se) {
      LOG.warn("Pipeline creation failed for repConfig {} " +
          "Datanodes may be used up. Try to see if any pipeline is in " +
              "ALLOCATED state, and then will wait for it to be OPEN",
              repConfig, se);
      List<Pipeline> allocatedPipelines = findPipelinesByState(repConfig,
              excludeList,
              Pipeline.PipelineState.ALLOCATED);
      if (!allocatedPipelines.isEmpty()) {
        List<PipelineID> allocatedPipelineIDs =
                allocatedPipelines.stream()
                        .map(p -> p.getId())
                        .collect(Collectors.toList());
        try {
          pipelineManager
                  .waitOnePipelineReady(allocatedPipelineIDs, 0);
        } catch (IOException e) {
          LOG.warn("Waiting for one of pipelines {} to be OPEN failed. ",
                  allocatedPipelineIDs, e);
          failureReason = "Waiting for one of pipelines to be OPEN failed. "
              + e.getMessage();
        }
      } else {
        failureReason = se.getMessage();
      }
    } catch (IOException e) {
      LOG.warn("Pipeline creation failed for repConfig: {}. "
          + "Retrying get pipelines call once.", repConfig, e);
      failureReason = e.getMessage();
    }

    // If Exception occurred or successful creation of pipeline do one
    // final try to fetch pipelines.
    containerInfo = getContainer(repConfig, owner, excludeList, req, datacenters);
    if (containerInfo != null) {
      return containerInfo;
    }

    // we have tried all strategies we know but somehow we are not able
    // to get a container for this block. Log that info and throw an exception.
    LOG.error(
        "Unable to allocate a block for the size: {}, repConfig: {}",
        size, repConfig);
    throw new IOException(
        "Unable to allocate a container to the block of size: " + size
            + ", replicationConfig: " + repConfig + ". " + failureReason);
  }

  @Nullable
  private ContainerInfo getContainer(ReplicationConfig repConfig, String owner,
      ExcludeList excludeList, PipelineRequestInformation req, String datacenters) {
    // Acquire pipeline manager lock, to avoid any updates to pipeline
    // while allocate container happens. This is to avoid scenario like
    // mentioned in HDDS-5655.
    pipelineManager.acquireReadLock();
    try {
      List<Pipeline> availablePipelines = findPipelinesByState(repConfig,
          excludeList, Pipeline.PipelineState.OPEN);
      return selectContainer(availablePipelines, req, owner, excludeList, datacenters);
    } finally {
      pipelineManager.releaseReadLock();
    }
  }

  private List<Pipeline> findPipelinesByState(
          final ReplicationConfig repConfig,
          final ExcludeList excludeList,
          final Pipeline.PipelineState pipelineState) {
    List<Pipeline> pipelines = pipelineManager.getPipelines(repConfig,
            pipelineState, excludeList.getDatanodes(),
            excludeList.getPipelineIds());
    if (pipelines.isEmpty() && !excludeList.isEmpty()) {
      // if no pipelines can be found, try finding pipeline without
      // exclusion
      pipelines = pipelineManager.getPipelines(repConfig, pipelineState);
    }
    return pipelines;
  }

  private @Nullable ContainerInfo selectContainer(
      List<Pipeline> availablePipelines, PipelineRequestInformation req,
      String owner, ExcludeList excludeList, String datacenters) {

    while (!availablePipelines.isEmpty()) {
      Pipeline pipeline = pipelineChoosePolicy.choosePipeline(
          availablePipelines, req);
      // pipeline must only use allowed datacenters
      if (datacenters != null && !datacenters.isEmpty()) {
        List<DatanodeDetails> excludedNodes = getExcludedNodesByDc(pipeline.getNodes(), datacenters);
        if (!excludedNodes.isEmpty()) {
          // there are nodes in pipeline besides allowed
          return null;
        }
      }

      // look for OPEN containers that match the criteria.
      final ContainerInfo containerInfo = containerManager.getMatchingContainer(
          req.getSize(), owner, pipeline, excludeList.getContainerIds(), datacenters);

      if (containerInfo != null) {
        return containerInfo;
      }

      availablePipelines.remove(pipeline);
    }

    return null;
  }

  private List<DatanodeDetails> getExcludedNodesByDc(List<DatanodeDetails> nodes, String datacenters) {
    Set<String> allowedDcs = Arrays.stream(datacenters.split(","))
        .collect(Collectors.toSet());
    return nodes.stream()
        .filter(node -> {
          String nodeDc = dcMapping.get(node.getHostName() + ":" +
              node.getPort(DatanodeDetails.Port.Name.RATIS).getValue());
          return !allowedDcs.contains(nodeDc);
        })
        .collect(Collectors.toList());
  }

}
