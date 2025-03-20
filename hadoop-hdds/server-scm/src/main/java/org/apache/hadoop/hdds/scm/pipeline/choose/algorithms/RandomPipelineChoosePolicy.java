/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Random choose policy that randomly chooses pipeline.
 * That are we just randomly place containers without any considerations of
 * utilization.
 */
public class RandomPipelineChoosePolicy implements PipelineChoosePolicy {

  @Override
  @SuppressWarnings("java:S2245") // no need for secure random
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    int pipelineIndex = choosePipelineIndex(pipelineList, pri);
    if (pipelineIndex == -1) {
      return null;
    }
    return pipelineList.get(pipelineIndex);
  }

  /**
   * Given a list of pipelines, return the index of the chosen pipeline.
   * @param pipelineList List of pipelines
   * @param pri          PipelineRequestInformation
   * @return Index in the list of the chosen pipeline, or -1 if no pipeline
   *         could be selected.
   */
  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList, PipelineRequestInformation pri) {
    Set<String> requestedDatacenters = pri.getDatacenters();

    List<Integer> matchingIndices = new ArrayList<>();
    for (int i = 0; i < pipelineList.size(); i++) {
      if (pipelineList.get(i).getDatacenters().equals(requestedDatacenters)) {
        matchingIndices.add(i);
      }
    }

    if (matchingIndices.isEmpty()) {
      return -1;
    }

    return matchingIndices.get((int) (Math.random() * matchingIndices.size()));
  }
}
