/*
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
package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * This is the handler that process container restore command.
 */
@Command(
    name = "restore",
    description = "Restore a container replica from the source datanode to the target datanode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class RestoreSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "Id of the container to restore")
  private long containerId;

  @Option(description = "UUID of the source datanode to restore from", names = { "-s", "--source"})
  private String sourceId;

  @Option(description = "UUID of the target datanode to restore to", names = { "-t", "--target"})
  private String targetId;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    scmClient.restoreContainerReplica(containerId, sourceId, targetId);
  }
}
