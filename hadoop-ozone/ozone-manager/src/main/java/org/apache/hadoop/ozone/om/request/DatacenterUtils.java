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

package org.apache.hadoop.ozone.om.request;

import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Datacenter related utils.
 */
public final class DatacenterUtils {
  private DatacenterUtils() {

  }

  /**
   * Resolves and formats datacenters metadata into a set of strings prefixed with a "/" character.
   * The input string is expected to be a comma-separated list of datacenter names.
   *
   * @param datacenters the comma-separated string containing datacenter names.
   * @return a set of strings where each string is a datacenter name prefixed with "/".
   */
  public static Set<String> resolveDatacenterMetadata(String datacenters) {
    if (isNotBlank(datacenters)) {
      return stream(datacenters.split(","))
          .map(datacenter -> "/" + datacenter.trim())
          .collect(Collectors.toSet());
    } else {
      return emptySet();
    }
  }
}
