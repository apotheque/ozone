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

package org.apache.hadoop.ozone.net;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;

/** A mapping generator that assigns nodes to data centers randomly while ensuring balanced distribution. */
public class RandomMappingGenerator extends CachedDNSToSwitchMapping {
  public static final Set<String> AVAILABLE_DCS = Stream.of("/dc1", "/dc2", "/dc3").collect(Collectors.toSet());

  private final Map<String, List<String>> mapping =
      AVAILABLE_DCS.stream().collect(Collectors.toMap(dc -> dc, dc -> new ArrayList<>()));

  public RandomMappingGenerator() {
    super(null);
  }

  @Override
  public List<String> resolve(List<String> nodes) {
    List<String> result = new ArrayList<>();

    for (String nodeName : nodes) {
      String location = mapping.entrySet().stream()
                            .min(Comparator.comparing(entry -> entry.getValue().size()))
                            .map(Map.Entry::getKey)
                            .orElseThrow(() -> new IllegalArgumentException("Mapping is suspiciously empty"));

      mapping.get(location).add(nodeName);
      result.add(location);
    }

    return result;
  }

  @Override
  public void reloadCachedMappings() {

  }

  @Override
  public void reloadCachedMappings(List<String> list) {

  }
}
