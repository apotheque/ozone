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

import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.ozone.om.request.DatacenterUtils.resolveDatacenterMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for {@link DatacenterUtils}.
 */
public class TestDatacenterUtils {
  private static Stream<Arguments> datacenterSource() {
    return Stream.of(
            arguments("dc1,dc2,dc3", Stream.of("/dc1", "/dc2", "/dc3").collect(toSet())),
            arguments("dc1, dc2 , dc3", Stream.of("/dc1", "/dc2", "/dc3").collect(toSet())),
            arguments("dc1", Stream.of("/dc1").collect(toSet()))
    );
  }

  @ParameterizedTest
  @MethodSource("datacenterSource")
  public void testResolveDatacenterMetadata(String datacenters, Set<String> expected) {
    Set<String> result = resolveDatacenterMetadata(datacenters);
    
    assertEquals(expected, result);
  }

  private static Stream<String> emptyDatacenterSource() {
    return Stream.of("", " ", null);
  }

  @ParameterizedTest
  @MethodSource("emptyDatacenterSource")
  public void testResolveDatacenterMetadataWithEmptyOrNullString(@Nullable String datacenters) {
    Set<String> result = resolveDatacenterMetadata(datacenters);
    
    assertTrue(result.isEmpty());
  }
}
