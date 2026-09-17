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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class EffectiveOwnershipPropertyTest extends TestBaseImpl
{
    @Test(timeout = 180000)
    public void ownershipConservesReplicationPerDatacenter() throws IOException
    {
        try (Cluster cluster = Cluster.build(4).withDCs(2)
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                             "{'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}"));
            Map<String, String> datacenters = new HashMap<>();
            for (IInvokableInstance node : cluster)
            {
                String endpoint = node.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
                String dc = node.callOnInstance(DatabaseDescriptor::getLocalDataCenter);
                datacenters.put(endpoint, dc);
            }
            qt().withFixedSeed(TEST_SEED.getLong(18424L)).withExamples(8)
                .forAll(integers().between(1, 2), integers().between(1, 2))
                .checkAssert((firstRf, secondRf) -> {
                    cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = " +
                                                     "{'class': 'NetworkTopologyStrategy', 'datacenter1': " + firstRf +
                                                     ", 'datacenter2': " + secondRf + "}"));
                    LinkedHashMap<String, Float> explicit = cluster.get(1).callOnInstance(
                        () -> StorageService.instance.effectiveOwnershipWithPort(KEYSPACE));
                    LinkedHashMap<String, Float> inferred = cluster.get(1).callOnInstance(
                        () -> StorageService.instance.effectiveOwnershipWithPort(null));
                    assertEquals(explicit, inferred);
                    assertEquals(new ArrayList<>(explicit.keySet()), new ArrayList<>(inferred.keySet()));
                    assertEquals(datacenters.keySet(), explicit.keySet());
                    Map<String, Float> totals = new HashMap<>();
                    List<String> order = new ArrayList<>();
                    explicit.forEach((endpoint, fraction) -> {
                        assertTrue(fraction >= 0 && fraction <= 1);
                        String dc = datacenters.get(endpoint);
                        order.add(dc);
                        totals.merge(dc, fraction, Float::sum);
                    });
                    assertEquals(List.of("datacenter1", "datacenter1", "datacenter2", "datacenter2"), order);
                    assertEquals(firstRf.doubleValue(), totals.get("datacenter1"), 0.00001);
                    assertEquals(secondRf.doubleValue(), totals.get("datacenter2"), 0.00001);
                });
            cluster.get(1).runOnInstance(() -> {
                Throwable missing = catchThrowable(() -> StorageService.instance.effectiveOwnershipWithPort("missing_ownership_keyspace"));
                assertNotNull(missing);
                assertEquals(IllegalArgumentException.class, missing.getClass());
                Throwable local = catchThrowable(() -> StorageService.instance.effectiveOwnershipWithPort("system"));
                assertNotNull(local);
                assertEquals(IllegalStateException.class, local.getClass());
            });
        }
    }
}
