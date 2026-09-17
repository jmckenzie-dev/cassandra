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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class MutationRoutingPropertyTest extends TestBaseImpl
{
    @Test(timeout = 180000)
    public void acknowledgedBatchesReachEveryReplicaWithoutLosingTimestamps() throws IOException
    {
        try (Cluster cluster = Cluster.build(4).withDCs(2)
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                             "{'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 2}"));
            String table = KEYSPACE + ".mutation_routing_property";
            cluster.schemaChange("CREATE TABLE " + table + " (k int PRIMARY KEY, v int)");

            qt().withFixedSeed(TEST_SEED.getLong(18424L)).withExamples(8)
                .forAll(integers().between(1, 4), integers().between(-64, 64))
                .checkAssert((partitions, value) -> {
                    for (int coordinator = 1; coordinator <= cluster.size(); coordinator++)
                    {
                        for (int mode = 0; mode < 2; mode++)
                        {
                            int firstKey = (value + 64) * 32 + (coordinator - 1) * 8 + mode * 4;
                            boolean logged = mode == 0;
                            cluster.coordinator(coordinator).execute(batch(table, firstKey, partitions, value, 2, logged), ALL);
                            assertReplicas(cluster, table, firstKey, partitions, value);

                            cluster.coordinator(coordinator).execute(batch(table, firstKey, partitions, value + 1, 1, logged), ALL);
                            assertReplicas(cluster, table, firstKey, partitions, value);
                        }
                    }
                });
        }
    }

    private static String batch(String table, int firstKey, int partitions, int value, long timestamp, boolean logged)
    {
        StringBuilder query = new StringBuilder(logged ? "BEGIN BATCH " : "BEGIN UNLOGGED BATCH ");
        for (int offset = 0; offset < partitions; offset++)
            query.append("INSERT INTO ").append(table).append(" (k, v) VALUES (")
                 .append(firstKey + offset).append(", ").append(value)
                 .append(") USING TIMESTAMP ").append(timestamp).append("; ");
        return query.append("APPLY BATCH").toString();
    }

    private static void assertReplicas(Cluster cluster, String table, int firstKey, int partitions, int value)
    {
        for (IInvokableInstance replica : cluster)
            for (int offset = 0; offset < partitions; offset++)
                assertRows(replica.executeInternal("SELECT v, writetime(v) FROM " + table + " WHERE k = ?", firstKey + offset),
                           row(value, 2L));
    }
}
