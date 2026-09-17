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
import java.util.Arrays;
import java.util.Comparator;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class ReadRoutingPropertyTest extends TestBaseImpl
{
    @Test(timeout = 180000)
    public void multiKeyReadLimitsPreserveRowsAcrossNormalAndAccordTables() throws IOException
    {
        try (Cluster cluster = Cluster.build(3).withConfig(config -> config.with(NETWORK, GOSSIP)).start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"));
            String[] tables = { KEYSPACE + ".normal_read_property", KEYSPACE + ".accord_read_property" };
            cluster.schemaChange("CREATE TABLE " + tables[0] + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'off'");
            cluster.schemaChange("CREATE TABLE " + tables[1] + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");

            qt().withFixedSeed(TEST_SEED.getLong(18424L)).withExamples(8)
                .forAll(integers().between(0, 4), integers().between(-32, 32))
                .checkAssert((partitions, value) -> {
                    for (String table : tables)
                        assertReadLimits(cluster, table, partitions, value);
                });
        }
    }

    private static void assertReadLimits(Cluster cluster, String table, int partitions, int value)
    {
        int firstKey = value * 8;
        StringBuilder keys = new StringBuilder(Integer.toString(firstKey + 7));
        Object[][] expected = new Object[partitions * 2][];
        for (int offset = 0; offset < partitions; offset++)
        {
            int key = firstKey + offset;
            keys.append(", ").append(key);
            for (int rowIndex = 0; rowIndex < 2; rowIndex++)
            {
                int clustering = offset * 2 + rowIndex + 1;
                cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2",
                                               ALL, key, clustering, value);
                expected[offset * 2 + rowIndex] = row(key, clustering, value);
            }
        }

        for (int coordinator = 1; coordinator <= cluster.size(); coordinator++)
        {
            for (boolean reverse : new boolean[]{ false, true })
            {
                String query = "SELECT k, c, v FROM " + table + " WHERE k IN (" + keys + ')' +
                               (reverse ? " ORDER BY c DESC" : "");
                Object[][] full = cluster.coordinator(coordinator).execute(query, ALL);
                Object[][] sorted = full.clone();
                Arrays.sort(sorted, Comparator.<Object[]>comparingInt(result -> (Integer) result[0])
                                              .thenComparingInt(result -> (Integer) result[1]));
                assertRows(sorted, expected);
                int limit = Math.max(1, partitions);
                assertRows(cluster.coordinator(coordinator).execute(query + " LIMIT " + limit, ALL),
                           Arrays.copyOf(full, Math.min(limit, full.length)));
            }
        }
    }
}
