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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class DrainDurabilityPropertyTest extends TestBaseImpl
{
    @Test(timeout = 180000)
    public void drainPersistsRowsDespitePreShutdownHookFailure() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start(), 1))
        {
            String table = KEYSPACE + ".drain_durability_property";
            cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'off'");
            qt().withFixedSeed(TEST_SEED.getLong(18424L)).withExamples(3)
                .forAll(integers().between(1, 4), integers().between(-16, 16))
                .checkAssert((count, value) -> {
                    List<Object[]> expected = new ArrayList<>();
                    for (int c = 0; c < count; c++)
                    {
                        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)",
                                                       ALL, value, c, value);
                        expected.add(row(c, value));
                    }
                    cluster.get(1).runOnInstance(() -> {
                        List<String> events = new ArrayList<>();
                        assertTrue(StorageService.instance.addPreShutdownHook(() -> {
                            events.add("pre");
                            throw new IllegalStateException("expected drain property hook failure");
                        }));
                        assertTrue(StorageService.instance.addPostShutdownHook(() -> events.add("post")));
                        drain();
                        assertTrue(StorageService.instance.isDrained());
                        assertEquals(List.of("pre", "post"), events);
                        drain();
                        assertEquals(List.of("pre", "post"), events);
                    });
                    try
                    {
                        cluster.get(1).shutdown().get();
                    }
                    catch (Exception e)
                    {
                        throw new AssertionError("Node shutdown failed", e);
                    }
                    cluster.get(1).startup();
                    assertRows(cluster.coordinator(1).execute("SELECT c, v FROM " + table + " WHERE k = ? AND c < ?",
                                                            ALL, value, count),
                               expected.toArray(new Object[0][]));
                });
        }
    }

    private static void drain()
    {
        try
        {
            StorageService.instance.drain();
        }
        catch (Exception e)
        {
            throw new AssertionError("Node drain failed", e);
        }
    }
}
