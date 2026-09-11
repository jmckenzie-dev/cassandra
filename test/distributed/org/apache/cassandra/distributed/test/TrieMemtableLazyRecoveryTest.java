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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrieMemtableLazyRecoveryTest extends TestBaseImpl
{
    @Test
    public void commitLogReplayActivatesLazyMemtable() throws Throwable
    {
        try (Cluster cluster = newCluster())
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.replay (pk int PRIMARY KEY, v int)"));
            assertDormant(cluster, "replay");
            for (int key = 0; key < 8; key++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.replay (pk, v) VALUES (?, ?)"), ONE, key, key + 10);

            String[] directories = cluster.get(1).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("replay");
                assertTrue(((TrieMemtable) cfs.getCurrentMemtable()).isInitialized());
                assertFalse(cfs.getCurrentMemtable().isClean());
                assertTrue(cfs.getLiveSSTables().isEmpty());
                try
                {
                    CommitLog.instance.sync(true);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
                return cfs.getDirectories().getCFDirectories().stream().map(Object::toString).toArray(String[]::new);
            });

            // The in-JVM shutdown closes the commit log without draining memtables.
            cluster.get(1).shutdown(false).get();
            for (String directory : directories)
            {
                Path path = Paths.get(directory);
                if (Files.exists(path))
                {
                    try (Stream<Path> files = Files.walk(path))
                    {
                        assertFalse("Shutdown flushed the replay table", files.anyMatch(file -> file.getFileName().toString().endsWith("-Data.db")));
                    }
                }
            }
            cluster.get(1).startup();

            for (int key = 0; key < 8; key++)
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s.replay WHERE pk = ?"), ONE, key), row(key, key + 10));
            flush(cluster, "replay");
            assertDormant(cluster, "replay");
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.replay (pk, v) VALUES (8, 18)"), ONE);
            flush(cluster, "replay");
            for (int key = 0; key < 9; key++)
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s.replay WHERE pk = ?"), ONE, key), row(key, key + 10));
            assertDormant(cluster, "replay");
        }
    }

    @Test
    public void indexesRemainQueryableWithDormantReplacement() throws Throwable
    {
        try (Cluster cluster = newCluster())
        {
            for (String indexType : new String[] { "sai", "legacy_local_table" })
            {
                String table = "indexed_" + indexType;
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v int)"));
                cluster.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + " (v) USING '" + indexType + "'"));
                await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    assertFalse(cfs.indexManager.listIndexes().isEmpty());
                    cfs.indexManager.listIndexes().forEach(index -> assertTrue(cfs.indexManager.isIndexQueryable(index)));
                }));
                assertDormant(cluster, table);

                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk, v) VALUES (1, 10)"), ONE);
                assertIndexRows(cluster, table, 10, row(1, 10));
                flush(cluster, table);
                assertDormant(cluster, table);
                assertIndexRows(cluster, table, 10, row(1, 10));
                assertDormant(cluster, table);

                cluster.coordinator(1).execute(withKeyspace("UPDATE %s." + table + " SET v = 20 WHERE pk = 1"), ONE);
                assertIndexRows(cluster, table, 10);
                assertIndexRows(cluster, table, 20, row(1, 20));
                flush(cluster, table);
                assertIndexRows(cluster, table, 10);
                assertIndexRows(cluster, table, 20, row(1, 20));
                assertDormant(cluster, table);

                cluster.coordinator(1).execute(withKeyspace("DELETE FROM %s." + table + " WHERE pk = 1"), ONE);
                flush(cluster, table);
                assertDormant(cluster, table);
                assertIndexRows(cluster, table, 20);
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore base = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    assertDormant(base);
                    if (indexType.equals("legacy_local_table"))
                    {
                        // Legacy index reads delete stale entries through a local write.
                        boolean foundIndex = false;
                        for (ColumnFamilyStore cfs : base.indexManager.getAllIndexColumnFamilyStores())
                        {
                            foundIndex = true;
                            assertTrue(cfs.name, ((TrieMemtable) cfs.getCurrentMemtable()).isInitialized());
                            assertFalse(cfs.name, cfs.getCurrentMemtable().isClean());
                        }
                        assertTrue(foundIndex);
                    }
                });
                flush(cluster, table);
                assertDormant(cluster, table);
                assertIndexRows(cluster, table, 20);
                assertDormant(cluster, table);
            }
        }
    }

    private Cluster newCluster() throws Throwable
    {
        return init(builder().withNodes(1).withSubnet(144).withConfig(config -> {
            config.with(NETWORK, GOSSIP);
            config.set("memtable", Map.of("configurations", Map.of("default", Map.of("class_name", "TrieMemtable",
                                                                                     "parameters", Map.of("lazy_initialization", "true")))));
            config.set("cursor_compaction_enabled", false);
        }).start());
    }

    private static void assertDormant(Cluster cluster, String table)
    {
        cluster.get(1).runOnInstance(() -> {
            for (ColumnFamilyStore cfs : Keyspace.open(KEYSPACE).getColumnFamilyStore(table).concatWithIndexes())
                assertDormant(cfs);
        });
    }

    private static void assertDormant(ColumnFamilyStore cfs)
    {
        assertTrue(cfs.getCurrentMemtable() instanceof TrieMemtable);
        assertFalse(cfs.name, ((TrieMemtable) cfs.getCurrentMemtable()).isInitialized());
        assertTrue(cfs.getCurrentMemtable().isClean());
    }

    private static void flush(Cluster cluster, String table)
    {
        cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table)
                                                 .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
    }

    private static void assertIndexRows(Cluster cluster, String table, int value, Object[]... expected)
    {
        assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s." + table + " WHERE v = ?"), ONE, value), expected);
    }
}
