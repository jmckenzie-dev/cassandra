/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdleMemtableFlushTest extends TestBaseImpl
{
    @Test
    public void schedulerRetiresOnlyEligibleTablesAndWritesReactivate() throws Throwable
    {
        try (Cluster cluster = newCluster("600ms"))
        {
            create(cluster, "eligible", "trie", "UnifiedCompactionStrategy");
            create(cluster, "stcs", "trie", "SizeTieredCompactionStrategy");
            create(cluster, "skiplist", "skiplist", "UnifiedCompactionStrategy");
            create(cluster, "eager", "eager", "UnifiedCompactionStrategy");
            for (String table : new String[] { "eligible", "stcs", "skiplist", "eager" })
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk,v) VALUES (1,10)"), ONE);
            await().atMost(30, TimeUnit.SECONDS).until(() -> dormant(cluster, "eligible"));
            for (String table : new String[] { "stcs", "skiplist", "eager" })
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    assertFalse(cfs.getCurrentMemtable().isClean());
                    assertTrue(cfs.getLiveSSTables().isEmpty());
                });
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.eligible"), ONE), row(1, 10));
            assertTrue(dormant(cluster, "eligible"));
            cluster.coordinator(1).execute(withKeyspace("UPDATE %s.eligible SET v = 20 WHERE pk = 1"), ONE);
            assertFalse(dormant(cluster, "eligible"));
            await().atMost(30, TimeUnit.SECONDS).until(() -> dormant(cluster, "eligible"));
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.eligible"), ONE), row(1, 20));
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.eligible"), ONE), row(1, 20));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.eligible (pk,v) VALUES (2,30)"), ONE);
            await().atMost(30, TimeUnit.SECONDS).until(() -> dormant(cluster, "eligible"));
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.eligible WHERE pk = 2"), ONE), row(2, 30));
        }
    }

    @Test
    public void configurationDefaultsToDisabled() throws Throwable
    {
        try (Cluster cluster = newCluster(null))
        {
            cluster.get(1).runOnInstance(() -> assertEquals(0, DatabaseDescriptor.getMemtableIdleTimeoutNanos()));
            create(cluster, "disabled", "trie", "UnifiedCompactionStrategy");
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.disabled (pk,v) VALUES (1,10)"), ONE);
            TimeUnit.SECONDS.sleep(1);
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("disabled");
                assertFalse(cfs.getCurrentMemtable().isClean());
                assertFalse(((TrieMemtable) cfs.getCurrentMemtable()).idleFlushEligible());
                assertTrue(cfs.getLiveSSTables().isEmpty());
            });
        }
    }

    @Test
    public void scheduledFlushPreservesIndexQueries() throws Throwable
    {
        try (Cluster cluster = newCluster("200ms"))
        {
            for (String indexType : new String[] { "sai", "legacy_local_table" })
            {
                String table = "indexed_" + indexType;
                create(cluster, table, "trie", "UnifiedCompactionStrategy");
                cluster.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + " (v) USING '" + indexType + "'"));
                await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    assertFalse(cfs.indexManager.listIndexes().isEmpty());
                    cfs.indexManager.listIndexes().forEach(index -> assertTrue(cfs.indexManager.isIndexQueryable(index)));
                }));
                for (int value = 0; value < 5; value++)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk,v) VALUES (1,?)"), ONE, value);
                    await().atMost(30, TimeUnit.SECONDS).until(() -> dormant(cluster, table));
                    assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s." + table + " WHERE v = ?"), ONE, value), row(1, value));
                    if (value > 0)
                        assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s." + table + " WHERE v = ?"), ONE, value - 1));
                }
                cluster.coordinator(1).execute(withKeyspace("DELETE FROM %s." + table + " WHERE pk = 1"), ONE);
                await().atMost(30, TimeUnit.SECONDS).until(() -> dormant(cluster, table));
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s." + table + " WHERE v = 4"), ONE));
            }
        }
    }

    private Cluster newCluster(String timeout) throws Throwable
    {
        return init(Cluster.build(1).withSubnet(146).withConfig(config -> {
            config.set("memtable", Map.of("configurations", Map.of(
                "trie", Map.of("class_name", "TrieMemtable", "parameters", Map.of("lazy_initialization", "true")),
                "eager", Map.of("class_name", "TrieMemtable", "parameters", Map.of("lazy_initialization", "false")),
                "skiplist", Map.of("class_name", "SkipListMemtable"))));
            config.set("cursor_compaction_enabled", true);
            if (timeout != null)
                config.set("memtable_idle_timeout", timeout);
        }).start());
    }

    private static void create(Cluster cluster, String table, String memtable, String strategy)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v int) WITH memtable = '" + memtable +
                                         "' AND compaction = {'class':'" + strategy + "'}"));
    }

    private static boolean dormant(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            return !((TrieMemtable) cfs.getCurrentMemtable()).isInitialized() && !cfs.getLiveSSTables().isEmpty()
                   && cfs.getTracker().getView().flushingMemtables.isEmpty();
        });
    }
}
