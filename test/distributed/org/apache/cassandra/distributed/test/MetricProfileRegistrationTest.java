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

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MetricProfileRegistrationTest extends TestBaseImpl
{
    private static final String TABLE = "profiled";

    @Test
    public void omittedProfilePreservesRegistrationAndRecording() throws Throwable
    {
        exerciseProfile(null, true);
    }

    @Test
    public void allProfilePreservesRegistrationAndRecording() throws Throwable
    {
        exerciseProfile("all_metrics.yml", true);
    }

    @Test
    public void simpleProfileFiltersExportsAndReleasesHiddenChildren() throws Throwable
    {
        exerciseProfile("simple_metrics.yml", false);
    }

    @Test
    public void simpleProfileCanRetainLegacyAliases() throws Throwable
    {
        exerciseAliasOption("simple_metrics.yml", false, true);
    }

    @Test
    public void allProfileCanOmitLegacyAliases() throws Throwable
    {
        exerciseAliasOption("all_metrics.yml", true, false);
    }

    private void exerciseAliasOption(String profile, boolean all, boolean aliases) throws Throwable
    {
        Path path = Files.createTempFile(Files.createDirectories(Paths.get("tmp")), "metric-aliases-", ".yml");
        try
        {
            String yaml = Files.readString(Paths.get("conf", profile));
            Files.writeString(path, yaml.replace("include_legacy_aliases: " + !aliases, "include_legacy_aliases: " + aliases));
            exerciseProfile(path.toAbsolutePath().toString(), all);
        }
        finally
        {
            Files.delete(path);
        }
    }

    private void exerciseProfile(String profile, boolean all) throws Throwable
    {
        String location = profile == null ? null : Paths.get("conf").resolve(profile).toAbsolutePath().toString();
        try (Cluster cluster = init(builder().withNodes(1).withConfig(config -> {
            config.with(JMX);
            config.set("metrics_config_file", location);
            config.set("optimized_metrics_enabled", true);
            config.set("compact_jmx_registration_enabled", true);
            config.set("memtable", Map.of("configurations", Map.of("default", Map.of("class_name", "TrieMemtable",
                                                                                     "parameters", Map.of("lazy_initialization", "true")))));
            config.set("cursor_compaction_enabled", false);
        }).start()))
        {
            int[] baseline = cluster.get(1).callOnInstance(() -> new int[]{ tableOwners(), children(TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY) });
            for (int generation = 0; generation < 3; generation++)
            {
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v int)"));
                cluster.get(1).runOnInstance(() -> {
                    assertExports(all);
                    assertEquals(baseline[0] + 1, tableOwners());
                    assertEquals(baseline[1] + 1, children(TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY));
                    assertEquals(1, children(Keyspace.open(KEYSPACE).metric.readLatency));
                    assertFalse(((TrieMemtable) table().getCurrentMemtable()).isInitialized());
                });

                for (int key = 0; key < 4; key++)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)"), ONE, key, key + 10);
                    assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s." + TABLE + " WHERE pk = ?"), ONE, key),
                               row(key, key + 10));
                }
                cluster.get(1).runOnInstance(() -> table().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
                for (int key = 0; key < 4; key++)
                    assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s." + TABLE + " WHERE pk = ?"), ONE, key),
                               row(key, key + 10));

                long[] history = cluster.get(1).callOnInstance(() -> {
                    TableMetrics metric = table().metric;
                    assertFalse(table().getLiveSSTables().isEmpty());
                    assertFalse(((TrieMemtable) table().getCurrentMemtable()).isInitialized());
                    assertTrue(metric.coordinatorReadLatency.getCount() >= 8);
                    assertTrue(metric.coordinatorWriteLatency.getCount() >= 4);
                    assertTrue(metric.readLatency.latency.getCount() >= 8);
                    assertTrue(metric.writeLatency.latency.getCount() >= 4);
                    assertTrue(metric.totalDiskSpaceUsed.getCount() > 0);
                    assertTrue(metric.uncompressedLiveDiskSpaceUsed.getCount() > 0);
                    assertEquals(0, metric.pendingFlushes.getCount());
                    assertTrue(((Number) ((Gauge<?>) Metrics.getMetrics().get(globalName("UncompressedLiveDiskSpaceUsed").getMetricName())).getValue()).longValue()
                               >= metric.uncompressedLiveDiskSpaceUsed.getCount());

                    long before = TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.latency.getCount();
                    long duration = TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.totalLatency.getCount();
                    metric.keyMigration.addNano(1_000_000);
                    metric.keyMigration.addNano(2_000_000);
                    assertEquals(before + 2, TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.latency.getCount());
                    assertEquals(duration + 3000, TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.totalLatency.getCount());
                    return history();
                });

                cluster.schemaChange(withKeyspace("DROP TABLE %s." + TABLE));
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> cluster.get(1).runOnInstance(() -> {
                    assertRegistered(tableName("Table", "ReadLatency"), false);
                    assertRegistered(tableName("ColumnFamily", "AllMemtablesHeapSize"), false);
                    assertRegistered(tableName("TrieMemtable", "Uncontended memtable puts"), false);
                    assertEquals(baseline[0], tableOwners());
                    assertEquals(baseline[1], children(TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY));
                    assertEquals(0, children(Keyspace.open(KEYSPACE).metric.readLatency));
                    assertArrayEquals("Dropping a child must preserve history exactly once", history, history());
                }));
            }
        }
    }

    private static void assertExports(boolean all)
    {
        boolean aliases = DatabaseDescriptor.getMetricProfile().includesLegacyAliases();
        for (String metric : new String[]{ "CoordinatorReadLatency", "CoordinatorWriteLatency", "CompressionRatio", "TotalDiskSpaceUsed" })
            assertRegistered(tableName("Table", metric), true);
        for (String type : new String[]{ "Table", "ColumnFamily" })
        {
            boolean selectedType = type.equals("Table") || aliases;
            assertRegistered(tableName(type, "AllMemtablesOnHeapDataSize"), selectedType);
            assertRegistered(tableName(type, "AllMemtablesHeapSize"), aliases);
            assertRegistered(tableName(type, "MemtableOnHeapDataSize"), all && selectedType);
            assertRegistered(tableName(type, "MemtableOnHeapSize"), all && aliases);
            assertRegistered(tableName(type, "UncompressedLiveDiskSpaceUsed"), all && selectedType);
            assertRegistered(tableName(type, "ReadRepairRequests"), all && selectedType);
        }
        assertRegistered(tableName("Table", "ReadLatency"), true);
        assertRegistered(tableName("Table", "KeyMigrationLatency"), all);
        assertRegistered(keyspaceName("ReadLatency"), true);
        assertRegistered(keyspaceName("ReadTotalLatency"), true);
        assertRegistered(tableName("Table", "ReadTotalLatency"), true);
        assertRegistered(tableName("Table", "WriteTotalLatency"), true);
        assertRegistered(keyspaceName("WriteTotalLatency"), true);
        assertRegistered(keyspaceName("UncompressedLiveDiskSpaceUsed"), all);
        assertRegistered(keyspaceName("KeyMigrationLatency"), all);
        assertRegistered(globalName("UncompressedLiveDiskSpaceUsed"), true);
        assertRegistered(globalName("KeyMigrationLatency"), true);
        assertRegistered(new MetricName("org.apache.cassandra.metrics", "ColumnFamily", "AllMemtablesHeapSize", "all",
                                        "org.apache.cassandra.metrics:type=ColumnFamily,name=AllMemtablesHeapSize"), aliases);
        assertRegistered(globalName("AllMemtablesHeapSize"), aliases);
        assertRegistered(tableName("TrieMemtable", "Uncontended memtable puts"), true);
        assertRegistered(tableName("TrieMemtable", "Contention timeLatency"), true);

        if (aliases)
            assertSame(table().metric.allMemtablesOnHeapDataSize,
                       Metrics.getMetrics().get(tableName("ColumnFamily", "AllMemtablesHeapSize").getMetricName()));
        assertSame(table().metric.coordinatorReadLatency,
                   Metrics.getMetrics().get(tableName("Table", "CoordinatorReadLatency").getMetricName()));
    }

    private static long[] history()
    {
        KeyspaceMetrics keyspace = Keyspace.open(KEYSPACE).metric;
        return new long[]{ keyspace.readLatency.latency.getCount(), keyspace.readLatency.totalLatency.getCount(),
                           keyspace.writeLatency.latency.getCount(), keyspace.writeLatency.totalLatency.getCount(),
                           TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.latency.getCount(),
                           TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.totalLatency.getCount() };
    }

    private static int tableOwners()
    {
        Map<?, ?> metrics = (Map<?, ?>) field(TableMetrics.class, "ALL_TABLE_METRICS", null);
        return ((Collection<?>) metrics.get("UncompressedLiveDiskSpaceUsed")).size();
    }

    private static int children(LatencyMetrics metric)
    {
        return ((Collection<?>) field(LatencyMetrics.class, "children", metric)).size();
    }

    private static Object field(Class<?> type, String name, Object instance)
    {
        try
        {
            Field field = type.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(instance);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }

    private static ColumnFamilyStore table()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    private static void assertRegistered(MetricName name, boolean expected)
    {
        assertEquals(name.getMetricName(), expected, Metrics.getMetrics().containsKey(name.getMetricName()));
        assertNotNull("The node must use a real MBean server", MBeanWrapper.instance.getMBeanServer());
        assertEquals(name.getMBeanName().toString(), expected, MBeanWrapper.instance.isRegistered(name.getMBeanName()));
    }

    private static MetricName tableName(String type, String metric)
    {
        String group = "org.apache.cassandra.metrics";
        return new MetricName(group, type, metric, KEYSPACE + '.' + TABLE,
                              group + ":type=" + type + ",keyspace=" + KEYSPACE + ",scope=" + TABLE + ",name=" + metric);
    }

    private static MetricName keyspaceName(String metric)
    {
        String group = "org.apache.cassandra.metrics";
        return new MetricName(group, KeyspaceMetrics.TYPE_NAME, metric, KEYSPACE,
                              group + ":type=Keyspace,keyspace=" + KEYSPACE + ",name=" + metric);
    }

    private static MetricName globalName(String metric)
    {
        String group = "org.apache.cassandra.metrics";
        return new MetricName(group, "Table", metric, "all", group + ":type=Table,name=" + metric);
    }
}
