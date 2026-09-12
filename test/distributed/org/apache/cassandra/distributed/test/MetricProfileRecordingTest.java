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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.metrics.NoOpMetrics;
import org.apache.cassandra.metrics.TableMetrics;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MetricProfileRecordingTest extends TestBaseImpl
{
    @Test
    public void simpleProfileSharesUnusedRecordersAndPreservesAggregateInputs() throws Throwable
    {
        try (Cluster cluster = newCluster(Paths.get("conf", "simple_metrics.yml")))
        {
            cluster.get(1).runOnInstance(() -> {
                TableMetrics first = metric("first");
                TableMetrics second = metric("second");
                KeyspaceMetrics keyspace = Keyspace.open(KEYSPACE).metric;
                assertSame(NoOpMetrics.HISTOGRAM, first.sstablesPerRangeReadHistogram.cf);
                assertSame(first.sstablesPerRangeReadHistogram.cf, second.sstablesPerRangeReadHistogram.cf);
                assertNotSame(NoOpMetrics.HISTOGRAM, keyspace.sstablesPerRangeReadHistogram);
                Histogram global = first.sstablesPerRangeReadHistogram.global;
                long globalCount = global.getCount();
                long[] globalBefore = global.getSnapshot().getValues();
                long[] keyspaceBefore = keyspace.sstablesPerRangeReadHistogram.getSnapshot().getValues();
                for (int sample : new int[]{ 1, 17, 256 })
                    first.sstablesPerRangeReadHistogram.update(sample);
                assertEquals(0, first.sstablesPerRangeReadHistogram.cf.getCount());
                assertEquals(3, keyspace.sstablesPerRangeReadHistogram.getCount());
                assertEquals(globalCount + 3, global.getCount());
                long[] recorded = difference(keyspace.sstablesPerRangeReadHistogram.getSnapshot().getValues(), keyspaceBefore);
                assertEquals(3, Arrays.stream(recorded).sum());
                assertArrayEquals(recorded, difference(global.getSnapshot().getValues(), globalBefore));

                assertSame(NoOpMetrics.HISTOGRAM, first.rowsMutatedPerWriteHistogram.cf);
                assertSame(NoOpMetrics.HISTOGRAM, keyspace.rowsMutatedPerWriteHistogram);
                long rows = first.rowsMutatedPerWriteHistogram.global.getCount();
                first.rowsMutatedPerWriteHistogram.update(11);
                assertEquals(rows + 1, first.rowsMutatedPerWriteHistogram.global.getCount());
                assertEquals(0, keyspace.rowsMutatedPerWriteHistogram.getCount());

                assertSame(NoOpMetrics.METER, first.bytesPreviewed.table);
                assertSame(NoOpMetrics.METER, keyspace.bytesPreviewed);
                long previewed = first.bytesPreviewed.global.getCount();
                first.bytesPreviewed.mark(4096);
                assertEquals(previewed + 4096, first.bytesPreviewed.global.getCount());
                assertEquals(0, keyspace.bytesPreviewed.getCount());

                assertSame(NoOpMetrics.TIMER, first.validationTime.cf);
                assertSame(NoOpMetrics.TIMER, keyspace.validationTime);
                long validations = first.validationTime.global.getCount();
                first.validationTime.update(20, TimeUnit.MILLISECONDS);
                assertEquals(validations + 1, first.validationTime.global.getCount());
                assertEquals(0, keyspace.validationTime.getCount());
                assertSame(NoOpMetrics.TIMER, first.coordinatorScanLatency);
                assertNotSame(NoOpMetrics.TIMER, first.coordinatorReadLatency);
                assertNotSame(NoOpMetrics.TIMER, first.coordinatorWriteLatency);

                assertSame(NoOpMetrics.METER, first.readRepairRequests);
                assertSame(first.readRepairRequests, second.readRepairRequests);
                first.readRepairRequests.mark(100);
                assertEquals(0, second.readRepairRequests.getCount());
                assertSame(NoOpMetrics.HISTOGRAM, first.rfpRowsCachedPerQuery);
                assertSame(first.rfpRowsCachedPerQuery, second.rfpRowsCachedPerQuery);
                first.rfpRowsCachedPerQuery.update(100);
                assertEquals(0, second.rfpRowsCachedPerQuery.getCount());
                assertNotSame(NoOpMetrics.COUNTER, first.uncompressedLiveDiskSpaceUsed);
                assertNotSame(NoOpMetrics.COUNTER, first.totalDiskSpaceUsed);

                assertNotSame(NoOpMetrics.METER, first.bytesAnticompacted.table);
                assertNotSame(NoOpMetrics.METER, first.bytesMutatedAnticompaction.table);
                first.bytesAnticompacted.mark(30);
                first.bytesMutatedAnticompaction.mark(10);
                assertEquals(30, first.bytesAnticompacted.table.getCount());
                assertEquals(10, first.bytesMutatedAnticompaction.table.getCount());
                assertEquals(0.25, first.mutatedAnticompactionGauge.getValue(), 0);
            });
        }
    }

    @Test
    public void unusedLatenciesShareStateAndDisabledLegacyViewsReturnNoRows() throws Throwable
    {
        try (Cluster cluster = newCluster(Paths.get("conf", "simple_metrics.yml")))
        {
            cluster.get(1).runOnInstance(() -> {
                TableMetrics first = metric("first");
                TableMetrics second = metric("second");
                KeyspaceMetrics keyspace = Keyspace.open(KEYSPACE).metric;
                LatencyMetrics empty = first.casPrepare;
                for (LatencyMetrics latency : new LatencyMetrics[]{ first.casPropose, first.casCommit, first.accordPostStreamRepair,
                                                                    first.viewSSTableIntervalTree, second.casPrepare, keyspace.casPrepare,
                                                                    keyspace.accordGetMaxConflicts, keyspace.idealCLWriteLatency,
                                                                    keyspace.accordPostStreamRepair, keyspace.viewSSTableIntervalTree })
                {
                    assertSame(empty, latency);
                    latency.addNano(1_000_000);
                    assertEquals(0, latency.latency.getCount());
                    assertEquals(0, latency.totalLatency.getCount());
                }
                for (LatencyMetrics latency : new LatencyMetrics[]{ first.readLatency, first.writeLatency, first.rangeLatency,
                                                                    first.keyMigration, first.accordRepair })
                {
                    assertNotSame(empty, latency);
                    long count = latency.latency.getCount();
                    latency.addNano(1_000_000);
                    assertEquals(count + 1, latency.latency.getCount());
                }
                long count = TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.latency.getCount();
                first.keyMigration.addNano(1_000_000);
                assertEquals(count + 1, TableMetrics.GLOBAL_KEY_MIGRATION_LATENCY.latency.getCount());
            });

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.first (pk, v) VALUES (1, 10)"), ONE);
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, v FROM %s.first WHERE pk = 1"), ONE), row(1, 10));
            for (String view : new String[]{ "rows_per_write", "coordinator_scan_latency" })
                assertRows(cluster.get(1).executeInternal("SELECT * FROM system_views." + view));
            Object[][] reads = cluster.get(1).executeInternal("SELECT count FROM system_views.coordinator_read_latency "
                                                            + "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "first");
            assertEquals(1, reads.length);
            assertTrue(((Number) reads[0][0]).longValue() > 0);
        }
    }

    @Test
    public void enabledKeyspaceLatencyRecordsHiddenChildrenAndPreservesTheirHistory() throws Throwable
    {
        Files.createDirectories(Paths.get("tmp"));
        Path profile = Files.createTempFile(Paths.get("tmp"), "parent-metric-profile-", ".yml");
        try
        {
            Files.writeString(profile, "mode: allowlist\n"
                                       + "table:\n"
                                       + "  required: [CoordinatorReadLatency, CoordinatorWriteLatency, CompressionRatio, TotalDiskSpaceUsed]\n"
                                       + "  optional: []\n"
                                       + "  disabled: []\n"
                                       + "keyspace:\n"
                                       + "  required: []\n"
                                       + "  optional: [CasPrepareLatency, CasPrepareTotalLatency]\n"
                                       + "  disabled: []\n");
            try (Cluster cluster = newCluster(profile))
            {
                cluster.get(1).runOnInstance(() -> {
                    LatencyMetrics parent = Keyspace.open(KEYSPACE).metric.casPrepare;
                    assertNotSame(metric("first").casPropose, metric("first").casPrepare);
                    assertNotSame(metric("first").casPrepare, metric("second").casPrepare);
                    metric("first").casPrepare.addNano(1_000_000);
                    metric("second").casPrepare.addNano(2_000_000);
                    assertEquals(2, parent.latency.getCount());
                    assertEquals(3000, parent.totalLatency.getCount());
                });
                for (String table : new String[]{ "first", "second" })
                {
                    cluster.schemaChange(withKeyspace("DROP TABLE %s." + table));
                    cluster.get(1).runOnInstance(() -> {
                        LatencyMetrics parent = Keyspace.open(KEYSPACE).metric.casPrepare;
                        assertEquals(2, parent.latency.getCount());
                        assertEquals(3000, parent.totalLatency.getCount());
                        assertEquals(2, Arrays.stream(parent.latency.getSnapshot().getValues()).sum());
                    });
                }
            }
        }
        finally
        {
            Files.delete(profile);
        }
    }

    private Cluster newCluster(Path profile) throws Throwable
    {
        String location = profile.toAbsolutePath().toString();
        Cluster cluster = init(builder().withNodes(1).withConfig(config -> {
            config.with(JMX);
            config.set("metrics_config_file", location);
            config.set("optimized_metrics_enabled", true);
            config.set("compact_jmx_registration_enabled", true);
            config.set("memtable", Map.of("configurations", Map.of("default", Map.of("class_name", "TrieMemtable",
                                                                                     "parameters", Map.of("lazy_initialization", "true")))));
            config.set("cursor_compaction_enabled", false);
        }).start());
        try
        {
            for (String table : new String[]{ "first", "second" })
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v int)"));
            return cluster;
        }
        catch (Throwable failure)
        {
            cluster.close();
            throw failure;
        }
    }

    private static TableMetrics metric(String table)
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric;
    }

    private static long[] difference(long[] after, long[] before)
    {
        assertEquals(before.length, after.length);
        long[] result = new long[after.length];
        for (int i = 0; i < result.length; i++)
            result[i] = after[i] - before[i];
        return result;
    }
}
