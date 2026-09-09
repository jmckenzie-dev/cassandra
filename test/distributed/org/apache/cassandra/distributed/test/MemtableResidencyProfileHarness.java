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

import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JsonUtils;

/** Measures empty-table residency and deterministic write/idle cycles without changing production policies. */
public final class MemtableResidencyProfileHarness extends ProfiledClusterHarness
{
    private static final String KEYSPACE = "memtable_residency";
    private static final String COUNTERS = "live_memtables,dirty_memtables,flushing_memtables,memtable_data_bytes," +
                                           "memtable_accounted_heap_bytes,memtable_accounted_offheap_bytes,pending_flushes," +
                                           "sstables,sstable_data_disk_bytes,flushes,bytes_flushed,compaction_bytes_written," +
                                           "compacting_sstables,pending_compactions,node_pool_heap_bytes,node_pool_offheap_bytes," +
                                           "node_pool_reclaiming_heap_bytes,node_pool_reclaiming_offheap_bytes,initialized_trie_memtables";

    private final Config config;
    private final int[] tableOrder;
    private final Map<String, Object> checkpoints = new LinkedHashMap<>();
    private Map<String, Object> effective;
    private long completedWrites;
    private long completedReads;
    private long completedRetirementRequests;
    private long failedRequests;
    private long lateWrites;
    private long maxLatenessNanos;

    MemtableResidencyProfileHarness(Config config)
    {
        super(config.out, "Memtable residency run", "Memtable residency summary",
              "residency-" + config.scenario + '-' + config.tables + "t", config.args, config.noProfile, false);
        this.config = config;
        tableOrder = config.tableOrder();
    }

    public static void main(String[] args) throws Throwable
    {
        Config config = Config.parse(args);
        try (WithProperties properties = new WithProperties()
                                         .set(CassandraRelevantProperties.LAZY_TOMBSTONE_HISTOGRAMS, config.lazyTombstoneHistograms)
                                         .set(CassandraRelevantProperties.GEOMETRIC_METER_ARRAYS, config.geometricMeterArrays))
        {
            new MemtableResidencyProfileHarness(config).execute();
        }
    }

    @Override
    protected void configureCluster(Cluster.Builder builder)
    {
        builder.withSubnet(config.subnet);
    }

    @Override
    protected void configureNode(IInstanceConfig node)
    {
        Map<String, Object> memtable = new LinkedHashMap<>();
        memtable.put("class_name", config.memtable);
        if (config.memtable.equals("TrieMemtable"))
            memtable.put("parameters", Map.of("lazy_initialization", Boolean.toString(!config.eagerMemtable)));
        node.set("memtable", Map.of("configurations", Map.of("default", memtable)));
        node.set("sstable", Map.of("selected_format", config.format));
        node.set("cursor_compaction_enabled", config.cursorCompaction);
        node.set("optimized_metrics_enabled", !config.legacyMetrics);
        node.set("compact_jmx_registration_enabled", config.compactJmx);
        node.set("adaptive_jmx_histogram_history_enabled", config.compactJmx);
        if (config.metricsProfile != null)
            node.set("metrics_config_file", config.metricsProfile);
        if (config.idleFlushMillis > 0)
            node.set("memtable_idle_timeout", config.idleFlushMillis + "ms");
        node.set("memtable_idle_flush_max_concurrent", config.idleFlushMaxConcurrent);
        if (config.memtableHeapMiB > 0)
            node.set("memtable_heap_space", config.memtableHeapMiB + "MiB");
    }

    @Override
    protected void postCluster()
    {
        effective = cluster.get(1).callOnInstance(() -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("listenAddress", DatabaseDescriptor.getListenAddress().getHostAddress());
            values.put("rpcAddress", DatabaseDescriptor.getRpcAddress().getHostAddress());
            values.put("storagePort", DatabaseDescriptor.getStoragePort());
            values.put("nativeTransportPort", DatabaseDescriptor.getNativeTransportPort());
            values.put("sstableFormat", DatabaseDescriptor.getSelectedSSTableFormat().name());
            values.put("memtableAllocation", DatabaseDescriptor.getMemtableAllocationType().name());
            values.put("memtableParameters", DatabaseDescriptor.getMemtableConfigurations().get("default").parameters);
            values.put("cursorCompaction", DatabaseDescriptor.cursorCompactionEnabled());
            values.put("idleFlushTimeoutNanos", DatabaseDescriptor.getMemtableIdleTimeoutNanos());
            values.put("idleFlushMaxConcurrent", DatabaseDescriptor.getMemtableIdleFlushMaxConcurrent());
            values.put("lazyTombstoneHistograms", CassandraRelevantProperties.LAZY_TOMBSTONE_HISTOGRAMS.getBoolean());
            values.put("geometricMeterArrays", CassandraRelevantProperties.GEOMETRIC_METER_ARRAYS.getBoolean());
            values.put("optimizedMetricsEnabled", DatabaseDescriptor.getOptimizedMetricsEnabled());
            values.put("heapMaxBytes", Runtime.getRuntime().maxMemory());
            values.put("processors", Runtime.getRuntime().availableProcessors());
            values.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
            values.put("memtableHeapLimit", AbstractAllocatorMemtable.MEMORY_POOL.onHeap.limit);
            values.put("memtableOffheapLimit", AbstractAllocatorMemtable.MEMORY_POOL.offHeap.limit);
            return values;
        });
    }

    @Override
    protected List<Phase> definePhases()
    {
        List<Phase> phases = new ArrayList<>();
        phases.add(new Phase("00-baseline", false, phase -> checkpoint("baseline", true), null));
        phases.add(new Phase("01-create", phase -> {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                                 "{'class':'SimpleStrategy','replication_factor':1}");
            for (int table = 0; table < config.tables; table++)
                cluster.schemaChange("CREATE TABLE " + tableName(table) +
                                     " (pk int, c int, v text, PRIMARY KEY (pk,c))" +
                                     " WITH compaction = " + config.compactionOptions() +
                                     " AND caching = {'keys':'NONE','rows_per_partition':'NONE'}");
            effective.putAll(cluster.get(1).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("t000000");
                return Map.of("memtableClass", cfs.getCurrentMemtable().getClass().getName(),
                              "tableParameters", cfs.metadata().params.toString());
            }));
        }));
        phases.add(new Phase("02-created", false, phase -> checkpoint("created", true), null));
        int cycles = config.scenario.equals("never-written") ? 0 : config.cycles;
        if (config.scenario.equals("trickle"))
            phases.add(new Phase("03-trickle", phase -> sampled("03-trickle", () -> writeCycle(-1))));
        for (int cycle = 0; cycle < (config.scenario.equals("trickle") ? 0 : cycles); cycle++)
        {
            final int current = cycle;
            String name = String.format(Locale.ROOT, "03-cycle-%03d", cycle);
            phases.add(new Phase(name, phase -> sampled(name, () -> writeCycle(current))));
            if (config.explicitRetirement)
                phases.add(new Phase(name + "-written", false, phase -> checkpoint(name + "-written", false), null));
            if (config.scenario.equals("written-flushed"))
                phases.add(new Phase(name + "-flush", phase -> flush()));
            phases.add(new Phase(name + "-observe", phase -> sampled(name + "-observe", () ->
                TimeUnit.MILLISECONDS.sleep(config.scenario.equals("trickle") ? 0 : config.idleMillis))));
            if (config.explicitRetirement)
            {
                phases.add(new Phase(name + "-pre-retire", false, phase -> checkpoint(name + "-pre-retire", false), null));
                phases.add(new Phase(name + "-retire", phase -> sampled(name + "-retire", () -> retire(current))));
                phases.add(new Phase(name + "-reclaimed", false, phase -> {
                    awaitSettled();
                    checkpoint(name + "-reclaimed", false);
                }, null));
            }
            if (config.idleFlushMillis > 0)
                phases.add(new Phase(name + "-idle-drain", phase -> sampled(name + "-idle-drain", this::awaitIdleFlushes)));
            phases.add(new Phase(name + "-policy", false, phase -> checkpoint(name + "-policy", config.settleEachCycle), null));
            phases.add(new Phase(name + "-read", phase -> {
                verify(current);
                if (config.explicitRetirement)
                    checkpoint(name + "-read", false);
            }));
        }
        phases.add(new Phase("04-hold", phase -> sampled("04-hold", () -> TimeUnit.MILLISECONDS.sleep(config.holdMillis))));
        phases.add(new Phase("05-policy", false, phase -> checkpoint("policy", false), null));
        phases.add(new Phase("06-settled", false, phase -> checkpoint("settled", true), null));
        phases.add(new Phase("07-verify", phase -> {
            verify(cycles - 1);
            checkpoint("verified", false);
        }));
        phases.add(new Phase("08-close", false, phase -> {
            cluster.close();
            cluster = null;
        }, null));
        return phases;
    }

    private static String tableName(int table)
    {
        return KEYSPACE + '.' + String.format(Locale.ROOT, "t%06d", table);
    }

    private void writeCycle(int cycle) throws Exception
    {
        Path path = runDirectory.resolve(String.format(Locale.ROOT, "writes-%03d.csv", cycle));
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW))
        {
            writer.write("operation,table,row,scheduled_ns,started_ns,finished_ns,service_ns,arrival_latency_ns,success\n");
            long start = System.nanoTime();
            int total = config.operationsPerCycle() * (cycle < 0 ? config.cycles : 1);
            for (int operation = 0; operation < total; operation++)
            {
                long scheduled = config.offsetNanos(operation);
                long delay = scheduled - (System.nanoTime() - start);
                if (delay > 0)
                    TimeUnit.NANOSECONDS.sleep(delay);
                int currentCycle = cycle < 0 ? operation / config.operationsPerCycle() : cycle;
                int localOperation = operation % config.operationsPerCycle();
                int table = config.tableFor(tableOrder, currentCycle, localOperation);
                int row = (config.overwrite ? 0 : currentCycle * config.rows) + localOperation / config.activeTables;
                String payload = config.payload(table, row);
                long began = System.nanoTime() - start;
                boolean success = false;
                try
                {
                    cluster.coordinator(1).execute("INSERT INTO " + tableName(table) + " (pk,c,v) VALUES (?,?,?)",
                                                   ConsistencyLevel.ONE, table, row, payload);
                    completedWrites++;
                    success = true;
                }
                finally
                {
                    long finished = System.nanoTime() - start;
                    if (!success)
                        failedRequests++;
                    long lateness = began - scheduled;
                    if (lateness > TimeUnit.MILLISECONDS.toNanos(1))
                        lateWrites++;
                    maxLatenessNanos = Math.max(maxLatenessNanos, lateness);
                    writer.write(operation + "," + table + ',' + row + ',' + scheduled + ',' + began + ',' + finished + ',' +
                                 (finished - began) + ',' + (finished - scheduled) + ',' + success + '\n');
                }
            }
        }
    }

    private void verify(int lastCycle) throws Exception
    {
        try (BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve("reads-" + lastCycle + '-' + completedReads + ".csv"),
                                                             StandardOpenOption.CREATE_NEW))
        {
            writer.write("table,rows,service_ns\n");
            for (int position = 0; position < config.tables; position++)
            {
                int table = tableOrder[position];
                List<Integer> expected = new ArrayList<>();
                for (int cycle = 0; cycle <= lastCycle; cycle++)
                    if (config.activeInCycle(position, cycle))
                        for (int row = 0; row < config.rows; row++)
                            if (!config.overwrite || !expected.contains(row))
                                expected.add((config.overwrite ? 0 : cycle * config.rows) + row);
                long started = System.nanoTime();
                Object[][] rows = cluster.coordinator(1).execute("SELECT c,v FROM " + tableName(table) + " WHERE pk=?",
                                                               ConsistencyLevel.ONE, table);
                long elapsed = System.nanoTime() - started;
                if (rows.length != expected.size())
                    throw new AssertionError("Row count for table " + table + ": expected " + expected.size() + ", got " + rows.length);
                for (int row = 0; row < rows.length; row++)
                    if (!expected.get(row).equals(rows[row][0]) || !config.payload(table, expected.get(row)).equals(rows[row][1]))
                        throw new AssertionError("Incorrect data in table " + table + ", result row " + row);
                completedReads++;
                writer.write(table + "," + rows.length + ',' + elapsed + '\n');
            }
        }
    }

    private void flush()
    {
        cluster.get(1).runOnInstance(() -> {
            for (ColumnFamilyStore cfs : Keyspace.open(KEYSPACE).getColumnFamilyStores())
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        });
    }

    private void retire(int cycle)
    {
        for (int operation = 0; operation < config.activeTables; operation++)
        {
            String table = String.format(Locale.ROOT, "t%06d", config.tableFor(tableOrder, cycle, operation));
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table)
                                                     .forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED));
            completedRetirementRequests++;
        }
    }

    private long[] counters()
    {
        return cluster.get(1).callOnInstance(() -> {
            long[] values = new long[19];
            if (Schema.instance.getKeyspaceInstance(KEYSPACE) != null)
            {
                for (ColumnFamilyStore cfs : Keyspace.open(KEYSPACE).getColumnFamilyStores())
                {
                    View view = cfs.getTracker().getView();
                    values[0] += view.liveMemtables.size();
                    values[2] += view.flushingMemtables.size();
                    for (Memtable memtable : view.getAllMemtables())
                    {
                        values[1] += memtable.isClean() ? 0 : 1;
                        values[3] += memtable.getLiveDataSize();
                        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
                        values[4] += usage.ownsOnHeap;
                        values[5] += usage.ownsOffHeap;
                        if (memtable instanceof TrieMemtable && ((TrieMemtable) memtable).isInitialized())
                            values[18]++;
                    }
                    values[6] += cfs.metric.pendingFlushes.getCount();
                    values[7] += view.liveSSTables().size();
                    for (SSTableReader sstable : view.liveSSTables())
                        values[8] += sstable.onDiskLength();
                    values[9] += cfs.metric.memtableSwitchCount.getCount();
                    values[10] += cfs.metric.bytesFlushed.getCount();
                    values[11] += cfs.metric.compactionBytesWritten.getCount();
                    values[12] += cfs.getTracker().getCompacting().size();
                    values[13] += cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
                }
            }
            values[14] = AbstractAllocatorMemtable.MEMORY_POOL.onHeap.used();
            values[15] = AbstractAllocatorMemtable.MEMORY_POOL.offHeap.used();
            values[16] = AbstractAllocatorMemtable.MEMORY_POOL.onHeap.getReclaiming();
            values[17] = AbstractAllocatorMemtable.MEMORY_POOL.offHeap.getReclaiming();
            return values;
        });
    }

    private void awaitSettled() throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.settleSeconds);
        int stable = 0;
        while (System.nanoTime() < deadline)
        {
            long[] values = counters();
            if (values[2] == 0 && values[6] == 0 && values[12] == 0 && values[13] == 0 && values[16] == 0 && values[17] == 0)
            {
                if (++stable == 3)
                    return;
            }
            else
                stable = 0;
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new IllegalStateException("Flush/compaction/reclamation did not settle: " + Arrays.toString(counters()));
    }

    private void awaitIdleFlushes() throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.settleSeconds)
                        + TimeUnit.MILLISECONDS.toNanos(config.idleFlushMillis);
        while (System.nanoTime() < deadline)
        {
            long[] values = counters();
            if (values[1] == 0 && values[2] == 0 && values[6] == 0 && values[16] == 0 && values[17] == 0)
                return;
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new IllegalStateException("Automatic idle flushing did not drain: " + Arrays.toString(counters()));
    }

    private void checkpoint(String name, boolean settled) throws Exception
    {
        if (settled)
        {
            awaitSettled();
            System.gc();
            TimeUnit.MILLISECONDS.sleep(500);
        }
        ResourceProfiler.HeapSnapshot heap = profiler.checkpoint();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("heapUsedBytes", heap.heapUsed);
        values.put("driverThreadAllocatedBytes", heap.threadAllocatedBytes);
        values.put("settledPostGc", settled);
        long[] counts = counters();
        String[] names = COUNTERS.split(",");
        for (int i = 0; i < names.length; i++)
            values.put(names[i], counts[i]);
        if (config.ucsScaling != null)
            values.put("compactionHistory", cluster.get(1).callOnInstance(() -> {
                long jobs = 0, bytesIn = 0, bytesOut = 0;
                for (UntypedResultSet.Row row : QueryProcessor.executeInternal("SELECT keyspace_name, bytes_in, bytes_out FROM system.compaction_history"))
                {
                    if (KEYSPACE.equals(row.getString("keyspace_name")))
                    {
                        jobs++;
                        bytesIn += row.getLong("bytes_in");
                        bytesOut += row.getLong("bytes_out");
                    }
                }
                return Map.of("jobs", jobs, "bytesIn", bytesIn, "bytesOut", bytesOut);
            }));
        checkpoints.put(name, values);
        Files.writeString(runDirectory.resolve("checkpoint-" + name + ".json"), JsonUtils.writeAsJsonString(values),
                          StandardOpenOption.CREATE_NEW);
        if (settled && config.heapDumps)
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                             .dumpHeap(runDirectory.resolve(name + ".hprof").toString(), true);
    }

    private void sampled(String name, CheckedWork work) throws Exception
    {
        try (BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(name + "-samples.csv"), StandardOpenOption.CREATE_NEW))
        {
            writer.write("elapsed_ns,heap_used_bytes," + COUNTERS + '\n');
            ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
            long start = System.nanoTime();
            ScheduledFuture<?> task = sampler.scheduleWithFixedDelay(() -> {
                try
                {
                    long[] counts = counters();
                    writer.write((System.nanoTime() - start) + "," + profiler.checkpoint().heapUsed);
                    for (long value : counts)
                        writer.write("," + value);
                    writer.newLine();
                    writer.flush();
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Sampling failed", e);
                }
            }, 0, config.sampleMillis, TimeUnit.MILLISECONDS);
            try
            {
                work.run();
            }
            finally
            {
                sampler.shutdown();
                if (!sampler.awaitTermination(30, TimeUnit.SECONDS))
                {
                    sampler.shutdownNow();
                    throw new IllegalStateException("Sampler did not stop");
                }
                if (!task.isCancelled())
                    task.get();
            }
        }
    }

    @Override
    protected Map<String, Object> runParameters()
    {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("scenario", config.scenario);
        values.put("compactionOptions", config.compactionOptions());
        values.put("overwrite", config.overwrite);
        values.put("idleFlushMillis", config.idleFlushMillis);
        values.put("idleFlushMaxConcurrent", config.idleFlushMaxConcurrent);
        values.put("memtableHeapMiB", config.memtableHeapMiB);
        values.put("metricsProfile", config.metricsProfile);
        values.put("settleEachCycle", config.settleEachCycle);
        values.put("subnet", config.subnet);
        values.put("tables", config.tables);
        values.put("activeTables", config.activeTables);
        values.put("rowsPerTablePerCycle", config.rows);
        values.put("cycles", config.cycles);
        values.put("payloadBytes", config.payloadBytes);
        values.put("seed", config.seed);
        values.put("offeredWritesPerSecond", config.rate);
        values.put("idleMillis", config.idleMillis);
        values.put("holdMillis", config.holdMillis);
        values.put("sampleMillis", config.sampleMillis);
        values.put("memtableInitialization", config.memtable.equals("TrieMemtable")
                                            ? (config.eagerMemtable ? "eager" : "lazy") : "implementation-default");
        values.put("effectiveConfiguration", effective);
        values.put("completedWrites", completedWrites);
        values.put("completedReadQueries", completedReads);
        values.put("explicitRetirement", config.explicitRetirement);
        values.put("lazyTombstoneHistograms", config.lazyTombstoneHistograms);
        values.put("geometricMeterArrays", config.geometricMeterArrays);
        values.put("optimizedMetricsEnabled", !config.legacyMetrics);
        values.put("completedRetirementRequests", completedRetirementRequests);
        values.put("failedWriteRequests", failedRequests);
        values.put("writesStartedOver1msLate", lateWrites);
        values.put("maxWriteLatenessNanos", maxLatenessNanos);
        values.put("checkpoints", checkpoints);
        values.put("allocationCounterScope", "harness main thread only; use JFR for background allocation");
        values.put("memoryScope", "whole JVM heap; table allocator counters exclude some object overhead; pool counters include system tables");
        values.put("policy", config.explicitRetirement
                             ? "explicit USER_FORCED flush of active tables after each observation pause; cursor compaction disabled"
                             : "existing flush triggers; cursor compaction disabled");
        return values;
    }

    @Override
    protected void writeSummaryDetails(BufferedWriter writer) throws java.io.IOException
    {
        writer.write(JsonUtils.writeAsJsonString(runParameters()));
        writer.newLine();
    }

    @Override
    protected String profiledArtifactNote()
    {
        return "operation CSVs, sampled counters, checkpoint JSON; optional retained-heap dumps";
    }

    private interface CheckedWork
    {
        void run() throws Exception;
    }

    static final class Config
    {
        String scenario = "never-written";
        int tables = 100;
        int subnet = 0;
        int activeTables = -1;
        int rows = 4;
        int cycles = 2;
        int payloadBytes = 128;
        int rate = 100;
        int idleMillis = 1000;
        int holdMillis = 1000;
        int sampleMillis = 1000;
        int settleSeconds = 60;
        long seed = 1;
        String memtable = "TrieMemtable";
        String format = "bti";
        Path out = Paths.get("logs");
        boolean noProfile;
        boolean heapDumps;
        boolean eagerMemtable;
        boolean explicitRetirement;
        boolean lazyTombstoneHistograms;
        boolean geometricMeterArrays;
        boolean legacyMetrics;
        boolean overwrite;
        boolean settleEachCycle;
        boolean cursorCompaction;
        String ucsScaling;
        String ucsMinSize = "100MiB";
        String metricsProfile;
        int idleFlushMillis;
        int idleFlushMaxConcurrent = 2;
        int memtableHeapMiB;
        boolean compactJmx;
        String[] args;

        static Config parse(String[] args)
        {
            Config c = new Config();
            c.args = args.clone();
            for (int i = 0; i < args.length; i++)
            {
                String option = args[i];
                if (option.equals("--no-profile"))
                    c.noProfile = true;
                else if (option.equals("--heap-dumps"))
                    c.heapDumps = true;
                else if (option.equals("--eager-memtable"))
                    c.eagerMemtable = true;
                else if (option.equals("--explicit-retirement"))
                    c.explicitRetirement = true;
                else if (option.equals("--lazy-tombstone-histograms"))
                    c.lazyTombstoneHistograms = true;
                else if (option.equals("--legacy-metrics"))
                    c.legacyMetrics = true;
                else if (option.equals("--geometric-meter-arrays"))
                    c.geometricMeterArrays = true;
                else if (option.equals("--overwrite"))
                    c.overwrite = true;
                else if (option.equals("--settle-each-cycle"))
                    c.settleEachCycle = true;
                else if (option.equals("--cursor-compaction"))
                    c.cursorCompaction = true;
                else if (option.equals("--compact-jmx"))
                    c.compactJmx = true;
                else
                {
                    if (++i == args.length)
                        throw new IllegalArgumentException("Missing value for " + option);
                    String value = args[i];
                    switch (option)
                    {
                        case "--scenario": c.scenario = value; break;
                        case "--tables": c.tables = Integer.parseInt(value); break;
                        case "--subnet": c.subnet = Integer.parseInt(value); break;
                        case "--active-tables": c.activeTables = Integer.parseInt(value); break;
                        case "--rows-per-table": c.rows = Integer.parseInt(value); break;
                        case "--cycles": c.cycles = Integer.parseInt(value); break;
                        case "--payload-bytes": c.payloadBytes = Integer.parseInt(value); break;
                        case "--rate": c.rate = Integer.parseInt(value); break;
                        case "--idle-ms": c.idleMillis = Integer.parseInt(value); break;
                        case "--hold-ms": c.holdMillis = Integer.parseInt(value); break;
                        case "--sample-ms": c.sampleMillis = Integer.parseInt(value); break;
                        case "--settle-seconds": c.settleSeconds = Integer.parseInt(value); break;
                        case "--seed": c.seed = Long.parseLong(value); break;
                        case "--memtable": c.memtable = value; break;
                        case "--format": c.format = value; break;
                        case "--out": c.out = Paths.get(value); break;
                        case "--ucs-scaling": c.ucsScaling = value; break;
                        case "--ucs-min-size": c.ucsMinSize = value; break;
                        case "--metrics-profile": c.metricsProfile = value; break;
                        case "--idle-flush-ms": c.idleFlushMillis = Integer.parseInt(value); break;
                        case "--idle-flush-max-concurrent": c.idleFlushMaxConcurrent = Integer.parseInt(value); break;
                        case "--memtable-heap-mib": c.memtableHeapMiB = Integer.parseInt(value); break;
                        default: throw new IllegalArgumentException("Unknown option: " + option);
                    }
                }
            }
            if (c.activeTables == -1)
                c.activeTables = c.tables;
            if (c.subnet < 0 || c.subnet > 255)
                throw new IllegalArgumentException("--subnet must be between 0 and 255");
            if (!List.of("never-written", "written-flushed", "idle-reactivate", "rotating-bursts", "trickle").contains(c.scenario))
                throw new IllegalArgumentException("Unknown scenario: " + c.scenario);
            if (c.tables < 1 || c.activeTables < 1 || c.activeTables > c.tables || c.rows < 1 || c.cycles < 1 ||
                c.payloadBytes < 1 || c.rate < 1 || c.rate > 1000000000 || c.idleMillis < 0 || c.holdMillis < 0 ||
                c.sampleMillis < 1 || c.settleSeconds < 1 || (long) c.activeTables * c.rows > Integer.MAX_VALUE ||
                (long) c.cycles * c.rows > Integer.MAX_VALUE ||
                (long) c.activeTables * c.rows * c.cycles > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Invalid count, duration, rate, or active-table range");
            if (!List.of("TrieMemtable", "SkipListMemtable", "ShardedSkipListMemtable").contains(c.memtable) ||
                !List.of("bti", "big").contains(c.format))
                throw new IllegalArgumentException("Unsupported memtable or SSTable format");
            if (c.eagerMemtable && !c.memtable.equals("TrieMemtable"))
                throw new IllegalArgumentException("--eager-memtable requires TrieMemtable");
            if (c.explicitRetirement && !List.of("idle-reactivate", "rotating-bursts").contains(c.scenario))
                throw new IllegalArgumentException("--explicit-retirement requires idle-reactivate or rotating-bursts");
            if (c.memtableHeapMiB < 0 || c.idleFlushMaxConcurrent < 1 || c.idleFlushMillis < 0 ||
                (c.idleFlushMillis > 0 && (c.ucsScaling == null || !c.memtable.equals("TrieMemtable") || c.eagerMemtable || c.explicitRetirement)))
                throw new IllegalArgumentException("Idle flushing requires lazy TrieMemtable and UCS, without explicit retirement");
            c.compactionOptions();
            return c;
        }

        String compactionOptions()
        {
            if (ucsScaling == null)
                return "{'class':'SizeTieredCompactionStrategy'}";
            if (!ucsScaling.matches("[TLN0-9, +\\-]+") || !ucsMinSize.matches("[0-9]+[A-Za-z]+"))
                throw new IllegalArgumentException("Invalid UCS scaling or minimum size");
            return "{'class':'UnifiedCompactionStrategy','scaling_parameters':'" + ucsScaling +
                   "','min_sstable_size':'" + ucsMinSize + "','base_shard_count':'4'}";
        }

        int operationsPerCycle()
        {
            return activeTables * rows;
        }

        long offsetNanos(int operation)
        {
            return (long) operation * TimeUnit.SECONDS.toNanos(1) / rate;
        }

        int[] tableOrder()
        {
            int[] order = new int[tables];
            for (int i = 0; i < tables; i++)
                order[i] = i;
            Random random = new Random(seed);
            for (int i = tables - 1; i > 0; i--)
            {
                int other = random.nextInt(i + 1);
                int value = order[i];
                order[i] = order[other];
                order[other] = value;
            }
            return order;
        }

        int tableFor(int[] order, int cycle, int operation)
        {
            long offset = scenario.equals("rotating-bursts") ? (long) cycle * activeTables : 0;
            return order[(int) ((offset + operation % activeTables) % tables)];
        }

        boolean activeInCycle(int position, int cycle)
        {
            long offset = scenario.equals("rotating-bursts") ? (long) cycle * activeTables : 0;
            return Math.floorMod(position - offset, tables) < activeTables;
        }

        String payload(int table, int row)
        {
            SplittableRandom random = new SplittableRandom(seed ^ ((long) table << 32) ^ row);
            char[] value = new char[payloadBytes];
            for (int i = 0; i < value.length; i++)
                value[i] = (char) ('!' + random.nextInt(94));
            return new String(value);
        }
    }
}
