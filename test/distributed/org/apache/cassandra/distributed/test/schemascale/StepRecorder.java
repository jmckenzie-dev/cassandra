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

package org.apache.cassandra.distributed.test.schemascale;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Owns steps.csv. Each checkpoint samples the node through the probe, writes one row, and checks the stop
 * conditions. A checkpoint returns a {@link BreakPoint} when a condition holds, otherwise null.
 * {@link #recordDdl} may run on many threads at once; the other methods run on one thread.
 */
public final class StepRecorder implements AutoCloseable
{
    public static final List<String> COLUMNS = List.of(
        "ramp", "step", "keyspaces", "tables", "columns_total", "max_cols_table", "wall_s",
        "ddl_count", "ddl_p50_ms", "ddl_p99_ms", "ddl_max_ms", "tcm_commit_p99_ms",
        "heap_used", "heap_post_gc", "heap_per_table", "heap_per_column", "alloc_bytes_total",
        "metaspace_used", "gc_count", "gc_time_ms", "threads",
        "mbeans", "scheduled_tasks",
        "tcm_epoch", "snapshot_bytes_computed", "snapshot_bytes_gauge", "snapshot_pct_max_mutation", "snapshot_pct_limit",
        "system_schema_bytes", "metadata_log_bytes", "node_tables",
        "ddl_concurrency", "ddl_per_s");

    /** Called after each row is written. */
    public interface Observer
    {
        void onCheckpoint(String ramp, long step, ScaleState state);
    }

    private final BufferedWriter writer;
    private final MetricsProbe probe;
    private final ScaleProfile.Stop stop;
    private final ScaleProfile.Profiling profiling;
    private final Observer observer;
    private final long startedAt = System.nanoTime();
    private final long timeoutNanos;
    private final Object windowLock = new Object();
    private long[] window = new long[1024];
    private int windowSize;
    private long windowStartedAt = System.nanoTime();
    private int ddlConcurrency = 1;
    private String ramp = "init";
    private long step;
    private volatile long rampStartedAt = System.nanoTime();
    private long baselineHeap;
    private long seenSnapshotFailures;
    private boolean snapshotBreakReported;
    private LongSupplier liveHeap;
    private int unconfirmedHeapStops;
    private Map<String, String> lastRow = new LinkedHashMap<>();

    public StepRecorder(Path stepsCsv, MetricsProbe probe, ScaleProfile.Stop stop, ScaleProfile.Profiling profiling,
                        long timeoutMillis, Observer observer) throws IOException
    {
        this.probe = probe;
        this.stop = stop;
        this.profiling = profiling;
        this.timeoutNanos = timeoutMillis * 1_000_000L;
        this.observer = observer;
        writer = Files.newBufferedWriter(stepsCsv, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                         StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        writer.write(String.join(",", COLUMNS));
        writer.newLine();
        writer.flush();
    }

    /** Sets the post-GC heap that heap_per_table and heap_per_column measure against. */
    public void setBaseline(Map<String, Long> sample)
    {
        baselineHeap = Math.max(0, sample.getOrDefault("heap_post_gc", 0L));
    }

    public long baselineHeap()
    {
        return baselineHeap;
    }

    public void beginRamp(String name)
    {
        ramp = name;
        step = 0;
        ddlConcurrency = 1;
        rampStartedAt = System.nanoTime();
        startWindow();
    }

    /** Clients that run DDL at once in this ramp; written to ddl_concurrency. */
    public void setDdlConcurrency(int clients)
    {
        ddlConcurrency = clients;
    }

    /** Empties the latency window and starts its clock; ddl_per_s counts from here. */
    public void startWindow()
    {
        synchronized (windowLock)
        {
            windowSize = 0;
            windowStartedAt = System.nanoTime();
        }
    }

    /** Thread safe. */
    public void recordDdl(long nanos)
    {
        synchronized (windowLock)
        {
            if (windowSize == window.length)
                window = Arrays.copyOf(window, windowSize * 2);
            window[windowSize++] = nanos;
        }
    }

    /** True when the ramp runs longer than the ramp timeout. Thread safe. */
    public boolean timedOut()
    {
        return System.nanoTime() - rampStartedAt > timeoutNanos;
    }

    public long step()
    {
        return step;
    }

    public Map<String, String> lastRow()
    {
        return lastRow;
    }

    public BreakPoint checkTimeout(ScaleState state)
    {
        long elapsed = System.nanoTime() - rampStartedAt;
        if (elapsed <= timeoutNanos)
            return null;
        return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.TIMEOUT, elapsed / 1e9, timeoutNanos / 1e9);
    }

    public BreakPoint exception(ScaleState state, Throwable failure)
    {
        return BreakPoint.exception(ramp, step, state, failure);
    }

    /**
     * Writes one row. A forced GC and a snapshot size computation run on every Nth step and on the last one.
     */
    public BreakPoint checkpoint(ScaleState state, boolean last)
    {
        step++;
        // the window ends before the GC and the sample, so ddl_per_s does not count their time
        long[] sorted;
        double windowSeconds;
        synchronized (windowLock)
        {
            sorted = Arrays.copyOf(window, windowSize);
            long now = System.nanoTime();
            windowSeconds = (now - windowStartedAt) / 1e9;
            windowSize = 0;
            windowStartedAt = now;
        }
        boolean gc = last || step % profiling.gcEvery == 0;
        if (gc)
            probe.gc();
        boolean snapshot = last || step % profiling.snapshotSizeEvery == 0;
        Map<String, Long> sample = probe.sample(snapshot);

        Arrays.sort(sorted);
        int ddlCount = sorted.length;
        double p50 = percentileMs(sorted, 0.50);
        double p99 = percentileMs(sorted, 0.99);
        double max = ddlCount == 0 ? 0 : sorted[ddlCount - 1] / 1e6;

        long heapPostGc = sample.getOrDefault("heap_post_gc", -1L);
        long heapMax = sample.getOrDefault("heap_max", -1L);
        long computed = sample.getOrDefault("snapshot_bytes_computed", -1L);
        long gauge = sample.getOrDefault("snapshot_bytes_gauge", -1L);
        long maxMutation = sample.getOrDefault("max_mutation_size", -1L);
        // Snapshots skip the commit log; what limits them is snapshot_size_limit. Older probes only report max_mutation_size.
        long snapshotLimit = sample.getOrDefault("snapshot_size_limit", -1L) > 0 ? sample.get("snapshot_size_limit") : maxMutation;
        long commitNs = sample.getOrDefault("tcm_commit_p99_ns", -1L);

        Map<String, String> row = new LinkedHashMap<>();
        row.put("ramp", ramp);
        row.put("step", Long.toString(step));
        row.put("keyspaces", Long.toString(state.keyspaces));
        row.put("tables", Long.toString(state.tables));
        row.put("columns_total", Long.toString(state.columns));
        row.put("max_cols_table", Long.toString(state.maxColsTable));
        row.put("wall_s", format((System.nanoTime() - startedAt) / 1e9));
        row.put("ddl_count", Integer.toString(ddlCount));
        row.put("ddl_p50_ms", format(p50));
        row.put("ddl_p99_ms", format(p99));
        row.put("ddl_max_ms", format(max));
        row.put("tcm_commit_p99_ms", commitNs < 0 ? "-1" : format(commitNs / 1e6));
        row.put("heap_used", str(sample, "heap_used"));
        row.put("heap_post_gc", Long.toString(heapPostGc));
        row.put("heap_per_table", perUnit(heapPostGc, state.tables));
        row.put("heap_per_column", perUnit(heapPostGc, state.columns));
        row.put("alloc_bytes_total", str(sample, "alloc_bytes_total"));
        row.put("metaspace_used", str(sample, "metaspace_used"));
        row.put("gc_count", str(sample, "gc_count"));
        row.put("gc_time_ms", str(sample, "gc_time_ms"));
        row.put("threads", str(sample, "threads"));
        row.put("mbeans", str(sample, "mbeans"));
        row.put("scheduled_tasks", str(sample, "scheduled_tasks"));
        row.put("tcm_epoch", str(sample, "tcm_epoch"));
        row.put("snapshot_bytes_computed", Long.toString(computed));
        row.put("snapshot_bytes_gauge", Long.toString(gauge));
        row.put("snapshot_pct_max_mutation",
                maxMutation > 0 ? format(100.0 * Math.max(computed, gauge) / maxMutation) : "-1");
        row.put("snapshot_pct_limit",
                snapshotLimit > 0 ? format(100.0 * Math.max(computed, gauge) / snapshotLimit) : "-1");
        row.put("system_schema_bytes", str(sample, "system_schema_bytes"));
        row.put("metadata_log_bytes", str(sample, "metadata_log_bytes"));
        row.put("node_tables", str(sample, "node_tables"));
        row.put("ddl_concurrency", Integer.toString(ddlConcurrency));
        row.put("ddl_per_s", ddlCount == 0 || windowSeconds <= 0 ? "0.000" : format(ddlCount / windowSeconds));
        write(row);
        lastRow = row;
        if (observer != null)
            observer.onCheckpoint(ramp, step, state);

        if (ddlCount > 0 && p99 > stop.ddlP99Ms)
            return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.DDL_P99, p99, stop.ddlP99Ms);
        if (gc && heapMax > 0 && heapPostGc >= 0 && (double) heapPostGc / heapMax > stop.heapPostGcFraction)
        {
            long heap = confirmedHeap(state.tables, heapPostGc, (long) (stop.heapPostGcFraction * heapMax));
            if (heap >= 0)
                return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.HEAP_FRACTION,
                                            (double) heap / heapMax, stop.heapPostGcFraction);
        }
        long heapLimit = stop.heapPostGcLimit();
        if (gc && heapLimit > 0 && heapPostGc >= heapLimit)
        {
            long heap = confirmedHeap(state.tables, heapPostGc, heapLimit);
            if (heap >= 0)
                return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.HEAP_BYTES, heap, heapLimit);
        }
        return snapshotBreak(state, sample, computed, snapshotLimit);
    }

    /**
     * Sets the source of live heap bytes that confirms a heap stop. Without one, the post-GC reading alone
     * stops the ramp.
     */
    public void confirmHeapWith(LongSupplier liveHeapBytes)
    {
        this.liveHeap = liveHeapBytes;
    }

    public int unconfirmedHeapStops()
    {
        return unconfirmedHeapStops;
    }

    /**
     * The heap bytes to stop on, or -1 when the live heap is below the limit and the ramp goes on. A live
     * total that cannot be read keeps the post-GC reading.
     */
    private long confirmedHeap(long tables, long heapPostGc, long limit)
    {
        long live = liveHeap == null ? -1 : liveHeap.getAsLong();
        if (live < 0)
            return heapPostGc;
        if (live >= limit)
            return live;
        unconfirmedHeapStops++;
        System.out.printf(Locale.ROOT, "Heap stop not confirmed at %d tables: post-GC %,d bytes >= limit %,d, " +
                                       "but the live histogram total is %,d bytes; continuing%n",
                          tables, heapPostGc, limit, live);
        return -1;
    }

    private BreakPoint snapshotBreak(ScaleState state, Map<String, Long> sample, long computed, long limit)
    {
        if (!stop.snapshotOverMaxMutation || snapshotBreakReported)
            return null;
        long failures = sample.getOrDefault("snapshot_store_failures", 0L);
        if (failures > seenSnapshotFailures)
        {
            seenSnapshotFailures = failures;
            snapshotBreakReported = true;
            return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.SNAPSHOT_SIZE, Math.max(computed, limit + 1), limit);
        }
        if (computed > 0 && limit > 0 && computed > limit)
        {
            snapshotBreakReported = true;
            return BreakPoint.threshold(ramp, step, state, BreakPoint.Reason.SNAPSHOT_SIZE, computed, limit);
        }
        return null;
    }

    private String perUnit(long heapPostGc, long units)
    {
        if (heapPostGc < 0 || units <= 0)
            return "-1";
        return Long.toString((heapPostGc - baselineHeap) / units);
    }

    private static double percentileMs(long[] sorted, double fraction)
    {
        if (sorted.length == 0)
            return 0;
        int index = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.max(0, index)] / 1e6;
    }

    private static String str(Map<String, Long> sample, String key)
    {
        return Long.toString(sample.getOrDefault(key, -1L));
    }

    private static String format(double value)
    {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private void write(Map<String, String> row)
    {
        List<String> cells = new ArrayList<>();
        for (String column : COLUMNS)
            cells.add(row.get(column));
        try
        {
            writer.write(String.join(",", cells));
            writer.newLine();
            writer.flush();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
    }
}
