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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Schema ramps, grid points, data workload and hold. It depends only on the executor, the probe and the
 * recorder, so the same code drives an in-JVM node and a separate process. Each DDL runs in a try/catch.
 * A failure or a crossed stop condition ends the ramp and the method returns the {@link BreakPoint}.
 */
public final class ScaleWorkload
{
    private static final int TIMEOUT_CHECK_EVERY = 64;
    private static final int HOLD_SAMPLE_SECONDS = 5;

    private final SchemaExecutor executor;
    private final MetricsProbe probe;
    private final ScaleProfile profile;
    private final StepRecorder recorder;
    private final ScaleState state;
    private final int spread;
    private final Path columnsCsv;
    private final Path workloadCsv;
    private final CheckpointListener listener;
    private final List<WideTable> wideTables = new ArrayList<>();
    private int narrowTables;

    /**
     * Called after a table checkpoint that did not stop the ramp, while no DDL runs. A returned break point
     * ends the ramp.
     */
    public interface CheckpointListener
    {
        /** @param last true for the checkpoint at rampTables.max */
        BreakPoint afterCheckpoint(int checkpoint, ScaleState state, boolean last);

        /** Before the DDL of one column step: one CREATE size, or the ALTERs up to the next alter checkpoint. */
        default void beforeColumnStep(String ramp, int columns)
        {
        }

        /** After the DDL of one column step, with no DDL running. */
        default void afterColumnStep(String ramp, int columns)
        {
        }
    }

    public ScaleWorkload(SchemaExecutor executor, MetricsProbe probe, ScaleProfile profile, StepRecorder recorder,
                         ScaleState state, int spread, Path columnsCsv, Path workloadCsv, CheckpointListener listener)
    {
        this.executor = executor;
        this.probe = probe;
        this.profile = profile;
        this.recorder = recorder;
        this.state = state;
        this.spread = spread;
        this.columnsCsv = columnsCsv;
        this.workloadCsv = workloadCsv;
        this.listener = listener;
    }

    private static final class WideTable
    {
        final String name;
        final int columns;

        WideTable(String name, int columns)
        {
            this.name = name;
            this.columns = columns;
        }
    }

    static String keyspaceName(int index)
    {
        return String.format(Locale.ROOT, "ks_%03d", index);
    }

    static String narrowTableName(int index)
    {
        return String.format(Locale.ROOT, "t%06d", index);
    }

    /** Counts column definitions in a CREATE TABLE body, skipping a trailing PRIMARY KEY clause. */
    static int countColumns(String schema)
    {
        int count = 0;
        int depth = 0;
        int start = 0;
        for (int i = 0; i <= schema.length(); i++)
        {
            char c = i < schema.length() ? schema.charAt(i) : ',';
            if (c == '(' || c == '<')
                depth++;
            else if (c == ')' || c == '>')
                depth--;
            else if (c == ',' && depth == 0)
            {
                String part = schema.substring(start, i).trim();
                if (!part.isEmpty() && !part.toUpperCase(Locale.ROOT).startsWith("PRIMARY KEY"))
                    count++;
                start = i + 1;
            }
        }
        return count;
    }

    private static String wideCreate(String qualifiedName, int columns, String columnType)
    {
        StringBuilder cql = new StringBuilder(columns * 12 + 64);
        cql.append("CREATE TABLE ").append(qualifiedName).append(" (pk int PRIMARY KEY");
        for (int i = 1; i < columns; i++)
            cql.append(", c").append(i).append(' ').append(columnType);
        return cql.append(')').toString();
    }

    /** Continues the tables ramp after the given count, for a node started from a saved schema. */
    public void resumeTables(long tables)
    {
        narrowTables = Math.toIntExact(tables);
    }

    public BreakPoint createKeyspaces()
    {
        for (int i = 0; i < spread; i++)
        {
            BreakPoint failure = ddl("CREATE KEYSPACE " + keyspaceName(i) +
                                     " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            if (failure != null)
                return failure;
            state.keyspaces++;
        }
        return recorder.checkpoint(state, true);
    }

    /**
     * Creates narrow tables from the current count up to rampTables.max with rampTables.ddlConcurrency clients.
     * The clients take table indexes from one shared counter. They stop at each checkpoint boundary, so a
     * checkpoint and its listener see an exact multiple of checkpointEvery and no DDL in flight.
     */
    public BreakPoint rampTables()
    {
        ScaleProfile.RampTables ramp = profile.rampTables;
        int columns = countColumns(ramp.schema);
        int clients = Math.max(1, ramp.ddlConcurrency);
        recorder.beginRamp("02-ramp-tables");
        recorder.setDdlConcurrency(clients);
        ExecutorService pool = clients > 1 ? Executors.newFixedThreadPool(clients, ddlThreads()) : null;
        try
        {
            int checkpoints = 0;
            int from = narrowTables;
            while (from < ramp.max)
            {
                int to = Math.min(ramp.max, (from / ramp.checkpointEvery + 1) * ramp.checkpointEvery);
                Segment segment = new Segment(from, to, ramp.schema);
                segment.run(pool, clients);
                for (int i = 0; i < segment.created.get(); i++)
                    state.addTable(columns);
                narrowTables += segment.created.get();
                if (segment.failure.get() != null)
                    return finish(recorder.exception(state, segment.failure.get()));
                if (segment.timedOut.get())
                {
                    BreakPoint timeout = recorder.checkTimeout(state);
                    return finish(timeout != null ? timeout : recorder.exception(state, new IllegalStateException("ramp timeout")));
                }

                boolean last = to == ramp.max;
                BreakPoint stop = recorder.checkpoint(state, last);
                if (stop != null)
                    return stop;
                if (listener != null)
                {
                    stop = listener.afterCheckpoint(++checkpoints, state, last);
                    if (stop != null)
                        return stop;
                }
                recorder.startWindow();
                from = to;
            }
            return null;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return finish(recorder.exception(state, e));
        }
        finally
        {
            if (pool != null)
                pool.shutdownNow();
        }
    }

    private static ThreadFactory ddlThreads()
    {
        AtomicInteger number = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "schema-scale-ddl-" + number.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /** The tables between two checkpoints. The first failure or a timeout stops every client. */
    private final class Segment
    {
        final AtomicInteger next;
        final int to;
        final String schema;
        final AtomicInteger created = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean timedOut = new AtomicBoolean();
        final AtomicBoolean stop = new AtomicBoolean();

        Segment(int from, int to, String schema)
        {
            this.next = new AtomicInteger(from);
            this.to = to;
            this.schema = schema;
        }

        void run(ExecutorService pool, int clients) throws InterruptedException
        {
            if (pool == null)
            {
                createTables();
                return;
            }
            List<Future<?>> running = new ArrayList<>(clients);
            for (int c = 0; c < clients; c++)
                running.add(pool.submit(this::createTables));
            for (Future<?> client : running)
            {
                try
                {
                    client.get();
                }
                catch (ExecutionException e)
                {
                    failure.compareAndSet(null, e.getCause());
                    stop.set(true);
                }
            }
        }

        private void createTables()
        {
            while (!stop.get())
            {
                int index = next.getAndIncrement();
                if (index >= to)
                    return;
                String cql = "CREATE TABLE " + keyspaceName(index % spread) + '.' + narrowTableName(index) + " (" + schema + ')';
                long startedAt = System.nanoTime();
                try
                {
                    executor.execute(cql);
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                    stop.set(true);
                    return;
                }
                recorder.recordDdl(System.nanoTime() - startedAt);
                created.incrementAndGet();
                if (recorder.timedOut())
                {
                    timedOut.set(true);
                    stop.set(true);
                }
            }
        }
    }

    public BreakPoint rampColumnsCreate()
    {
        ScaleProfile.RampColumns ramp = profile.rampColumns;
        recorder.beginRamp("03-ramp-columns-create");
        try (BufferedWriter log = columnsLog())
        {
            List<Integer> sizes = ramp.createSizes;
            for (int s = 0; s < sizes.size(); s++)
            {
                int size = sizes.get(s);
                beforeColumnStep("03-ramp-columns-create", size);
                for (int k = 0; k < ramp.tables; k++)
                {
                    String name = String.format(Locale.ROOT, "wide_c%d_%d", size, k);
                    String cql = wideCreate(keyspaceName(0) + '.' + name, size, ramp.columnType);
                    BreakPoint failure = timedDdl(cql, size, log);
                    if (failure != null)
                    {
                        afterColumnStep("03-ramp-columns-create", size);
                        return failure;
                    }
                    state.addTable(size);
                    wideTables.add(new WideTable(keyspaceName(0) + '.' + name, size));
                }
                afterColumnStep("03-ramp-columns-create", size);
                BreakPoint stop = recorder.checkpoint(state, s == sizes.size() - 1);
                if (stop != null)
                    return stop;
                stop = recorder.checkTimeout(state);
                if (stop != null)
                    return finish(stop);
            }
        }
        catch (IOException e)
        {
            return recorder.exception(state, e);
        }
        return null;
    }

    public BreakPoint rampColumnsAlter()
    {
        ScaleProfile.RampColumns ramp = profile.rampColumns;
        recorder.beginRamp("04-ramp-columns-alter");
        try (BufferedWriter log = columnsLog())
        {
            String[] names = new String[ramp.tables];
            for (int k = 0; k < ramp.tables; k++)
            {
                names[k] = keyspaceName(0) + String.format(Locale.ROOT, ".wide_alter_%d", k);
                BreakPoint failure = ddl("CREATE TABLE " + names[k] + " (pk int PRIMARY KEY)");
                if (failure != null)
                    return failure;
                state.addTable(1);
            }
            // Each profile window is named for the checkpoint where it ends, so it shares a folder with that
            // checkpoint's step files.
            beforeColumnStep("04-ramp-columns-alter", nextAlterCheckpoint(0, ramp));
            for (int column = 1; column < ramp.alterAddMax; column++)
            {
                boolean sampled = column % ramp.alterCheckpointEvery == 0;
                for (int k = 0; k < ramp.tables; k++)
                {
                    String cql = "ALTER TABLE " + names[k] + " ADD c" + column + ' ' + ramp.columnType;
                    BreakPoint failure = sampled && k == 0 ? timedDdl(cql, column + 1, log) : ddl(cql);
                    if (failure != null)
                    {
                        afterColumnStep("04-ramp-columns-alter", column);
                        return failure;
                    }
                    state.columns++;
                    state.maxColsTable = Math.max(state.maxColsTable, column + 1);
                }
                boolean last = column + 1 == ramp.alterAddMax;
                if (last || sampled)
                {
                    afterColumnStep("04-ramp-columns-alter", column + 1);
                    if (!last)
                        beforeColumnStep("04-ramp-columns-alter", nextAlterCheckpoint(column, ramp));
                    BreakPoint stop = recorder.checkpoint(state, last);
                    if (stop != null)
                        return stop;
                    stop = recorder.checkTimeout(state);
                    if (stop != null)
                        return finish(stop);
                }
            }
            for (String name : names)
                wideTables.add(new WideTable(name, ramp.alterAddMax));
        }
        catch (IOException e)
        {
            return recorder.exception(state, e);
        }
        return null;
    }

    /** The column count of the alter checkpoint after the one at {@code column} (0 for the start). */
    static int nextAlterCheckpoint(int column, ScaleProfile.RampColumns ramp)
    {
        int next = (column / ramp.alterCheckpointEvery + 1) * ramp.alterCheckpointEvery;
        return next < ramp.alterAddMax ? next + 1 : ramp.alterAddMax;
    }

    private void beforeColumnStep(String ramp, int columns)
    {
        if (listener != null)
            listener.beforeColumnStep(ramp, columns);
    }

    private void afterColumnStep(String ramp, int columns)
    {
        if (listener != null)
            listener.afterColumnStep(ramp, columns);
    }

    /** Creates tables of equal width on a fresh node. */
    public BreakPoint gridPoint(int tables, int columns)
    {
        recorder.beginRamp(String.format(Locale.ROOT, "05-grid-t%d-c%d", tables, columns));
        BreakPoint failure = createKeyspaces();
        if (failure != null)
            return failure;
        int every = Math.max(1, tables / 10);
        for (int i = 0; i < tables; i++)
        {
            failure = ddl(wideCreate(keyspaceName(i % spread) + '.' + narrowTableName(i), columns, "int"));
            if (failure != null)
                return failure;
            state.addTable(columns);
            boolean last = i + 1 == tables;
            if (last || (i + 1) % every == 0)
            {
                BreakPoint stop = recorder.checkpoint(state, last);
                if (stop != null)
                    return stop;
            }
            if (i % TIMEOUT_CHECK_EVERY == 0)
            {
                BreakPoint timeout = recorder.checkTimeout(state);
                if (timeout != null)
                    return finish(timeout);
            }
        }
        return null;
    }

    /** Reads and writes rows in narrow tables, then inserts and selects a row in each wide table. */
    public List<BreakPoint> runWorkload() throws IOException
    {
        ScaleProfile.Workload load = profile.workload;
        List<BreakPoint> breaks = new ArrayList<>();
        recorder.beginRamp("08-workload");
        try (BufferedWriter log = Files.newBufferedWriter(workloadCsv, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
        {
            log.write("op,count,errors,p50_ms,p99_ms,max_ms");
            log.newLine();
            int sample = Math.min(load.sampleTables, narrowTables);
            OpStats writes = new OpStats("narrow_write");
            OpStats reads = new OpStats("narrow_read");
            for (int s = 0; s < sample; s++)
            {
                int index = (int) ((long) s * narrowTables / sample);
                String table = keyspaceName(index % spread) + '.' + narrowTableName(index);
                for (int row = 0; row < load.rowsPerTable; row++)
                    timedOp(writes, breaks, "INSERT INTO " + table + " (pk, c, v) VALUES (?, ?, ?)", row, row, "v-" + index + '-' + row);
                for (int row = 0; row < load.rowsPerTable; row++)
                    timedOp(reads, breaks, "SELECT * FROM " + table + " WHERE pk = ?", row);
            }
            writes.write(log);
            reads.write(log);

            OpStats inserts = new OpStats("wide_insert_json");
            OpStats selects = new OpStats("wide_select");
            for (WideTable table : wideTables)
            {
                timedOp(inserts, breaks, "INSERT INTO " + table.name + " JSON ?", wideJson(table.columns));
                String select = load.wideSelectStar ? "SELECT * FROM " : "SELECT pk FROM ";
                timedOp(selects, breaks, select + table.name + " WHERE pk = ?", 1);
            }
            inserts.write(log);
            selects.write(log);
        }
        BreakPoint stop = recorder.checkpoint(state, true);
        if (stop != null)
            breaks.add(stop);
        return breaks;
    }

    public BreakPoint hold(int seconds) throws InterruptedException
    {
        recorder.beginRamp("09-steady-hold");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (true)
        {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0)
                return recorder.checkpoint(state, true);
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.SECONDS.toNanos(HOLD_SAMPLE_SECONDS)));
            BreakPoint stop = recorder.checkpoint(state, false);
            if (stop != null)
                return stop;
        }
    }

    private static String wideJson(int columns)
    {
        StringBuilder json = new StringBuilder(columns * 12 + 16);
        json.append("{\"pk\": 1");
        for (int i = 1; i < columns; i++)
            json.append(", \"c").append(i).append("\": ").append(i);
        return json.append('}').toString();
    }

    private void timedOp(OpStats stats, List<BreakPoint> breaks, String cql, Object... binds)
    {
        long startedAt = System.nanoTime();
        try
        {
            executor.query(cql, binds);
            stats.add(System.nanoTime() - startedAt);
        }
        catch (Throwable t)
        {
            if (stats.errors++ == 0)
                breaks.add(BreakPoint.exception("08-workload-" + stats.op, recorder.step(), state, t));
        }
    }

    private static final class OpStats
    {
        final String op;
        long[] nanos = new long[64];
        int count;
        int errors;

        OpStats(String op)
        {
            this.op = op;
        }

        void add(long value)
        {
            if (count == nanos.length)
                nanos = Arrays.copyOf(nanos, count * 2);
            nanos[count++] = value;
        }

        void write(BufferedWriter out) throws IOException
        {
            long[] sorted = Arrays.copyOf(nanos, count);
            Arrays.sort(sorted);
            out.write(String.format(Locale.ROOT, "%s,%d,%d,%.3f,%.3f,%.3f", op, count, errors,
                                    at(sorted, 0.50), at(sorted, 0.99), count == 0 ? 0 : sorted[count - 1] / 1e6));
            out.newLine();
        }

        private static double at(long[] sorted, double fraction)
        {
            return sorted.length == 0 ? 0 : sorted[Math.max(0, (int) Math.ceil(fraction * sorted.length) - 1)] / 1e6;
        }
    }

    /** Runs one DDL and feeds its latency to the recorder. Returns a break point when it throws. */
    private BreakPoint ddl(String cql)
    {
        long startedAt = System.nanoTime();
        try
        {
            executor.execute(cql);
        }
        catch (Throwable t)
        {
            return finish(recorder.exception(state, t));
        }
        recorder.recordDdl(System.nanoTime() - startedAt);
        return null;
    }

    /** Like {@link #ddl} and also writes a columns.csv row with the system_schema growth. */
    private BreakPoint timedDdl(String cql, int columns, BufferedWriter log)
    {
        long before = probe.systemSchemaBytes();
        long startedAt = System.nanoTime();
        try
        {
            executor.execute(cql);
        }
        catch (Throwable t)
        {
            return finish(recorder.exception(state, t));
        }
        long elapsed = System.nanoTime() - startedAt;
        recorder.recordDdl(elapsed);
        long after = probe.systemSchemaBytes();
        try
        {
            log.write(String.format(Locale.ROOT, "%d,%.3f,%d,%d", columns, elapsed / 1e6,
                                    before < 0 || after < 0 ? -1 : after - before,
                                    cql.getBytes(StandardCharsets.UTF_8).length));
            log.newLine();
            log.flush();
        }
        catch (IOException e)
        {
            return recorder.exception(state, e);
        }
        return null;
    }

    // Writes a last row so the failed ramp still shows its final size.
    private BreakPoint finish(BreakPoint reason)
    {
        try
        {
            recorder.checkpoint(state, false);
        }
        catch (Throwable ignored)
        {
            // the node may be unusable
        }
        return reason;
    }

    private BufferedWriter columnsLog() throws IOException
    {
        boolean exists = Files.exists(columnsCsv);
        BufferedWriter log = Files.newBufferedWriter(columnsCsv, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                     StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        if (!exists)
        {
            log.write("columns,ddl_ms,system_schema_delta_bytes,cql_bytes");
            log.newLine();
        }
        return log;
    }
}
