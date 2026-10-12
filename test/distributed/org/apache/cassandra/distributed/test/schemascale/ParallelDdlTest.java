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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** rampTables.ddlConcurrency with a fake executor and probe, and the thread safety of the DDL window. No node starts. */
public class ParallelDdlTest
{
    private static final Path TMP = Path.of("tmp", "parallel-ddl-test").toAbsolutePath();
    private static final Pattern CREATE = Pattern.compile("^CREATE TABLE ks_(\\d{3})\\.t(\\d{6}) \\(");
    private static final int SPREAD = 3;

    /** Records each created table index; counts the DDL in flight. */
    private static final class FakeExecutor implements SchemaExecutor
    {
        final Queue<Integer> created = new ConcurrentLinkedQueue<>();
        final Queue<String> badKeyspace = new ConcurrentLinkedQueue<>();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final int failAt;

        FakeExecutor(int failAt)
        {
            this.failAt = failAt;
        }

        @Override
        public void execute(String cql) throws Exception
        {
            Matcher matcher = CREATE.matcher(cql);
            if (!matcher.find())
                throw new IllegalArgumentException("Unexpected CQL: " + cql);
            int index = Integer.parseInt(matcher.group(2));
            if (Integer.parseInt(matcher.group(1)) != index % SPREAD)
                badKeyspace.add(cql);
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try
            {
                TimeUnit.MICROSECONDS.sleep(200);
                if (index == failAt)
                    throw new IllegalStateException("fake failure at table " + index);
                created.add(index);
            }
            finally
            {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public Object[][] query(String cql, Object... binds)
        {
            return new Object[0][];
        }
    }

    private static final MetricsProbe PROBE = new MetricsProbe()
    {
        @Override
        public Map<String, Long> sample(boolean withSnapshotSize)
        {
            Map<String, Long> values = new LinkedHashMap<>();
            values.put("heap_post_gc", 1L << 20);
            values.put("heap_max", 1L << 34);
            return values;
        }

        @Override
        public void gc()
        {
        }
    };

    /** One listener call: the table count, the last flag and the DDL in flight at that moment. */
    private static final class Checkpoint
    {
        final long tables;
        final boolean last;
        final int inFlight;

        Checkpoint(long tables, boolean last, int inFlight)
        {
            this.tables = tables;
            this.last = last;
            this.inFlight = inFlight;
        }
    }

    private static ScaleProfile profile(int max, int every, int clients)
    {
        ScaleProfile profile = new ScaleProfile();
        profile.variants.put("stock", new LinkedHashMap<>());
        profile.rampTables = new ScaleProfile.RampTables();
        profile.rampTables.max = max;
        profile.rampTables.checkpointEvery = every;
        profile.rampTables.ddlConcurrency = clients;
        profile.stop.ddlP99Ms = 600_000;
        profile.stop.snapshotOverMaxMutation = false;
        profile.profiling.gcEvery = 1;
        profile.validate();
        return profile;
    }

    private static ScaleWorkload workload(String name, FakeExecutor executor, ScaleProfile profile, StepRecorder recorder,
                                          ScaleState state, List<Checkpoint> checkpoints)
    {
        Path directory = TMP.resolve(name);
        return new ScaleWorkload(executor, PROBE, profile, recorder, state, SPREAD, directory.resolve("columns.csv"),
                                 directory.resolve("workload.csv"),
                                 (checkpoint, scale, last) -> {
                                     checkpoints.add(new Checkpoint(scale.tables, last, executor.inFlight.get()));
                                     return null;
                                 });
    }

    private static StepRecorder recorder(String name, ScaleProfile profile) throws IOException
    {
        return new StepRecorder(Files.createDirectories(TMP.resolve(name)).resolve("steps.csv"), PROBE, profile.stop,
                                profile.profiling, 3_600_000L, null);
    }

    @Test
    public void sixteenClientsCreateEveryTableOnceAndPauseAtCheckpoints() throws IOException
    {
        ScaleProfile profile = profile(2000, 250, 16);
        FakeExecutor executor = new FakeExecutor(-1);
        ScaleState state = new ScaleState();
        List<Checkpoint> checkpoints = new ArrayList<>();
        BreakPoint stop;
        try (StepRecorder recorder = recorder("sixteen", profile))
        {
            stop = workload("sixteen", executor, profile, recorder, state, checkpoints).rampTables();
        }
        assertNull(String.valueOf(stop), stop);
        assertNoGapsOrDuplicates(executor.created, 0, 2000);
        assertTrue(executor.badKeyspace.toString(), executor.badKeyspace.isEmpty());
        assertEquals(2000, state.tables);
        assertEquals(2000L * 3, state.columns);
        assertTrue("16 clients run DDL at once", executor.maxInFlight.get() > 1);

        assertEquals(8, checkpoints.size());
        for (int i = 0; i < checkpoints.size(); i++)
        {
            Checkpoint checkpoint = checkpoints.get(i);
            assertEquals(250L * (i + 1), checkpoint.tables);
            assertEquals(i == checkpoints.size() - 1, checkpoint.last);
            assertEquals("no DDL runs during a checkpoint", 0, checkpoint.inFlight);
        }

        List<String> lines = Files.readAllLines(TMP.resolve("sixteen").resolve("steps.csv"), StandardCharsets.UTF_8);
        List<String> header = Arrays.asList(lines.get(0).split(","));
        assertEquals(List.of("ddl_concurrency", "ddl_per_s"), header.subList(header.size() - 2, header.size()));
        assertEquals(9, lines.size());
        for (String line : lines.subList(1, lines.size()))
        {
            String[] cells = line.split(",");
            assertEquals("02-ramp-tables", cells[header.indexOf("ramp")]);
            assertEquals("250", cells[header.indexOf("ddl_count")]);
            assertEquals("16", cells[header.indexOf("ddl_concurrency")]);
            assertTrue(line, Double.parseDouble(cells[header.indexOf("ddl_per_s")]) > 0);
        }
    }

    @Test
    public void oneClientKeepsTheSequentialOrder() throws IOException
    {
        ScaleProfile profile = profile(300, 100, 1);
        FakeExecutor executor = new FakeExecutor(-1);
        List<Checkpoint> checkpoints = new ArrayList<>();
        try (StepRecorder recorder = recorder("one", profile))
        {
            assertNull(workload("one", executor, profile, recorder, new ScaleState(), checkpoints).rampTables());
        }
        List<Integer> order = new ArrayList<>(executor.created);
        for (int i = 0; i < order.size(); i++)
            assertEquals(Integer.valueOf(i), order.get(i));
        assertEquals(1, executor.maxInFlight.get());
        assertEquals(3, checkpoints.size());
    }

    @Test
    public void aFailureStopsEveryClientAndGivesOneBreakPoint() throws Exception
    {
        ScaleProfile profile = profile(2000, 250, 16);
        FakeExecutor executor = new FakeExecutor(777);
        ScaleState state = new ScaleState();
        List<Checkpoint> checkpoints = new ArrayList<>();
        BreakPoint stop;
        try (StepRecorder recorder = recorder("failure", profile))
        {
            stop = workload("failure", executor, profile, recorder, state, checkpoints).rampTables();
        }
        assertNotNull(stop);
        assertEquals(BreakPoint.Reason.EXCEPTION, stop.reason);
        assertTrue(stop.exception, stop.exception.contains("fake failure at table 777"));
        int created = executor.created.size();
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals("no client runs after the ramp returns", created, executor.created.size());
        assertEquals(0, executor.inFlight.get());
        assertFalse(executor.created.contains(777));
        assertTrue(String.valueOf(created), created >= 750 && created < 1000);
        assertEquals(created, new HashSet<>(executor.created).size());
        assertEquals(created, state.tables);
        assertEquals(List.of(250L, 500L, 750L), checkpoints.stream().map(c -> c.tables).collect(Collectors.toList()));
    }

    @Test
    public void aResumedRampStartsAfterTheSavedCount() throws IOException
    {
        ScaleProfile profile = profile(1500, 250, 16);
        FakeExecutor executor = new FakeExecutor(-1);
        ScaleState state = new ScaleState();
        state.tables = 1000;
        List<Checkpoint> checkpoints = new ArrayList<>();
        try (StepRecorder recorder = recorder("resume", profile))
        {
            ScaleWorkload workload = workload("resume", executor, profile, recorder, state, checkpoints);
            workload.resumeTables(1000);
            assertNull(workload.rampTables());
        }
        assertNoGapsOrDuplicates(executor.created, 1000, 1500);
        assertEquals(1500, state.tables);
        assertEquals(2, checkpoints.size());
        assertEquals(1250, checkpoints.get(0).tables);
        assertTrue(checkpoints.get(1).last);
    }

    @Test
    public void ddlWindowIsThreadSafe() throws Exception
    {
        ScaleProfile profile = profile(1000, 100, 16);
        int threads = 16;
        int perThread = 20_000;
        try (StepRecorder recorder = recorder("window", profile))
        {
            recorder.beginRamp("02-ramp-tables");
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> writers = new ArrayList<>();
            for (int t = 0; t < threads; t++)
            {
                int base = t * perThread;
                Thread writer = new Thread(() -> {
                    try
                    {
                        start.await();
                    }
                    catch (InterruptedException e)
                    {
                        return;
                    }
                    for (int i = 1; i <= perThread; i++)
                        recorder.recordDdl((base + i) * 1_000_000L);
                });
                writer.start();
                writers.add(writer);
            }
            start.countDown();
            for (Thread writer : writers)
                writer.join();
            assertNull(recorder.checkpoint(new ScaleState(), false));
            Map<String, String> row = recorder.lastRow();
            assertEquals(Integer.toString(threads * perThread), row.get("ddl_count"));
            assertEquals(String.format(Locale.ROOT, "%.3f", (double) threads * perThread), row.get("ddl_max_ms"));
            assertEquals("1", row.get("ddl_concurrency"));

            assertNull(recorder.checkpoint(new ScaleState(), false));
            assertEquals("the window empties at each checkpoint", "0", recorder.lastRow().get("ddl_count"));
        }
    }

    private static void assertNoGapsOrDuplicates(Queue<Integer> created, int from, int to)
    {
        List<Integer> all = new ArrayList<>(created);
        Set<Integer> distinct = new HashSet<>(all);
        assertEquals("no duplicates", all.size(), distinct.size());
        assertEquals(to - from, all.size());
        for (int i = from; i < to; i++)
            assertTrue("missing table " + i, distinct.contains(i));
    }
}
