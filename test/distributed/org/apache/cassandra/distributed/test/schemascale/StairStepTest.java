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
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Stair-step profile keys, the absolute heap stop and the jcmd capture. No node starts. */
public class StairStepTest
{
    private static final Path PROFILE = Path.of("test", "conf", "schemascale", "stair-tables.yaml");
    private static final Path TMP = Path.of("tmp", "stair-step-test").toAbsolutePath();

    @Test
    public void stairProfileEnablesOnlyTheTablesRamp() throws IOException
    {
        ScaleProfile profile = ScaleProfile.load(PROFILE, List.of());
        assertEquals(1_000_000, profile.rampTables.max);
        assertEquals(1000, profile.rampTables.checkpointEvery);
        assertNull(profile.rampColumns);
        assertNull(profile.grid);
        assertNull(profile.workload);
        assertNull(profile.restart);
        assertEquals(0, profile.holdSeconds);
        assertTrue(profile.captureEveryStep);
        assertTrue(profile.captureFinal);
        assertEquals(5, profile.restartEvery);
        assertEquals(25L << 30, profile.stop.heapPostGcLimit());
        assertEquals(1, profile.profiling.gcEvery);
        assertFalse(profile.stop.snapshotOverMaxMutation);
        assertEquals(600000, profile.stop.ddlP99Ms);
    }

    @Test
    public void overridesSetTheNewKeys() throws IOException
    {
        ScaleProfile profile = ScaleProfile.load(PROFILE, List.of("stop.heapPostGcBytes=1500MiB", "restartEvery=2",
                                                                  "captureEveryStep=false", "rampTables.checkpointEvery=250"));
        assertEquals(1500L << 20, profile.stop.heapPostGcLimit());
        assertEquals(2, profile.restartEvery);
        assertFalse(profile.captureEveryStep);
        assertEquals(250, profile.rampTables.checkpointEvery);
    }

    @Test
    public void otherProfilesKeepTheirBehavior() throws IOException
    {
        ScaleProfile profile = ScaleProfile.load(Path.of("test", "conf", "schemascale", "smoke.yaml"), List.of());
        assertEquals(-1, profile.stop.heapPostGcLimit());
        assertFalse(profile.captureEveryStep);
        assertFalse(profile.captureFinal);
        assertEquals(0, profile.restartEvery);
    }

    @Test
    public void badKeysFail() throws IOException
    {
        assertRejected("stop.heapPostGcBytes=lots");
        assertRejected("stop.heapPostGcBytes=0MiB");
        assertRejected("restartEvery=-1");
    }

    private static void assertRejected(String override) throws IOException
    {
        try
        {
            ScaleProfile.load(PROFILE, List.of(override));
            fail("Accepted " + override);
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    @Test
    public void heapBytesStopNeedsAForcedGc() throws IOException
    {
        ScaleProfile profile = ScaleProfile.load(PROFILE, List.of("stop.heapPostGcBytes=1GiB"));
        Path steps = Files.createDirectories(TMP).resolve("steps.csv");
        long[] postGc = { 2L << 30 };
        MetricsProbe probe = new MetricsProbe()
        {
            public Map<String, Long> sample(boolean withSnapshotSize)
            {
                Map<String, Long> values = new LinkedHashMap<>();
                values.put("heap_post_gc", postGc[0]);
                values.put("heap_max", 31L << 30);
                return values;
            }

            public void gc()
            {
            }
        };
        ScaleState state = new ScaleState();
        try (StepRecorder recorder = new StepRecorder(steps, probe, profile.stop, profile.profiling, 3_600_000L, null))
        {
            recorder.beginRamp("02-ramp-tables");
            BreakPoint stop = recorder.checkpoint(state, false);
            assertNotNull(stop);
            assertEquals(BreakPoint.Reason.HEAP_BYTES, stop.reason);
            assertEquals(1L << 30, (long) stop.threshold);
            postGc[0] = 512L << 20;
            assertNull(recorder.checkpoint(state, false));
        }
    }

    /** A post-GC reading over the limit stops the ramp only when the live histogram total agrees. */
    @Test
    public void heapStopNeedsLiveHeapConfirmation() throws IOException
    {
        ScaleProfile profile = ScaleProfile.load(PROFILE, List.of("stop.heapPostGcBytes=1GiB"));
        Path steps = Files.createDirectories(TMP).resolve("steps-confirm.csv");
        MetricsProbe probe = new MetricsProbe()
        {
            public Map<String, Long> sample(boolean withSnapshotSize)
            {
                Map<String, Long> values = new LinkedHashMap<>();
                values.put("heap_post_gc", 2L << 30);
                values.put("heap_max", 31L << 30);
                return values;
            }

            public void gc()
            {
            }
        };
        long[] live = { 512L << 20 };
        ScaleState state = new ScaleState();
        try (StepRecorder recorder = new StepRecorder(steps, probe, profile.stop, profile.profiling, 3_600_000L, null))
        {
            recorder.confirmHeapWith(() -> live[0]);
            recorder.beginRamp("02-ramp-tables");
            assertNull("live heap under the limit: keep going", recorder.checkpoint(state, false));
            assertEquals(1, recorder.unconfirmedHeapStops());

            live[0] = 3L << 30;
            BreakPoint stop = recorder.checkpoint(state, false);
            assertNotNull(stop);
            assertEquals(BreakPoint.Reason.HEAP_BYTES, stop.reason);
            assertEquals("the stop records the live total", 3L << 30, (long) stop.value);

            live[0] = -1;
            assertNotNull("no live total: the post-GC reading decides", recorder.checkpoint(state, false));
        }
    }

    @Test
    public void histogramTotalParsesTheTotalLine() throws IOException
    {
        Path histogram = Files.createDirectories(TMP).resolve("histogram-total.txt");
        Files.write(histogram, List.of("12345:", " num     #instances         #bytes  class name (module)",
                                       "   1:         90023      183971272  [J (java.base@25)",
                                       "Total     167490842    13100006000"));
        assertEquals(13100006000L, StepCapture.histogramTotal(histogram));
        Files.write(histogram, List.of("no total here"));
        assertEquals(-1L, StepCapture.histogramTotal(histogram));
    }

    @Test
    public void threadCountsGroupByNamePrefix()
    {
        List<String> lines = List.of("\"MutationStage-1\" #45 [123] daemon prio=5 os_prio=31 cpu=1ms",
                                     "\"MutationStage-2\" #46 [124] daemon prio=5 os_prio=31",
                                     "\"pool-5-thread-3\" #50 [130] prio=5",
                                     "\"VM Thread\" os_prio=31 cpu=5ms tid=0x1",
                                     "\"7\" #9 [1] prio=5",
                                     "   java.lang.Thread.State: RUNNABLE");
        Map<String, Integer> counts = JcmdStepCapture.threadCounts(lines);
        assertEquals(Integer.valueOf(2), counts.get("MutationStage"));
        assertEquals(Integer.valueOf(1), counts.get("pool-5-thread"));
        assertEquals(Integer.valueOf(1), counts.get("VM Thread"));
        assertEquals(Integer.valueOf(1), counts.get("7"));
        assertEquals(4, counts.size());
    }

    @Test
    public void captureWritesFilesAndSurvivesFailures() throws IOException
    {
        Path home = fakeJavaHome("Thread.print", 1);
        List<String> warnings = new ArrayList<>();
        Path directory = TMP.resolve("capture").resolve("t0001000");
        Files.deleteIfExists(directory.resolve("node.jfr"));
        JcmdStepCapture capture = new ProcessCaptureForTest(home, warnings);
        capture.captureStep(1000, directory);

        assertTrue(Files.size(directory.resolve("class-histogram.txt")) > 0);
        assertTrue(Files.size(directory.resolve("heap-info.txt")) > 0);
        assertTrue(Files.size(directory.resolve("nmt-summary.txt")) > 0);
        assertFalse("A failed Thread.print leaves no threads.txt", Files.exists(directory.resolve("threads.txt")));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("Thread.print") && w.contains("status 1")));
        assertTrue(Files.readString(directory.resolve("capture.log")).contains("JFR.dump"));
    }

    @Test
    public void finalCaptureWithoutHeapDumpNeverRunsIt() throws IOException
    {
        Path home = fakeJavaHome("none", 0);
        List<String> warnings = new ArrayList<>();
        Path directory = TMP.resolve("no-dump-run").resolve("final");
        Map<String, Object> facts = new ProcessCaptureForTest(home, warnings).captureFinal(directory, 1, false);
        assertEquals("skipped: finalHeapDump", facts.get("heap.hprof"));
        assertFalse(Files.exists(directory.resolve("heap.hprof")));
        Path log = directory.resolve("capture.log");
        assertFalse(Files.exists(log) && Files.readString(log).contains("GC.heap_dump"));
        assertTrue(Files.size(directory.resolve("class-histogram.txt")) > 0);
    }

    @Test
    public void finalCaptureSkipsTheHeapDumpWithoutDiskAndReportsADeadNode() throws IOException
    {
        Path home = fakeJavaHome("none", 0);
        List<String> warnings = new ArrayList<>();
        Path directory = TMP.resolve("final-run").resolve("final");
        Map<String, Object> facts = new ProcessCaptureForTest(home, warnings).captureFinal(directory, Long.MAX_VALUE / 4, true);
        assertEquals(true, facts.get("nodeAlive"));
        assertTrue(String.valueOf(facts.get("heap.hprof")).startsWith("skipped"));
        assertFalse(Files.exists(directory.resolve("heap.hprof")));
        assertTrue(Files.size(directory.resolve("class-histogram.txt")) > 0);
        assertTrue(Files.exists(directory.resolve("thread-dump.txt")));

        Map<String, Object> dead = new JcmdStepCapture(home, () -> Long.MAX_VALUE, warnings::add, LocalDateTime::now,
                                                       Duration.ofSeconds(30), Duration.ofSeconds(30))
        {
            boolean recordsFlights()
            {
                return true;
            }

            String flightNote()
            {
                return "";
            }
        }.captureFinal(TMP.resolve("dead-run").resolve("final"), 1, true);
        assertEquals(false, dead.get("nodeAlive"));
        assertNotNull(dead.get("note"));
    }

    // The fake jcmd prints its arguments; it exits with failStatus for the command named failing.
    private static Path fakeJavaHome(String failing, int failStatus) throws IOException
    {
        Path home = TMP.resolve("java-home-" + failing);
        Path bin = Files.createDirectories(home.resolve("bin"));
        Path jcmd = bin.resolve("jcmd");
        Files.writeString(jcmd, "#!/bin/bash\n" +
                                "echo \"$@\"\n" +
                                "case \"$*\" in *" + failing + "*) exit " + failStatus + " ;; esac\n" +
                                "exit 0\n", StandardCharsets.UTF_8);
        Files.setPosixFilePermissions(jcmd, PosixFilePermissions.fromString("rwxr-xr-x"));
        return home;
    }

    private static final class ProcessCaptureForTest extends JcmdStepCapture
    {
        ProcessCaptureForTest(Path home, List<String> warnings)
        {
            super(home, () -> ProcessHandle.current().pid(), warnings::add, LocalDateTime::now,
                  Duration.ofSeconds(30), Duration.ofSeconds(30));
        }

        boolean recordsFlights()
        {
            return true;
        }

        String flightNote()
        {
            return "";
        }
    }
}
