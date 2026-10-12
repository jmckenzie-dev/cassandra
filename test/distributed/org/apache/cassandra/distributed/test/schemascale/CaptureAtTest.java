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
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** captureAt scheduling and the Step 8 profile keys. No node starts. */
public class CaptureAtTest
{
    private static final Path PROFILES = Path.of("test", "conf", "schemascale");

    private static ScaleProfile load(String name, String... overrides) throws IOException
    {
        return ScaleProfile.load(PROFILES.resolve(name + ".yaml"), List.of(overrides));
    }

    @Test
    public void parallelProfileCapturesProfilesAndSavesAtTenThousand() throws IOException
    {
        ScaleProfile profile = load("stair-tables-parallel");
        assertEquals(16, profile.rampTables.ddlConcurrency);
        assertEquals(10000, profile.rampTables.max);
        assertEquals(List.of(10000), profile.snapshotAt);
        assertEquals(List.of(1000, 2000, 5000, 10000), profile.captureAt);
        assertEquals("mbean", profile.profiling.nodeProfiler);
        assertEquals(30, profile.profiling.nodeProfileSeconds);
        assertEquals(List.of("cpu", "wall"), profile.profiling.nodeProfileEvents);
        assertTrue(profile.nodeProfiling());
        assertNull(profile.startFrom);

        CaptureSchedule first = CaptureSchedule.at(profile, 1000, false, true, true);
        assertTrue(first.capture);
        assertTrue(first.profile);
        assertTrue("the restart probe follows the profile", first.restart);
        assertFalse(first.snapshot);

        CaptureSchedule between = CaptureSchedule.at(profile, 3000, false, true, true);
        assertFalse(between.any());
        assertFalse(between.profile);
        assertFalse(between.restart);

        CaptureSchedule end = CaptureSchedule.at(profile, 10000, true, true, true);
        assertTrue(end.capture);
        assertTrue(end.snapshot);
        assertFalse("no DDL follows the last count", end.profile);
        assertFalse("the snapshot restarts the node already", end.restart);

        CaptureSchedule noProfiler = CaptureSchedule.at(profile, 2000, false, true, false);
        assertTrue(noProfiler.capture);
        assertFalse(noProfiler.profile);
        assertTrue(noProfiler.restart);
    }

    @Test
    public void coldStartCapturesAtTheSavedCountWithoutSaving() throws IOException
    {
        ScaleProfile profile = load("coldstart");
        assertEquals("t10000", profile.startFrom);
        assertEquals(10000, profile.rampTables.max);
        assertFalse(profile.nodeProfiling());
        CaptureSchedule start = CaptureSchedule.at(profile, 10000, true, false, false);
        assertTrue(start.capture);
        assertFalse(start.snapshot);
        assertFalse(start.profile);
        assertTrue(start.restart);

        CaptureSchedule withSnapshotKey = CaptureSchedule.at(load("coldstart", "snapshotAt=[10000]"), 10000, true, false, false);
        assertFalse("a run never saves its own start count", withSnapshotKey.snapshot);
    }

    @Test
    public void sweepProfilesWallWithoutARestart() throws IOException
    {
        ScaleProfile profile = load("ddl-sweep", "rampTables.ddlConcurrency=8");
        assertEquals(8, profile.rampTables.ddlConcurrency);
        assertEquals(12000, profile.rampTables.max);
        assertEquals(List.of("wall"), profile.profiling.nodeProfileEvents);
        assertFalse(profile.captureRestart);
        CaptureSchedule start = CaptureSchedule.at(profile, 10000, false, false, true);
        assertTrue(start.capture);
        assertTrue(start.profile);
        assertFalse(start.restart);
    }

    @Test
    public void defaultsKeepOlderProfilesUnchanged() throws IOException
    {
        for (String name : List.of("smoke", "stair-tables", "ramp-tables", "ramp-columns", "grid", "full"))
        {
            ScaleProfile profile = load(name);
            assertTrue(name, profile.captureAt.isEmpty());
            assertTrue(name, profile.snapshotAt.isEmpty());
            assertNull(name, profile.startFrom);
            assertFalse(name, profile.nodeProfiling());
            if (profile.rampTables != null)
                assertEquals(name, 1, profile.rampTables.ddlConcurrency);
            assertFalse(name, CaptureSchedule.at(profile, 1000, false, true, true).any());
        }
    }

    @Test
    public void badStepEightKeysFail() throws IOException
    {
        assertRejected("captureAt=[1500]");          // not a multiple of checkpointEvery 1000
        assertRejected("snapshotAt=[999]");
        assertRejected("captureAt=[0]");
        assertRejected("rampTables.ddlConcurrency=0");
        assertRejected("rampTables.ddlConcurrency=257");
        assertRejected("profiling.nodeProfiler=jcmd");
        assertRejected("profiling.nodeProfileEvents=[cpu,bogus]");
        assertRejected("profiling.nodeProfileSeconds=0");
        assertRejected("startFrom=' '");
        load("stair-tables-parallel", "captureAt=[10000]", "rampTables.ddlConcurrency=256", "profiling.nodeProfiler=agentpath");
    }

    @Test
    public void heapDumpOnlyAtAHeapStopByDefault() throws IOException
    {
        ScaleProfile profile = load("stair-tables-parallel");
        assertEquals(ScaleProfile.HEAP_DUMP_ON_HEAP_STOP, profile.finalHeapDump);
        assertTrue(profile.heapDumpAt(BreakPoint.Reason.HEAP_BYTES));
        assertTrue(profile.heapDumpAt(BreakPoint.Reason.HEAP_FRACTION));
        assertFalse(profile.heapDumpAt(BreakPoint.Reason.EXCEPTION));
        assertFalse("the ramp maximum is not a heap stop", profile.heapDumpAt(null));

        assertTrue(load("stair-tables-parallel", "finalHeapDump=always").heapDumpAt(null));
        assertFalse(load("stair-tables-parallel", "finalHeapDump=never").heapDumpAt(BreakPoint.Reason.HEAP_BYTES));
        assertRejected("finalHeapDump=sometimes");
    }

    private static void assertRejected(String override) throws IOException
    {
        try
        {
            load("stair-tables-parallel", override);
            fail("Accepted " + override);
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }
}
