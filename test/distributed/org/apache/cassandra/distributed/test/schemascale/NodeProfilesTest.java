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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipFile;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Node profile windows against a fake AsyncProfilerMBean, the -agentpath library and the node JVM options. No node starts. */
public class NodeProfilesTest
{
    private static final Path TMP = Path.of("tmp", "node-profiles-test").toAbsolutePath();

    /** Records the MBean calls. execute writes the file named by "file=" on stop, like async-profiler. */
    private static final class FakeControl implements NodeProfilerControl
    {
        final List<String> calls = new ArrayList<>();
        final Path resultDirectory;
        boolean enabled = true;
        boolean unsafe = true;
        boolean cpuWorks = true;
        boolean agent;
        Path pending;
        String pendingSafe;

        FakeControl(Path resultDirectory)
        {
            this.resultDirectory = resultDirectory;
        }

        synchronized int callCount()
        {
            return calls.size();
        }

        @Override
        public synchronized boolean enabled()
        {
            return enabled;
        }

        @Override
        public synchronized String execute(String command) throws IOException
        {
            calls.add(command);
            if (!unsafe)
                throw new RuntimeException("RuntimeMBeanException", new SecurityException("not permitted"));
            if (command.startsWith("start,event=cpu,") && !cpuWorks)
                throw new RuntimeException("wrapped", new IllegalStateException("perf_event_open failed"));
            if (command.startsWith("start,"))
                pending = Path.of(command.substring(command.indexOf("file=") + 5));
            if (command.equals("stop") && pending != null)
            {
                Files.writeString(pending, "jfr", StandardCharsets.UTF_8);
                pending = null;
            }
            return "OK";
        }

        @Override
        public synchronized boolean start(Map<String, String> parameters)
        {
            calls.add("start" + new TreeMap<>(parameters));
            pendingSafe = parameters.get("outputFileName");
            return true;
        }

        @Override
        public synchronized boolean stop(Map<String, String> parameters) throws IOException
        {
            calls.add("stop" + parameters);
            Files.createDirectories(resultDirectory);
            Files.writeString(resultDirectory.resolve(pendingSafe), "safe-jfr", StandardCharsets.UTF_8);
            return true;
        }

        @Override
        public byte[] fetch(String resultFile)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path resultDirectory()
        {
            return resultDirectory;
        }

        @Override
        public String loadPath()
        {
            return "mbean";
        }

        @Override
        public synchronized boolean useAgentOnNextStart()
        {
            agent = true;
            return true;
        }
    }

    private static Path clean(String name) throws IOException
    {
        Path directory = TMP.resolve(name);
        SchemaSnapshots.deleteTree(directory);
        return Files.createDirectories(directory);
    }

    @Test
    public void executeWindowsWriteOneFilePerEventAndRetryCpuWithItimer() throws IOException
    {
        Path base = clean("execute");
        Path step = base.resolve("steps").resolve("t0001000");
        FakeControl control = new FakeControl(base.resolve("profiler"));
        control.cpuWorks = false;
        try (NodeProfiles profiles = new NodeProfiles(control, "mbean", List.of("cpu", "wall"), 0))
        {
            assertTrue(profiles.start(1000, step));
            profiles.awaitIdle();
            assertFalse(profiles.busy());
            assertEquals(List.of(NodeProfiles.startCommand("cpu", step.resolve("node-cpu.jfr")),
                                 NodeProfiles.startCommand("itimer", step.resolve("node-cpu.jfr")),
                                 "stop",
                                 NodeProfiles.startCommand("wall", step.resolve("node-wall.jfr")),
                                 "stop"),
                         control.calls);
            assertTrue(Files.isRegularFile(step.resolve("node-cpu.jfr")));
            assertTrue(Files.isRegularFile(step.resolve("node-wall.jfr")));

            Map<String, Object> point = profiles.results().get(0);
            assertEquals(1000L, point.get("tables"));
            List<?> windows = (List<?>) point.get("windows");
            Map<?, ?> cpu = (Map<?, ?>) windows.get(0);
            assertEquals("itimer", cpu.get("eventUsed"));
            assertEquals("execute", cpu.get("method"));
            assertNotNull(cpu.get("cpuError"));
            assertEquals("wall", ((Map<?, ?>) windows.get(1)).get("eventUsed"));
            assertTrue(profiles.drainWarnings().isEmpty());
            assertNull(profiles.path().get("fallbackReason"));
        }
    }

    @Test
    public void safeModeUsesStartAndStopAndMovesTheFile() throws IOException
    {
        Path base = clean("safe");
        Path step = base.resolve("steps").resolve("t0002000");
        FakeControl control = new FakeControl(base.resolve("node").resolve("logs").resolve("profiler"));
        control.unsafe = false;
        try (NodeProfiles profiles = new NodeProfiles(control, "mbean", List.of("wall"), 0))
        {
            profiles.start(2000, step);
            profiles.awaitIdle();
        }
        assertEquals("start{duration=60s, events=wall, outputFileName=node-wall-t2000.jfr, outputFormat=jfr}", control.calls.get(1));
        assertEquals("safe-jfr", Files.readString(step.resolve("node-wall.jfr"), StandardCharsets.UTF_8));
        assertFalse(Files.exists(control.resultDirectory.resolve("node-wall-t2000.jfr")));
    }

    @Test
    public void aServiceWithoutAProfilerSwitchesToTheAgentPath() throws IOException
    {
        Path base = clean("fallback");
        FakeControl control = new FakeControl(base.resolve("profiler"));
        control.enabled = false;
        try (NodeProfiles profiles = new NodeProfiles(control, "mbean", List.of("cpu"), 0))
        {
            profiles.start(1000, base.resolve("t1000"));
            profiles.awaitIdle();
            assertTrue(control.agent);
            assertTrue(control.calls.isEmpty());
            assertNotNull(profiles.path().get("fallbackReason"));
            assertEquals(1, profiles.drainWarnings().size());
            assertTrue(profiles.drainWarnings().isEmpty());
        }
    }

    @Test
    public void stopNowEndsALongWindowEarly() throws IOException
    {
        Path base = clean("stop-now");
        FakeControl control = new FakeControl(base.resolve("profiler"));
        try (NodeProfiles profiles = new NodeProfiles(control, "mbean", List.of("cpu", "wall"), 600))
        {
            long startedAt = System.nanoTime();
            profiles.start(5000, base.resolve("t5000"));
            while (control.callCount() == 0 && System.nanoTime() - startedAt < 10_000_000_000L)
                Thread.onSpinWait();
            profiles.stopNow();
            assertTrue("stopNow returns at once", System.nanoTime() - startedAt < 30_000_000_000L);
            List<?> windows = (List<?>) profiles.results().get(0).get("windows");
            assertEquals(true, ((Map<?, ?>) windows.get(0)).get("truncated"));
            assertNotNull(((Map<?, ?>) windows.get(1)).get("skipped"));
            assertEquals("stop", control.calls.get(control.calls.size() - 1));
        }
    }

    @Test
    public void agentLibraryComesFromTheJarForThisPlatform() throws IOException
    {
        assertEquals("macos/libasyncProfiler.so", AsyncProfilerLibrary.entryFor("Mac OS X", "aarch64"));
        assertEquals("linux-x64/libasyncProfiler.so", AsyncProfilerLibrary.entryFor("Linux", "amd64"));
        assertEquals("linux-arm64/libasyncProfiler.so", AsyncProfilerLibrary.entryFor("Linux", "aarch64"));
        assertNull(AsyncProfilerLibrary.entryFor("Windows 11", "amd64"));

        Path jar = AsyncProfilerLibrary.findJar(Path.of("").toAbsolutePath());
        String entry = AsyncProfilerLibrary.entryFor("Mac OS X", "aarch64");
        Path library = AsyncProfilerLibrary.extract(jar, entry, clean("agent"));
        long expected;
        try (ZipFile zip = new ZipFile(jar.toFile()))
        {
            expected = zip.getEntry(entry).getSize();
        }
        assertEquals(expected, Files.size(library));
        assertEquals(AsyncProfilerLibrary.FILE_NAME, library.getFileName().toString());
    }

    @Test
    public void nodeOptionsCarryTheProfilerFlagsTheAgentAndTheJfc()
    {
        Path node = TMP.resolve("node");
        ProcessNodeSettings plain = ProcessNodeSettings.fromEnvironment(Map.of(), Path.of("").toAbsolutePath(), Path.of("/jdk"), true);
        String none = plain.jvmExtraOptions(TMP, node, 17199, TMP.resolve("node.jfr"));
        assertFalse(none.contains("async_profiler"));
        assertFalse(none.contains("-agentpath"));
        assertTrue(none.contains("settings=profile,"));

        Path jfc = Path.of("test", "conf", "schemascale", "node-profile.jfc").toAbsolutePath();
        ProcessNodeSettings profiled = plain.withProfiling(jfc, true);
        String mbean = profiled.jvmExtraOptions(TMP, node, 17199, TMP.resolve("node.jfr"));
        assertTrue(mbean.contains("-Dcassandra.async_profiler.enabled=true"));
        assertTrue(mbean.contains("-Dcassandra.async_profiler.unsafe_mode=true"));
        assertTrue(mbean.contains("settings=" + jfc + ','));
        assertFalse(mbean.contains("-agentpath"));

        String agent = profiled.jvmExtraOptions(TMP, node, 17199, null, node.resolve("libasyncProfiler.so"));
        assertTrue(agent.endsWith("-agentpath:" + node.resolve("libasyncProfiler.so")));
        assertTrue(Files.isRegularFile(jfc));
    }

    @Test
    public void savedPortsAreUsedOnlyWhenAllAreFree()
    {
        ProcessNodeSettings settings = ProcessNodeSettings.fromEnvironment(Map.of(), Path.of("").toAbsolutePath(), Path.of("/jdk"), false);
        NodeConfigRenderer.Ports fresh = new NodeConfigRenderer.Ports(30001, 30002, 30003, 30004);
        List<Integer> busy = new ArrayList<>();
        ProcessRuntimeFactory factory = new ProcessRuntimeFactory(settings, () -> fresh, port -> !busy.contains(port), false);
        Map<String, Object> saved = new LinkedHashMap<>();
        saved.put("storagePort", 7012);
        saved.put("sslStoragePort", 7013);
        saved.put("nativeTransportPort", 9042);
        saved.put("jmxPort", 7199);

        factory.preferPorts(saved);
        NodeConfigRenderer.Ports reused = factory.nextPorts();
        assertEquals(7012, reused.storage);
        assertEquals(9042, reused.nativeTransport);
        assertEquals("the preference holds for one node only", 30001, factory.nextPorts().storage);

        busy.add(7199);
        factory.preferPorts(saved);
        assertEquals(30001, factory.nextPorts().storage);
    }
}
