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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Config rendering and launch settings of process mode. No node starts. */
public class ProcessModeConfigTest
{
    private static final Path HOME = Path.of("").toAbsolutePath();
    private static final Path NODE = Path.of("tmp", "process-mode-config-test", "node").toAbsolutePath();
    private static final NodeConfigRenderer.Ports PORTS = new NodeConfigRenderer.Ports(17000, 17001, 19042, 17199);

    @Test
    public void renderSetsLayoutAndKeepsBase()
    {
        Map<String, Object> base = new LinkedHashMap<>();
        base.put("cluster_name", "Test Cluster");
        base.put("listen_address", "localhost");
        Map<String, Object> config = NodeConfigRenderer.render(base, Map.of(), NODE, PORTS);

        assertEquals("Test Cluster", config.get("cluster_name"));
        assertEquals("127.0.0.1", config.get("listen_address"));
        assertEquals("127.0.0.1", config.get("rpc_address"));
        assertEquals(17000, config.get("storage_port"));
        assertEquals(19042, config.get("native_transport_port"));
        assertEquals(List.of(NODE.resolve("data").resolve("data").toString()), config.get("data_file_directories"));
        assertEquals(NODE.resolve("data").resolve("commitlog").toString(), config.get("commitlog_directory"));
        assertTrue(seeds(config).contains("127.0.0.1:17000"));
    }

    @Test
    public void overridesWinAndInputsStayUnchanged()
    {
        Map<String, Object> base = new LinkedHashMap<>();
        base.put("native_transport_port", 9042);
        Map<String, Object> overrides = new LinkedHashMap<>();
        overrides.put("native_transport_port", 9999);
        overrides.put("max_mutation_size", "1MiB");
        Map<String, Object> config = NodeConfigRenderer.render(base, overrides, NODE, PORTS);

        assertEquals(9999, config.get("native_transport_port"));
        assertEquals("1MiB", config.get("max_mutation_size"));
        assertEquals(1, base.size());
        assertEquals(9042, base.get("native_transport_port"));
        assertEquals(2, overrides.size());
    }

    @Test
    public void overrideKeepsYamlBooleanWordsAsText() throws IOException
    {
        // ant strips the quotes, so the harness sees off, which YAML 1.1 reads as false
        ScaleProfile profile = ScaleProfile.load(HOME.resolve("test/conf/schemascale/hit-tables.yaml"),
                                                 List.of("profiling.nodeProfiler=off", "captureFinal=false"));
        assertEquals("off", profile.profiling.nodeProfiler);
        assertEquals(false, profile.captureFinal);
    }

    @Test
    public void everyVariantOfTheProfilesRendersAConfigTheNodeAccepts() throws IOException
    {
        Map<String, Object> base = NodeConfigRenderer.load(HOME.resolve("conf").resolve("cassandra.yaml"));
        for (String profileName : List.of("smoke", "ramp-tables", "ramp-columns", "grid", "full", "stair-tables",
                                          "stair-tables-parallel", "stair-tables-worstcase", "stair-columns-worstcase", "coldstart", "ddl-sweep",
                                          "hit-tables"))
        {
            ScaleProfile profile = ScaleProfile.load(HOME.resolve("test/conf/schemascale").resolve(profileName + ".yaml"), List.of());
            for (String variant : profile.variants.keySet())
            {
                Path node = NODE.resolve(profileName + '-' + variant);
                Map<String, Object> overrides = profile.nodeConfig(variant);
                Map<String, Object> config = NodeConfigRenderer.render(base, overrides, node, PORTS);
                Path yaml = node.resolve("conf").resolve("cassandra.yaml");
                NodeConfigRenderer.write(config, yaml);

                Map<String, Object> reloaded = NodeConfigRenderer.load(yaml);
                for (Map.Entry<String, Object> override : overrides.entrySet())
                    assertEquals(profileName + '/' + variant + ' ' + override.getKey(), override.getValue(), reloaded.get(override.getKey()));
                assertTrue(Files.isDirectory(node.resolve("data").resolve("commitlog")));
                Config parsed = new YamlConfigurationLoader().loadConfig(yaml.toUri().toURL());
                assertNotNull(parsed);
                assertEquals(PORTS.nativeTransport, parsed.native_transport_port);
                assertEquals("127.0.0.1", parsed.listen_address);
            }
        }
    }

    @Test
    public void portPickerPrefersTheDefaultNativePort()
    {
        AtomicInteger next = new AtomicInteger(30000);
        NodeConfigRenderer.Ports free = NodeConfigRenderer.Ports.pick(next::incrementAndGet, port -> true);
        assertEquals(9042, free.nativeTransport);

        NodeConfigRenderer.Ports busy = NodeConfigRenderer.Ports.pick(next::incrementAndGet, port -> port != 9042);
        assertFalse(busy.nativeTransport == 9042);
        assertEquals(4, Stream.of(busy.storage, busy.sslStorage, busy.nativeTransport, busy.jmx).distinct().count());
    }

    @Test
    public void systemPortsAreFreeAndDistinct()
    {
        int first = NodeConfigRenderer.Ports.systemFreePort();
        int second = NodeConfigRenderer.Ports.systemFreePort();
        assertTrue(first > 0 && second > 0);
        assertTrue(NodeConfigRenderer.Ports.systemIsFree(first));
    }

    @Test
    public void heapDefaultsAndCap()
    {
        ProcessNodeSettings defaults = settings(Map.of());
        assertEquals("31g", defaults.heap);
        assertEquals("8G", defaults.directMemory);
        assertEquals("32g", settings(Map.of("MAX_HEAP_SIZE", "32g")).heap);
        assertEquals("32768m", settings(Map.of("MAX_HEAP_SIZE", "32768m")).heap);
        for (String tooBig : List.of("33g", "32769m", "40G", "34359738369", "1t"))
            assertRejected(Map.of("MAX_HEAP_SIZE", tooBig));
        for (String junk : List.of("lots", "g", "-1g", "12x", "0", "0m"))
            assertRejected(Map.of("MAX_HEAP_SIZE", junk));
        assertRejected(Map.of("MAX_DIRECT_MEMORY_SIZE", "many"));
    }

    @Test
    public void sizesMatchTheLauncherForms()
    {
        assertEquals(31 * 1024, ProcessNodeSettings.parseMebibytes("31g"));
        assertEquals(31 * 1024, ProcessNodeSettings.parseMebibytes("31744M"));
        assertEquals(32 * 1024, ProcessNodeSettings.parseMebibytes("32768m"));
        assertEquals(48 * 1024, ProcessNodeSettings.parseMebibytes("48G"));
        assertEquals(1, ProcessNodeSettings.parseMebibytes("1048576"));
        assertEquals(0, ProcessNodeSettings.parseMebibytes("0"));
        assertEquals(32L * 1024 * 1024 * 1024, ProcessNodeSettings.parseBytes("32g"));

        Random random = new Random(42);
        for (int i = 0; i < 1000; i++)
        {
            long mebibytes = random.nextInt(1 << 20);
            assertEquals(mebibytes, ProcessNodeSettings.parseMebibytes(mebibytes + "m"));
            assertEquals(mebibytes * 1024, ProcessNodeSettings.parseMebibytes(mebibytes + "g"));
            assertEquals(mebibytes, ProcessNodeSettings.parseMebibytes(mebibytes * 1024 + "k"));
        }
    }

    @Test
    public void launcherOptionsStayAndMissingOnesAreAdded()
    {
        String launcher = "-XX:MaxDirectMemorySize=8g -Xlog:gc*:file=/run/gc.log -XX:NativeMemoryTracking=detail " +
                          "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/run -XX:-PerfDisableSharedMem";
        String kept = settings(Map.of("JVM_EXTRA_OPTS", launcher)).jvmExtraOptions(Path.of("/other"), NODE, 17199, null);
        assertTrue(kept.startsWith(launcher));
        assertFalse(kept.contains("/other/gc.log"));
        assertFalse(kept.contains("NativeMemoryTracking=summary"));

        String added = settings(Map.of()).jvmExtraOptions(Path.of("/run"), NODE, 17199, Path.of("/run/node.jfr"));
        assertTrue(added.contains("-Xlog:gc*,safepoint:file=/run/gc.log"));
        assertTrue(added.contains("-XX:NativeMemoryTracking=summary"));
        assertTrue(added.contains("-XX:HeapDumpPath=/run"));
        assertTrue(added.contains("-XX:-PerfDisableSharedMem"));
        assertTrue(added.contains("-Dcassandra.jmx.local.port=17199"));
        assertTrue(added.contains("-Dcassandra.ring_delay_ms=" + ProcessNodeSettings.RING_DELAY_MILLIS));
        assertTrue(added.contains("-XX:-AlwaysPreTouch"));
        assertTrue(added.contains("-XX:StartFlightRecording=filename=/run/node.jfr,settings=profile,dumponexit=true,maxage=6h,maxsize=1g"));
        assertFalse(settings(Map.of()).jvmExtraOptions(Path.of("/run"), NODE, 17199, null).contains("StartFlightRecording"));
    }

    @Test
    public void nodeEnvironmentPutsTheJavaHomeFirst()
    {
        Map<String, String> inherited = new LinkedHashMap<>();
        inherited.put("PATH", "/old/jdk11/bin:/usr/bin");
        inherited.put("MAX_HEAP_SIZE", "16g");
        inherited.put("HOME", "/home/test");
        ProcessNodeSettings settings = settings(inherited);
        Map<String, String> env = settings.nodeEnvironment(NODE, NODE.resolve("conf"), "-Dx=1");

        assertEquals(settings.javaHome.resolve("bin") + File.pathSeparator + "/old/jdk11/bin:/usr/bin", env.get("PATH"));
        assertEquals(settings.javaHome.toString(), env.get("JAVA_HOME"));
        assertEquals("16g", env.get("MAX_HEAP_SIZE"));
        assertEquals(NODE.resolve("conf").toString(), env.get("CASSANDRA_CONF"));
        assertEquals(NODE.resolve("logs").toString(), env.get("CASSANDRA_LOG_DIR"));
        assertEquals("-Dx=1", env.get("JVM_EXTRA_OPTS"));
        assertEquals("/home/test", env.get("HOME"));
        assertNotNull(env.get("HEAP_NEWSIZE"));
        assertEquals("true", env.get("CASSANDRA_JDK_UNSUPPORTED"));
    }

    @Test
    public void maxMutationSizeComesFromTheConfig()
    {
        assertEquals(1024 * 1024, ProcessRuntimeFactory.maxMutationBytes(Map.of("max_mutation_size", "1MiB")));
        assertEquals(16 * 1024 * 1024, ProcessRuntimeFactory.maxMutationBytes(Map.of("commitlog_segment_size", "32MiB")));
        assertEquals(16 * 1024 * 1024, ProcessRuntimeFactory.maxMutationBytes(Map.of()));
        assertEquals(2 * 1024 * 1024, ProcessRuntimeFactory.maxMutationBytes(Map.of("max_mutation_size", "2048KiB",
                                                                                   "commitlog_segment_size", "32MiB")));
    }

    @Test
    public void snapshotSizeLimitIsTheSmallerOfValueAndMessageLimits()
    {
        long mib = 1024 * 1024;
        assertEquals("defaults: 128 MiB messages under 256 MiB values", 128 * mib, ProcessRuntimeFactory.snapshotSizeLimitBytes(Map.of()));
        assertEquals(64 * mib, ProcessRuntimeFactory.snapshotSizeLimitBytes(Map.of("max_value_size", "64MiB")));
        assertEquals(32 * mib, ProcessRuntimeFactory.snapshotSizeLimitBytes(Map.of("internode_max_message_size", "32MiB")));
        assertEquals(16 * mib, ProcessRuntimeFactory.snapshotSizeLimitBytes(Map.of("internode_application_receive_queue_reserve_endpoint_capacity", "16MiB")));
        assertEquals("max_mutation_size does not apply", 128 * mib, ProcessRuntimeFactory.snapshotSizeLimitBytes(Map.of("max_mutation_size", "1MiB")));
    }

    private static ProcessNodeSettings settings(Map<String, String> environment)
    {
        return ProcessNodeSettings.fromEnvironment(environment, HOME, Path.of("/jdk"), false);
    }

    private static void assertRejected(Map<String, String> environment)
    {
        try
        {
            settings(environment);
            fail("Expected a rejection for " + environment);
        }
        catch (IllegalArgumentException expected)
        {
            assertNotNull(expected.getMessage());
        }
    }

    private static String seeds(Map<String, Object> config)
    {
        Map<?, ?> provider = (Map<?, ?>) ((List<?>) config.get("seed_provider")).get(0);
        Map<?, ?> parameters = (Map<?, ?>) ((List<?>) provider.get("parameters")).get(0);
        return String.valueOf(parameters.get("seeds"));
    }
}
