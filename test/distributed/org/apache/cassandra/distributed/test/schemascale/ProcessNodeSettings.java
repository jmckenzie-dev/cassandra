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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * How a process-mode node JVM starts: Java home, heap limits and JVM options. The launcher passes the
 * limits through MAX_HEAP_SIZE, MAX_DIRECT_MEMORY_SIZE and JVM_EXTRA_OPTS; this class reads them, applies the
 * same cap and adds what is missing.
 */
final class ProcessNodeSettings
{
    static final String DEFAULT_HEAP = "31g";
    static final String DEFAULT_DIRECT_MEMORY = "8G";
    static final long MAX_HEAP_MIB = 32L * 1024;
    static final long DEFAULT_STARTUP_TIMEOUT_MILLIS = 3_600_000L;
    static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 600_000L;
    static final int RING_DELAY_MILLIS = 1000;
    // Bounds the node flight recording, so the disk repository and each window dump stay small.
    static final String FLIGHT_MAX_AGE = "6h";
    static final String FLIGHT_MAX_SIZE = "1g";

    final Path cassandraHome;
    final Path javaHome;
    final String heap;
    final String directMemory;
    final List<String> inheritedJvmOptions;
    final boolean flightRecording;
    final long startupTimeoutMillis;
    final long shutdownTimeoutMillis;
    final Map<String, String> environment;
    /** JFR settings file of the node recording, or null for the JDK 'profile' settings. */
    final Path flightSettings;
    /** Start the node with the AsyncProfilerService enabled and in unsafe mode. */
    final boolean asyncProfilerService;

    ProcessNodeSettings(Path cassandraHome, Path javaHome, String heap, String directMemory, List<String> inheritedJvmOptions,
                        boolean flightRecording, long startupTimeoutMillis, long shutdownTimeoutMillis,
                        Map<String, String> environment)
    {
        this(cassandraHome, javaHome, heap, directMemory, inheritedJvmOptions, flightRecording, startupTimeoutMillis,
             shutdownTimeoutMillis, environment, null, false);
    }

    ProcessNodeSettings(Path cassandraHome, Path javaHome, String heap, String directMemory, List<String> inheritedJvmOptions,
                        boolean flightRecording, long startupTimeoutMillis, long shutdownTimeoutMillis,
                        Map<String, String> environment, Path flightSettings, boolean asyncProfilerService)
    {
        long heapBytes = parseBytes(heap);
        if (heapBytes <= 0)
            throw new IllegalArgumentException("Node heap '" + heap + "' must be above zero");
        if (heapBytes > MAX_HEAP_MIB << 20)
            throw new IllegalArgumentException("Node heap '" + heap + "' is above the 32g cap; lower MAX_HEAP_SIZE");
        this.cassandraHome = cassandraHome;
        this.javaHome = javaHome;
        this.heap = heap;
        this.directMemory = directMemory;
        this.inheritedJvmOptions = List.copyOf(inheritedJvmOptions);
        this.flightRecording = flightRecording;
        this.startupTimeoutMillis = startupTimeoutMillis;
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
        this.environment = Map.copyOf(environment);
        this.flightSettings = flightSettings;
        this.asyncProfilerService = asyncProfilerService;
    }

    /** A copy with the node JFR settings file (null for 'profile') and the AsyncProfilerService flags. */
    ProcessNodeSettings withProfiling(Path flightSettingsFile, boolean profilerService)
    {
        return new ProcessNodeSettings(cassandraHome, javaHome, heap, directMemory, inheritedJvmOptions, flightRecording,
                                       startupTimeoutMillis, shutdownTimeoutMillis, environment, flightSettingsFile, profilerService);
    }

    /**
     * Reads the limits the launcher exports. An unset MAX_HEAP_SIZE means 31g and an unset
     * MAX_DIRECT_MEMORY_SIZE means 8G. A heap above 32g throws.
     */
    static ProcessNodeSettings fromEnvironment(Map<String, String> environment, Path cassandraHome, Path javaHome,
                                               boolean flightRecording)
    {
        String heap = valueOrDefault(environment.get("MAX_HEAP_SIZE"), DEFAULT_HEAP);
        String direct = valueOrDefault(environment.get("MAX_DIRECT_MEMORY_SIZE"), DEFAULT_DIRECT_MEMORY);
        parseMebibytes(direct);
        List<String> inherited = tokens(environment.get("JVM_EXTRA_OPTS"));
        return new ProcessNodeSettings(cassandraHome, javaHome, heap, direct, inherited, flightRecording,
                                       DEFAULT_STARTUP_TIMEOUT_MILLIS, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, environment);
    }

    private static String valueOrDefault(String value, String fallback)
    {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    static List<String> tokens(String text)
    {
        if (text == null || text.isBlank())
            return new ArrayList<>();
        return new ArrayList<>(Arrays.asList(text.trim().split("\\s+")));
    }

    /** Converts 31g, 32768m, 31744M, 48G or a plain byte count to MiB, as the launcher does. */
    static long parseMebibytes(String text)
    {
        return parseBytes(text) >> 20;
    }

    static long parseBytes(String text)
    {
        String value = text.trim();
        int split = 0;
        while (split < value.length() && Character.isDigit(value.charAt(split)))
            split++;
        if (split == 0)
            throw new IllegalArgumentException("Cannot parse size '" + text + "'");
        long amount = Long.parseLong(value.substring(0, split));
        String unit = value.substring(split).toLowerCase(Locale.ROOT);
        long bytes;
        switch (unit)
        {
            case "":
            case "b":
                bytes = amount;
                break;
            case "k":
            case "kb":
                bytes = amount << 10;
                break;
            case "m":
            case "mb":
                bytes = amount << 20;
                break;
            case "g":
            case "gb":
                bytes = amount << 30;
                break;
            case "t":
            case "tb":
                bytes = amount << 40;
                break;
            default:
                throw new IllegalArgumentException("Cannot parse size '" + text + "'");
        }
        return bytes;
    }

    /** Environment of the node shell: the inherited one, the Java home first on PATH, and the memory limits. */
    Map<String, String> nodeEnvironment(Path nodeDirectory, Path configDirectory, String jvmExtraOptions)
    {
        Map<String, String> env = new LinkedHashMap<>(environment);
        env.put("JAVA_HOME", javaHome.toString());
        String path = environment.getOrDefault("PATH", "/usr/bin:/bin");
        env.put("PATH", javaHome.resolve("bin") + File.pathSeparator + path);
        env.put("CASSANDRA_HOME", cassandraHome.toString());
        env.put("CASSANDRA_CONF", configDirectory.toString());
        env.put("CASSANDRA_LOG_DIR", nodeDirectory.resolve("logs").toString());
        env.put("MAX_HEAP_SIZE", heap);
        // cassandra-env.sh rejects MAX_HEAP_SIZE without HEAP_NEWSIZE unless the collector is G1; non-CMS collectors ignore it
        env.putIfAbsent("HEAP_NEWSIZE", "1G");
        env.put("MAX_DIRECT_MEMORY_SIZE", directMemory);
        env.put("CASSANDRA_JDK_UNSUPPORTED", "true");
        env.put("JVM_EXTRA_OPTS", jvmExtraOptions);
        return env;
    }

    /**
     * The JVM_EXTRA_OPTS value: what the launcher passed, defaults for what it left out, then the options
     * this run needs. The node shell appends it last, so these values win over jvm-server.options.
     *
     * @param runDirectory where gc.log and heap dumps go when the launcher did not say
     * @param jfr the flight recording file, or null for none
     */
    String jvmExtraOptions(Path runDirectory, Path nodeDirectory, int jmxPort, Path jfr)
    {
        return jvmExtraOptions(runDirectory, nodeDirectory, jmxPort, jfr, null);
    }

    /** @param agentLibrary libasyncProfiler to load with -agentpath, or null */
    String jvmExtraOptions(Path runDirectory, Path nodeDirectory, int jmxPort, Path jfr, Path agentLibrary)
    {
        List<String> options = new ArrayList<>(inheritedJvmOptions);
        addIfMissing(options, "-Xlog:gc", "-Xlog:gc*,safepoint:file=" + runDirectory.resolve("gc.log") +
                                          ":time,uptime,level,tags:filecount=5,filesize=50m");
        addIfMissing(options, "-XX:NativeMemoryTracking=", "-XX:NativeMemoryTracking=summary");
        addIfMissing(options, "-XX:+HeapDumpOnOutOfMemoryError", "-XX:+HeapDumpOnOutOfMemoryError");
        addIfMissing(options, "-XX:HeapDumpPath=", "-XX:HeapDumpPath=" + runDirectory);
        if (!startsWithAny(options, "-XX:-PerfDisableSharedMem", "-XX:+PerfDisableSharedMem"))
            options.add("-XX:-PerfDisableSharedMem");
        options.add("-Dcassandra.ring_delay_ms=" + RING_DELAY_MILLIS);
        options.add("-Dcassandra.jmx.local.port=" + jmxPort);
        options.add("-Djava.io.tmpdir=" + nodeDirectory.resolve("tmp"));
        // jvm-server.options pre-touches the whole heap; leave it lazy so the RSS shows real use
        options.add("-XX:-AlwaysPreTouch");
        if (jfr != null)
            options.add("-XX:StartFlightRecording=filename=" + jfr + ",settings=" + (flightSettings == null ? "profile" : flightSettings.toString()) +
                        ",dumponexit=true,maxage=" + FLIGHT_MAX_AGE + ",maxsize=" + FLIGHT_MAX_SIZE);
        if (asyncProfilerService || agentLibrary != null)
        {
            options.add("-Dcassandra.async_profiler.enabled=true");
            options.add("-Dcassandra.async_profiler.unsafe_mode=true");
        }
        if (agentLibrary != null)
            options.add("-agentpath:" + agentLibrary);
        return String.join(" ", options);
    }

    private static void addIfMissing(List<String> options, String prefix, String option)
    {
        if (!startsWithAny(options, prefix))
            options.add(option);
    }

    private static boolean startsWithAny(List<String> options, String... prefixes)
    {
        for (String option : options)
            for (String prefix : prefixes)
                if (option.startsWith(prefix))
                    return true;
        return false;
    }
}
