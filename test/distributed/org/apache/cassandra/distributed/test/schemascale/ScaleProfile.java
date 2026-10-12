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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import org.apache.cassandra.config.DataStorageSpec;

/**
 * A schema scale run definition, loaded from YAML. Field names match the YAML keys. A section that is
 * absent (null) turns its phase off.
 */
public final class ScaleProfile
{
    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");
    static final int MAX_DDL_CONCURRENCY = 256;
    static final List<String> NODE_PROFILERS = List.of("off", "mbean", "agentpath");
    static final List<String> NODE_PROFILE_EVENTS = List.of("cpu", "itimer", "wall", "alloc", "lock");

    public String name;
    public Map<String, Map<String, Object>> variants = new LinkedHashMap<>();
    public Map<String, Object> node = new LinkedHashMap<>();
    public List<Integer> keyspaceSpread = new ArrayList<>(List.of(1));
    public RampTables rampTables;
    public RampColumns rampColumns;
    public Grid grid;
    public Stop stop = new Stop();
    public Workload workload;
    public Restart restart;
    public int holdSeconds;
    public Profiling profiling = new Profiling();
    /** Capture class histogram, heap info, NMT, threads and a JFR window after every table checkpoint. Process mode gets the JFR part. */
    public boolean captureEveryStep;
    /** Capture heap dump, histogram, NMT diff, JFR and thread dump when the tables ramp ends, before the node stops. */
    public boolean captureFinal;
    /**
     * When captureFinal writes heap.hprof: heap-stop (only when the ramp ends on a heap stop, where memory is tight),
     * always, or never. A dump of a 31g heap is 15-35 GB of disk.
     */
    public String finalHeapDump = HEAP_DUMP_ON_HEAP_STOP;
    public static final String HEAP_DUMP_ON_HEAP_STOP = "heap-stop";
    public static final String HEAP_DUMP_ALWAYS = "always";
    public static final String HEAP_DUMP_NEVER = "never";

    /** Whether captureFinal writes heap.hprof for a ramp that ended with {@code stop}; null is the ramp maximum. */
    public boolean heapDumpAt(BreakPoint.Reason stop)
    {
        switch (finalHeapDump)
        {
            case HEAP_DUMP_ALWAYS: return true;
            case HEAP_DUMP_NEVER: return false;
            default: return stop == BreakPoint.Reason.HEAP_BYTES || stop == BreakPoint.Reason.HEAP_FRACTION;
        }
    }
    /** Restart the node gracefully every N table checkpoints; 0 is off. */
    public int restartEvery;
    /**
     * Table counts where the tables ramp stops the node, copies its data to snapshotDir/&lt;schema key&gt;/t&lt;N&gt;/
     * and starts it again. Process mode only.
     */
    public List<Integer> snapshotAt = new ArrayList<>();
    /** A saved schema to start from: a snapshot directory, or t&lt;N&gt; under snapshotDir/&lt;schema key&gt;/. Process mode only. */
    public String startFrom;
    /** Root of the saved schemas. */
    public String snapshotDir = "logs/schema-snapshots";
    /**
     * Table counts that get the full captures (step files, node profiles) and the restart probe. When set,
     * captureEveryStep and restartEvery have no effect.
     */
    public List<Integer> captureAt = new ArrayList<>();
    /** captureAt points also run the restart probe (after the node profile of the point ends). */
    public boolean captureRestart = true;

    public static final class RampTables
    {
        public int max = 1000;
        public int checkpointEvery = 100;
        public String schema = "pk int, c int, v text, PRIMARY KEY (pk, c)";
        /** Clients that run CREATE TABLE at the same time on one driver session. */
        public int ddlConcurrency = 1;
    }

    public static final class RampColumns
    {
        public int tables = 2;
        public List<Integer> createSizes = new ArrayList<>();
        public int alterAddMax;
        public int alterCheckpointEvery = 500;
        public String columnType = "int";
    }

    public static final class Grid
    {
        public List<Integer> tables = new ArrayList<>();
        public List<Integer> columns = new ArrayList<>();
        public long columnBudget = 10_000_000L;
    }

    public static final class Stop
    {
        public long ddlP99Ms = 5000;
        public double heapPostGcFraction = 0.85;
        /** Absolute post-GC heap limit such as 25GiB; null is off. Checked together with heapPostGcFraction. */
        public String heapPostGcBytes;
        /**
         * Stop when a TCM snapshot store fails or the snapshot passes snapshot_size_limit (the smaller of
         * max_value_size and internode_max_message_size). The name predates snapshots skipping the commit log.
         */
        public boolean snapshotOverMaxMutation = true;
        public String rampTimeout = "4h";
        public boolean abortOnBreak;

        /** heapPostGcBytes in bytes, or -1 when unset. */
        public long heapPostGcLimit()
        {
            if (heapPostGcBytes == null || heapPostGcBytes.isBlank())
                return -1;
            return new DataStorageSpec.LongBytesBound(heapPostGcBytes.trim()).toBytes();
        }
    }

    public static final class Workload
    {
        public int sampleTables = 1000;
        public int rowsPerTable = 10;
        public boolean wideSelectStar = true;
    }

    public static final class Restart
    {
        public boolean graceful = true;
        public boolean kill = true;
    }

    public static final class Profiling
    {
        public boolean asyncProfiler = true;
        public int histogramEvery = 10000;
        public int snapshotSizeEvery = 5;
        public int gcEvery = 5;
        /** How the node loads async-profiler for captureAt profiles: off, mbean or agentpath. Process mode only. */
        public String nodeProfiler = "off";
        /** async-profiler events, one window each, in order. */
        public List<String> nodeProfileEvents = new ArrayList<>(List.of("cpu", "wall"));
        /** Length of each node profile window. */
        public int nodeProfileSeconds = 30;
        /** JFR settings of the node flight recording in process mode; the JDK 'profile' settings when the file is missing. */
        public String nodeJfrSettings = "test/conf/schemascale/node-profile.jfc";
    }

    /** Loads a YAML file and applies "a.b=value" overrides before binding. */
    public static ScaleProfile load(Path file, List<String> overrides) throws IOException
    {
        Map<String, Object> tree;
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8))
        {
            tree = new Yaml().load(reader);
        }
        if (tree == null)
            tree = new LinkedHashMap<>();
        for (String override : overrides)
            applyOverride(tree, override);

        ScaleProfile profile = new Yaml(new Constructor(ScaleProfile.class, new LoaderOptions())).load(new Yaml().dump(tree));
        if (profile.name == null)
            profile.name = file.getFileName().toString().replaceFirst("\\.ya?ml$", "");
        profile.validate();
        return profile;
    }

    @SuppressWarnings("unchecked")
    static void applyOverride(Map<String, Object> tree, String override)
    {
        int eq = override.indexOf('=');
        if (eq <= 0)
            throw new IllegalArgumentException("Override must look like a.b=value: " + override);
        String[] path = override.substring(0, eq).split("\\.");
        String raw = override.substring(eq + 1);
        Object value = new Yaml().load(raw);
        // YAML 1.1 reads on/off/yes/no as booleans, and ant strips quotes from -Dargs, so keep such words as text
        if (value instanceof Boolean && !raw.equalsIgnoreCase("true") && !raw.equalsIgnoreCase("false"))
            value = raw;
        Map<String, Object> current = tree;
        for (int i = 0; i < path.length - 1; i++)
        {
            Object next = current.get(path[i]);
            if (!(next instanceof Map))
            {
                next = new LinkedHashMap<String, Object>();
                current.put(path[i], next);
            }
            current = (Map<String, Object>) next;
        }
        current.put(path[path.length - 1], value);
    }

    /** Node config for a variant: the shared node section with the variant keys on top. */
    public Map<String, Object> nodeConfig(String variant)
    {
        Map<String, Object> variantConfig = variants.get(variant);
        if (variantConfig == null)
            throw new IllegalArgumentException("Unknown variant '" + variant + "'; known: " + variants.keySet());
        Map<String, Object> merged = new LinkedHashMap<>(node);
        merged.putAll(variantConfig);
        // The node resolves a relative metrics file on its class path; a path from the repo root works in both modes.
        Object metricsFile = merged.get("metrics_config_file");
        if (metricsFile instanceof String && Files.isRegularFile(Path.of((String) metricsFile)))
            merged.put("metrics_config_file", Path.of((String) metricsFile).toAbsolutePath().toString());
        return merged;
    }

    public boolean nodeProfiling()
    {
        return !"off".equals(profiling.nodeProfiler);
    }

    public long rampTimeoutMillis()
    {
        return parseDurationMillis(stop.rampTimeout);
    }

    public static long parseDurationMillis(String text)
    {
        Matcher matcher = DURATION.matcher(text.trim().toLowerCase(Locale.ROOT));
        if (!matcher.matches())
            throw new IllegalArgumentException("Bad duration '" + text + "'; use a number and ms, s, m, h or d");
        long amount = Long.parseLong(matcher.group(1));
        switch (matcher.group(2))
        {
            case "ms": return amount;
            case "s": return amount * 1000L;
            case "m": return amount * 60_000L;
            case "h": return amount * 3_600_000L;
            default: return amount * 86_400_000L;
        }
    }

    public void validate()
    {
        require(variants != null && !variants.isEmpty(), "variants must define at least one variant");
        require(keyspaceSpread != null && !keyspaceSpread.isEmpty(), "keyspaceSpread must not be empty");
        for (int spread : keyspaceSpread)
            require(spread >= 1, "keyspaceSpread values must be at least 1");
        if (rampTables != null)
        {
            require(rampTables.max >= 1, "rampTables.max must be at least 1");
            require(rampTables.checkpointEvery >= 1, "rampTables.checkpointEvery must be at least 1");
            require(rampTables.schema != null && !rampTables.schema.isBlank(), "rampTables.schema is required");
            require(rampTables.ddlConcurrency >= 1 && rampTables.ddlConcurrency <= MAX_DDL_CONCURRENCY,
                    "rampTables.ddlConcurrency must be in [1, " + MAX_DDL_CONCURRENCY + ']');
        }
        requireTableCounts("snapshotAt", snapshotAt);
        requireTableCounts("captureAt", captureAt);
        require(startFrom == null || !startFrom.isBlank(), "startFrom must not be blank");
        require(startFrom == null || rampTables != null, "startFrom needs a rampTables section");
        require(snapshotDir != null && !snapshotDir.isBlank(), "snapshotDir is required");
        require(profiling.nodeProfiler != null && NODE_PROFILERS.contains(profiling.nodeProfiler),
                "profiling.nodeProfiler must be one of " + NODE_PROFILERS);
        require(profiling.nodeProfileSeconds >= 1, "profiling.nodeProfileSeconds must be at least 1");
        require(profiling.nodeProfileEvents != null && !profiling.nodeProfileEvents.isEmpty(),
                "profiling.nodeProfileEvents must not be empty");
        for (String event : profiling.nodeProfileEvents)
            require(event != null && NODE_PROFILE_EVENTS.contains(event), "profiling.nodeProfileEvents values must be in " + NODE_PROFILE_EVENTS);
        if (rampColumns != null)
        {
            require(rampColumns.tables >= 1, "rampColumns.tables must be at least 1");
            require(rampColumns.alterAddMax >= 0, "rampColumns.alterAddMax must not be negative");
            require(rampColumns.alterCheckpointEvery >= 1, "rampColumns.alterCheckpointEvery must be at least 1");
            require(rampColumns.columnType != null && rampColumns.columnType.matches("[a-z]+"),
                    "rampColumns.columnType must be a simple CQL type such as int");
            for (int size : rampColumns.createSizes)
                require(size >= 1, "rampColumns.createSizes values must be at least 1");
        }
        if (grid != null)
        {
            require(grid.columnBudget >= 1, "grid.columnBudget must be at least 1");
            for (int tables : grid.tables)
                require(tables >= 1, "grid.tables values must be at least 1");
            for (int columns : grid.columns)
                require(columns >= 2, "grid.columns values must be at least 2");
        }
        require(stop.ddlP99Ms >= 0, "stop.ddlP99Ms must not be negative");
        require(stop.heapPostGcFraction > 0 && stop.heapPostGcFraction <= 1, "stop.heapPostGcFraction must be in (0, 1]");
        if (stop.heapPostGcBytes != null && !stop.heapPostGcBytes.isBlank())
            require(stop.heapPostGcLimit() > 0, "stop.heapPostGcBytes must be above zero");
        require(restartEvery >= 0, "restartEvery must not be negative");
        require(List.of(HEAP_DUMP_ON_HEAP_STOP, HEAP_DUMP_ALWAYS, HEAP_DUMP_NEVER).contains(finalHeapDump),
                "finalHeapDump must be heap-stop, always or never");
        rampTimeoutMillis();
        if (workload != null)
        {
            require(workload.sampleTables >= 0, "workload.sampleTables must not be negative");
            require(workload.rowsPerTable >= 1, "workload.rowsPerTable must be at least 1");
        }
        require(holdSeconds >= 0, "holdSeconds must not be negative");
        require(profiling.histogramEvery >= 1, "profiling.histogramEvery must be at least 1");
        require(profiling.snapshotSizeEvery >= 1, "profiling.snapshotSizeEvery must be at least 1");
        require(profiling.gcEvery >= 1, "profiling.gcEvery must be at least 1");
    }

    // A count must be hit by a checkpoint: a multiple of checkpointEvery, or the ramp max.
    private void requireTableCounts(String key, List<Integer> counts)
    {
        require(counts != null, key + " must be a list");
        if (counts.isEmpty())
            return;
        require(rampTables != null, key + " needs a rampTables section");
        for (Integer count : counts)
            require(count != null && count >= 1 && (count % rampTables.checkpointEvery == 0 || count == rampTables.max),
                    key + " values must be multiples of rampTables.checkpointEvery or rampTables.max, got " + count);
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new IllegalArgumentException("Invalid profile: " + message);
    }
}
