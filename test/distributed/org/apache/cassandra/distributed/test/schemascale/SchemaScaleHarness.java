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
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.ProfiledClusterHarness;
import org.apache.cassandra.distributed.test.ResourceProfiler;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Ramps schema size until the node breaks, and records where and why. The phases, metrics and break
 * points are described by a {@link ScaleProfile}. A break ends only its own ramp; later phases run at the
 * size reached, unless stop.abortOnBreak is set.
 *
 * <pre>
 * SchemaScaleHarness --profile smoke [--variant stock] [--spread 1] [--mode injvm|process]
 *                    [--run-dir path] [--set a.b=v]... [--fail-on-break]
 * </pre>
 */
public final class SchemaScaleHarness extends ProfiledClusterHarness
{
    private static final String DEFAULT_PROFILE_DIR = "test/conf/schemascale";
    private static final String TABLES_RAMP = "02-ramp-tables";

    /** Command line options. */
    public static final class Options
    {
        public String profile;
        public String variant = "stock";
        public Integer spread;
        public String mode = "injvm";
        public Path runDirectory;
        public final List<String> overrides = new ArrayList<>();
        public boolean failOnBreak;

        static Options parse(String[] args)
        {
            Options options = new Options();
            for (int i = 0; i < args.length; i++)
            {
                switch (args[i])
                {
                    case "--profile":
                        options.profile = value(args, ++i, "--profile");
                        break;
                    case "--variant":
                        options.variant = value(args, ++i, "--variant");
                        break;
                    case "--spread":
                        options.spread = Integer.parseInt(value(args, ++i, "--spread"));
                        break;
                    case "--mode":
                        options.mode = value(args, ++i, "--mode");
                        break;
                    case "--run-dir":
                        options.runDirectory = Path.of(value(args, ++i, "--run-dir"));
                        break;
                    case "--set":
                        options.overrides.add(value(args, ++i, "--set"));
                        break;
                    case "--fail-on-break":
                        options.failOnBreak = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }
            if (options.profile == null)
                throw new IllegalArgumentException("--profile is required");
            if (!options.mode.equals("injvm") && !options.mode.equals("process"))
                throw new IllegalArgumentException("--mode must be injvm or process");
            return options;
        }

        private static String value(String[] args, int index, String option)
        {
            if (index >= args.length)
                throw new IllegalArgumentException("Missing value for " + option);
            return args[index];
        }
    }

    /** One phase body. It records break points through {@link #record}. */
    private interface Body
    {
        void run(ResourceProfiler.PhaseResult phase) throws Exception;
    }

    /** Work done while the node is stopped, such as copying its data. */
    private interface StoppedWork
    {
        void run() throws Exception;
    }

    private final Options options;
    private final ScaleProfile profile;
    private final RuntimeFactory runtimeFactory;
    private final int spread;
    private final Map<String, Object> nodeConfig;
    private final List<BreakPoint> breakPoints = new ArrayList<>();
    private final SchemaSnapshots snapshots;
    private final String schemaKey;
    private final List<Map<String, Object>> savedSchemas = new ArrayList<>();

    private NodeRuntime runtime;
    private StepCapture capture;
    private StepRecorder recorder;
    private ScaleState state;
    private ScaleWorkload workload;
    private boolean nodeRunning;
    private boolean aborted;
    private long baselineNodeTables;
    private long heapMax = -1;
    private long directMax = -1;
    private long lastHistogramTables;
    private Map<String, Object> stopFacts;
    private NodeProfiles nodeProfiles;
    // manifest and restore facts when the run started from a saved schema
    private Map<String, Object> resumed;
    // a captureAt restart probe waits until the node profile of that point ends
    private boolean pendingRestart;

    public SchemaScaleHarness(Options options, ScaleProfile profile, RuntimeFactory runtimeFactory, String[] args)
    {
        super(options.runDirectory != null ? options.runDirectory : Path.of("logs"),
              "Schema scale run", "Schema scale summary",
              profile.name + '-' + options.variant + "-ks" + spreadOf(options, profile), args,
              !profile.profiling.asyncProfiler, false, options.runDirectory != null);
        this.options = options;
        this.profile = profile;
        this.runtimeFactory = runtimeFactory;
        this.spread = spreadOf(options, profile);
        this.nodeConfig = profile.nodeConfig(options.variant);
        this.schemaKey = profile.rampTables == null ? null
                                                    : SchemaSnapshots.schemaKey(profile.rampTables.schema, spread,
                                                                                FBUtilities.getReleaseVersionString());
        this.snapshots = SchemaSnapshots.create(Path.of(profile.snapshotDir));
    }

    private static int spreadOf(Options options, ScaleProfile profile)
    {
        return options.spread != null ? options.spread : profile.keyspaceSpread.get(0);
    }

    /** Parses the command line, loads the profile and picks the runtime for the mode. */
    public static SchemaScaleHarness create(String[] args) throws IOException
    {
        Options options = Options.parse(args);
        Path file = Path.of(options.profile);
        if (!Files.isRegularFile(file))
            file = Path.of(DEFAULT_PROFILE_DIR, options.profile + ".yaml");
        ScaleProfile profile = ScaleProfile.load(file, options.overrides);
        return new SchemaScaleHarness(options, profile, runtimeFactory(options, profile), args);
    }

    private static RuntimeFactory runtimeFactory(Options options, ScaleProfile profile)
    {
        if (options.mode.equals("injvm"))
            return (directory, config) -> new InJvmNodeRuntime(config);
        return ProcessRuntimeFactory.fromEnvironment(profile.profiling.asyncProfiler, profile.profiling);
    }

    public static void main(String[] args) throws Throwable
    {
        int exitCode = 0;
        try
        {
            SchemaScaleHarness harness = create(args);
            harness.execute();
            if (harness.options.failOnBreak && !harness.breakPoints.isEmpty())
                exitCode = 2;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            exitCode = 1;
        }
        System.out.flush();
        System.err.flush();
        System.exit(exitCode);
    }

    public List<BreakPoint> breakPoints()
    {
        return breakPoints;
    }

    public Path runDirectory()
    {
        return runDirectory;
    }

    @Override
    protected Cluster buildCluster() throws Exception
    {
        Path snapshot = null;
        if (profile.startFrom != null)
        {
            if (!processMode())
                throw new IllegalArgumentException("startFrom needs --mode process");
            snapshot = snapshots.resolve(profile.startFrom, schemaKey);
            preferSavedPorts(SchemaSnapshots.readManifest(snapshot).get("ports"));
        }
        runtime = runtimeFactory.create(runDirectory.resolve("node"), nodeConfig);
        if (processMode())
            useNodeProcess();
        if (snapshot != null)
        {
            resumed = snapshots.restore(snapshot, schemaKey, nodeData());
            System.out.printf(Locale.ROOT, "Restored %s tables from %s in %s ms (%s)%n", resumed.get("tables"), snapshot,
                              resumed.get("restoreMs"), resumed.get("restoreMethod"));
        }
        long startedAt = System.nanoTime();
        runtime.start();
        long startupMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        nodeRunning = true;
        state = new ScaleState();
        capture = newCapture();
        recorder = newRecorder(runtime, runDirectory.resolve("steps.csv"), true);
        workload = newWorkload(runtime, recorder, state, spread, new ScaleWorkload.CheckpointListener()
        {
            public BreakPoint afterCheckpoint(int checkpoint, ScaleState scale, boolean last)
            {
                return afterTablesCheckpoint(checkpoint, scale, last);
            }

            public void beforeColumnStep(String ramp, int columns)
            {
                startColumnStep(ramp, columns);
            }

            public void afterColumnStep(String ramp, int columns)
            {
                endColumnStep(ramp, columns);
            }
        });
        if (resumed != null)
            resumeFromSnapshot(startupMs);
        nodeProfiles = newNodeProfiles();
        return null;
    }

    // TCM keeps the node addresses; the same ports avoid an address change at the first start
    @SuppressWarnings("unchecked")
    private void preferSavedPorts(Object ports)
    {
        if (ports instanceof Map)
            runtimeFactory.preferPorts((Map<String, ?>) ports);
    }

    private Path nodeData()
    {
        return runDirectory.resolve("node").resolve("data");
    }

    // The node runs on the restored data: check the table count, then continue the ramp from it.
    private void resumeFromSnapshot(long startupMs) throws Exception
    {
        long tables = ((Number) resumed.get("tables")).longValue();
        if (tables > profile.rampTables.max)
            throw new IllegalStateException("rampTables.max " + profile.rampTables.max + " is below the saved table count " + tables);
        long found = userTables();
        if (found != tables)
            throw new IllegalStateException("The node started from " + resumed.get("restoredFrom") + " has " + found +
                                            " tables in " + spread + " keyspaces; the manifest says " + tables);
        int columns = ScaleWorkload.countColumns(profile.rampTables.schema);
        state.keyspaces = spread;
        state.tables = tables;
        state.columns = tables * columns;
        state.maxColsTable = tables > 0 ? columns : 0;
        workload.resumeTables(tables);
        writeRestartRow("startFrom", tables, new RestartTiming(startupMs, RestartLogParser.parse(runtime.logFile(), 0)), heapPostGcNow());
        resumed.put("startupMs", startupMs);
        System.out.printf(Locale.ROOT, "Started from a saved schema: %d tables, startup %d ms; the ramp continues to %d%n",
                          tables, startupMs, profile.rampTables.max);
    }

    // Tables in the ramp keyspaces, by a partition read per keyspace.
    private long userTables() throws Exception
    {
        long count = 0;
        for (int i = 0; i < spread; i++)
        {
            Object[][] rows = runtime.executor().query("SELECT count(*) FROM system_schema.tables WHERE keyspace_name = ?",
                                                       ScaleWorkload.keyspaceName(i));
            count += ((Number) rows[0][0]).longValue();
        }
        return count;
    }

    private NodeProfiles newNodeProfiles()
    {
        if (!profile.nodeProfiling())
            return null;
        if (!processMode())
        {
            profiler.warn("profiling.nodeProfiler needs --mode process; no node profiles in this run");
            return null;
        }
        NodeProfilerControl control = runtime.profilerControl();
        if (control == null)
        {
            profiler.warn("The node has no async-profiler control; no node profiles in this run");
            return null;
        }
        return new NodeProfiles(control, profile.profiling.nodeProfiler, profile.profiling.nodeProfileEvents,
                                profile.profiling.nodeProfileSeconds);
    }

    private void drainProfileWarnings()
    {
        if (nodeProfiles != null)
            for (String warning : nodeProfiles.drainWarnings())
                profiler.warn(warning);
    }

    private boolean processMode()
    {
        return options.mode.equals("process");
    }

    private StepCapture newCapture()
    {
        Path javaHome = Path.of(CassandraRelevantProperties.JAVA_HOME.getString());
        return processMode() ? new ProcessStepCapture(javaHome, runtime::pid, profiler::warn)
                             : new InJvmStepCapture(javaHome, profiler::warn);
    }

    // jcmd and the heap numbers follow the node process. async-profiler can only attach to this JVM.
    private void useNodeProcess()
    {
        profiler.useProcess(runtime::pid);
        profiler.useCheckpointSource(this::nodeCheckpoint);
        profiler.disableAsyncProfiler("process mode; the node is profiled by its own flight recording (node*.jfr)");
        if (profile.profiling.asyncProfiler)
            profiler.warn("Process mode: no per-phase async-profiler sessions. The node JVM records node.jfr instead.");
    }

    private ResourceProfiler.HeapSnapshot nodeCheckpoint()
    {
        if (!nodeRunning)
            return ResourceProfiler.HeapSnapshot.of(-1, -1, -1, 0);
        try
        {
            Map<String, Long> sample = runtime.probe().sample(false);
            return ResourceProfiler.HeapSnapshot.of(sample.getOrDefault("heap_used", -1L), -1L,
                                                    sample.getOrDefault("alloc_bytes_total", -1L),
                                                    (int) Math.max(0, sample.getOrDefault("mbeans", 0L)));
        }
        catch (RuntimeException e)
        {
            return ResourceProfiler.HeapSnapshot.of(-1, -1, -1, 0);
        }
    }

    @Override
    protected void shutdown()
    {
        if (nodeProfiles != null)
        {
            nodeProfiles.close();
            drainProfileWarnings();
        }
        if (recorder != null)
        {
            try
            {
                recorder.close();
            }
            catch (IOException e)
            {
                profiler.warn("Could not close steps.csv: " + e);
            }
        }
        if (runtime != null)
            runtime.close();
    }

    private StepRecorder newRecorder(NodeRuntime node, Path steps, boolean observe) throws IOException
    {
        StepRecorder.Observer observer = observe ? this::observeCheckpoint : null;
        StepRecorder recorder = new StepRecorder(steps, node.probe(), profile.stop, profile.profiling,
                                                 profile.rampTimeoutMillis(), observer);
        Path scratch = steps.resolveSibling("live-heap-histogram.txt");
        recorder.confirmHeapWith(() -> capture == null ? -1 : capture.liveHeapBytes(scratch));
        return recorder;
    }

    private ScaleWorkload newWorkload(NodeRuntime node, StepRecorder steps, ScaleState scale, int keyspaces,
                                      ScaleWorkload.CheckpointListener listener)
    {
        return new ScaleWorkload(node.executor(), node.probe(), profile, steps, scale, keyspaces,
                                 runDirectory.resolve("columns.csv"), runDirectory.resolve("workload.csv"), listener);
    }

    private void observeCheckpoint(String ramp, long step, ScaleState scale)
    {
        maybeHistogram(ramp, step, scale);
        if (profile.captureAt.isEmpty() && profile.captureEveryStep && ramp.equals(TABLES_RAMP))
            capture.captureStep(scale.tables, stepDirectory(scale.tables));
    }

    private Path stepDirectory(long tables)
    {
        return runDirectory.resolve("steps").resolve(String.format(Locale.ROOT, "t%07d", tables));
    }

    // Column ramps, with captureEveryStep: a node profile across the step's DDL, then the step files.
    private Path columnStepDirectory(String ramp, int columns)
    {
        String kind = ramp.endsWith("alter") ? "alter" : "create";
        return runDirectory.resolve("steps").resolve(String.format(Locale.ROOT, "%s-c%07d", kind, columns));
    }

    private void startColumnStep(String ramp, int columns)
    {
        if (profile.captureEveryStep && nodeProfiles != null)
            nodeProfiles.start(columns, columnStepDirectory(ramp, columns));
    }

    private void endColumnStep(String ramp, int columns)
    {
        if (nodeProfiles != null)
            nodeProfiles.stopNow();
        drainProfileWarnings();
        if (profile.captureEveryStep)
            capture.captureStep(columns, columnStepDirectory(ramp, columns));
    }

    private void maybeHistogram(String ramp, long step, ScaleState scale)
    {
        if (!profile.profiling.asyncProfiler || !ramp.equals(TABLES_RAMP))
            return;
        if (scale.tables - lastHistogramTables >= profile.profiling.histogramEvery)
        {
            lastHistogramTables = scale.tables;
            profiler.histogram("histogram-tables-" + scale.tables + ".txt");
        }
    }

    @Override
    protected List<Phase> definePhases()
    {
        List<Phase> phases = new ArrayList<>();
        phases.add(phase("00-baseline", this::baseline));
        phases.add(phase("01-keyspaces", phase -> {
            recorder.beginRamp("01-keyspaces");
            // a saved schema has its keyspaces already
            record(resumed != null ? recorder.checkpoint(state, true) : workload.createKeyspaces());
        }));
        if (profile.rampTables != null)
            phases.add(phase(TABLES_RAMP, this::rampTables, () -> {
                if (!profile.captureFinal)
                    histogram("histogram-02-after.txt");
            }));
        if (profile.rampColumns != null && !profile.rampColumns.createSizes.isEmpty())
            phases.add(phase("03-ramp-columns-create", phase -> record(workload.rampColumnsCreate())));
        if (profile.rampColumns != null && profile.rampColumns.alterAddMax > 1)
            phases.add(phase("04-ramp-columns-alter", phase -> record(workload.rampColumnsAlter())));
        if (profile.grid != null && !profile.grid.tables.isEmpty() && !profile.grid.columns.isEmpty())
            phases.add(phase("05-grid", this::grid));
        if (profile.restart != null && profile.restart.graceful)
            phases.add(phase("06-restart-graceful", phase -> restartNode("06-restart-graceful", "graceful")));
        if (profile.restart != null && profile.restart.kill)
            phases.add(phase("07-restart-kill", phase -> restartNode("07-restart-kill", "kill")));
        if (profile.workload != null)
            phases.add(phase("08-workload", this::dataWorkload));
        if (profile.holdSeconds > 0)
            phases.add(phase("09-steady-hold", phase -> {
                ensureRunning();
                record(workload.hold(profile.holdSeconds));
                phase.postGc(profiler.checkpoint());
            }, () -> histogram("histogram-09-after.txt")));
        phases.add(phase("10-teardown", this::teardown));
        return phases;
    }

    private Phase phase(String name, Body body)
    {
        return phase(name, body, null);
    }

    private Phase phase(String name, Body body, Runnable after)
    {
        return new Phase(name, phaseResult -> {
            if (aborted)
            {
                phaseResult.skipped().add("skipped: stop.abortOnBreak after a break point");
                return;
            }
            try
            {
                body.run(phaseResult);
            }
            catch (Throwable t)
            {
                record(BreakPoint.exception(name, recorder == null ? 0 : recorder.step(),
                                            state == null ? new ScaleState() : state, t));
            }
        }, after);
    }

    private void histogram(String fileName)
    {
        if (profile.profiling.asyncProfiler)
            profiler.histogram(fileName);
    }

    private void record(BreakPoint breakPoint)
    {
        if (breakPoint == null)
            return;
        breakPoints.add(breakPoint);
        System.out.println("BREAK POINT: " + breakPoint);
        if (profile.stop.abortOnBreak)
            aborted = true;
    }

    private void ensureRunning() throws Exception
    {
        if (!nodeRunning)
        {
            runtime.restart();
            nodeRunning = true;
        }
    }

    private void baseline(ResourceProfiler.PhaseResult phase)
    {
        runtime.probe().gc();
        profiler.nmtBaseline();
        histogram("histogram-00-baseline.txt");
        Map<String, Long> sample = runtime.probe().sample(true);
        // from a saved schema, heap_per_table counts from the heap of the empty node that made it
        Object savedBaseline = resumed == null ? null : resumed.get("baselineHeapPostGc");
        if (savedBaseline instanceof Number && ((Number) savedBaseline).longValue() > 0)
            recorder.setBaseline(Map.of("heap_post_gc", ((Number) savedBaseline).longValue()));
        else
            recorder.setBaseline(sample);
        baselineNodeTables = Math.max(0, sample.getOrDefault("node_tables", 0L) - state.tables);
        heapMax = sample.getOrDefault("heap_max", Runtime.getRuntime().maxMemory());
        directMax = sample.getOrDefault("direct_max", -1L);
        recorder.beginRamp("00-baseline");
        record(recorder.checkpoint(state, true));
        phase.postGc(profiler.checkpoint());
    }

    private void grid(ResourceProfiler.PhaseResult phase) throws Exception
    {
        ScaleProfile.Grid grid = profile.grid;
        runtime.stopGraceful();
        nodeRunning = false;
        Path gridLog = runDirectory.resolve("grid.csv");
        try (BufferedWriter out = Files.newBufferedWriter(gridLog, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
        {
            out.write("tables,columns,status,ddl_p99_ms,heap_post_gc,heap_per_table,heap_per_column,mbeans,tcm_epoch");
            out.newLine();
            for (int tables : grid.tables)
            {
                for (int columns : grid.columns)
                {
                    if (aborted)
                        return;
                    if ((long) tables * columns > grid.columnBudget)
                    {
                        System.out.printf(Locale.ROOT, "Skipping grid point %d x %d: over columnBudget %d%n",
                                          tables, columns, grid.columnBudget);
                        continue;
                    }
                    gridPoint(tables, columns, out);
                }
            }
        }
    }

    private void gridPoint(int tables, int columns, BufferedWriter out) throws Exception
    {
        Path directory = runDirectory.resolve("grid").resolve("t" + tables + "_c" + columns);
        Files.createDirectories(directory);
        BreakPoint result = null;
        Map<String, String> row = new LinkedHashMap<>();
        try (NodeRuntime node = runtimeFactory.create(directory.resolve("node"), nodeConfig))
        {
            node.start();
            try (StepRecorder steps = newRecorder(node, directory.resolve("steps.csv"), false))
            {
                node.probe().gc();
                steps.setBaseline(node.probe().sample(false));
                ScaleWorkload pointWorkload = newWorkload(node, steps, new ScaleState(), Math.max(1, spread), null);
                result = pointWorkload.gridPoint(tables, columns);
                row = steps.lastRow();
            }
        }
        catch (Throwable t)
        {
            result = BreakPoint.exception("05-grid-t" + tables + "-c" + columns, 0, new ScaleState(), t);
        }
        record(result);
        out.write(String.join(",", String.valueOf(tables), String.valueOf(columns),
                              result == null ? "ok" : result.reason.name(),
                              row.getOrDefault("ddl_p99_ms", "-1"), row.getOrDefault("heap_post_gc", "-1"),
                              row.getOrDefault("heap_per_table", "-1"), row.getOrDefault("heap_per_column", "-1"),
                              row.getOrDefault("mbeans", "-1"), row.getOrDefault("tcm_epoch", "-1")));
        out.newLine();
        out.flush();
    }

    /** Result of one stop and start cycle. */
    private static final class RestartTiming
    {
        final long startupMs;
        final RestartLogParser timings;

        RestartTiming(long startupMs, RestartLogParser timings)
        {
            this.startupMs = startupMs;
            this.timings = timings;
        }
    }

    private RestartTiming restartCycle(String kind) throws Exception
    {
        return restartCycle(kind, null);
    }

    /** Stops the node (a running node profile ends first), runs whileStopped, then starts the node again. */
    private RestartTiming restartCycle(String kind, StoppedWork whileStopped) throws Exception
    {
        if (nodeProfiles != null)
            nodeProfiles.stopNow();
        Path log = runtime.logFile();
        long offset = log != null && Files.isRegularFile(log) ? Files.size(log) : 0;
        if (nodeRunning)
        {
            if (kind.equals("kill"))
                runtime.kill();
            else
                runtime.stopGraceful();
            nodeRunning = false;
        }
        if (whileStopped != null)
            whileStopped.run();
        long startedAt = System.nanoTime();
        runtime.restart();
        long startupMs = (System.nanoTime() - startedAt) / 1_000_000L;
        nodeRunning = true;
        if (processMode())
            profiler.nmtBaseline(); // the restarted JVM has no baseline; teardown diffs against this one
        return new RestartTiming(startupMs, RestartLogParser.parse(log, offset));
    }

    // Heap after a forced GC on the restarted node; a cold start shows the footprint without DDL garbage.
    private long heapPostGcNow()
    {
        try
        {
            runtime.probe().gc();
            return runtime.probe().sample(false).getOrDefault("heap_post_gc", -1L);
        }
        catch (RuntimeException e)
        {
            profiler.warn("Could not read the heap after restart: " + e);
            return -1;
        }
    }

    private void writeRestartRow(String kind, long tables, RestartTiming timing, long heapPostGc) throws IOException
    {
        Path restartCsv = runDirectory.resolve("restart.csv");
        boolean exists = Files.exists(restartCsv);
        try (BufferedWriter out = Files.newBufferedWriter(restartCsv, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                          StandardOpenOption.APPEND, StandardOpenOption.WRITE))
        {
            if (!exists)
            {
                out.write("kind,tables,startup_ms,log_replay_ms,commitlog_replay_ms,heap_post_gc");
                out.newLine();
            }
            out.write(kind + ',' + tables + ',' + timing.startupMs + ',' + timing.timings.logReplayMs + ',' +
                      timing.timings.commitlogReplayMs + ',' + heapPostGc);
            out.newLine();
        }
    }

    private void restartNode(String ramp, String kind) throws Exception
    {
        RestartTiming timing = restartCycle(kind);
        writeRestartRow(kind, state.tables, timing, heapPostGcNow());

        recorder.beginRamp(ramp);
        record(recorder.checkpoint(state, true));
        long nodeTables = recorder.lastRow().isEmpty() ? -1 : Long.parseLong(recorder.lastRow().get("node_tables"));
        if (nodeTables >= 0 && nodeTables - baselineNodeTables != state.tables)
            record(BreakPoint.exception(ramp, recorder.step(), state,
                                        new IllegalStateException("Node reports " + (nodeTables - baselineNodeTables) +
                                                                  " user tables after " + kind + " restart, expected " +
                                                                  state.tables)));
    }

    /**
     * After each tables checkpoint, with no DDL running. Without captureAt: a snapshot at a snapshotAt count,
     * and the restart probe every restartEvery checkpoints. With captureAt: the step files, a snapshot, a
     * node profile while the DDL goes on, and the restart probe once that profile ends.
     */
    private BreakPoint afterTablesCheckpoint(int checkpoint, ScaleState scale, boolean last)
    {
        drainProfileWarnings();
        if (profile.captureAt.isEmpty())
        {
            if (profile.snapshotAt.contains((int) scale.tables))
                return saveSchema(scale);
            return last ? null : restartProbe(checkpoint, scale);
        }
        if (pendingRestart && (nodeProfiles == null || !nodeProfiles.busy()))
        {
            pendingRestart = false;
            BreakPoint stop = restartProbeNow(scale);
            if (stop != null)
                return stop;
        }
        return capturePoint(scale, last, true);
    }

    private BreakPoint capturePoint(ScaleState scale, boolean last, boolean allowSnapshot)
    {
        long tables = scale.tables;
        CaptureSchedule plan = CaptureSchedule.at(profile, tables, last, allowSnapshot, nodeProfiles != null);
        if (!plan.any())
            return null;
        Path directory = stepDirectory(tables);
        if (plan.capture)
            capture.captureStep(tables, directory);
        if (plan.snapshot)
        {
            BreakPoint stop = saveSchema(scale);
            if (stop != null)
                return stop;
        }
        boolean profiling = plan.profile && nodeProfiles.start(tables, directory);
        if (plan.capture && last && nodeProfiles != null)
            System.out.println("No node profile at " + tables + " tables: no DDL follows this count");
        if (!plan.restart)
            return null;
        if (profiling)
        {
            pendingRestart = true;
            return null;
        }
        return restartProbeNow(scale);
    }

    // Captures at the start count of a run from a saved schema, before any DDL.
    private BreakPoint captureAtStart()
    {
        if (resumed == null || profile.captureAt.isEmpty())
            return null;
        return capturePoint(state, state.tables >= profile.rampTables.max, false);
    }

    /** Stops the node, copies its data to the snapshot store, and starts it again. A copy failure only warns. */
    private BreakPoint saveSchema(ScaleState scale)
    {
        long tables = scale.tables;
        if (!processMode())
        {
            profiler.warn("snapshotAt needs --mode process; no snapshot at " + tables + " tables");
            return null;
        }
        Path target = snapshots.directory(schemaKey, tables);
        if (Files.exists(target))
        {
            profiler.warn("A saved schema exists at " + target + "; it stays and this run does not save one");
            return null;
        }
        System.out.println("Saving the schema at " + tables + " tables to " + target);
        Map<String, Object> facts = manifestFacts(scale);
        AtomicReference<Map<String, Object>> saved = new AtomicReference<>();
        try
        {
            RestartTiming timing = restartCycle("graceful", () -> {
                try
                {
                    saved.set(snapshots.save(nodeData(), schemaKey, tables, facts));
                }
                catch (Exception e)
                {
                    profiler.warn("Could not save the schema at " + tables + " tables: " + e);
                }
            });
            long heapPostGc = heapPostGcNow();
            writeRestartRow("snapshot", tables, timing, heapPostGc);
            if (saved.get() != null)
            {
                Map<String, Object> result = saved.get();
                result.put("startupMsAfterCopy", timing.startupMs);
                savedSchemas.add(result);
                System.out.printf(Locale.ROOT, "Saved %d tables in %s ms (%s), %s bytes; restart took %d ms%n", tables,
                                  result.get("copyMs"), result.get("copyMethod"), result.get("bytes"), timing.startupMs);
            }
            return checkNodeTables("02-ramp-tables-snapshot", scale);
        }
        catch (Throwable t)
        {
            return BreakPoint.exception("02-ramp-tables-snapshot", recorder.step(), scale, t);
        }
    }

    private Map<String, Object> manifestFacts(ScaleState scale)
    {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("keyspaces", scale.keyspaces);
        facts.put("columnsPerTable", ScaleWorkload.countColumns(profile.rampTables.schema));
        facts.put("tableSchema", profile.rampTables.schema);
        facts.put("cassandraVersion", FBUtilities.getReleaseVersionString());
        facts.put("gitSha", gitSha());
        facts.put("sourceRun", runDirectory.toString());
        facts.put("profile", profile.name);
        facts.put("variant", options.variant);
        facts.put("nodeConfig", nodeConfig);
        Object savedBaseline = resumed == null ? null : resumed.get("baselineHeapPostGc");
        facts.put("baselineHeapPostGc", savedBaseline instanceof Number ? savedBaseline : recorder.baselineHeap());
        if (resumed != null)
            facts.put("startFrom", resumed.get("restoredFrom"));
        Map<String, Object> ports = new LinkedHashMap<>();
        for (String key : List.of("storagePort", "sslStoragePort", "nativeTransportPort", "jmxPort"))
            ports.put(key, runtime.summary().get(key));
        facts.put("ports", ports);
        return facts;
    }

    private static String gitSha()
    {
        try
        {
            Process git = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start();
            String output = new String(git.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            if (!git.waitFor(30, TimeUnit.SECONDS) || git.exitValue() != 0)
                return "unknown";
            return output;
        }
        catch (Exception e)
        {
            return "unknown";
        }
    }

    // Graceful restart in the middle of the tables ramp, every restartEvery checkpoints. A failure ends the ramp.
    private BreakPoint restartProbe(int checkpoint, ScaleState scale)
    {
        if (profile.restartEvery <= 0 || checkpoint % profile.restartEvery != 0)
            return null;
        return restartProbeNow(scale);
    }

    private BreakPoint checkNodeTables(String ramp, ScaleState scale)
    {
        long nodeTables = runtime.probe().sample(true).getOrDefault("node_tables", -1L);
        if (nodeTables >= 0 && nodeTables - baselineNodeTables != scale.tables)
            return BreakPoint.exception(ramp, recorder.step(), scale,
                                        new IllegalStateException("Node reports " + (nodeTables - baselineNodeTables) +
                                                                  " user tables after the restart, expected " + scale.tables));
        return null;
    }

    private BreakPoint restartProbeNow(ScaleState scale)
    {
        System.out.println("Restart probe at " + scale.tables + " tables");
        try
        {
            RestartTiming timing = restartCycle("graceful");
            long heapPostGc = heapPostGcNow();
            writeRestartRow("graceful", scale.tables, timing, heapPostGc);
            System.out.printf(Locale.ROOT, "Restart probe: startup %d ms, log replay %d ms, heap after GC %d bytes%n",
                              timing.startupMs, timing.timings.logReplayMs, heapPostGc);
            return checkNodeTables("02-ramp-tables-restart", scale);
        }
        catch (Throwable t)
        {
            return BreakPoint.exception("02-ramp-tables-restart", recorder.step(), scale, t);
        }
    }

    private void rampTables(ResourceProfiler.PhaseResult phase)
    {
        BreakPoint stop;
        try
        {
            stop = captureAtStart();
            if (stop == null)
                stop = workload.rampTables();
        }
        catch (Throwable t)
        {
            stop = BreakPoint.exception(TABLES_RAMP, recorder.step(), state, t);
        }
        if (nodeProfiles != null)
            nodeProfiles.stopNow();
        drainProfileWarnings();
        if (pendingRestart)
        {
            pendingRestart = false;
            System.out.println("The ramp ended before the pending restart probe ran; it is dropped");
        }
        record(stop);
        recordStop(stop);
    }

    // Reason, table count and (when enabled) the stop artifacts, taken while the node still runs.
    private void recordStop(BreakPoint stop)
    {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("reason", stop == null ? "RAMP_MAX" : stop.reason.name());
        facts.put("tables", state.tables);
        facts.put("step", recorder.step());
        if (stop != null)
            facts.put("breakPoint", stop.toMap());
        stopFacts = facts;
        if (!profile.captureFinal)
            return;
        long heapHint = -1;
        try
        {
            Map<String, Long> sample = runtime.probe().sample(false);
            heapHint = Math.max(sample.getOrDefault("heap_used", -1L), sample.getOrDefault("heap_post_gc", -1L));
        }
        catch (RuntimeException e)
        {
            profiler.warn("Could not sample the node before the final capture: " + e);
        }
        facts.put("finalCapture", capture.captureFinal(runDirectory.resolve("final"), heapHint,
                                                             profile.heapDumpAt(stop == null ? null : stop.reason)));
    }

    private void dataWorkload(ResourceProfiler.PhaseResult phase) throws Exception
    {
        ensureRunning();
        for (BreakPoint breakPoint : workload.runWorkload())
            record(breakPoint);
    }

    private void teardown(ResourceProfiler.PhaseResult phase)
    {
        profiler.nmtSummary("nmt-end.txt");
        profiler.nmtDiff("nmt-diff.txt");
        Path log = runtime.logFile();
        if (log == null || !Files.isRegularFile(log))
        {
            profiler.warn("No node system.log to copy.");
            return;
        }
        try
        {
            Files.copy(log, runDirectory.resolve("system.log"), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            profiler.warn("Could not copy the node system.log: " + e);
        }
    }

    @Override
    protected Map<String, Object> runParameters()
    {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("profile", profile.name);
        parameters.put("variant", options.variant);
        parameters.put("keyspaceSpread", spread);
        parameters.put("mode", options.mode);
        parameters.put("nodeConfig", nodeConfig);
        parameters.put("overrides", options.overrides);
        parameters.put("heapMax", heapMax >= 0 ? heapMax : Runtime.getRuntime().maxMemory());
        parameters.put("directMax", directMax);
        parameters.put("rssLimit", rssLimit());
        parameters.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
        List<Map<String, Object>> breaks = new ArrayList<>();
        for (BreakPoint breakPoint : breakPoints)
            breaks.add(breakPoint.toMap());
        parameters.put("breakPoints", breaks);
        parameters.put("captureEveryStep", profile.captureEveryStep);
        parameters.put("restartEvery", profile.restartEvery);
        parameters.put("captureAt", profile.captureAt);
        parameters.put("snapshotAt", profile.snapshotAt);
        parameters.put("ddlConcurrency", profile.rampTables == null ? 1 : profile.rampTables.ddlConcurrency);
        parameters.put("schemaKey", schemaKey);
        parameters.put("savedSchemas", savedSchemas);
        if (resumed != null)
            parameters.put("startFrom", resumed);
        Map<String, Object> profilerPath = nodeProfiles != null ? nodeProfiles.path() : new LinkedHashMap<>();
        if (nodeProfiles == null)
        {
            profilerPath.put("requested", profile.profiling.nodeProfiler);
            profilerPath.put("loadPath", "none");
        }
        parameters.put("nodeProfilerPath", profilerPath);
        if (nodeProfiles != null)
            parameters.put("nodeProfiles", nodeProfiles.results());
        if (stopFacts != null)
        {
            parameters.put("stopReason", stopFacts.get("reason"));
            parameters.put("tablesAtStop", stopFacts.get("tables"));
            parameters.put("stop", stopFacts);
        }
        if (processMode() && runtime != null)
        {
            parameters.put("node", new LinkedHashMap<>(runtime.summary()));
            parameters.put("probeNotes", List.of("snapshot_bytes_computed is snapshot_bytes_gauge (no JMX source)",
                                                 "node_tables is read only on samples that compute the snapshot size",
                                                 "heap numbers in the phase table come from the node JVM"));
        }
        return parameters;
    }

    // Bytes when the launcher passes a number, otherwise the raw text, or null when unset.
    private static Object rssLimit()
    {
        String text = System.getProperty("schemascale.rssLimit"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        if (text == null)
            return null;
        try
        {
            return Long.parseLong(text.trim());
        }
        catch (NumberFormatException e)
        {
            return text;
        }
    }

    @Override
    protected void writeSummaryDetails(BufferedWriter writer) throws IOException
    {
        writer.write("Profile: " + profile.name + ", variant: " + options.variant + ", keyspace spread: " + spread +
                     ", mode: " + options.mode);
        writer.newLine();
        writer.write("Scale reached: " + (state == null ? "none" : state.tables + " tables, " + state.columns + " columns"));
        writer.newLine();
        writer.write("Break points: " + (breakPoints.isEmpty() ? "none" : breakPoints.size()));
        writer.newLine();
        for (BreakPoint breakPoint : breakPoints)
        {
            writer.write("  " + breakPoint);
            writer.newLine();
        }
    }

    @Override
    protected String profiledArtifactNote()
    {
        return "steps.csv, columns.csv, restart.csv, workload.csv, grid.csv, histogram-*.txt, nmt-*.txt, steps/t*/ (node-*.jfr), final/";
    }
}
