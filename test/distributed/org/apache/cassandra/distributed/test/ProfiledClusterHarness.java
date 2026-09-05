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

package org.apache.cassandra.distributed.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.utils.JsonUtils;

/**
 * Shared driver for resource-profiling harnesses: builds a one-node in-JVM cluster, runs ordered phases
 * around {@link ResourceProfiler} instrumentation, mirrors console output into the run directory, and
 * writes summary.txt/summary.json. Subclasses declare phases and run-specific flags, fields, and naming.
 */
public abstract class ProfiledClusterHarness
{
    private static final DateTimeFormatter RUN_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final Path outputDirectory;
    private final String runTitle;
    private final String summaryTitle;
    private final String runDirectorySuffix;
    private final String[] originalArgs;
    private final boolean noProfile;
    private final boolean skipCpu;

    protected ResourceProfiler profiler;
    protected Path runDirectory;
    protected Cluster cluster;

    protected ProfiledClusterHarness(Path outputDirectory, String runTitle, String summaryTitle,
                                     String runDirectorySuffix, String[] originalArgs, boolean noProfile,
                                     boolean skipCpu)
    {
        this.outputDirectory = outputDirectory;
        this.runTitle = runTitle;
        this.summaryTitle = summaryTitle;
        this.runDirectorySuffix = runDirectorySuffix;
        this.originalArgs = originalArgs;
        this.noProfile = noProfile;
        this.skipCpu = skipCpu;
    }

    /** Ordered phases to run; bodies receive the per-phase record and may checkpoint via the profiler. */
    protected abstract List<Phase> definePhases();

    /** Run-specific parameters for summary.json; insertion order is preserved in the file. */
    protected abstract Map<String, Object> runParameters();

    /** Suffix describing the profiled artifact layout for the summary "Artifacts:" line. */
    protected abstract String profiledArtifactNote();

    /** Hook invoked before the cluster starts. */
    protected void preCluster() { }

    /** Configure cluster provisioning before creating the nodes. */
    protected void configureCluster(Cluster.Builder builder) { }

    /** Configure the node before it starts. */
    protected void configureNode(IInstanceConfig instanceConfig) { }

    /** Hook invoked after the cluster starts, before the first phase. */
    protected void postCluster() { }

    /** Run-specific line (with newline) written to summary.txt after the JDK line. */
    protected void writeSummaryDetails(BufferedWriter writer) throws IOException { }

    /** Run-specific note written under a phase's artifacts line in summary.txt, or null. */
    protected String phaseNote(String phaseName)
    {
        return null;
    }

    public final void execute() throws Throwable
    {
        runDirectory = outputDirectory.toAbsolutePath()
                                      .resolve(RUN_TIMESTAMP.format(LocalDateTime.now()) + "-" + runDirectorySuffix);
        Files.createDirectories(runDirectory);

        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        OutputStream consoleFile = Files.newOutputStream(runDirectory.resolve("console.txt"),
                                                         StandardOpenOption.CREATE_NEW,
                                                         StandardOpenOption.WRITE);
        Object consoleLock = new Object();
        System.setOut(new PrintStream(new TeeOutputStream(originalOut, consoleFile, consoleLock), true,
                                      StandardCharsets.UTF_8.name()));
        System.setErr(new PrintStream(new TeeOutputStream(originalErr, consoleFile, consoleLock), true,
                                      StandardCharsets.UTF_8.name()));

        profiler = new ResourceProfiler(runDirectory, !noProfile);
        Throwable failure = null;
        try
        {
            System.out.println(runTitle + ": " + runDirectory);
            runPhases();
        }
        catch (Throwable t)
        {
            failure = t;
            profiler.warn("Fatal error: " + t);
            t.printStackTrace(System.err);
        }
        finally
        {
            cleanup();
            try
            {
                writeSummaries(failure);
            }
            catch (Throwable summaryFailure)
            {
                summaryFailure.printStackTrace(System.err);
                if (failure == null)
                    failure = summaryFailure;
            }

            System.out.flush();
            System.err.flush();
            System.setOut(originalOut);
            System.setErr(originalErr);
            consoleFile.close();
        }

        if (failure != null)
            throw failure;
    }

    private void runPhases() throws Throwable
    {
        CassandraRelevantProperties.ASYNC_PROFILER_ENABLED.setBoolean(true);
        profiler.enableAllocationTracking();
        profiler.probeCpu(skipCpu);
        profiler.startJdkRecording();

        preCluster();
        Cluster.Builder builder = Cluster.build(1);
        configureCluster(builder);
        cluster = builder.withConfig(c -> {
            c.with(Feature.values());
            configureNode(c);
        }).start();
        postCluster();

        for (Phase phase : definePhases())
            runPhase(phase);
    }

    private void runPhase(Phase phase) throws Throwable
    {
        System.out.println("Starting phase " + phase.name);
        ResourceProfiler.PhaseResult phaseResult = new ResourceProfiler.PhaseResult(phase.name);
        phaseResult.before = profiler.checkpoint();
        long startedAt = System.nanoTime();
        if (phase.profileSession)
        {
            if (!noProfile && !profiler.cpuAvailable() && profiler.cpuSkipReason() != null)
                phaseResult.skipped.add("cpu: " + profiler.cpuSkipReason());
            profiler.startSession(phase.name, phaseResult);
        }

        Throwable failure = null;
        try
        {
            phase.body.run(phaseResult);
        }
        catch (Throwable t)
        {
            failure = t;
            phaseResult.errors.add(t.toString());
        }
        finally
        {
            if (phase.profileSession)
                profiler.stopSession(phaseResult);
            profiler.dumpJdkRecording(phase.name, phaseResult);
            phaseResult.elapsedNanos = System.nanoTime() - startedAt;
            phaseResult.after = profiler.checkpoint();
            phaseResult.captureArtifacts(runDirectory);
            profiler.phases().add(phaseResult);
            System.out.printf(Locale.ROOT, "Finished phase %s in %.3f s%n",
                              phase.name, phaseResult.elapsedNanos / 1_000_000_000.0d);
        }

        if (failure != null)
            throw failure;

        if (phase.after != null)
            phase.after.run();
    }

    private void cleanup()
    {
        profiler.cleanupAsyncProfiler();

        if (cluster != null)
        {
            try
            {
                cluster.close();
            }
            catch (Throwable t)
            {
                profiler.warn("Could not close the cluster during cleanup: " + t);
            }
            cluster = null;
        }

        profiler.cleanupRecording();
    }

    private void writeSummaries(Throwable failure) throws IOException
    {
        writeTextSummary(failure);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("args", Arrays.asList(originalArgs));
        summary.put("jdkVersion", CassandraRelevantProperties.JAVA_VERSION.getString());
        summary.put("runDirectory", runDirectory.toString());
        summary.putAll(runParameters());
        summary.put("profileEnabled", !noProfile);
        summary.put("perfEventParanoid", profiler.perfEventParanoid());
        summary.put("kptrRestrict", profiler.kptrRestrict());
        summary.put("cpuAvailable", profiler.cpuAvailable());
        summary.put("cpuSkipReason", profiler.cpuSkipReason());
        summary.put("failure", failure == null ? null : failure.toString());
        summary.put("warnings", profiler.warnings());
        List<Map<String, Object>> phaseMaps = new ArrayList<>();
        for (ResourceProfiler.PhaseResult phase : profiler.phases())
            phaseMaps.add(phase.toMap());
        summary.put("phases", phaseMaps);

        JsonUtils.JSON_OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                    .writeValue(runDirectory.resolve("summary.json").toFile(), summary);
    }

    private void writeTextSummary(Throwable failure) throws IOException
    {
        try (BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve("summary.txt"),
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE,
                                                             StandardOpenOption.TRUNCATE_EXISTING,
                                                             StandardOpenOption.WRITE))
        {
            writer.write(summaryTitle);
            writer.newLine();
            writer.write("Run directory: " + runDirectory);
            writer.newLine();
            writer.write("JDK: " + CassandraRelevantProperties.JAVA_VERSION.getString());
            writer.newLine();
            writeSummaryDetails(writer);
            writer.write("Kernel: perf_event_paranoid=" + profiler.perfEventParanoid() +
                         ", kptr_restrict=" + profiler.kptrRestrict());
            writer.newLine();
            writer.write("CPU profiling: " + (profiler.cpuAvailable() ? "enabled" : "skipped: " + profiler.cpuSkipReason()));
            writer.newLine();
            String artifacts = noProfile
                               ? "<phase>.jdk.jfr only (async-profiler disabled)"
                               : "<phase>.jdk.jfr and <phase>.ap.jfr per phase; " + profiledArtifactNote();
            writer.write("Artifacts: " + artifacts);
            writer.newLine();
            writer.write("Status: " + (failure == null ? "success" : "failed: " + failure));
            writer.newLine();
            writer.newLine();
            writer.write("phase\telapsed_s\tallocated_bytes\theap_before\theap_after\theap_post_gc\tmbeans_delta");
            writer.newLine();
            for (ResourceProfiler.PhaseResult phase : profiler.phases())
            {
                writer.write(String.format(Locale.ROOT, "%s\t%.3f\t%d\t%d\t%d\t%d\t%d",
                                           phase.name,
                                           phase.elapsedNanos / 1_000_000_000.0d,
                                           phase.allocatedBytesDelta(),
                                           phase.before.heapUsed,
                                           phase.after.heapUsed,
                                           phase.postGc == null ? -1 : phase.postGc.heapUsed,
                                           phase.after.mBeanCount - phase.before.mBeanCount));
                writer.newLine();
                writer.write("  artifacts: " + phase.artifacts);
                writer.newLine();
                String note = phaseNote(phase.name);
                if (note != null)
                {
                    writer.write("  " + note);
                    writer.newLine();
                }
                if (!phase.skipped.isEmpty())
                {
                    writer.write("  skipped: " + phase.skipped);
                    writer.newLine();
                }
                if (!phase.errors.isEmpty())
                {
                    writer.write("  errors: " + phase.errors);
                    writer.newLine();
                }
            }
            if (!profiler.warnings().isEmpty())
            {
                writer.newLine();
                writer.write("Warnings:");
                writer.newLine();
                for (String warning : profiler.warnings())
                {
                    writer.write("- " + warning);
                    writer.newLine();
                }
            }
        }
    }

    protected static final class Phase
    {
        final String name;
        final boolean profileSession;
        final ResourceProfiler.PhaseBody body;
        final Runnable after;

        protected Phase(String name, ResourceProfiler.PhaseBody body)
        {
            this(name, true, body, null);
        }

        protected Phase(String name, ResourceProfiler.PhaseBody body, Runnable after)
        {
            this(name, true, body, after);
        }

        protected Phase(String name, boolean profileSession, ResourceProfiler.PhaseBody body, Runnable after)
        {
            this.name = name;
            this.profileSession = profileSession;
            this.body = body;
            this.after = after;
        }
    }

    private static final class TeeOutputStream extends OutputStream
    {
        private final OutputStream console;
        private final OutputStream file;
        private final Object lock;

        private TeeOutputStream(OutputStream console, OutputStream file, Object lock)
        {
            this.console = console;
            this.file = file;
            this.lock = lock;
        }

        @Override
        public void write(int value) throws IOException
        {
            synchronized (lock)
            {
                console.write(value);
                file.write(value);
            }
        }

        @Override
        public void write(byte[] values, int offset, int length) throws IOException
        {
            synchronized (lock)
            {
                console.write(values, offset, length);
                file.write(values, offset, length);
            }
        }

        @Override
        public void flush() throws IOException
        {
            synchronized (lock)
            {
                console.flush();
                file.flush();
            }
        }
    }
}
