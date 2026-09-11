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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import com.sun.management.ThreadMXBean;

import org.apache.cassandra.service.StartupChecks;

import jdk.jfr.Recording;
import one.profiler.AsyncProfiler;

/**
 * Instrumentation core for resource-profiling harnesses: async-profiler sessions (one multi-event JFR
 * session per phase plus optional single-event HTML windows), JDK Flight Recorder recording, heap and
 * allocation checkpoints, self jcmd class histograms, warning collection, and artifact size tracking.
 * Callers name phases and artifacts; this class has no knowledge of what the phases do.
 */
public final class ResourceProfiler
{
    private static final String CPU_REMEDIATION = "Try 'sysctl kernel.perf_event_paranoid=1' and " +
                                                  "'sysctl kernel.kptr_restrict=0'.";

    private final Path outputDirectory;
    private final boolean profileEnabled;
    private final List<PhaseResult> phases = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final long mainThreadId = Thread.currentThread().getId();
    private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private ThreadMXBean threadMXBean;
    private Recording recording;
    private AsyncProfiler asyncProfiler;
    private boolean asyncProfilerInitializationAttempted;
    private Path activeAsyncProfile;
    private boolean cpuAvailable;
    private Integer perfEventParanoid;
    private Integer kptrRestrict;
    private String cpuSkipReason;

    public ResourceProfiler(Path outputDirectory, boolean profileEnabled)
    {
        this.outputDirectory = outputDirectory;
        this.profileEnabled = profileEnabled;
    }

    public void warn(String warning)
    {
        warnings.add(warning);
        System.err.println("WARNING: " + warning);
    }

    public List<PhaseResult> phases()
    {
        return phases;
    }

    public List<String> warnings()
    {
        return warnings;
    }

    public boolean cpuAvailable()
    {
        return cpuAvailable;
    }

    public Integer perfEventParanoid()
    {
        return perfEventParanoid;
    }

    public Integer kptrRestrict()
    {
        return kptrRestrict;
    }

    public String cpuSkipReason()
    {
        return cpuSkipReason;
    }

    public void enableAllocationTracking()
    {
        java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        if (!(platformBean instanceof ThreadMXBean))
        {
            warn("Thread allocation counters are unavailable on this JVM.");
            return;
        }

        threadMXBean = (ThreadMXBean) platformBean;
        if (!threadMXBean.isThreadAllocatedMemorySupported())
        {
            warn("Thread allocation counters are not supported on this JVM.");
            threadMXBean = null;
            return;
        }

        if (!threadMXBean.isThreadAllocatedMemoryEnabled())
            threadMXBean.setThreadAllocatedMemoryEnabled(true);
    }

    public void probeCpu(boolean skipCpu)
    {
        if (!profileEnabled)
        {
            cpuSkipReason = "Profiling disabled by --no-profile.";
            return;
        }
        if (skipCpu)
        {
            cpuSkipReason = "CPU profiling disabled by --skip-cpu.";
            warn(cpuSkipReason);
            return;
        }

        try
        {
            StartupChecks.AsyncProfilerKernelParamsCheck check = new StartupChecks.AsyncProfilerKernelParamsCheck();
            perfEventParanoid = check.readPerfEventParanoid();
            kptrRestrict = check.readKptrRestrict();
            cpuAvailable = check.hasCorrectKernelParams();
            if (!cpuAvailable)
                cpuSkipReason = "Kernel parameters do not permit CPU profiling. " + CPU_REMEDIATION;
        }
        catch (Throwable t)
        {
            cpuSkipReason = "Could not check kernel parameters for CPU profiling: " + t;
        }

        if (cpuSkipReason != null)
            warn(cpuSkipReason);
    }

    public void startJdkRecording()
    {
        if (!profileEnabled)
            return;

        try
        {
            recording = new Recording();
            recording.enable("jdk.GCHeapSummary").withPeriod(Duration.ofSeconds(1));
            recording.enable("jdk.MetaspaceSummary").withPeriod(Duration.ofSeconds(5));
            recording.enable("jdk.GCPhasePause").withThreshold(Duration.ZERO);
            recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
            recording.enable("jdk.NativeMethodSample").withPeriod(Duration.ofMillis(10));
            try
            {
                recording.enable("jdk.ObjectAllocationSample").withPeriod(Duration.ofMillis(10));
            }
            catch (Throwable t)
            {
                warn("jdk.ObjectAllocationSample is unavailable on this JVM: " + t);
            }
            recording.start();
        }
        catch (Throwable t)
        {
            recording = null;
            warn("JDK Flight Recorder is unavailable: " + t);
        }
    }

    public void startSession(String phaseName, PhaseResult phase)
    {
        if (!profileEnabled)
        {
            phase.skipped.add("async-profiler: disabled by --no-profile");
            return;
        }

        Path jfr = outputDirectory.resolve(phaseName + ".ap.jfr");
        String events = cpuAvailable ? "event=alloc,wall,cpu" : "event=alloc,wall";
        if (ap("start," + events + ",file=" + jfr, phase) == null)
        {
            phase.skipped.add("async-profiler: could not start");
            return;
        }
        activeAsyncProfile = jfr;
    }

    public void stopSession(PhaseResult phase)
    {
        if (activeAsyncProfile == null)
            return;

        Path jfr = activeAsyncProfile;
        activeAsyncProfile = null;
        ap("stop,file=" + jfr, phase);
        checkArtifact(phase, jfr);
    }

    // Single-event HTML window for optional dedicated extra passes; the many-tables harness derives
    // HTML post-run from each phase's multi-event JFR instead.
    public void htmlWindow(String phaseName, String eventSpec, String eventName, PhaseResult phase, PhaseBody body) throws Throwable
    {
        Path html = outputDirectory.resolve(phaseName + "-" + eventName + ".html");
        boolean started = ap("start," + eventSpec + ",file=" + html, phase) != null;
        if (!started)
            phase.skipped.add(html.getFileName() + ": could not start");
        try
        {
            body.run(phase);
        }
        finally
        {
            if (started)
            {
                ap("stop,file=" + html, phase);
                checkArtifact(phase, html);
            }
        }
    }

    public void dumpJdkRecording(String phaseName, PhaseResult phase)
    {
        if (recording == null)
        {
            if (!profileEnabled)
                phase.skipped.add("JDK Flight Recorder: unavailable");
            return;
        }

        Path output = outputDirectory.resolve(phaseName + ".jdk.jfr");
        try
        {
            recording.dump(output);
        }
        catch (Throwable t)
        {
            String message = "Could not dump JDK Flight Recorder data for " + phaseName + ": " + t;
            phase.errors.add(message);
            warn(message);
        }
    }

    public HeapSnapshot checkpoint()
    {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long allocated = threadMXBean == null ? -1 : threadMXBean.getThreadAllocatedBytes(mainThreadId);
        return new HeapSnapshot(heap.getUsed(), heap.getCommitted(), allocated, mBeanServer.getMBeanCount());
    }

    public void histogram(String fileName)
    {
        Path output = outputDirectory.resolve(fileName);
        Process process;
        try
        {
            process = new ProcessBuilder("jcmd", Long.toString(ProcessHandle.current().pid()), "GC.class_histogram")
                      .redirectErrorStream(true)
                      .redirectOutput(output.toFile())
                      .start();
        }
        catch (IOException e)
        {
            warn("Could not start jcmd for " + fileName + ": " + e);
            return;
        }

        try
        {
            if (!process.waitFor(30, TimeUnit.SECONDS))
            {
                process.destroyForcibly();
                warn("jcmd timed out for " + fileName);
            }
            else if (process.exitValue() != 0)
            {
                warn("jcmd exited with status " + process.exitValue() + " for " + fileName);
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            warn("jcmd was interrupted for " + fileName);
        }
    }

    public void cleanupAsyncProfiler()
    {
        if (activeAsyncProfile == null)
            return;

        try
        {
            asyncProfiler.execute("stop,file=" + activeAsyncProfile);
        }
        catch (Throwable t)
        {
            warn("Could not stop async-profiler during cleanup: " + t);
        }
        activeAsyncProfile = null;
    }

    public void cleanupRecording()
    {
        if (recording != null)
        {
            try
            {
                recording.stop();
                recording.close();
            }
            catch (Throwable t)
            {
                warn("Could not stop JDK Flight Recorder during cleanup: " + t);
            }
            recording = null;
        }
    }

    private void checkArtifact(PhaseResult phase, Path artifact)
    {
        if (!isNonEmpty(artifact))
            phase.errors.add("async-profiler output is missing or empty: " + artifact.getFileName());
    }

    private String ap(String command, PhaseResult phase)
    {
        AsyncProfiler instance = asyncProfilerInstance(phase);
        if (instance == null)
            return null;

        try
        {
            return instance.execute(command);
        }
        catch (Throwable t)
        {
            String message = "async-profiler command failed ('" + command + "'): " + t;
            phase.errors.add(message);
            warn(message);
            return null;
        }
    }

    private AsyncProfiler asyncProfilerInstance(PhaseResult phase)
    {
        if (asyncProfiler != null)
            return asyncProfiler;
        if (asyncProfilerInitializationAttempted)
            return null;

        asyncProfilerInitializationAttempted = true;
        try
        {
            asyncProfiler = AsyncProfiler.getInstance();
            return asyncProfiler;
        }
        catch (Throwable t)
        {
            String message = "async-profiler is unavailable: " + t;
            phase.skipped.add(message);
            warn(message);
            return null;
        }
    }

    private static boolean isNonEmpty(Path path)
    {
        try
        {
            return Files.isRegularFile(path) && Files.size(path) > 0;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    interface PhaseBody
    {
        void run(PhaseResult phase) throws Throwable;
    }

    static final class PhaseResult
    {
        final String name;
        final List<String> skipped = new ArrayList<>();
        final List<String> errors = new ArrayList<>();
        final Map<String, Long> artifacts = new LinkedHashMap<>();
        HeapSnapshot before;
        HeapSnapshot after;
        HeapSnapshot postGc;
        long elapsedNanos;

        PhaseResult(String name)
        {
            this.name = name;
        }

        long allocatedBytesDelta()
        {
            if (before.threadAllocatedBytes < 0 || after.threadAllocatedBytes < 0)
                return -1;
            return after.threadAllocatedBytes - before.threadAllocatedBytes;
        }

        void captureArtifacts(Path runDirectory)
        {
            String[] suffixes = { ".ap.jfr", ".jdk.jfr", "-alloc.html", "-wall.html", "-cpu.html" };
            for (String suffix : suffixes)
            {
                Path artifact = runDirectory.resolve(name + suffix);
                if (!Files.isRegularFile(artifact))
                    continue;
                try
                {
                    artifacts.put(artifact.getFileName().toString(), Files.size(artifact));
                }
                catch (IOException e)
                {
                    artifacts.put(artifact.getFileName().toString(), -1L);
                }
            }
        }

        Map<String, Object> toMap()
        {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("name", name);
            values.put("elapsedNanos", elapsedNanos);
            values.put("allocatedBytesDelta", allocatedBytesDelta());
            values.put("before", before.toMap());
            values.put("after", after.toMap());
            values.put("postGc", postGc == null ? null : postGc.toMap());
            values.put("mBeanCountDelta", after.mBeanCount - before.mBeanCount);
            values.put("artifacts", artifacts);
            values.put("skipped", skipped);
            values.put("errors", errors);
            return values;
        }
    }

    static final class HeapSnapshot
    {
        final long heapUsed;
        final long heapCommitted;
        final long threadAllocatedBytes;
        final int mBeanCount;

        private HeapSnapshot(long heapUsed, long heapCommitted, long threadAllocatedBytes, int mBeanCount)
        {
            this.heapUsed = heapUsed;
            this.heapCommitted = heapCommitted;
            this.threadAllocatedBytes = threadAllocatedBytes;
            this.mBeanCount = mBeanCount;
        }

        private Map<String, Object> toMap()
        {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("heapUsed", heapUsed);
            values.put("heapCommitted", heapCommitted);
            values.put("threadAllocatedBytes", threadAllocatedBytes);
            values.put("mBeanCount", mBeanCount);
            return values;
        }
    }
}
