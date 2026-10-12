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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link StepCapture} that runs the jcmd of the configured Java home against a process id. The id comes
 * from a supplier, so it follows a restarted node. Every command has a timeout.
 */
abstract class JcmdStepCapture implements StepCapture
{
    static final long ASSUMED_HEAP_BYTES = 32L << 30;
    static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(15);
    static final Duration DUMP_TIMEOUT = Duration.ofHours(3);
    private static final int HEAP_DUMP_THREADS = 4;
    private static final Pattern THREAD_NAME = Pattern.compile("^\"(.*)\"\\s+(?:#\\d+|os_prio=|prio=|tid=|\\[).*");
    private static final Pattern TRAILING_NUMBER = Pattern.compile("(?:[-_:# ]*\\d+)+$");

    private final Path javaHome;
    private final LongSupplier pid;
    private final Consumer<String> warnings;
    private final Supplier<LocalDateTime> clock;
    private final Duration commandTimeout;
    private final Duration dumpTimeout;

    private long windowPid = -1;
    private LocalDateTime windowStart;
    private boolean flightNoteLogged;

    JcmdStepCapture(Path javaHome, LongSupplier pid, Consumer<String> warnings, Supplier<LocalDateTime> clock,
                    Duration commandTimeout, Duration dumpTimeout)
    {
        this.javaHome = javaHome;
        this.pid = pid;
        this.warnings = warnings;
        this.clock = clock;
        this.commandTimeout = commandTimeout;
        this.dumpTimeout = dumpTimeout;
    }

    /** True when the JVM runs a flight recording that holds node data only. */
    abstract boolean recordsFlights();

    /** Logged once when {@link #recordsFlights} is false. */
    abstract String flightNote();

    @Override
    public long liveHeapBytes(Path scratch)
    {
        long node = pid.getAsLong();
        if (!alive(node) || !run(node, commandTimeout, output(scratch), "GC.class_histogram"))
            return -1;
        return StepCapture.histogramTotal(scratch);
    }

    @Override
    public void captureStep(long tables, Path directory)
    {
        long startedAt = System.nanoTime();
        long node = pid.getAsLong();
        if (!prepare(directory) || !alive(node))
        {
            warnings.accept("Step capture for " + tables + " tables skipped: process " + node + " is not running");
            return;
        }
        run(node, commandTimeout, output(directory.resolve("class-histogram.txt")), "GC.class_histogram");
        run(node, commandTimeout, output(directory.resolve("heap-info.txt")), "GC.heap_info");
        run(node, commandTimeout, output(directory.resolve("nmt-summary.txt")), "VM.native_memory", "summary");
        threads(node, directory.resolve("threads.txt"), directory.resolve("threads.raw"));
        if (recordsFlights())
            flightWindow(node, directory);
        else if (!flightNoteLogged)
        {
            flightNoteLogged = true;
            System.out.println("Step capture: " + flightNote());
        }
        System.out.printf(Locale.ROOT, "Captured step files for %d tables in %.1f s: %s%n", tables,
                          (System.nanoTime() - startedAt) / 1e9, directory);
    }

    @Override
    public Map<String, Object> captureFinal(Path directory, long heapUsedHint, boolean heapDump)
    {
        Map<String, Object> facts = new LinkedHashMap<>();
        long node = pid.getAsLong();
        boolean alive = alive(node);
        facts.put("pid", node);
        facts.put("nodeAlive", alive);
        if (!prepare(directory))
        {
            facts.put("error", "could not create " + directory);
            return facts;
        }
        if (!alive)
        {
            facts.put("note", "The node was not running at the stop. Use the HeapDumpOnOutOfMemoryError output in the run directory.");
            facts.put("oomDumps", oomDumps(directory.getParent()));
            warnings.accept("Final capture: node process " + node + " is not running; nothing to dump");
            return facts;
        }
        artifact(facts, "class-histogram.txt", directory, () ->
            run(node, commandTimeout, output(directory.resolve("class-histogram.txt")), "GC.class_histogram"));
        artifact(facts, "thread-dump.txt", directory, () ->
            run(node, commandTimeout, output(directory.resolve("thread-dump.txt")), "Thread.print"));
        artifact(facts, "nmt-diff.txt", directory, () ->
            run(node, commandTimeout, output(directory.resolve("nmt-diff.txt")), "VM.native_memory", "summary.diff"));
        if (recordsFlights())
            artifact(facts, "node.jfr", directory, () ->
                run(node, commandTimeout, log(directory), "JFR.dump", "filename=" + directory.resolve("node.jfr").toAbsolutePath()));
        else
            facts.put("node.jfr", flightNote());
        if (heapDump)
            heapDump(facts, node, directory, heapUsedHint);
        else
            facts.put("heap.hprof", "skipped: finalHeapDump");
        return facts;
    }

    private void heapDump(Map<String, Object> facts, long node, Path directory, long heapUsedHint)
    {
        Path dump = directory.resolve("heap.hprof").toAbsolutePath();
        long need = 2 * (heapUsedHint > 0 ? heapUsedHint : ASSUMED_HEAP_BYTES);
        long free;
        try
        {
            free = Files.getFileStore(directory).getUsableSpace();
            Files.deleteIfExists(dump);
        }
        catch (IOException e)
        {
            warnings.accept("Heap dump skipped: cannot read the free disk space of " + directory + ": " + e);
            facts.put("heap.hprof", "skipped: " + e);
            return;
        }
        if (free < need)
        {
            String reason = "skipped: " + free + " bytes free, need " + need;
            warnings.accept("Heap dump " + reason);
            facts.put("heap.hprof", reason);
            return;
        }
        System.out.println("Heap dump of process " + node + " to " + dump + " (live objects, " + free + " bytes free)");
        long startedAt = System.nanoTime();
        boolean ok = run(node, dumpTimeout, log(directory), "GC.heap_dump", "-parallel=" + HEAP_DUMP_THREADS, dump.toString());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        long bytes = size(dump);
        System.out.printf(Locale.ROOT, "Heap dump %s in %.1f s, %d bytes%n", ok ? "finished" : "FAILED", elapsedMs / 1e3, bytes);
        facts.put("heap.hprof", artifactFacts(ok, elapsedMs, bytes));
    }

    private void artifact(Map<String, Object> facts, String name, Path directory, BooleanSupplier body)
    {
        long startedAt = System.nanoTime();
        boolean ok = body.getAsBoolean();
        facts.put(name, artifactFacts(ok, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt), size(directory.resolve(name))));
    }

    private static Map<String, Object> artifactFacts(boolean ok, long elapsedMs, long bytes)
    {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("ok", ok);
        facts.put("elapsedMs", elapsedMs);
        facts.put("bytes", bytes);
        return facts;
    }

    private void flightWindow(long node, Path directory)
    {
        LocalDateTime end = clock.get().truncatedTo(ChronoUnit.SECONDS);
        if (node != windowPid)
        {
            windowPid = node;
            windowStart = null;
        }
        List<String> args = new ArrayList<>();
        args.add("JFR.dump");
        args.add("filename=" + directory.resolve("node.jfr").toAbsolutePath());
        if (windowStart != null)
        {
            if (!end.isAfter(windowStart))
                return;
            args.add("begin=" + windowStart);
        }
        args.add("end=" + end);
        if (run(node, commandTimeout, log(directory), args.toArray(new String[0])))
            windowStart = end;
    }

    private void threads(long node, Path summary, Path raw)
    {
        if (!run(node, commandTimeout, output(raw), "Thread.print"))
            return;
        try
        {
            List<String> lines = Files.readAllLines(raw, StandardCharsets.ISO_8859_1);
            Map<String, Integer> counts = threadCounts(lines);
            int total = 0;
            for (int count : counts.values())
                total += count;
            List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
            sorted.sort((a, b) -> {
                int byCount = Integer.compare(b.getValue(), a.getValue());
                return byCount != 0 ? byCount : a.getKey().compareTo(b.getKey());
            });
            StringBuilder text = new StringBuilder("total ").append(total).append('\n');
            for (Map.Entry<String, Integer> entry : sorted)
                text.append(entry.getValue()).append(' ').append(entry.getKey()).append('\n');
            Files.writeString(summary, text, StandardCharsets.UTF_8);
        }
        catch (IOException | RuntimeException e)
        {
            warnings.accept("Could not summarize threads: " + e);
        }
        finally
        {
            try
            {
                Files.deleteIfExists(raw);
            }
            catch (IOException e)
            {
                // a leftover temp file does no harm
            }
        }
    }

    /** Counts the threads of a Thread.print output by name with the trailing numbers removed. */
    static Map<String, Integer> threadCounts(List<String> lines)
    {
        Map<String, Integer> counts = new TreeMap<>();
        for (String line : lines)
        {
            Matcher matcher = THREAD_NAME.matcher(line);
            if (!matcher.matches())
                continue;
            String name = matcher.group(1);
            String prefix = TRAILING_NUMBER.matcher(name).replaceFirst("");
            counts.merge(prefix.isEmpty() ? name : prefix, 1, Integer::sum);
        }
        return counts;
    }

    private boolean prepare(Path directory)
    {
        try
        {
            Files.createDirectories(directory);
            return true;
        }
        catch (IOException e)
        {
            warnings.accept("Could not create " + directory + ": " + e);
            return false;
        }
    }

    private static boolean alive(long node)
    {
        return node > 0 && ProcessHandle.of(node).map(ProcessHandle::isAlive).orElse(false);
    }

    private static ProcessBuilder.Redirect output(Path file)
    {
        return ProcessBuilder.Redirect.to(file.toFile());
    }

    private static ProcessBuilder.Redirect log(Path directory)
    {
        return ProcessBuilder.Redirect.appendTo(directory.resolve("capture.log").toFile());
    }

    private static long size(Path file)
    {
        try
        {
            return Files.isRegularFile(file) ? Files.size(file) : -1;
        }
        catch (IOException e)
        {
            return -1;
        }
    }

    private static List<Map<String, Object>> oomDumps(Path runDirectory)
    {
        List<Map<String, Object>> dumps = new ArrayList<>();
        if (runDirectory == null)
            return dumps;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(runDirectory, "*.hprof"))
        {
            for (Path file : files)
            {
                Map<String, Object> dump = new LinkedHashMap<>();
                dump.put("file", file.toString());
                dump.put("bytes", size(file));
                dumps.add(dump);
            }
        }
        catch (IOException e)
        {
            // none to list
        }
        return dumps;
    }

    /** Runs one jcmd command. Returns false and logs a warning on a timeout, a failure or an error. */
    private boolean run(long node, Duration timeout, ProcessBuilder.Redirect output, String... command)
    {
        List<String> line = new ArrayList<>();
        line.add(javaHome.resolve("bin").resolve("jcmd").toString());
        line.add(Long.toString(node));
        line.addAll(List.of(command));
        String label = String.join(" ", command);
        try
        {
            Process process = new ProcessBuilder(line).redirectErrorStream(true).redirectOutput(output).start();
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS))
            {
                process.destroyForcibly();
                warnings.accept("jcmd " + label + " timed out after " + timeout);
                return false;
            }
            if (process.exitValue() != 0)
            {
                warnings.accept("jcmd " + label + " exited with status " + process.exitValue());
                return false;
            }
            return true;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            warnings.accept("jcmd " + label + " was interrupted");
            return false;
        }
        catch (IOException | RuntimeException e)
        {
            warnings.accept("Could not run jcmd " + label + ": " + e);
            return false;
        }
    }
}
