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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Node-side async-profiler windows at the capture points. Each point runs one window per event, in order,
 * on a background thread, so the DDL clients keep running while the node is profiled. Each window writes
 * {@code node-<event>.jfr} into the step directory.
 *
 * <p>A window first uses AsyncProfilerMBean.execute ("start,event=E,jfr,file=F" and "stop"), which needs
 * the node's unsafe mode and writes F directly. When execute is refused (SecurityException), the window
 * uses AsyncProfilerMBean.start and stop, which write under the node log directory, and the file is moved
 * or fetched into the step directory. When cpu does not start, the window retries with itimer.
 * When the service has no profiler and the run asked for "mbean", the next node start loads the
 * library with -agentpath instead.
 */
public final class NodeProfiles implements AutoCloseable
{
    // AsyncProfilerMBean.start stops the session by itself after this extra time if no stop arrives
    private static final int SAFE_DURATION_SLACK_SECONDS = 60;

    private final NodeProfilerControl control;
    private final String requested;
    private final List<String> events;
    private final int seconds;
    private final ExecutorService worker;
    private final List<Map<String, Object>> results = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private Future<?> running;
    private CountDownLatch cancel;
    private String fallbackReason;

    /**
     * @param requested profiling.nodeProfiler: mbean or agentpath
     * @param seconds length of each window; 0 stops each window at once
     */
    public NodeProfiles(NodeProfilerControl control, String requested, List<String> events, int seconds)
    {
        this.control = control;
        this.requested = requested;
        this.events = List.copyOf(events);
        this.seconds = seconds;
        this.worker = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "schema-scale-node-profiler");
            thread.setDaemon(true);
            return thread;
        });
    }

    public synchronized boolean busy()
    {
        return running != null && !running.isDone();
    }

    /** Starts the windows for one capture point. Returns false when the previous point still runs. */
    public synchronized boolean start(long tables, Path directory)
    {
        if (busy())
        {
            warn("Node profile at " + tables + " tables skipped: the previous one still runs");
            return false;
        }
        CountDownLatch stop = new CountDownLatch(1);
        cancel = stop;
        running = worker.submit(() -> profilePoint(tables, directory, stop));
        return true;
    }

    /** Ends the running window at once, skips the remaining ones and waits. */
    public void stopNow()
    {
        CountDownLatch stop;
        synchronized (this)
        {
            stop = cancel;
        }
        if (stop != null)
            stop.countDown();
        awaitIdle();
    }

    /** Waits for the running point to finish. */
    public void awaitIdle()
    {
        Future<?> current;
        synchronized (this)
        {
            current = running;
        }
        if (current == null)
            return;
        try
        {
            current.get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            warn("Node profile failed: " + e.getCause());
        }
    }

    /** One map per capture point, for summary.json. */
    public synchronized List<Map<String, Object>> results()
    {
        return new ArrayList<>(results);
    }

    /** Warnings since the last call; the caller logs them on its own thread. */
    public synchronized List<String> drainWarnings()
    {
        List<String> drained = new ArrayList<>(warnings);
        warnings.clear();
        return drained;
    }

    /** requested, the load path of the running node, and the fallback reason when there was one. */
    public synchronized Map<String, Object> path()
    {
        Map<String, Object> path = new LinkedHashMap<>();
        path.put("requested", requested);
        path.put("loadPath", control.loadPath());
        path.put("fallbackReason", fallbackReason);
        return path;
    }

    @Override
    public void close()
    {
        stopNow();
        worker.shutdownNow();
    }

    private void profilePoint(long tables, Path directory, CountDownLatch stop)
    {
        Map<String, Object> point = new LinkedHashMap<>();
        point.put("tables", tables);
        point.put("loadPath", control.loadPath());
        List<Map<String, Object>> windows = new ArrayList<>();
        point.put("windows", windows);
        synchronized (this)
        {
            results.add(point);
        }
        try
        {
            if (!control.enabled())
            {
                point.put("error", "AsyncProfilerService has no profiler (isEnabled is false)");
                fallback("AsyncProfilerService isEnabled was false at " + tables + " tables");
                return;
            }
        }
        catch (Exception e)
        {
            point.put("error", "AsyncProfilerService MBean failed: " + e);
            warn("Node profile at " + tables + " tables skipped: " + e);
            return;
        }
        for (String event : events)
        {
            if (stop.getCount() == 0)
            {
                Map<String, Object> skipped = new LinkedHashMap<>();
                skipped.put("event", event);
                skipped.put("skipped", "stopped early (ramp end, snapshot or node stop)");
                synchronized (this)
                {
                    windows.add(skipped);
                }
                continue;
            }
            Map<String, Object> window = window(event, tables, directory, stop);
            synchronized (this)
            {
                windows.add(window);
            }
        }
    }

    private Map<String, Object> window(String event, long tables, Path directory, CountDownLatch stop)
    {
        Map<String, Object> window = new LinkedHashMap<>();
        String name = event.equals("itimer") ? "cpu" : event;
        Path file = directory.resolve("node-" + name + ".jfr").toAbsolutePath();
        String safeName = "node-" + name + "-t" + tables + ".jfr";
        window.put("event", event);
        window.put("file", file.toString());
        window.put("requestedSeconds", seconds);
        String method = "execute";
        String used = event;
        long startedAt = System.nanoTime();
        boolean started = false;
        try
        {
            Files.createDirectories(directory);
            Files.deleteIfExists(file);
            try
            {
                control.execute(startCommand(used, file));
            }
            catch (Exception e)
            {
                if (causedBy(e, SecurityException.class))
                {
                    method = "start";
                    control.start(safeParameters(event, safeName));
                }
                else if (event.equals("cpu"))
                {
                    window.put("cpuError", rootMessage(e));
                    used = "itimer";
                    control.execute(startCommand(used, file));
                }
                else
                {
                    throw e;
                }
            }
            started = true;
            startedAt = System.nanoTime();
            boolean cancelled = stop.await(seconds, TimeUnit.SECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            started = false;
            if (method.equals("execute"))
                control.execute("stop");
            else
                collect(safeName, file, control.stop(new HashMap<>()));
            window.put("eventUsed", used);
            window.put("method", method);
            window.put("seconds", elapsedMs / 1000.0);
            window.put("truncated", cancelled);
            window.put("bytes", Files.isRegularFile(file) ? Files.size(file) : -1L);
            System.out.printf(Locale.ROOT, "Node %s profile at %d tables: %.1f s%s, %s%n", used, tables, elapsedMs / 1000.0,
                              cancelled ? " (stopped early)" : "", file);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            window.put("error", "interrupted");
        }
        catch (Exception e)
        {
            window.put("error", rootMessage(e));
            warn("Node " + event + " profile at " + tables + " tables failed: " + rootMessage(e));
        }
        finally
        {
            if (started)
                quietStop(method);
        }
        return window;
    }

    static String startCommand(String event, Path file)
    {
        return "start,event=" + event + ",jfr,file=" + file;
    }

    private Map<String, String> safeParameters(String event, String fileName)
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("events", event);
        parameters.put("outputFormat", "jfr");
        parameters.put("duration", (seconds + SAFE_DURATION_SLACK_SECONDS) + "s");
        parameters.put("outputFileName", fileName);
        return parameters;
    }

    // AsyncProfilerMBean.start wrote under the node log directory: move the file, or fetch it over JMX.
    private void collect(String safeName, Path target, boolean stopped) throws Exception
    {
        if (!stopped)
            warn("AsyncProfilerMBean.stop returned false for " + safeName + "; the session may have timed out");
        Path directory = control.resultDirectory();
        Path written = directory == null ? null : directory.resolve(safeName);
        if (written != null && Files.isRegularFile(written))
        {
            Files.move(written, target, StandardCopyOption.REPLACE_EXISTING);
            return;
        }
        Files.write(target, control.fetch(safeName));
    }

    private void quietStop(String method)
    {
        try
        {
            if (method.equals("execute"))
                control.execute("stop");
            else
                control.stop(new HashMap<>());
        }
        catch (Exception e)
        {
            // no session, or the node is gone
        }
    }

    private synchronized void fallback(String reason)
    {
        if (fallbackReason != null || !requested.equals("mbean") || !control.loadPath().equals("mbean"))
        {
            warn("Node profile skipped: " + reason);
            return;
        }
        if (control.useAgentOnNextStart())
        {
            fallbackReason = reason;
            warn("Node profile skipped: " + reason + ". The next node start loads async-profiler with -agentpath.");
        }
        else
        {
            warn("Node profile skipped: " + reason + "; no -agentpath fallback in this mode");
        }
    }

    // collected only; the harness logs them on its own thread, see drainWarnings
    private synchronized void warn(String message)
    {
        warnings.add(message);
    }

    static boolean causedBy(Throwable failure, Class<? extends Throwable> type)
    {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause())
            if (type.isInstance(t))
                return true;
        return false;
    }

    static String rootMessage(Throwable failure)
    {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root)
            root = root.getCause();
        return root == failure ? failure.toString() : failure.getClass().getSimpleName() + ": " + root;
    }
}
