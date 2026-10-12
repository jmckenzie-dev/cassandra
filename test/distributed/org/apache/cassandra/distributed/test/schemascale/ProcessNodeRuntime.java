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
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * One node run by bin/cassandra in its own JVM. The config, data, logs and pid file live under the node
 * directory. Readiness is a successful CQL query; the executor and the probe stay valid across restarts.
 */
public final class ProcessNodeRuntime implements NodeRuntime
{
    private static final long POLL_MILLIS = 1000;
    private static final int STOP_WAIT_SECONDS = 120;

    private final Path directory;
    private final Path runDirectory;
    private final Map<String, Object> config;
    private final NodeConfigRenderer.Ports ports;
    private final ProcessNodeSettings settings;
    private final DriverSchemaExecutor executor;
    private final JmxMetricsProbe probe;
    private final Map<String, Object> summary = new LinkedHashMap<>();

    private Process process;
    private Path flightRecording;
    private int incarnation;
    private volatile boolean agentOnStart;
    private volatile boolean agentLoaded;

    /**
     * @param directory the node directory, {@code <run>/node}
     * @param config the full rendered cassandra.yaml content, see {@link NodeConfigRenderer#render}
     */
    ProcessNodeRuntime(Path directory, Map<String, Object> config, NodeConfigRenderer.Ports ports, ProcessNodeSettings settings,
                       DriverSchemaExecutor executor, JmxMetricsProbe probe)
    {
        this.directory = directory.toAbsolutePath();
        this.runDirectory = this.directory.getParent();
        this.config = config;
        this.ports = ports;
        this.settings = settings;
        this.executor = executor;
        this.probe = probe;
    }

    @Override
    public void start() throws Exception
    {
        prepareDirectories();
        launch();
    }

    @Override
    public void restart() throws Exception
    {
        if (process != null && process.isAlive())
            throw new IllegalStateException("Node still runs; stop it first");
        launch();
    }

    @Override
    public void stopGraceful() throws Exception
    {
        executor.disconnect();
        try
        {
            probe.drain();
        }
        catch (Exception e)
        {
            System.err.println("Node drain failed, stopping anyway: " + e);
        }
        probe.disconnect();
        terminate();
    }

    @Override
    public void kill() throws Exception
    {
        executor.disconnect();
        probe.disconnect();
        if (process != null)
        {
            process.destroyForcibly();
            process.waitFor(STOP_WAIT_SECONDS, TimeUnit.SECONDS);
        }
        killPidFile();
        Files.deleteIfExists(pidFile());
        // a killed JVM leaves an empty recording file
        if (flightRecording != null && Files.isRegularFile(flightRecording) && Files.size(flightRecording) == 0)
            Files.delete(flightRecording);
    }

    @Override
    public SchemaExecutor executor()
    {
        return executor;
    }

    @Override
    public MetricsProbe probe()
    {
        return probe;
    }

    @Override
    public long pid()
    {
        return process == null ? -1 : process.pid();
    }

    @Override
    public Path logFile()
    {
        return directory.resolve("logs").resolve("system.log");
    }

    @Override
    public Map<String, Object> summary()
    {
        return summary;
    }

    /** Loads libasyncProfiler with -agentpath from the next start on. */
    void useAgentAtStart(boolean agent)
    {
        agentOnStart = agent;
    }

    @Override
    public NodeProfilerControl profilerControl()
    {
        if (!settings.asyncProfilerService && !agentOnStart)
            return null;
        return new NodeProfilerControl()
        {
            @Override
            public boolean enabled() throws Exception
            {
                return probe.profilerEnabled();
            }

            @Override
            public String execute(String command) throws Exception
            {
                return (String) probe.invokeProfiler("execute", new Object[]{ command }, new String[]{ String.class.getName() });
            }

            @Override
            public boolean start(Map<String, String> parameters) throws Exception
            {
                return (Boolean) probe.invokeProfiler("start", new Object[]{ new HashMap<>(parameters) }, new String[]{ Map.class.getName() });
            }

            @Override
            public boolean stop(Map<String, String> parameters) throws Exception
            {
                return (Boolean) probe.invokeProfiler("stop", new Object[]{ new HashMap<>(parameters) }, new String[]{ Map.class.getName() });
            }

            @Override
            public byte[] fetch(String resultFile) throws Exception
            {
                return (byte[]) probe.invokeProfiler("fetch", new Object[]{ resultFile }, new String[]{ String.class.getName() });
            }

            @Override
            public Path resultDirectory()
            {
                // AsyncProfilerService writes to <cassandra.logdir>/profiler; CASSANDRA_LOG_DIR is <node>/logs
                return directory.resolve("logs").resolve("profiler");
            }

            @Override
            public String loadPath()
            {
                return agentLoaded ? "agentpath" : "mbean";
            }

            @Override
            public boolean useAgentOnNextStart()
            {
                agentOnStart = true;
                return true;
            }
        };
    }

    @Override
    public void close()
    {
        try
        {
            executor.close();
            probe.close();
            if (process != null && process.isAlive())
                terminate();
        }
        catch (Throwable t)
        {
            System.err.println("Could not stop the node process: " + t);
            if (process != null)
                process.destroyForcibly();
        }
    }

    private void prepareDirectories() throws IOException
    {
        Files.createDirectories(directory.resolve("logs"));
        Files.createDirectories(directory.resolve("tmp"));
        Path conf = directory.resolve("conf");
        Path source = settings.cassandraHome.resolve("conf");
        try (Stream<Path> files = Files.walk(source))
        {
            for (Path file : (Iterable<Path>) files::iterator)
            {
                Path target = conf.resolve(source.relativize(file).toString());
                if (Files.isDirectory(file))
                    Files.createDirectories(target);
                else
                    Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        NodeConfigRenderer.write(config, conf.resolve("cassandra.yaml"));
        System.out.println("Node config: " + conf.resolve("cassandra.yaml") +
                           ", native_transport_port=" + ports.nativeTransport +
                           ", jmx_port=" + ports.jmx +
                           ", native_transport_max_frame_size=" + config.getOrDefault("native_transport_max_frame_size", "16MiB (default)") +
                           ", native_transport_timeout=" + config.getOrDefault("native_transport_timeout", "12s (default)"));
    }

    private void launch() throws Exception
    {
        incarnation++;
        Path jfr = settings.flightRecording ? runDirectory.resolve(incarnation == 1 ? "node.jfr" : "node-" + incarnation + ".jfr") : null;
        flightRecording = jfr;
        Path agent = agentOnStart ? AsyncProfilerLibrary.extract(AsyncProfilerLibrary.findJar(settings.cassandraHome),
                                                                 AsyncProfilerLibrary.entryFor(CassandraRelevantProperties.OS_NAME.getString(),
                                                                                               CassandraRelevantProperties.OS_ARCH.getString()),
                                                                 directory)
                                  : null;
        agentLoaded = agent != null;
        String extra = settings.jvmExtraOptions(runDirectory, directory, ports.jmx, jfr, agent);
        Map<String, String> environment = settings.nodeEnvironment(directory, directory.resolve("conf"), extra);

        ProcessBuilder builder = new ProcessBuilder(settings.cassandraHome.resolve("bin").resolve("cassandra").toString(),
                                                    "-f", "-p", pidFile().toString());
        builder.directory(settings.cassandraHome.toFile());
        builder.environment().clear();
        builder.environment().putAll(environment);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(directory.resolve("logs").resolve("stdout.log").toFile()));
        Files.deleteIfExists(pidFile());
        process = builder.start();
        System.out.println("Node process " + process.pid() + " started (run " + incarnation + "), heap " + settings.heap +
                           ", direct memory " + settings.directMemory + ", JVM_EXTRA_OPTS=" + extra);
        summary.put("pid", process.pid());
        summary.put("nativeTransportPort", ports.nativeTransport);
        summary.put("jmxPort", ports.jmx);
        summary.put("storagePort", ports.storage);
        summary.put("sslStoragePort", ports.sslStorage);
        summary.put("asyncProfiler", agent != null ? "agentpath " + agent : settings.asyncProfilerService ? "mbean" : "off");
        summary.put("javaHome", settings.javaHome.toString());
        summary.put("heapLimit", settings.heap);
        summary.put("directMemoryLimit", settings.directMemory);
        summary.put("jvmExtraOpts", extra);
        summary.put("startCount", incarnation);
        awaitReady();
        try
        {
            summary.put("jvmArguments", probe.jvmArguments());
        }
        catch (Exception e)
        {
            summary.put("jvmArguments", "unavailable: " + e);
        }
    }

    private void awaitReady() throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.startupTimeoutMillis);
        Exception last = null;
        while (System.nanoTime() < deadline)
        {
            if (!process.isAlive())
                throw new IllegalStateException("Node exited with code " + process.exitValue() + " during startup.\n" + logTail());
            if (nativePortOpen())
            {
                try
                {
                    executor.connect();
                    executor.query("SELECT release_version FROM system.local");
                    return;
                }
                catch (RuntimeException e)
                {
                    last = e;
                    executor.disconnect();
                }
            }
            Thread.sleep(POLL_MILLIS);
        }
        process.destroyForcibly();
        throw new IllegalStateException("Node not ready after " + settings.startupTimeoutMillis + " ms; last error: " + last +
                                        "\n" + logTail());
    }

    private boolean nativePortOpen()
    {
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress(NodeConfigRenderer.LOOPBACK, ports.nativeTransport), 1000);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    // SIGTERM runs the Cassandra shutdown hook and the JFR dump; the node is killed if it does not exit in time.
    private void terminate() throws Exception
    {
        if (process == null)
            return;
        process.destroy();
        if (!process.waitFor(settings.shutdownTimeoutMillis, TimeUnit.MILLISECONDS))
        {
            System.err.println("Node did not stop in " + settings.shutdownTimeoutMillis + " ms; killing it");
            process.destroyForcibly();
            process.waitFor(STOP_WAIT_SECONDS, TimeUnit.SECONDS);
        }
        Files.deleteIfExists(pidFile());
    }

    private void killPidFile()
    {
        try
        {
            Path file = pidFile();
            if (!Files.isRegularFile(file))
                return;
            long pid = Long.parseLong(Files.readString(file, StandardCharsets.UTF_8).trim());
            ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
        }
        catch (IOException | NumberFormatException e)
        {
            // no usable pid file
        }
    }

    private Path pidFile()
    {
        return directory.resolve("cassandra.pid");
    }

    private String logTail()
    {
        for (String name : List.of("stdout.log", "system.log"))
        {
            Path file = directory.resolve("logs").resolve(name);
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8))
            {
                List<String> all = lines.toList();
                if (!all.isEmpty())
                    return "Last lines of " + file + ":\n" + String.join("\n", all.subList(Math.max(0, all.size() - 40), all.size()));
            }
            catch (IOException | UncheckedIOException e)
            {
                // try the next file
            }
        }
        return "No node log found.";
    }
}
