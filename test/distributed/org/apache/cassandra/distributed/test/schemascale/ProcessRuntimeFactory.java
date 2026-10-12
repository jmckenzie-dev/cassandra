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
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;

/**
 * Creates {@link ProcessNodeRuntime} nodes. Each node gets its own ports and its own copy of the config.
 * The base config is conf/cassandra.yaml of the Cassandra home, with the profile overrides on top, the same
 * overrides that in-JVM mode applies.
 */
public final class ProcessRuntimeFactory implements RuntimeFactory
{
    private static final long DEFAULT_MAX_MUTATION_BYTES = 16L * 1024 * 1024;

    private final ProcessNodeSettings settings;
    private final Supplier<NodeConfigRenderer.Ports> ports;
    private final IntPredicate isFree;
    private final boolean agentAtStart;
    private Map<String, ?> preferred;

    ProcessRuntimeFactory(ProcessNodeSettings settings, Supplier<NodeConfigRenderer.Ports> ports)
    {
        this(settings, ports, NodeConfigRenderer.Ports::systemIsFree, false);
    }

    ProcessRuntimeFactory(ProcessNodeSettings settings, Supplier<NodeConfigRenderer.Ports> ports, IntPredicate isFree,
                          boolean agentAtStart)
    {
        this.settings = settings;
        this.ports = ports;
        this.isFree = isFree;
        this.agentAtStart = agentAtStart;
    }

    /**
     * A factory for this JVM's Java home and the launcher environment. The Cassandra home is CASSANDRA_HOME
     * when set, otherwise the working directory.
     */
    public static ProcessRuntimeFactory fromEnvironment(boolean flightRecording)
    {
        return fromEnvironment(flightRecording, new ScaleProfile.Profiling());
    }

    /**
     * Also applies the node profiling keys: profiling.nodeJfrSettings (relative to the Cassandra home) for the
     * node flight recording, and profiling.nodeProfiler (mbean or agentpath) for the AsyncProfilerService flags.
     */
    public static ProcessRuntimeFactory fromEnvironment(boolean flightRecording, ScaleProfile.Profiling profiling)
    {
        Map<String, String> environment = System.getenv(); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        String home = environment.get("CASSANDRA_HOME");
        Path cassandraHome = (home == null || home.isBlank() ? Path.of("") : Path.of(home)).toAbsolutePath();
        Path javaHome = Path.of(CassandraRelevantProperties.JAVA_HOME.getString());
        Path jfc = profiling.nodeJfrSettings == null ? null : cassandraHome.resolve(profiling.nodeJfrSettings);
        if (jfc != null && !Files.isRegularFile(jfc))
        {
            System.err.println("WARNING: node JFR settings " + jfc + " not found; the node uses the JDK 'profile' settings");
            jfc = null;
        }
        boolean profilerService = !"off".equals(profiling.nodeProfiler);
        ProcessNodeSettings settings = ProcessNodeSettings.fromEnvironment(environment, cassandraHome, javaHome, flightRecording)
                                                          .withProfiling(jfc, profilerService);
        return new ProcessRuntimeFactory(settings, () -> NodeConfigRenderer.Ports.pick(NodeConfigRenderer.Ports::systemFreePort,
                                                                                          NodeConfigRenderer.Ports::systemIsFree),
                                         NodeConfigRenderer.Ports::systemIsFree, "agentpath".equals(profiling.nodeProfiler));
    }

    @Override
    public synchronized void preferPorts(Map<String, ?> wanted)
    {
        preferred = wanted;
    }

    // The preferred ports when all four are set and free; else fresh ones, and the node announces a new address.
    synchronized NodeConfigRenderer.Ports nextPorts()
    {
        Map<String, ?> wanted = preferred;
        preferred = null;
        if (wanted != null)
        {
            int storage = port(wanted, "storagePort");
            int ssl = port(wanted, "sslStoragePort");
            int nativePort = port(wanted, "nativeTransportPort");
            int jmx = port(wanted, "jmxPort");
            if (storage > 0 && ssl > 0 && nativePort > 0 && jmx > 0 &&
                isFree.test(storage) && isFree.test(ssl) && isFree.test(nativePort) && isFree.test(jmx))
                return new NodeConfigRenderer.Ports(storage, ssl, nativePort, jmx);
            System.err.println("WARNING: saved ports " + wanted + " are not all set and free; using new ports. " +
                               "The node then commits an address change to the cluster metadata at startup.");
        }
        return ports.get();
    }

    private static int port(Map<String, ?> ports, String key)
    {
        Object value = ports.get(key);
        return value instanceof Number ? ((Number) value).intValue() : -1;
    }

    @Override
    public NodeRuntime create(Path directory, Map<String, Object> nodeConfig) throws IOException
    {
        NodeConfigRenderer.Ports nodePorts = nextPorts();
        Map<String, Object> base = NodeConfigRenderer.load(settings.cassandraHome.resolve("conf").resolve("cassandra.yaml"));
        Map<String, Object> config = NodeConfigRenderer.render(base, nodeConfig, directory, nodePorts);
        DriverSchemaExecutor executor = new DriverSchemaExecutor(new InetSocketAddress(NodeConfigRenderer.LOOPBACK, nodePorts.nativeTransport));
        JmxMetricsProbe probe = new JmxMetricsProbe(NodeConfigRenderer.LOOPBACK, nodePorts.jmx, maxMutationBytes(config),
                                                    snapshotSizeLimitBytes(config), executor);
        ProcessNodeRuntime runtime = new ProcessNodeRuntime(directory, config, nodePorts, settings, executor, probe);
        runtime.useAgentAtStart(agentAtStart);
        return runtime;
    }

    /** max_mutation_size of the config in bytes; the node default is half the commit log segment size. */
    static long maxMutationBytes(Map<String, Object> config)
    {
        Object value = config.get("max_mutation_size");
        if (value != null)
            return new DataStorageSpec.IntKibibytesBound(value.toString()).toBytes();
        Object segment = config.get("commitlog_segment_size");
        if (segment != null)
            return new DataStorageSpec.IntKibibytesBound(segment.toString()).toBytes() / 2;
        return DEFAULT_MAX_MUTATION_BYTES;
    }

    /**
     * The largest TCM snapshot the node can use, as MetadataSnapshots.snapshotSizeLimit computes it: the smaller of
     * max_value_size (default 256 MiB) and internode_max_message_size (default: the smaller send and receive
     * endpoint reserve capacity, 128 MiB each). Snapshots skip the commit log, so max_mutation_size does not apply.
     */
    static long snapshotSizeLimitBytes(Map<String, Object> config)
    {
        Object value = config.get("max_value_size");
        long maxValue = value != null ? new DataStorageSpec.IntMebibytesBound(value.toString()).toBytes() : 256L * 1024 * 1024;
        Object message = config.get("internode_max_message_size");
        long maxMessage = message != null
                          ? new DataStorageSpec.IntBytesBound(message.toString()).toBytes()
                          : Math.min(reserveBytes(config, "internode_application_send_queue_reserve_endpoint_capacity"),
                                     reserveBytes(config, "internode_application_receive_queue_reserve_endpoint_capacity"));
        return Math.min(maxValue, maxMessage);
    }

    private static long reserveBytes(Map<String, Object> config, String key)
    {
        Object value = config.get(key);
        return value != null ? new DataStorageSpec.IntBytesBound(value.toString()).toBytes() : 128L * 1024 * 1024;
    }
}
