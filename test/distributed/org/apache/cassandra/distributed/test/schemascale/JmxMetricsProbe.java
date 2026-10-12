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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.ThreadMXBean;

import org.apache.cassandra.profiler.AsyncProfilerMBean;

/**
 * Samples a node JVM over JMX and returns the same keys as {@link InJvmMetricsProbe}. A sample reads the
 * platform MBeans and a few named MBeans; it never lists the per-table MBeans, so it stays cheap at 100k tables.
 *
 * <p>Differences from the in-JVM probe:
 * <ul>
 * <li>{@code snapshot_bytes_computed} has no JMX source. The probe reports {@code snapshot_bytes_gauge}
 * instead (the size of the last stored snapshot), and -1 when no snapshot exists yet. It lags the real
 * schema size by up to metadata_snapshot_frequency epochs.</li>
 * <li>{@code scheduled_tasks} is always -1. The scheduled executors are not registered in JMX.</li>
 * <li>{@code node_tables} comes from a CQL count on system_schema.tables through the injected table counter,
 * and only on samples with {@code withSnapshotSize}, because it is O(schema). Other samples report -1.</li>
 * <li>{@code max_mutation_size} and {@code snapshot_size_limit} come from the constructor, since the node has
 * no JMX attribute for them.</li>
 * <li>The memtable part of {@code system_schema_bytes} and {@code metadata_log_bytes} uses
 * AllMemtablesLiveDataSize. Under a metrics allowlist that drops it, the probe uses the on-heap plus off-heap
 * memtable data sizes.</li>
 * <li>{@code tcm_commit_p99_ns} is the JMX 99th percentile (microseconds) times 1000.</li>
 * </ul>
 * The connection opens on first use and reopens after {@link #disconnect}, so the probe survives node restarts.
 */
public final class JmxMetricsProbe implements MetricsProbe, AutoCloseable
{
    private static final String METRICS = "org.apache.cassandra.metrics:";
    private static final String SCHEMA_KEYSPACE = "system_schema";
    private static final String METADATA_KEYSPACE = "system_cluster_metadata";

    private static final ObjectName STORAGE_SERVICE = name("org.apache.cassandra.db:type=StorageService");
    private static final ObjectName EPOCH = name(METRICS + "type=TCM,name=Epoch");
    private static final ObjectName COMMIT_LATENCY = name(METRICS + "type=TCM,name=CommitSuccessLatency");
    private static final ObjectName SNAPSHOT_SIZE = name(METRICS + "type=TCM,name=LastSnapshotSize");
    private static final ObjectName SNAPSHOT_FAILURES = name(METRICS + "type=TCM,name=SnapshotStoreFailures");
    private static final ObjectName ASYNC_PROFILER = name(AsyncProfilerMBean.MBEAN_NAME);

    private final JMXServiceURL url;
    private final long maxMutationSize;
    private final long snapshotSizeLimit;
    private final SchemaExecutor tableCounter;

    private Connection connection;
    private long heapAfterGc = -1;
    private List<String> schemaTables;
    private List<String> metadataTables;
    private final Set<String> reported = new HashSet<>();

    /**
     * @param maxMutationSize the node max_mutation_size in bytes, or -1 when unknown
     * @param snapshotSizeLimit the largest TCM snapshot the node can use, in bytes, or -1 when unknown
     * @param tableCounter runs the CQL that counts tables, or null to skip node_tables
     */
    public JmxMetricsProbe(String host, int port, long maxMutationSize, long snapshotSizeLimit, SchemaExecutor tableCounter)
    {
        try
        {
            this.url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ':' + port + "/jmxrmi");
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }
        this.maxMutationSize = maxMutationSize;
        this.snapshotSizeLimit = snapshotSizeLimit;
        this.tableCounter = tableCounter;
    }

    @Override
    public synchronized Map<String, Long> sample(boolean withSnapshotSize)
    {
        Map<String, Long> values = new LinkedHashMap<>();
        for (String key : InJvmMetricsProbe.NodeSide.KEYS)
            values.put(key, -1L);
        try
        {
            Connection jmx = connect();
            guarded("jvm", () -> jvm(jmx, values));
            guarded("node", () -> node(jmx, values, withSnapshotSize));
            guarded("tcm", () -> tcm(jmx, values));
            guarded("storage", () -> storage(jmx, values));
            values.put("max_mutation_size", maxMutationSize);
            values.put("snapshot_size_limit", snapshotSizeLimit);
        }
        catch (IOException e)
        {
            disconnect();
            report("connect", e);
        }
        return values;
    }

    @Override
    public synchronized void gc()
    {
        try
        {
            Connection jmx = connect();
            jmx.memory.gc();
            heapAfterGc = jmx.memory.getHeapMemoryUsage().getUsed();
        }
        catch (IOException | RuntimeException e)
        {
            disconnect();
            report("gc", e);
        }
    }

    @Override
    public synchronized long systemSchemaBytes()
    {
        try
        {
            Connection jmx = connect();
            return keyspaceBytes(jmx, SCHEMA_KEYSPACE, schemaTables());
        }
        catch (Exception e)
        {
            disconnect();
            report("system_schema_bytes", e);
            return -1;
        }
    }

    /** Runs StorageService.drain, which flushes all memtables and stops accepting writes. */
    public synchronized void drain() throws Exception
    {
        connect().server.invoke(STORAGE_SERVICE, "drain", null, null);
    }

    /** Calls an operation of the node's AsyncProfilerMBean. */
    public synchronized Object invokeProfiler(String operation, Object[] parameters, String[] signature) throws Exception
    {
        try
        {
            return connect().server.invoke(ASYNC_PROFILER, operation, parameters, signature);
        }
        catch (IOException e)
        {
            disconnect();
            throw e;
        }
    }

    /** AsyncProfilerMBean.isEnabled, the "Enabled" attribute. */
    public synchronized boolean profilerEnabled() throws Exception
    {
        try
        {
            return (Boolean) connect().server.getAttribute(ASYNC_PROFILER, "Enabled");
        }
        catch (IOException e)
        {
            disconnect();
            throw e;
        }
    }

    /** The input arguments of the node JVM. */
    public synchronized List<String> jvmArguments() throws IOException
    {
        return new ArrayList<>(connect().runtime.getInputArguments());
    }

    /** Drops the connection. The next call opens a new one. */
    public synchronized void disconnect()
    {
        if (connection == null)
            return;
        try
        {
            connection.connector.close();
        }
        catch (IOException e)
        {
            // the node may be gone already
        }
        connection = null;
    }

    @Override
    public void close()
    {
        disconnect();
    }

    private Connection connect() throws IOException
    {
        if (connection == null)
            connection = new Connection(JMXConnectorFactory.connect(url, null));
        return connection;
    }

    private interface Group
    {
        void run() throws Exception;
    }

    // One failing group leaves its keys at -1 and does not hide the others. The first failure of a group is printed.
    private void guarded(String group, Group body)
    {
        try
        {
            body.run();
        }
        catch (Exception e)
        {
            report(group, e);
            if (e instanceof IOException)
                disconnect();
        }
    }

    private void report(String group, Exception e)
    {
        if (reported.add(group + ':' + e.getClass().getName()))
            System.err.println("JMX probe: " + group + " failed: " + e);
    }

    private void jvm(Connection jmx, Map<String, Long> values) throws IOException
    {
        MemoryUsage heap = jmx.memory.getHeapMemoryUsage();
        values.put("heap_used", heap.getUsed());
        values.put("heap_max", heap.getMax());
        long postGc = 0;
        boolean collected = false;
        long metaspace = -1;
        for (MemoryPoolMXBean pool : jmx.pools)
        {
            if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null)
            {
                postGc += pool.getCollectionUsage().getUsed();
                collected = true;
            }
            if (pool.getName().equals("Metaspace"))
                metaspace = pool.getUsage().getUsed();
        }
        // a collector without per-pool collection usage: use the heap read right after the last forced GC
        values.put("heap_post_gc", collected ? postGc : heapAfterGc);
        values.put("metaspace_used", metaspace);
        long direct = Long.parseLong(jmx.hotspot.getVMOption("MaxDirectMemorySize").getValue());
        values.put("direct_max", direct > 0 ? direct : heap.getMax());
        long count = 0;
        long time = 0;
        for (GarbageCollectorMXBean collector : jmx.collectors)
        {
            count += Math.max(0, collector.getCollectionCount());
            time += Math.max(0, collector.getCollectionTime());
        }
        values.put("gc_count", count);
        values.put("gc_time_ms", time);
        values.put("threads", (long) jmx.threads.getThreadCount());
        values.put("alloc_bytes_total", jmx.threads.getTotalThreadAllocatedBytes());
    }

    private void node(Connection jmx, Map<String, Long> values, boolean withTables) throws Exception
    {
        values.put("mbeans", (long) jmx.server.getMBeanCount());
        values.put("node_keyspaces", (long) ((Collection<?>) jmx.server.getAttribute(STORAGE_SERVICE, "Keyspaces")).size());
        if (withTables && tableCounter != null)
        {
            Object[][] rows = tableCounter.query("SELECT count(*) FROM system_schema.tables");
            values.put("node_tables", ((Number) rows[0][0]).longValue());
        }
    }

    private void tcm(Connection jmx, Map<String, Long> values) throws Exception
    {
        values.put("tcm_epoch", ((Number) jmx.server.getAttribute(EPOCH, "Value")).longValue());
        double micros = ((Number) jmx.server.getAttribute(COMMIT_LATENCY, "99thPercentile")).doubleValue();
        values.put("tcm_commit_p99_ns", (long) (micros * 1000));
        long gauge = ((Number) jmx.server.getAttribute(SNAPSHOT_SIZE, "Value")).longValue();
        values.put("snapshot_bytes_gauge", gauge);
        values.put("snapshot_bytes_computed", gauge > 0 ? gauge : -1);
        values.put("snapshot_store_failures", ((Number) jmx.server.getAttribute(SNAPSHOT_FAILURES, "Count")).longValue());
    }

    private void storage(Connection jmx, Map<String, Long> values) throws Exception
    {
        values.put("system_schema_bytes", keyspaceBytes(jmx, SCHEMA_KEYSPACE, schemaTables()));
        long log = keyspaceBytes(jmx, METADATA_KEYSPACE, metadataTables());
        log += tableBytes(jmx, "system", "local_metadata_log");
        values.put("metadata_log_bytes", log);
    }

    private List<String> schemaTables() throws Exception
    {
        if (schemaTables == null)
            schemaTables = tablesOf(SCHEMA_KEYSPACE);
        return schemaTables;
    }

    private List<String> metadataTables() throws Exception
    {
        if (metadataTables == null)
            metadataTables = tablesOf(METADATA_KEYSPACE);
        return metadataTables;
    }

    /**
     * Lists a system keyspace's tables through CQL, not a JMX pattern query. queryNames with a key pattern
     * makes every registered ObjectName build and cache its key property map: about 800 bytes per MBean,
     * kept for the MBean's life. At 40k tables (10M MBeans) one query added 8 GB of live heap to the node.
     */
    private List<String> tablesOf(String keyspace) throws Exception
    {
        List<String> tables = new ArrayList<>();
        if (tableCounter == null)
            return tables;
        for (Object[] row : tableCounter.query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace))
            tables.add((String) row[0]);
        return tables;
    }

    private static long keyspaceBytes(Connection jmx, String keyspace, List<String> tables) throws Exception
    {
        long bytes = 0;
        for (String table : tables)
            bytes += tableBytes(jmx, keyspace, table);
        return bytes;
    }

    private static long tableBytes(Connection jmx, String keyspace, String table) throws Exception
    {
        String prefix = METRICS + "type=Table,keyspace=" + keyspace + ",scope=" + table + ",name=";
        long bytes = ((Number) jmx.server.getAttribute(name(prefix + "TotalDiskSpaceUsed"), "Count")).longValue();
        try
        {
            return bytes + ((Number) jmx.server.getAttribute(name(prefix + "AllMemtablesLiveDataSize"), "Value")).longValue();
        }
        catch (InstanceNotFoundException e)
        {
            return bytes + ((Number) jmx.server.getAttribute(name(prefix + "AllMemtablesOnHeapDataSize"), "Value")).longValue() +
                   ((Number) jmx.server.getAttribute(name(prefix + "AllMemtablesOffHeapDataSize"), "Value")).longValue();
        }
    }

    private static ObjectName name(String name)
    {
        try
        {
            return new ObjectName(name);
        }
        catch (MalformedObjectNameException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    /** One JMX connection and the platform MXBean proxies built on it. */
    private static final class Connection
    {
        final JMXConnector connector;
        final MBeanServerConnection server;
        final MemoryMXBean memory;
        final ThreadMXBean threads;
        final RuntimeMXBean runtime;
        final HotSpotDiagnosticMXBean hotspot;
        final List<MemoryPoolMXBean> pools;
        final List<GarbageCollectorMXBean> collectors;

        Connection(JMXConnector connector) throws IOException
        {
            this.connector = connector;
            this.server = connector.getMBeanServerConnection();
            this.memory = ManagementFactory.getPlatformMXBean(server, MemoryMXBean.class);
            this.threads = ManagementFactory.getPlatformMXBean(server, ThreadMXBean.class);
            this.runtime = ManagementFactory.getPlatformMXBean(server, RuntimeMXBean.class);
            this.hotspot = ManagementFactory.getPlatformMXBean(server, HotSpotDiagnosticMXBean.class);
            this.pools = ManagementFactory.getPlatformMXBeans(server, MemoryPoolMXBean.class);
            this.collectors = ManagementFactory.getPlatformMXBeans(server, GarbageCollectorMXBean.class);
        }
    }
}
