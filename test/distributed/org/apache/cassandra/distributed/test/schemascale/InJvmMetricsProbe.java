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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.management.MBeanServer;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Samples node 1 of an in-JVM cluster. The instance runs in its own class loader, so the lambdas
 * sent to it return only primitive arrays.
 */
public final class InJvmMetricsProbe implements MetricsProbe
{
    private final Cluster cluster;

    public InJvmMetricsProbe(Cluster cluster)
    {
        this.cluster = cluster;
    }

    @Override
    public Map<String, Long> sample(boolean withSnapshotSize)
    {
        long[] values = cluster.get(1).callOnInstance(sampleTask(withSnapshotSize));
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < NodeSide.KEYS.length; i++)
            result.put(NodeSide.KEYS[i], values[i]);
        return result;
    }

    @Override
    public void gc()
    {
        ManagementFactory.getMemoryMXBean().gc();
    }

    @Override
    public long systemSchemaBytes()
    {
        return cluster.get(1).callOnInstance(systemSchemaTask());
    }

    private static IIsolatedExecutor.SerializableCallable<long[]> sampleTask(boolean withSnapshotSize)
    {
        return () -> NodeSide.sample(withSnapshotSize);
    }

    private static IIsolatedExecutor.SerializableCallable<Long> systemSchemaTask()
    {
        return () -> NodeSide.keyspaceBytes(SchemaConstants.SCHEMA_KEYSPACE_NAME);
    }

    /** Runs inside the instance class loader. Touches only JDK types and node internals. */
    static final class NodeSide
    {
        static final String[] KEYS = {
            "heap_used", "heap_max", "heap_post_gc", "direct_max", "metaspace_used", "gc_count", "gc_time_ms",
            "threads", "alloc_bytes_total", "mbeans", "scheduled_tasks", "node_keyspaces", "node_tables",
            "tcm_epoch", "tcm_commit_p99_ns", "snapshot_bytes_computed", "snapshot_bytes_gauge",
            "snapshot_store_failures", "max_mutation_size", "snapshot_size_limit", "system_schema_bytes", "metadata_log_bytes" };

        private static final Map<String, Integer> INDEX = new HashMap<>();

        static
        {
            for (int i = 0; i < KEYS.length; i++)
                INDEX.put(KEYS[i], i);
        }

        private NodeSide() {}

        static long[] sample(boolean withSnapshotSize)
        {
            long[] values = new long[KEYS.length];
            Arrays.fill(values, -1L);
            jvm(values);
            node(values);
            tcm(values, withSnapshotSize);
            storage(values);
            return values;
        }

        private static void put(long[] values, String key, long value)
        {
            values[INDEX.get(key)] = value;
        }

        private static void jvm(long[] values)
        {
            try
            {
                MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
                put(values, "heap_used", heap.getUsed());
                put(values, "heap_max", heap.getMax());
                long postGc = 0;
                boolean collected = false;
                long metaspace = -1;
                for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans())
                {
                    if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null)
                    {
                        postGc += pool.getCollectionUsage().getUsed();
                        collected = true;
                    }
                    if (pool.getName().equals("Metaspace"))
                        metaspace = pool.getUsage().getUsed();
                }
                put(values, "heap_post_gc", collected ? postGc : heap.getUsed());
                put(values, "metaspace_used", metaspace);
                long direct = Long.parseLong(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                                                              .getVMOption("MaxDirectMemorySize").getValue());
                put(values, "direct_max", direct > 0 ? direct : heap.getMax());
                long count = 0;
                long time = 0;
                for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
                {
                    count += Math.max(0, gc.getCollectionCount());
                    time += Math.max(0, gc.getCollectionTime());
                }
                put(values, "gc_count", count);
                put(values, "gc_time_ms", time);
                put(values, "threads", ManagementFactory.getThreadMXBean().getThreadCount());
                put(values, "alloc_bytes_total",
                    ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean()).getTotalThreadAllocatedBytes());
            }
            catch (Throwable t)
            {
                // leave the remaining keys at -1
            }
        }

        private static void node(long[] values)
        {
            try
            {
                MBeanServer server = MBeanWrapper.instance.getMBeanServer();
                if (server == null)
                    server = ManagementFactory.getPlatformMBeanServer();
                put(values, "mbeans", server.getMBeanCount());
                put(values, "scheduled_tasks",
                    (long) ScheduledExecutors.scheduledTasks.getPendingTaskCount() +
                    ScheduledExecutors.optionalTasks.getPendingTaskCount() +
                    ScheduledExecutors.nonPeriodicTasks.getPendingTaskCount() +
                    ScheduledExecutors.scheduledFastTasks.getPendingTaskCount());
                long tables = 0;
                int keyspaces = 0;
                for (KeyspaceMetadata ksm : ClusterMetadata.current().schema.getKeyspaces())
                {
                    keyspaces++;
                    tables += ksm.tables.size();
                }
                put(values, "node_keyspaces", keyspaces);
                put(values, "node_tables", tables);
            }
            catch (Throwable t)
            {
                // leave the remaining keys at -1
            }
        }

        private static void tcm(long[] values, boolean withSnapshotSize)
        {
            try
            {
                ClusterMetadata current = ClusterMetadata.current();
                put(values, "tcm_epoch", current.epoch.getEpoch());
                TCMMetrics metrics = TCMMetrics.instance;
                put(values, "tcm_commit_p99_ns", (long) metrics.commitSuccessLatency.getSnapshot().get99thPercentile());
                put(values, "snapshot_bytes_gauge", metrics.lastSnapshotSize.getValue());
                put(values, "snapshot_store_failures", metrics.snapshotStoreFailures.getCount());
                put(values, "max_mutation_size", DatabaseDescriptor.getMaxMutationSize());
                put(values, "snapshot_size_limit", Math.min(DatabaseDescriptor.getMaxValueSize(), DatabaseDescriptor.getInternodeMaxMessageSizeInBytes()));
                if (withSnapshotSize)
                    put(values, "snapshot_bytes_computed",
                        VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, current,
                                                                 Version.minCommonSerializationVersion()));
            }
            catch (Throwable t)
            {
                // leave the remaining keys at -1
            }
        }

        private static void storage(long[] values)
        {
            try
            {
                put(values, "system_schema_bytes", keyspaceBytes(SchemaConstants.SCHEMA_KEYSPACE_NAME));
                put(values, "metadata_log_bytes",
                    keyspaceBytes(SchemaConstants.METADATA_KEYSPACE_NAME) +
                    tableBytes(SchemaConstants.SYSTEM_KEYSPACE_NAME, "local_metadata_log"));
            }
            catch (Throwable t)
            {
                // leave the remaining keys at -1
            }
        }

        static long keyspaceBytes(String keyspace)
        {
            long bytes = 0;
            for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
                bytes += cfs.metric.totalDiskSpaceUsed.getCount() + cfs.metric.allMemtablesLiveDataSize.getValue();
            return bytes;
        }

        private static long tableBytes(String keyspace, String table)
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            return cfs.metric.totalDiskSpaceUsed.getCount() + cfs.metric.allMemtablesLiveDataSize.getValue();
        }
    }
}
