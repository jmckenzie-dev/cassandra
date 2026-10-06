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

package org.apache.cassandra.tcm.transformations;

import java.lang.management.ManagementFactory;

import com.sun.management.ThreadMXBean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * One CREATE TABLE through {@link AlterSchema#execute} must not cost in proportion to the tables already present.
 *
 * <p>execute runs inside the serial metadata commit, so per-DDL O(tables) work there caps DDL throughput and, at
 * about 49k tables in one keyspace, timed commits out. It used to rebuild the keyspace's {@link Tables}, check
 * the epoch of every table and hash every keyspace to group them by replication. Like {@code TablesScalingTest}, this asserts allocation growth.
 */
public class AlterSchemaScalingTest
{
    private static final int SMALL = 400;
    private static final int LARGE = 3200;

    /** An O(n) pass allocates ~8x more at LARGE; the diff-only path is ~1x. */
    private static final double MAX_GROWTH = 3.0;

    private static final int WARMUP = 10;
    private static final int ITERATIONS = 20;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @SuppressWarnings("unused")
    private static volatile Object sink;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
        assertTrue("This test requires per-thread allocation counting",
                   THREADS.isThreadAllocatedMemorySupported() && THREADS.isThreadAllocatedMemoryEnabled());
    }

    @Test
    public void createTableDoesNotScaleWithExistingTables()
    {
        long small = bytesPerExecute(metadataWithTables(SMALL), SMALL);
        long large = bytesPerExecute(metadataWithTables(LARGE), LARGE);
        double growth = (double) large / small;
        assertTrue(String.format("execute allocated %,d bytes at %,d tables and %,d bytes at %,d tables (%.1fx)",
                                 small, SMALL, large, LARGE, growth),
                   growth < MAX_GROWTH);
    }

    /**
     * Allocation misses walks that only look up: Keyspaces.diff and Keyspaces.withAddedOrUpdated each did a BTree
     * lookup per table, about 90 ms per CREATE TABLE at 41k tables under the TCM commit lock. CPU time is noisier
     * than allocation, so the table counts differ 16x (an O(n) walk is ~16x or more) and the bar is 4x.
     */
    @Test
    public void createTableCpuDoesNotScaleWithExistingTables()
    {
        long small = cpuPerExecute(metadataWithTables(1000), 1000);
        long large = cpuPerExecute(metadataWithTables(16000), 16000);
        double growth = (double) large / small;
        assertTrue(String.format("execute used %,d ns of CPU at 1,000 tables and %,d ns at 16,000 tables (%.1fx)",
                                 small, large, growth),
                   growth < 4.0);
    }

    private static long cpuPerExecute(ClusterMetadata prev, int newTable)
    {
        AlterSchema alter = new AlterSchema(new AddTable(newTable));
        for (int i = 0; i < 200; i++)
            sink = alter.execute(prev);

        // The fastest of several batches, so a GC or a scheduler pause in one batch does not count.
        long best = Long.MAX_VALUE;
        for (int batch = 0; batch < 5; batch++)
        {
            long start = THREADS.getCurrentThreadCpuTime();
            for (int i = 0; i < 50; i++)
                sink = alter.execute(prev);
            best = Math.min(best, (THREADS.getCurrentThreadCpuTime() - start) / 50);
        }
        return best;
    }

    @Test
    public void createTableKeepsExistingTablesAndSetsTheEpoch()
    {
        ClusterMetadata prev = metadataWithTables(50);
        Transformation.Result result = new AlterSchema(new AddTable(50)).execute(prev);
        assertTrue(result.toString(), result.isSuccess());

        KeyspaceMetadata ks = result.success().metadata.schema.getKeyspaces().getNullable("ks");
        assertEquals(51, ks.tables.size());
        for (int i = 0; i < 50; i++)
            assertNotNull("table_" + i, ks.tables.getNullable("table_" + i));
        assertEquals(prev.nextEpoch(), ks.tables.getNullable("table_50").epoch);
    }

    private static long bytesPerExecute(ClusterMetadata prev, int newTable)
    {
        AlterSchema alter = new AlterSchema(new AddTable(newTable));
        for (int i = 0; i < WARMUP; i++)
            sink = alter.execute(prev);

        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        for (int i = 0; i < ITERATIONS; i++)
            sink = alter.execute(prev);
        return (THREADS.getThreadAllocatedBytes(id) - start) / ITERATIONS;
    }

    private static ClusterMetadata metadataWithTables(int count)
    {
        Tables.Builder tables = Tables.builder();
        for (int i = 0; i < count; i++)
            tables.add(table(i));
        KeyspaceMetadata ks = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), tables.build());
        ClusterMetadata empty = new ClusterMetadata(Murmur3Partitioner.instance);
        return empty.transformer().with(new DistributedSchema(Keyspaces.of(ks))).build().metadata;
    }

    private static TableMetadata table(int i)
    {
        return TableMetadata.builder("ks", "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }

    private static class AddTable implements SchemaTransformation
    {
        private final TableMetadata table;

        AddTable(int i)
        {
            table = table(i);
        }

        @Override
        public Keyspaces apply(ClusterMetadata metadata)
        {
            Keyspaces keyspaces = metadata.schema.getKeyspaces();
            KeyspaceMetadata ks = keyspaces.getNullable("ks");
            return keyspaces.withAddedOrUpdated(ks.withSwapped(ks.tables.with(table)));
        }

        @Override
        public boolean compatibleWith(ClusterMetadata metadata)
        {
            return true;
        }
    }
}
