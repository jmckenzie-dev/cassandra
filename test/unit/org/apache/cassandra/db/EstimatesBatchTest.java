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

package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.util.Map;

import com.sun.management.ThreadMXBean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link SystemKeyspace.EstimatesBatch} must write every table's estimates, replace the previous pass, and cost per
 * table the same at any table count. One write per table made a pass O(tables^2), because every write copied the
 * shared partition's whole tombstone list in the memtable.
 */
public class EstimatesBatchTest
{
    private static final int SMALL = 500;
    private static final int LARGE = 4000;

    /** The per-table path grows ~8x per table across this range; a batched pass is ~1x. */
    private static final double MAX_GROWTH = 3.0;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        assertTrue("This test requires per-thread allocation counting",
                   THREADS.isThreadAllocatedMemorySupported() && THREADS.isThreadAllocatedMemoryEnabled());
    }

    @Test
    public void batchWritesEveryTableAndReplacesThePreviousPass()
    {
        pass("est_values", 500, 1);
        pass("est_values", 500, 2);

        UntypedResultSet sizes = executeInternal("SELECT partitions_count FROM system.size_estimates WHERE keyspace_name = 'est_values'");
        assertEquals(500, sizes.size());
        sizes.forEach(row -> assertEquals(2, row.getLong("partitions_count")));

        UntypedResultSet tables = executeInternal("SELECT range_type, partitions_count FROM system.table_estimates WHERE keyspace_name = 'est_values'");
        assertEquals(1000, tables.size());
        tables.forEach(row -> assertEquals(2, row.getLong("partitions_count")));
    }

    @Test
    public void costPerTableDoesNotScaleWithTableCount()
    {
        long small = bytesPerTable("est_small", SMALL);
        long large = bytesPerTable("est_large", LARGE);
        double growth = (double) large / small;
        assertTrue(String.format("a pass allocated %,d bytes per table at %,d tables and %,d at %,d tables (%.1fx)",
                                 small, SMALL, large, LARGE, growth),
                   growth < MAX_GROWTH);
    }

    /** Measures a pass into a memtable that already holds the previous pass, as the recorder does every interval. */
    private static long bytesPerTable(String keyspace, int tables)
    {
        pass(keyspace, tables, 1);
        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        pass(keyspace, tables, 2);
        return (THREADS.getThreadAllocatedBytes(id) - start) / tables;
    }

    private static void pass(String keyspace, int tables, long count)
    {
        Map<Range<Token>, Pair<Long, Long>> estimates = Map.of(new Range<>(new LongToken(-100), new LongToken(100)), Pair.create(count, 64L));
        SystemKeyspace.EstimatesBatch batch = new SystemKeyspace.EstimatesBatch(keyspace);
        for (int i = 0; i < tables; i++)
        {
            batch.add(String.format("table_%05d", i), estimates, estimates);
            if (batch.rows() >= SizeEstimatesRecorder.ROWS_PER_MUTATION)
                batch.apply();
        }
        batch.apply();
    }
}
