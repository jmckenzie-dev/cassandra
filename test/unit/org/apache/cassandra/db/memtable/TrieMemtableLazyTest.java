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
package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TrieMemtableLazyTest extends CQLTester
{
    @Test
    public void factoryValidatesAndDistinguishesInitializationPolicy()
    {
        TrieMemtable.Factory defaultFactory = (TrieMemtable.Factory) TrieMemtable.factory(new HashMap<>());
        Map<String, String> explicitOptions = new HashMap<>(Map.of("lazy_initialization", "TrUe"));
        TrieMemtable.Factory explicitFactory = (TrieMemtable.Factory) TrieMemtable.factory(explicitOptions);
        TrieMemtable.Factory eagerFactory = (TrieMemtable.Factory) TrieMemtable.factory(new HashMap<>(Map.of("lazy_initialization", "false")));
        assertTrue(explicitOptions.isEmpty());
        assertEquals(defaultFactory, explicitFactory);
        assertEquals(defaultFactory.hashCode(), explicitFactory.hashCode());
        assertNotEquals(defaultFactory, eagerFactory);
        assertNotEquals(defaultFactory, TrieMemtable.factory(new HashMap<>(Map.of("shards", "2"))));
        assertTrue(defaultFactory.lazyInitialization);
        assertFalse(eagerFactory.lazyInitialization);
        for (String invalid : new String[] { "", "0", "1", "yes", " true", "false " })
            assertThatThrownBy(() -> TrieMemtable.factory(new HashMap<>(Map.of("lazy_initialization", invalid))))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("lazy_initialization must be true or false");
    }

    @Test
    public void readsAndEmptyFlushesDoNotInitialize() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        TrieMemtable memtable = current(cfs);
        assertDormant(memtable);

        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
        assertNull(memtable.rowIterator(key));
        assertNull(memtable.rowIterator(key, Slices.ALL, ColumnFilter.all(cfs.metadata()), false,
                                       SSTableReadsListener.NOOP_LISTENER));
        try (UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(cfs.metadata()),
                                                                                 DataRange.allData(cfs.getPartitioner()),
                                                                                 SSTableReadsListener.NOOP_LISTENER))
        {
            assertFalse(partitions.hasNext());
            assertEquals(cfs.metadata(), partitions.metadata());
        }

        PartitionPosition min = cfs.getPartitioner().getMinimumToken().minKeyBound();
        for (PartitionPosition[] bounds : new PartitionPosition[][] { { null, null }, { min, key }, { key, null } })
        {
            Memtable.FlushablePartitionSet<?> set = memtable.getFlushSet(bounds[0], bounds[1]);
            assertEquals(0, set.partitionCount());
            assertEquals(0, set.partitionKeysSize());
            assertFalse(set.iterator().hasNext());
            assertSame(memtable, set.memtable());
            assertSame(bounds[0], set.from());
            assertSame(bounds[1], set.to());
            assertEquals(cfs.metadata(), set.metadata());
        }

        assertEmpty(execute("SELECT * FROM %s"));
        assertEmpty(execute("SELECT * FROM %s WHERE pk = ?", 1));
        assertDormant(memtable);
        cfs.switchMemtable(ColumnFamilyStore.FlushReason.UNIT_TESTS).get(30, TimeUnit.SECONDS);
        assertNotSame(memtable, current(cfs));
        assertDormant(current(cfs));
        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void concurrentFirstWritesRetainEveryRow() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        int writers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(writers);
        try
        {
            for (int cycle = 0; cycle < 4; cycle++)
            {
                TrieMemtable memtable = current(cfs);
                assertDormant(memtable);
                CountDownLatch ready = new CountDownLatch(writers);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> writes = new ArrayList<>();
                for (int writer = 0; writer < writers; writer++)
                {
                    int value = cycle * writers + writer;
                    Mutation mutation = new RowUpdateBuilder(cfs.metadata(), value + 1L, value)
                                        .clustering(0).add("v", value).build();
                    writes.add(executor.submit(() -> {
                        ready.countDown();
                        assertTrue(start.await(30, TimeUnit.SECONDS));
                        mutation.apply();
                        return null;
                    }));
                }
                try
                {
                    assertTrue(ready.await(30, TimeUnit.SECONDS));
                }
                finally
                {
                    start.countDown();
                }
                for (Future<?> write : writes)
                    write.get(30, TimeUnit.SECONDS);
                assertTrue(memtable.isInitialized());
                assertEquals(writers, memtable.partitionCount());
                flush();
                assertDormant(current(cfs));
                for (int value = 0; value < (cycle + 1) * writers; value++)
                    assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", value), row(value, 0, value));
                assertDormant(current(cfs));
            }
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test
    public void firstWriteCanArriveAfterSwitchOut() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        TrieMemtable old = current(cfs);
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 1L, 7).clustering(0).add("v", 9).buildUpdate();
        org.apache.cassandra.utils.concurrent.Future<CommitLogPosition> flushing;
        CommitLogPosition position;
        try (CassandraWriteContext context = CassandraWriteContext.fromContext(Keyspace.open(keyspace()).getWriteHandler()
                                                                                     .beginWrite(new Mutation(update), true)))
        {
            position = context.getPosition();
            assertTrue(position != null);
            flushing = cfs.switchMemtable(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            assertNotSame(old, current(cfs));
            assertFalse(flushing.isDone());
            assertDormant(old);
            assertSame(old, cfs.getTracker().getMemtableFor(context.getGroup(), position));
            cfs.apply(update, context, true);
            assertTrue(old.isInitialized());
            assertDormant(current(cfs));
            assertTrue(old.getCommitLogLowerBound().compareTo(position) <= 0);
        }
        CommitLogPosition upper = flushing.get(30, TimeUnit.SECONDS);
        assertTrue(upper.compareTo(position) >= 0);
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", 7), row(7, 0, 9));
        assertDormant(current(cfs));
        assertFalse(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void iteratorRemainsReadableAcrossFlush() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 11)");
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
        try (OpOrder.Group read = cfs.readOrdering.start();
             UnfilteredRowIterator rows = current(cfs).rowIterator(key))
        {
            flush();
            assertDormant(current(cfs));
            int count = 0;
            while (rows.hasNext())
            {
                assertTrue(rows.next().isRow());
                count++;
            }
            assertEquals(2, count);
        }
        assertRows(execute("SELECT ck, v FROM %s WHERE pk = 1"), row(0, 10), row(1, 11));
        assertDormant(current(cfs));
    }

    @Test
    public void schemaAndTruncatePreserveLazyReplacement() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("ALTER TABLE %s ADD added int");
        assertDormant(current(cfs));
        execute("INSERT INTO %s (pk, ck, v, added) VALUES (1, 0, 2, 3)");
        flush();
        assertDormant(current(cfs));
        assertRows(execute("SELECT v, added FROM %s WHERE pk = 1"), row(2, 3));

        execute("TRUNCATE %s");
        assertDormant(current(cfs));
        assertEmpty(execute("SELECT * FROM %s"));
        execute("INSERT INTO %s (pk, ck, v, added) VALUES (2, 0, 4, 5)");
        execute("TRUNCATE %s");
        assertDormant(current(cfs));
        execute("INSERT INTO %s (pk, ck, v, added) VALUES (3, 0, 6, 7)");
        flush();
        assertRows(execute("SELECT pk, v, added FROM %s"), row(3, 6, 7));
        assertDormant(current(cfs));
    }

    @Test
    public void tombstoneOnlyFirstWriteSuppressesOlderData() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");
        assertTrue(current(cfs).isInitialized());
        assertFalse(current(cfs).isClean());
        assertEquals(20L, current(cfs).getMinTimestamp());
        flush();
        assertDormant(current(cfs));
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10) USING TIMESTAMP 10");
        assertEmpty(execute("SELECT * FROM %s WHERE pk = 1"));
        flush();
        assertEmpty(execute("SELECT * FROM %s WHERE pk = 1"));
        assertDormant(current(cfs));
    }

    private ColumnFamilyStore createTrieTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static TrieMemtable current(ColumnFamilyStore cfs)
    {
        return (TrieMemtable) cfs.getCurrentMemtable();
    }

    private static void assertDormant(TrieMemtable memtable)
    {
        assertFalse(memtable.isInitialized());
        assertTrue(memtable.isClean());
        assertEquals(0, memtable.partitionCount());
        assertEquals(0, memtable.operationCount());
        assertEquals(0, memtable.getLiveDataSize());
        assertEquals(0, memtable.partitionKeysTotalSize());
        assertEquals(0, memtable.unusedReservedMemory());
        assertEquals(Long.MAX_VALUE, memtable.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, memtable.getMinLocalDeletionTime());
        assertTrue(memtable.columns().isEmpty());
        assertEquals(EncodingStats.NO_STATS, memtable.encodingStats());
        Memtable.getMemoryUsage(memtable);
        memtable.toString();
        assertFalse(memtable.isInitialized());
    }
}
