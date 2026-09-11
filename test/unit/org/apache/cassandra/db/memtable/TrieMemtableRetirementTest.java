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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.USER_FORCED;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TrieMemtableRetirementTest extends CQLTester
{
    @Test
    public void cleanRetirementDoesNotSwitchOrCreateSSTables() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        TrieMemtable empty = current(cfs);
        for (int attempt = 0; attempt < 3; attempt++)
        {
            retire(cfs);
            assertSame(empty, current(cfs));
            assertDormant(empty);
            assertTrue(cfs.getLiveSSTables().isEmpty());
        }

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10)");
        retire(cfs);
        assertReclaimed(empty);
        TrieMemtable replacement = current(cfs);
        assertNotSame(empty, replacement);
        assertEquals(1, cfs.getLiveSSTables().size());
        for (int attempt = 0; attempt < 3; attempt++)
        {
            retire(cfs);
            assertSame(replacement, current(cfs));
            assertDormant(replacement);
            assertEquals(1, cfs.getLiveSSTables().size());
        }
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 0, 10));
        assertDormant(replacement);
    }

    @Test
    public void dirtyRetirementReclaimsStorageAndReactivatesOnWrite() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10)");
        TrieMemtable old = current(cfs);
        assertTrue(ownedBytes(old) > 0);

        retire(cfs);
        assertReclaimed(old);
        TrieMemtable replacement = current(cfs);
        assertNotSame(old, replacement);
        assertDormant(replacement);
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 0, 10));
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 0, 10));
        assertDormant(replacement);

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 11)");
        assertSame(replacement, current(cfs));
        assertTrue(replacement.isInitialized());
        assertTrue(ownedBytes(replacement) > 0);
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 0, 10), row(1, 1, 11));
        retire(cfs);
        assertReclaimed(replacement);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 0, 10), row(1, 1, 11));
        assertDormant(current(cfs));
    }

    @Test
    public void readerPinsStorageAfterRetirementCompletes() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 11)");
        TrieMemtable old = current(cfs);
        long owned = ownedBytes(old);
        assertTrue(owned > 0);
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
        try (OpOrder.Group read = cfs.readOrdering.start();
             UnfilteredRowIterator rows = old.rowIterator(key))
        {
            retire(cfs);
            assertDormant(current(cfs));
            assertEquals(1, cfs.getLiveSSTables().size());
            assertEquals("The outstanding reader must prevent reclamation", owned, ownedBytes(old));
            int count = 0;
            while (rows.hasNext())
            {
                assertTrue(rows.next().isRow());
                count++;
            }
            assertEquals(2, count);
        }
        assertReclaimed(old);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 0, 10), row(1, 1, 11));
        assertDormant(current(cfs));
    }

    @Test
    public void acceptedWriterAndReplacementWriterSurviveRetirement() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 10)");
        TrieMemtable old = current(cfs);
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 1L, 1).clustering(0).add("v", 11).buildUpdate();
        Future<CommitLogPosition> retiring;
        Future<CommitLogPosition> repeated;
        CommitLogPosition position;
        TrieMemtable replacement;
        try (CassandraWriteContext context = CassandraWriteContext.fromContext(Keyspace.open(keyspace()).getWriteHandler()
                                                                                     .beginWrite(new Mutation(update), true)))
        {
            position = context.getPosition();
            retiring = cfs.forceFlush(USER_FORCED);
            replacement = current(cfs);
            assertNotSame(old, replacement);
            assertDormant(replacement);
            assertFalse(retiring.isDone());
            assertSame(old, cfs.getTracker().getMemtableFor(context.getGroup(), position));

            repeated = cfs.forceFlush(USER_FORCED);
            assertFalse(repeated.isDone());
            assertSame(replacement, current(cfs));

            new RowUpdateBuilder(cfs.metadata(), 2L, 2).clustering(0).add("v", 12).build().apply();
            assertTrue(replacement.isInitialized());
            assertEquals(1, replacement.partitionCount());
            cfs.apply(update, context, true);
            assertEquals(2, old.partitionCount());
            assertFalse(retiring.isDone());
        }

        assertTrue(retiring.get(30, TimeUnit.SECONDS).compareTo(position) >= 0);
        assertTrue(repeated.get(30, TimeUnit.SECONDS).compareTo(position) >= 0);
        assertReclaimed(old);
        assertSame(replacement, current(cfs));
        assertFalse(replacement.isClean());
        for (int key = 0; key < 3; key++)
            assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", key), row(key, 0, key + 10));
        retire(cfs);
        assertReclaimed(replacement);
        for (int key = 0; key < 3; key++)
            assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", key), row(key, 0, key + 10));
        assertDormant(current(cfs));
    }

    @Test
    public void retirementFlushesDirtyIndexWhenBaseIsClean() throws Throwable
    {
        ColumnFamilyStore cfs = createTrieTable();
        createIndex("CREATE INDEX ON %s (v) USING 'legacy_local_table'");
        ColumnFamilyStore index = Iterables.getOnlyElement(cfs.indexManager.getAllIndexColumnFamilyStores());
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 10)");
        retire(cfs);
        execute("DELETE FROM %s WHERE pk = 1");
        retire(cfs);
        assertDormant(current(cfs));
        assertTrue(index.getCurrentMemtable().isClean());

        // Reading the stale legacy index entry writes its deletion into the index memtable.
        assertEmpty(execute("SELECT pk, ck, v FROM %s WHERE v = 10"));
        assertDormant(current(cfs));
        AbstractAllocatorMemtable dirtyIndex = (AbstractAllocatorMemtable) index.getCurrentMemtable();
        assertFalse(dirtyIndex.isClean());
        assertTrue(ownedBytes(dirtyIndex) > 0);
        TrieMemtable cleanBase = current(cfs);

        retire(cfs);
        assertNotSame(cleanBase, current(cfs));
        assertNotSame(dirtyIndex, index.getCurrentMemtable());
        assertReclaimed(dirtyIndex);
        assertDormant(current(cfs));
        assertTrue(index.getCurrentMemtable().isClean());
        assertEmpty(execute("SELECT pk, ck, v FROM %s WHERE v = 10"));
        assertTrue(index.getCurrentMemtable().isClean());
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

    private static void retire(ColumnFamilyStore cfs) throws Exception
    {
        cfs.forceFlush(USER_FORCED).get(30, TimeUnit.SECONDS);
    }

    private static void assertDormant(TrieMemtable memtable)
    {
        assertFalse(memtable.isInitialized());
        assertTrue(memtable.isClean());
        assertEquals(0, ownedBytes(memtable));
    }

    private static long ownedBytes(AbstractAllocatorMemtable memtable)
    {
        return memtable.getAllocator().onHeap().owns() + memtable.getAllocator().offHeap().owns();
    }

    private static void assertReclaimed(AbstractAllocatorMemtable memtable)
    {
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(0, ownedBytes(memtable)));
    }
}
