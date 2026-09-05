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

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.USER_FORCED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Run in an isolated JVM: failed flushing memtables intentionally retain their allocator until shutdown. */
public class TrieMemtableRetirementFailureTest extends CQLTester
{
    @Test
    public void failedRetirementRetainsDataMemoryAndCommitLog() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 1L, 1).clustering(0).add("v", 10).buildUpdate();
        CommitLogPosition position;
        try (CassandraWriteContext context = CassandraWriteContext.fromContext(Keyspace.open(keyspace()).getWriteHandler()
                                                                                     .beginWrite(new Mutation(update), true)))
        {
            position = context.getPosition();
            cfs.apply(update, context, true);
        }
        CommitLogSegment segment = CommitLog.instance.segmentManager.getActiveSegments().stream()
                                                    .filter(candidate -> candidate.id == position.segmentId)
                                                    .findFirst().orElseThrow();
        assertTrue(segment.getDirtyTableIds().contains(cfs.metadata().id));
        TrieMemtable old = (TrieMemtable) cfs.getCurrentMemtable();
        long owned = old.getAllocator().onHeap().owns() + old.getAllocator().offHeap().owns();
        assertTrue(owned > 0);

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Directories.DataDirectory[] dataDirectories = cfs.getDirectories().getWriteableLocations();
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.ignore);
            try (Closeable directories = Util.markDirectoriesUnwriteable(cfs))
            {
                Future<CommitLogPosition> retiring;
                try (OpOrder.Group write = Keyspace.writeOrder.start())
                {
                    retiring = cfs.forceFlush(USER_FORCED);
                    assertFalse(retiring.isDone());
                    // Let switching finish, then fail disk selection while the writer waits on this group.
                    for (Directories.DataDirectory directory : dataDirectories)
                        DisallowedDirectories.maybeMarkUnwritable(directory.location);
                }
                try
                {
                    retiring.get(30, TimeUnit.SECONDS);
                    fail("Retirement must report the flush failure");
                }
                catch (ExecutionException e)
                {
                    Throwable failure = e;
                    while (!(failure instanceof FSWriteError) && failure.getCause() != null)
                        failure = failure.getCause();
                    assertTrue("Expected a flush write failure: " + failure, failure instanceof FSWriteError);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }

        assertTrue(cfs.getTracker().getView().flushingMemtables.contains(old));
        assertEquals(owned, old.getAllocator().onHeap().owns() + old.getAllocator().offHeap().owns());
        assertFalse(old.isClean());
        assertTrue(cfs.getLiveSSTables().isEmpty());
        TrieMemtable replacement = (TrieMemtable) cfs.getCurrentMemtable();
        assertNotSame(old, replacement);
        assertFalse(replacement.isInitialized());
        assertTrue(replacement.isClean());
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 0, 10));
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 0, 10));
        assertFalse(replacement.isInitialized());
        assertTrue(CommitLog.instance.segmentManager.getActiveSegments().contains(segment));
        assertTrue(segment.getDirtyTableIds().contains(cfs.metadata().id));
    }
}
