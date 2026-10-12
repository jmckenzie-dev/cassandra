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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link SystemKeyspaceStorage#getEntries(Epoch)} serves the entries this instance appended from memory. The result
 * must equal what a storage without that memory (a fresh instance, as after a restart) reads from the table.
 */
public class SystemKeyspaceStorageTailTest
{
    private MetadataSnapshots snapshots = MetadataSnapshots.NO_OP;
    private final List<Entry> appended = new ArrayList<>();

    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @Before
    public void setup()
    {
        truncate();
    }

    @After
    public void tearDown()
    {
        truncate();
    }

    private static void truncate()
    {
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.METADATA_LOG).truncateBlockingWithoutSnapshot();
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME).truncateBlockingWithoutSnapshot();
    }

    private SystemKeyspaceStorage newStorage()
    {
        return new SystemKeyspaceStorage(() -> snapshots);
    }

    private void append(SystemKeyspaceStorage storage, long from, long to)
    {
        for (long e = from; e <= to; e++)
        {
            Entry entry = new Entry(new Entry.Id(e), Epoch.create(e), CustomTransformation.make((int) e));
            appended.add(entry);
            storage.append(entry);
        }
    }

    private Entry appendedAt(long epoch)
    {
        return appended.stream().filter(e -> e.epoch.getEpoch() == epoch).findFirst().get();
    }

    private static List<Entry> entries(LogReader.EntryHolder holder)
    {
        List<Entry> list = new ArrayList<>();
        holder.iterator().forEachRemaining(list::add);
        return list;
    }

    private static void assertSameEpochsAndIds(List<Entry> expected, List<Entry> actual)
    {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(expected.get(i).epoch, actual.get(i).epoch);
            assertEquals(expected.get(i).id, actual.get(i).id);
            assertEquals(expected.get(i).transform.kind(), actual.get(i).transform.kind());
        }
    }

    @Test
    public void continuousLogMatchesTableAndIsServedFromMemory() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage();
        append(storage, 1, 50);
        SystemKeyspaceStorage restarted = newStorage();

        for (long since = 0; since <= 50; since++)
        {
            List<Entry> fromTail = entries(storage.getEntries(Epoch.create(since)));
            assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(since))), fromTail);
            if (since < 49)
                assertSame("since " + since + " should come from the tail", appendedAt(since + 1), fromTail.get(0));
        }
    }

    @Test
    public void restartedStorageReadsTheTable() throws IOException
    {
        append(newStorage(), 1, 10);
        List<Entry> entries = entries(newStorage().getEntries(Epoch.create(4)));
        assertEquals(6, entries.size());
        assertNotSame(appendedAt(5), entries.get(0));
    }

    @Test
    public void truncateDropsTheTail() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage();
        append(storage, 1, 10);
        storage.truncate();
        assertTrue(entries(storage.getEntries(Epoch.create(2))).isEmpty());
    }

    @Test
    public void sinceOlderThanTheTailReadsTheTable() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage();
        append(storage, 1, 1100);
        SystemKeyspaceStorage restarted = newStorage();

        // The tail holds the last 1024 entries: 77..1100
        List<Entry> older = entries(storage.getEntries(Epoch.create(75)));
        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(75))), older);
        assertEquals(1025, older.size());
        assertNotSame(appendedAt(76), older.get(0));

        List<Entry> oldest = entries(storage.getEntries(Epoch.create(76)));
        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(76))), oldest);
        assertSame(appendedAt(77), oldest.get(0));

        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.EMPTY)), entries(storage.getEntries(Epoch.EMPTY)));
    }

    @Test
    public void gapStartsANewTailAndFallsBackAcrossIt() throws IOException
    {
        // Epochs 11..49 are not appended, as after catching up via a ForceSnapshot
        SystemKeyspaceStorage storage = newStorage();
        append(storage, 1, 10);
        append(storage, 50, 60);
        SystemKeyspaceStorage restarted = newStorage();

        List<Entry> acrossGap = entries(storage.getEntries(Epoch.create(3)));
        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(3))), acrossGap);
        assertEquals(7 + 11, acrossGap.size());
        assertNotSame(appendedAt(50), acrossGap.get(7));

        List<Entry> afterGap = entries(storage.getEntries(Epoch.create(49)));
        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(49))), afterGap);
        assertSame(appendedAt(50), afterGap.get(0));
    }

    @Test
    public void logStateWithSnapshotBeyondTheEntriesMatchesTheTable() throws IOException
    {
        ClusterMetadata snapshot = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(50), Murmur3Partitioner.instance);
        snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
        snapshots.storeSnapshot(snapshot);
        // Entries 1..10, then a ForceSnapshot to 50 that is not in the log, then 51..60
        SystemKeyspaceStorage storage = newStorage();
        append(storage, 1, 10);
        append(storage, 51, 60);
        SystemKeyspaceStorage restarted = newStorage();

        for (long since : new long[]{ 3, 9, 10, 50, 55 })
        {
            LogState expected = restarted.getLogState(Epoch.create(since), true);
            LogState actual = storage.getLogState(Epoch.create(since), true);
            assertEquals("base state since " + since, expected.baseState, actual.baseState);
            assertSameEpochsAndIds(expected.entries, actual.entries);
        }

        // The snapshot at 50 is beyond the continuous entries 4..10, so neither storage may return them as complete
        assertThrowsIllegalState(() -> restarted.getLogState(Epoch.create(3), false));
        assertThrowsIllegalState(() -> storage.getLogState(Epoch.create(3), false));
    }

    private static void assertThrowsIllegalState(Runnable r)
    {
        try
        {
            r.run();
        }
        catch (IllegalStateException e)
        {
            return;
        }
        fail("expected IllegalStateException");
    }

    private void appendSized(SystemKeyspaceStorage storage, long epoch, int bytes)
    {
        Entry entry = new Entry(new Entry.Id(epoch), Epoch.create(epoch), CustomTransformation.make("x".repeat(bytes)));
        appended.add(entry);
        storage.append(entry);
    }

    @Test
    public void entryAboveTheByteBoundIsNotServedFromTheTail() throws IOException
    {
        SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshots, 200 * 1024);
        append(storage, 1, 5);
        appendSized(storage, 6, 300 * 1024);
        append(storage, 7, 9);
        SystemKeyspaceStorage restarted = newStorage();

        // The oversized entry is not kept and ends the run, so only 7..9 are in the tail
        for (long since : new long[]{ 0, 4, 5, 6, 8 })
            assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(since))), entries(storage.getEntries(Epoch.create(since))));
        assertNotSame(appendedAt(1), entries(storage.getEntries(Epoch.EMPTY)).get(0));
        assertNotSame(appendedAt(6), entries(storage.getEntries(Epoch.create(5))).get(0));
        assertSame(appendedAt(7), entries(storage.getEntries(Epoch.create(6))).get(0));
    }

    @Test
    public void mediumEntriesTrimTheFrontToKeepTheTotalUnderTheBound() throws IOException
    {
        long bound = 200 * 1024;
        SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshots, bound);
        for (long e = 1; e <= 30; e++)
        {
            appendSized(storage, e, 50 * 1024);
            assertTrue(storage.tailBytes() <= bound);
        }
        SystemKeyspaceStorage restarted = newStorage();

        // 50 KB entries: the last three fit
        assertSame(appendedAt(30), entries(storage.getEntries(Epoch.create(29))).get(0));
        assertSame(appendedAt(28), entries(storage.getEntries(Epoch.create(27))).get(0));
        List<Entry> older = entries(storage.getEntries(Epoch.create(10)));
        assertSameEpochsAndIds(entries(restarted.getEntries(Epoch.create(10))), older);
        assertNotSame(appendedAt(11), older.get(0));
    }

    @Test
    public void concurrentAppendsNeverShowAGap() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage();
        AtomicLong lastAppended = new AtomicLong(0);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try
            {
                while (!done.get())
                {
                    long since = Math.max(0, lastAppended.get() - 5);
                    long expectedNext = since + 1;
                    for (Entry entry : entries(storage.getEntries(Epoch.create(since))))
                    {
                        assertEquals(expectedNext, entry.epoch.getEpoch());
                        assertEquals(expectedNext, entry.id.entryId);
                        expectedNext++;
                    }
                }
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        reader.start();
        // 2500 appends trim the front of the tail while the reader runs
        for (long e = 1; e <= 2500; e++)
        {
            storage.append(new Entry(new Entry.Id(e), Epoch.create(e), CustomTransformation.make((int) e)));
            lastAppended.set(e);
        }
        done.set(true);
        reader.join();
        if (failure.get() != null)
            throw new AssertionError(failure.get());
    }
}
