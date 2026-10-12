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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SystemKeyspaceStorageJournalTest
{
    private static final long TAIL_BYTES = 16 * 1024 * 1024;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private MetadataLogJournalTest.RecordingChannels channels;
    private Path journalDir;
    private Config.MetadataLogDurability originalDurability;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @Before
    public void setup() throws IOException
    {
        originalDurability = DatabaseDescriptor.getMetadataLogDurability();
        DatabaseDescriptor.setMetadataLogDurability(Config.MetadataLogDurability.journal);
        journalDir = folder.newFolder().toPath().resolve("system").resolve("metadata-log-journal");
        channels = new MetadataLogJournalTest.RecordingChannels();
        truncateTable();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setMetadataLogDurability(originalDurability);
        truncateTable();
    }

    private static ColumnFamilyStore table()
    {
        return ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.METADATA_LOG);
    }

    private static void truncateTable()
    {
        table().truncateBlockingWithoutSnapshot();
    }

    private MetadataLogJournal newJournal()
    {
        return new MetadataLogJournal(() -> journalDir, channels);
    }

    private SystemKeyspaceStorage newStorage(Supplier<Future<?>> flush, long delayMs)
    {
        return new SystemKeyspaceStorage(() -> MetadataSnapshots.NO_OP, TAIL_BYTES, newJournal(), delayMs, flush);
    }

    // Flushes never run on their own: a test calls flushJournal itself
    private SystemKeyspaceStorage newStorage(Supplier<Future<?>> flush)
    {
        return newStorage(flush, TimeUnit.HOURS.toMillis(1));
    }

    private static Future<?> realFlush()
    {
        return table().forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private static Entry entry(long epoch)
    {
        return new Entry(new Entry.Id(epoch), Epoch.create(epoch), CustomTransformation.make((int) epoch));
    }

    private static void append(SystemKeyspaceStorage storage, long from, long to)
    {
        for (long e = from; e <= to; e++)
            storage.append(entry(e));
    }

    private static long switches()
    {
        return table().metric.memtableSwitchCount.getCount();
    }

    private static List<Long> tableEpochs() throws IOException
    {
        List<Long> epochs = new ArrayList<>();
        SystemKeyspaceStorage reader = new SystemKeyspaceStorage(() -> MetadataSnapshots.NO_OP);
        reader.getEntries(Epoch.EMPTY).iterator().forEachRemaining(e -> epochs.add(e.epoch.getEpoch()));
        Collections.sort(epochs);
        return epochs;
    }

    private List<Path> journalFiles() throws IOException
    {
        return newJournal().read(false).files;
    }

    @Test
    public void journalModeDoesNotFlushPerEntryAndForcesOncePerEntry() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 1);
        long before = switches();
        int eventsBefore = channels.events.size();

        append(storage, 2, 21);

        assertEquals("no flush per entry", before, switches());
        List<String> events = channels.events.subList(eventsBefore, channels.events.size());
        assertEquals("one write and one force per entry", 40, events.size());
        for (int i = 0; i < events.size(); i += 2)
        {
            assertEquals("write", events.get(i));
            assertEquals("force", events.get(i + 1));
        }
        assertEquals(1, journalFiles().size());
        assertEquals(21, newJournal().read(false).records.size());
    }

    @Test
    public void flushModeFlushesEveryEntryAndWritesNoJournal() throws IOException
    {
        DatabaseDescriptor.setMetadataLogDurability(Config.MetadataLogDurability.flush);
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        long before = switches();

        append(storage, 1, 5);

        assertTrue("a flush per entry", switches() - before >= 5);
        assertTrue(channels.events.isEmpty());
        assertFalse(Files.exists(journalDir));
    }

    @Test
    public void failedInsertThrowsAndWritesNoRecord() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 1);
        int events = channels.events.size();

        String tooBig = "x".repeat(DatabaseDescriptor.getMaxMutationSize() + 1024);
        Entry big = new Entry(new Entry.Id(2), Epoch.create(2), CustomTransformation.make(tooBig));
        try
        {
            storage.append(big);
            fail("append must throw when the INSERT fails");
        }
        catch (MutationExceededMaxSizeException expected)
        {
            // expected
        }

        assertEquals("nothing written to the journal", events, channels.events.size());
        assertEquals(1, newJournal().read(false).records.size());
        append(storage, 3, 3); // a failed INSERT does not fail the journal
        assertEquals(2, newJournal().read(false).records.size());
    }

    @Test
    public void journalFailureThrowsAndStaysFailed() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 1);
        channels.failNext = "force";
        try
        {
            storage.append(entry(2));
            fail();
        }
        catch (FSWriteError expected)
        {
            // expected
        }
        int events = channels.events.size();
        try
        {
            storage.append(entry(3));
            fail();
        }
        catch (FSWriteError expected)
        {
            // expected
        }
        assertEquals(events, channels.events.size());
    }

    @Test
    public void recoveryInsertsFlushesAndDeletesTheFiles() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 10);
        assertEquals(1, journalFiles().size());
        truncateTable(); // the crash: the memtable is gone, the journal file is not
        assertTrue(tableEpochs().isEmpty());

        long before = switches();
        newStorage(SystemKeyspaceStorageJournalTest::realFlush).recoverJournal();

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), tableEpochs());
        assertTrue("recovery flushes the table", switches() > before);
        assertTrue(table().getLiveSSTables().size() > 0);
        assertTrue(journalFiles().isEmpty());
    }

    @Test
    public void recoveryRunsInFlushModeToo() throws Exception
    {
        append(newStorage(SystemKeyspaceStorageJournalTest::realFlush), 1, 3);
        truncateTable();
        DatabaseDescriptor.setMetadataLogDurability(Config.MetadataLogDurability.flush);

        newStorage(SystemKeyspaceStorageJournalTest::realFlush).recoverJournal();

        assertEquals(List.of(1L, 2L, 3L), tableEpochs());
        assertTrue(journalFiles().isEmpty());
    }

    @Test
    public void recoveryOfFilesThatAFlushAlreadyCoveredIsIdempotent() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 5);
        // Crash between the flush and the delete: the files stay, and their entries are already in sstables
        table().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(1, journalFiles().size());

        newStorage(SystemKeyspaceStorageJournalTest::realFlush).recoverJournal();
        newStorage(SystemKeyspaceStorageJournalTest::realFlush).recoverJournal();

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), tableEpochs());
        assertTrue(journalFiles().isEmpty());
    }

    @Test
    public void recoveryWithNoJournalDoesNothing() throws Exception
    {
        long before = switches();
        newStorage(SystemKeyspaceStorageJournalTest::realFlush).recoverJournal();
        assertEquals(before, switches());
        assertFalse(Files.exists(journalDir));
    }

    @Test
    public void flushDeletesTheFilesItClosed() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 5);
        List<Path> first = journalFiles();

        storage.flushJournal().get(30, TimeUnit.SECONDS);
        assertTrue(journalFiles().isEmpty());
        assertEquals(1, first.size());

        append(storage, 6, 6); // opens a new file
        assertEquals(1, journalFiles().size());
        assertFalse(first.get(0).equals(journalFiles().get(0)));
    }

    @Test
    public void failedFlushKeepsTheFiles() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage(() -> ImmediateFuture.failure(new RuntimeException("flush failed")));
        append(storage, 1, 5);

        try
        {
            storage.flushJournal().get(30, TimeUnit.SECONDS);
            fail();
        }
        catch (ExecutionException expected)
        {
            assertEquals("flush failed", expected.getCause().getMessage());
        }
        assertEquals(1, journalFiles().size());
        assertEquals(5, newJournal().read(false).records.size());
    }

    @Test
    public void overlappingFlushesDeleteOnlyTheirOwnFiles() throws Exception
    {
        List<AsyncPromise<CommitLogPosition>> flushes = new ArrayList<>();
        AtomicInteger started = new AtomicInteger();
        SystemKeyspaceStorage storage = newStorage(() -> {
            AsyncPromise<CommitLogPosition> promise = new AsyncPromise<>();
            flushes.add(promise);
            started.incrementAndGet();
            return promise;
        });

        append(storage, 1, 2);
        Future<?> a = storage.flushJournal(); // closes file 1
        append(storage, 3, 4);
        Future<?> b = storage.flushJournal(); // closes file 2
        append(storage, 5, 5); // file 3 stays open
        assertEquals(3, journalFiles().size());

        flushes.get(1).setSuccess(null);
        b.get(30, TimeUnit.SECONDS);
        List<Path> remaining = journalFiles();
        assertEquals("only B's file is gone", 2, remaining.size());
        assertEquals("journal-0000000001.log", remaining.get(0).getFileName().toString());
        assertEquals("journal-0000000003.log", remaining.get(1).getFileName().toString());

        flushes.get(0).setSuccess(null);
        a.get(30, TimeUnit.SECONDS);
        assertEquals("the open file stays", 1, journalFiles().size());
        assertEquals(1, newJournal().read(false).records.size());
        assertEquals(2, started.get());
    }

    @Test
    public void scheduledFlushRunsAfterTheDelay() throws Exception
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush, 50);
        append(storage, 1, 3);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!journalFiles().isEmpty() && System.nanoTime() < deadline)
            Thread.sleep(20);
        assertTrue("the background flush deletes the file", journalFiles().isEmpty());
        assertEquals(List.of(1L, 2L, 3L), tableEpochs());
    }

    @Test
    public void truncateDeletesTheJournalFiles() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 3);
        assertEquals(1, journalFiles().size());
        storage.truncate();
        assertTrue(journalFiles().isEmpty());
        assertTrue(tableEpochs().isEmpty());
    }

    @Test
    public void tailIsKeptInJournalMode() throws IOException
    {
        SystemKeyspaceStorage storage = newStorage(SystemKeyspaceStorageJournalTest::realFlush);
        append(storage, 1, 5);
        List<Long> epochs = new ArrayList<>();
        storage.getEntries(Epoch.create(2)).iterator().forEachRemaining(e -> epochs.add(e.epoch.getEpoch()));
        assertEquals(List.of(3L, 4L, 5L), epochs);
    }

    @Test
    public void durabilitySettingRejectsNull()
    {
        try
        {
            DatabaseDescriptor.setMetadataLogDurability(null);
            fail();
        }
        catch (ConfigurationException expected)
        {
            // expected
        }
        assertEquals(Config.MetadataLogDurability.journal, DatabaseDescriptor.getMetadataLogDurability());
    }
}
