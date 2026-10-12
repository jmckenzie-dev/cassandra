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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertFalse;

/**
 * Measures the cost of making one local_metadata_log append durable: today's blocking memtable flush against an
 * INSERT plus a commit log fsync and against the journal (INSERT plus a journal write and force), idle and while
 * another table flushes. Real fsync (skip_sync off).
 */
public class MetadataLogDurabilityCostTest
{
    static
    {
        CassandraRelevantProperties.TEST_CASSANDRA_SKIP_SYNC.setBoolean(false);
    }

    private static final int WARMUP = 20;
    private static final int ITERATIONS = 300;
    private static final String BG_KS = "durability_bg";
    private static final String BG_TABLE = "bg";
    private static final String INSERT = String.format("INSERT INTO %s.%s (epoch, entry_id, transformation, kind) VALUES (?,?,?,?)",
                                                       SYSTEM_KEYSPACE_NAME, SystemKeyspace.METADATA_LOG);

    private static final AtomicLong epoch = new AtomicLong(1);

    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
        SchemaLoader.createKeyspace(BG_KS, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(BG_KS, BG_TABLE));
        // test/conf enables them; the default is off, and each flush would add hard links
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }

    private interface Step
    {
        void run(SystemKeyspaceStorage storage, Entry entry) throws IOException;
    }

    private static Entry nextEntry()
    {
        long e = epoch.getAndIncrement();
        return new Entry(new Entry.Id(e), Epoch.create(e), CustomTransformation.make((int) e));
    }

    private static void insert(Entry entry) throws IOException
    {
        executeInternal(INSERT, entry.epoch.getEpoch(), entry.id.entryId,
                        entry.transform.kind().toVersionedBytes(entry.transform), entry.transform.kind().id);
    }

    private static long[] measure(Step step) throws IOException
    {
        SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> MetadataSnapshots.NO_OP);
        storage.truncate(); // the table and any journal files
        long[] nanos = new long[ITERATIONS];
        for (int i = -WARMUP; i < ITERATIONS; i++)
        {
            Entry entry = nextEntry();
            long start = System.nanoTime();
            step.run(storage, entry);
            if (i >= 0)
                nanos[i] = System.nanoTime() - start;
        }
        return nanos;
    }

    private static void report(String name, long[] nanos)
    {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        double mean = Arrays.stream(sorted).average().orElse(0) / 1e6;
        System.out.println(String.format("DURABILITY | %-34s | mean %8.3f ms | p50 %8.3f | p90 %8.3f | p99 %8.3f | max %8.3f | per s %7.1f",
                          name, mean, ms(sorted, 0.50), ms(sorted, 0.90), ms(sorted, 0.99),
                          sorted[sorted.length - 1] / 1e6, 1000 / mean));
    }

    private static double ms(long[] sorted, double q)
    {
        return sorted[Math.min(sorted.length - 1, (int) (q * sorted.length))] / 1e6;
    }

    private static final Step INSERT_ONLY = (storage, entry) -> insert(entry);
    private static final Step APPEND_WITH_FLUSH = SystemKeyspaceStorage::append;
    private static final Step APPEND_TO_JOURNAL = SystemKeyspaceStorage::append;
    private static final Step INSERT_THEN_COMMITLOG_SYNC = (storage, entry) -> {
        insert(entry);
        CommitLog.instance.sync(true);
    };

    // One small write and fsync of a plain file: the device cost of one force, for scale
    private static Step fileForce(FileChannel channel)
    {
        return (storage, entry) -> {
            channel.write(ByteBuffer.allocate(512));
            channel.force(false);
        };
    }

    private static void runAll(String suffix) throws IOException
    {
        File probe = new File(DatabaseDescriptor.getAllDataFileLocations()[0], "durability-probe");
        try (FileChannel channel = FileChannel.open(probe.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE))
        {
            report("512 B write + FileChannel.force" + suffix, measure(fileForce(channel)));
        }
        probe.delete();
        report("insert only" + suffix, measure(INSERT_ONLY));
        report("append + blocking flush" + suffix, measure(APPEND_WITH_FLUSH));
        DatabaseDescriptor.setMetadataLogDurability(Config.MetadataLogDurability.journal);
        try
        {
            report("append to journal + force" + suffix, measure(APPEND_TO_JOURNAL));
        }
        finally
        {
            DatabaseDescriptor.setMetadataLogDurability(Config.MetadataLogDurability.flush);
            new SystemKeyspaceStorage(() -> MetadataSnapshots.NO_OP).truncate();
        }
        report("insert + CommitLog.sync(true)" + suffix, measure(INSERT_THEN_COMMITLOG_SYNC));
    }

    @Test
    public void measureDurabilityCost() throws Exception
    {
        assertFalse("fsync must be real", SyncUtil.SKIP_SYNC);
        System.out.println(String.format("DURABILITY | commitlog_sync %s, %d iterations after %d warmup",
                          DatabaseDescriptor.getCommitLogSync(), ITERATIONS, WARMUP));
        runAll("");

        // A second table flushes 8 MiB memtables in a loop; its post-flush tasks share the single-threaded
        // post-flush executor with the metadata log flushes.
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong bgFlushes = new AtomicLong();
        Thread bg = new Thread(() -> {
            String value = "x".repeat(64 * 1024);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(BG_KS, BG_TABLE);
            long key = 0;
            while (!stop.get())
            {
                for (int i = 0; i < 128; i++, key++)
                    executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)", BG_KS, BG_TABLE),
                                    Long.toString(key), "n", value);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                bgFlushes.incrementAndGet();
            }
        }, "background-flusher");
        bg.start();
        try
        {
            runAll(" (bg flushes)");
        }
        finally
        {
            stop.set(true);
            bg.join();
        }
        System.out.println(String.format("DURABILITY | background flushes: %d", bgFlushes.get()));
    }
}
