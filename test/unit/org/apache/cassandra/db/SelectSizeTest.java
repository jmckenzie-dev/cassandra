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

import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.SelectSizeStatement;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.logIfIntellij;
import static org.junit.Assert.assertTrue;

public class SelectSizeTest
{
    private static final String KEYSPACE1 = "SelectSizeTest";
    private static final String TABLE1 = "Compressed1";
    private static final String STATIC_COL = "StaticColumn";
    private static final int PARTITION_COUNT = 100;

    private static final int HIGH_THROTTLE = 10000;
    private static final int LOW_THROTTLE = 128;
    private static final int FEATURE_DISABLED = 0;

    private IPartitioner partitioner;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        TableMetadata testMetadata = SchemaLoader.standardCFMD(KEYSPACE1, TABLE1)
                                                 .compression(CompressionParams.lz4())
                                                 .addStaticColumn(new ColumnIdentifier(STATIC_COL, true), AsciiType.instance)
                                                 .build();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), testMetadata);
    }

    @Before
    public void beforeTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TABLE1).truncateBlocking();
    }

    @Test
    public void serialization() throws Exception
    {
        SelectSizeCommand expected = new SelectSizeCommand(KEYSPACE1, TABLE1, ByteBufferUtil.bytes(1), SelectSizeStatement.Type.UNCOMPRESSED);
        long size = SelectSizeCommand.serializer.serializedSize(expected, MessagingService.current_version);

        byte[] bytes = new byte[(int) size];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        DataOutputBuffer out = new DataOutputBufferFixed(bb);
        SelectSizeCommand.serializer.serialize(expected, out, MessagingService.current_version);
        Assert.assertEquals(size, bb.position());

        bb.rewind();
        DataInputBuffer inputBuffer = new DataInputBuffer(bb, true);
        SelectSizeCommand actual = SelectSizeCommand.serializer.deserialize(inputBuffer, MessagingService.current_version);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void executeLocally() throws Exception
    {
        // no key matching 0, should return 0
        SelectSizeCommand cmd0 = new SelectSizeCommand(KEYSPACE1, TABLE1, ByteBufferUtil.bytes(0), SelectSizeStatement.Type.UNCOMPRESSED);
        Assert.assertEquals(0, cmd0.executeLocally());

        // row with key 1 inserted in setupClass, should return non-negative. Can round down to 0 if small insert and compressed.
        SelectSizeCommand cmd1 = new SelectSizeCommand(KEYSPACE1, TABLE1, ByteBufferUtil.bytes(1), SelectSizeStatement.Type.UNCOMPRESSED);
        Assert.assertTrue(cmd1.executeLocally() >= 0);
    }

    @Test
    public void testSimpleCase()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TABLE1);
        partitioner = cfs.getPartitioner();

        // Need this to access and test the getLiveUncompressedSize method.
        SelectSizeCommand liveUncompressedCommand = new SelectSizeCommand(KEYSPACE1, TABLE1, dk(1).getKey(), SelectSizeStatement.Type.LIVE_UNCOMPRESSED);

        // If we only insert 1 row the tolerances get a little funky; 26 bytes compared to 30 passes our threshold.
        for (int i = 0; i < 1000; i++)
        {
            new RowUpdateBuilder(cfs.metadata.get(), 1, String.valueOf(1))
            .clustering(String.valueOf(i))
            .add("val", String.valueOf(i))
            .build()
            .apply();
        }

        Set<SSTableReader> beforeFlush = cfs.getLiveSSTables();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Set<SSTableReader> afterFlush = cfs.getLiveSSTables();
        Sets.SetView<SSTableReader> sstables = Sets.difference(afterFlush, beforeFlush);

        Assert.assertEquals(1, sstables.size());

        SSTableReader sstable = sstables.iterator().next();
        long uncExpected = sstable.uncompressedLength();

        long uncompressedSize = sstable.tryGetUncompressedSerializedRowSize(dk(1));
        Assert.assertEquals(uncExpected, uncompressedSize);

        long liveUncompressedSize = liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(1));

        // Since we use an estimate on EncodingStats for our size + don't have previous entry size as used in ColumnIndex
        // serialization, we just want to confirm that our estimate here is +/- 10% from the uncompressedSize on disk.
        float observedSizeDifference = (float)Math.abs(uncompressedSize - liveUncompressedSize) / (float)uncompressedSize;
        logIfIntellij("uncompressedSize:       " + uncompressedSize);
        logIfIntellij("liveUncompressedSize:   " + liveUncompressedSize);
        logIfIntellij("observedSizeDifference: " + observedSizeDifference);
        logIfIntellij("allowable difference:   " + SelectSizeCommand.ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN);
        assertTrue((float)Math.abs(uncompressedSize - liveUncompressedSize) / (float)uncompressedSize < SelectSizeCommand.ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN);

        ColumnMetadata cDef = cfs.metadata.get().getColumn(ByteBufferUtil.bytes(STATIC_COL));
        // Add and test a static column
        new RowUpdateBuilder(cfs.metadata.get(), 1, String.valueOf(1))
        .add(cDef, "Testing a static column to make sure it's part of our calculation.")
        .build()
        .apply();

        long sizeWithStatic = liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(1));
        Assert.assertTrue(sizeWithStatic > liveUncompressedSize);
    }

    @Test
    public void testHighThrottling()
    {
        wrapRun(this::complexTest, HIGH_THROTTLE);
    }

    @Test
    public void testLowThrottling()
    {
        wrapRun(this::complexTest, LOW_THROTTLE);
    }

    @Test
    public void testWithFeatureDisabled()
    {
        wrapRun(() -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE1);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TABLE1);
            partitioner = cfs.getPartitioner();

            // Need this to access and test the getLiveUncompressedSize method.
            SelectSizeCommand liveUncompressedCommand = new SelectSizeCommand(KEYSPACE1, TABLE1, dk(1).getKey(), SelectSizeStatement.Type.LIVE_UNCOMPRESSED);

            populateData(cfs);
            Assert.assertEquals(-1, liveUncompressedCommand.executeLocally());
        }, FEATURE_DISABLED);
    }

    private void wrapRun(Runnable r, int throttleValue)
    {
        int preRateLimit = DatabaseDescriptor.getSelectSizeThroughputMebibytesPerSec();
        try
        {
            DatabaseDescriptor.setSelectSizeThroughputMebibytesPerSec(throttleValue);
            r.run();
        }
        finally
        {
            DatabaseDescriptor.setSelectSizeThroughputMebibytesPerSec(preRateLimit);
        }
    }

    private void complexTest()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TABLE1);
        partitioner = cfs.getPartitioner();

        populateData(cfs);

        // Need this to access and test the getLiveUncompressedSize method.
        SelectSizeCommand liveUncompressedCommand = new SelectSizeCommand(KEYSPACE1, TABLE1, dk(1).getKey(), SelectSizeStatement.Type.LIVE_UNCOMPRESSED);

        Set<SSTableReader> beforeFlush = cfs.getLiveSSTables();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Set<SSTableReader> afterFlush = cfs.getLiveSSTables();
        Sets.SetView<SSTableReader> sstables = Sets.difference(afterFlush, beforeFlush);

        // Even w/all the PK's and data, we should still be within 1 SSTable in size
        Assert.assertEquals(1, sstables.size());

        SSTableReader sstable = sstables.iterator().next();
        long uncExpected = sstable.uncompressedLength();

        long uncompressedSize = 0;
        // Sum up all the uncompressed sizes and confirm they are == the on disk size
        for (int p = 0; p < PARTITION_COUNT; p++)
        {
            DecoratedKey dk = dk(p);
            long start = sstable.maybeGetPositionForKey(dk);
            if (start == -1)
                continue;
            long end = sstable.maybeGetPositionForNextKey(dk);
            uncompressedSize += sstable.getUncompressedSerializedRowSizeByBounds(dk, start, end);
        }
        // This differs slightly from the uncompressed test above where we check 1 PK. In this case we're checking all
        // that we inserted to confirm that a summation of everything == the value of the file on disk
        Assert.assertEquals(uncExpected, uncompressedSize);

        long liveUncompressedSize = 0;
        // Check the summation of a few more orders of magnitude of data to make sure it remains within error margin.
        for (int p = 0; p < PARTITION_COUNT; p++)
            liveUncompressedSize += liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(p));

        // Since we use an estimate on EncodingStats for our size + don't have previous entry size as used in ColumnIndex
        // serialization, we just want to confirm that our estimate here is +/- 10% from the uncompressedSize on disk.
        float margin = (float)Math.abs(uncompressedSize - liveUncompressedSize) / (float)uncompressedSize;
        assertTrue(margin < SelectSizeCommand.ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN);

        long ts = 2000;
        // Now we perform a single row deletion on dk 2, recalculate, and confirm it's dropped
        int idxToDelete = 2;
        new RowUpdateBuilder(cfs.metadata.get(), ts++, String.valueOf(idxToDelete))
        .clustering(String.valueOf(idxToDelete))
        .delete("val")
        .build()
        .applyUnsafe();

        long oneColDeletedSize = 0;
        for (int p = 0; p < PARTITION_COUNT; p++)
            oneColDeletedSize += liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(p));

        assertTrue("Expected size with one column deleted to be less than live Uncompressed. Deleted: " + oneColDeletedSize + ". liveUncompressed: " + liveUncompressedSize,
                   oneColDeletedSize < liveUncompressedSize);

        // Now we delete an entire partition and confirm that this value is < than the logical size w/one cell deletion
        idxToDelete = 3;

        // delete the partition
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata.get(),
                                                         dk(idxToDelete),
                                                         ts,
                                                         FBUtilities.nowInSeconds())
        ).applyUnsafe();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Sum up and make sure it's smaller still now
        long partitionDeletionSize = 0;
        for (int p = 0; p < PARTITION_COUNT; p++)
            partitionDeletionSize += liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(p));
        assertTrue(partitionDeletionSize < oneColDeletedSize);

        Assert.assertEquals(0, liveUncompressedCommand.getLiveUncompressedSize(cfs.metadata, dk(1001)));
    }

    private void populateData(ColumnFamilyStore cfs)
    {
        // Insert PK's w/1024 clustering values each
        for (int p = 0; p < PARTITION_COUNT; p++)
        {
            for (int c = 0; c < 1024; c++)
            {
                new RowUpdateBuilder(cfs.metadata.get(), c, String.valueOf(p))
                .clustering(String.valueOf(c))
                .add("val", String.valueOf(c))
                .build()
                .applyUnsafe();
            }
        }
    }

    private DecoratedKey dk(int i)
    {
        return new BufferDecoratedKey(t(i), ByteBufferUtil.bytes(String.valueOf(i)));
    }

    Token t(int i)
    {
        return partitioner.getToken(ByteBufferUtil.bytes(String.valueOf(i)));
    }
}

