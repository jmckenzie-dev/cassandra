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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.SelectSizeStatement;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.cql3.statements.SelectSizeStatement.Type;

@SuppressWarnings({ "UnstableApiUsage" })
public class SelectSizeCommand
{
    public static final IVersionedSerializer<SelectSizeCommand> serializer = new Serializer();

    public final String keyspace;
    public final String table;
    public final ByteBuffer key;
    public final Type selectSizeType;

    /**
     * Since we use the {@link EncodingStats} from the iterator in our estimate rather than the SSTable where it's written,
     * we need to have some kind of bound where we have unit tests confirm we're within some "acceptable limit" if our
     * RowIndex building logic changes.
     * <p>
     * We store this here as it's effectively a sentinel saying "This estimate is allowed to be At Least This Wrong", which
     * is a fundamental attribute of the calculation / command.
     * <p>
     * In effect: DO NOT USE THIS VALUE OUTSIDE OF TESTING.
     */
    @VisibleForTesting
    public static final float ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN = .1f;

    public SelectSizeCommand(String keyspace, String table, ByteBuffer key, Type selectSizeType)
    {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(key);

        this.keyspace = keyspace;
        this.table = table;
        this.key = key;
        this.selectSizeType = selectSizeType;
    }

    public long executeLocally()
    {
        Keyspace keyspace = Keyspace.open(this.keyspace);
        DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(key);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);

        switch (selectSizeType)
        {
            case UNCOMPRESSED:
                return getUncompressedSize(cfs, dk);
            case COMPRESSED:
                return getCompressedSize(cfs, dk);
            case LIVE_UNCOMPRESSED:
                return getLiveUncompressedSize(cfs.metadata, dk);
                default:
            throw new RuntimeException(String.format("Got unknown select size type: %s", selectSizeType));
        }
    }

    public long getUncompressedSize(ColumnFamilyStore cfs, DecoratedKey dk)
    {
        long size = 0;
        try (@SuppressWarnings("unused") OpOrder.Group op = cfs.readOrdering.start())
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, dk));
            for (SSTableReader sstable : view.sstables)
            {
                size += sstable.tryGetUncompressedSerializedRowSize(dk);
            }
        }

        return size;
    }

    private long getCompressedSize(ColumnFamilyStore cfs, DecoratedKey dk)
    {
        long compressedSize = 0;
        try (@SuppressWarnings("unused") OpOrder.Group op = cfs.readOrdering.start())
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, dk));
            for (SSTableReader sstable : view.sstables)
            {
                long start = sstable.maybeGetPositionForKey(dk);
                if (start == AbstractRowIndexEntry.NOT_FOUND)
                    continue;
                long end = sstable.maybeGetPositionForNextKey(dk);
                compressedSize += sstable.estimateCompressedSerializedRowSize(dk, start, end);
            }
        }
        return compressedSize;
    }

    /**
     * For a given partition key, we need to determine roughly how much uncompressed space the live data in that partition takes up.
     * We don't have access to the {@link EncodingStats} from the various SSTables where atoms are serialized, so our
     * estimates for delta encoding for timestamps is going to be off as are various {@link org.apache.cassandra.utils.vint.VIntCoding}
     * <p>
     * @return  Size of the partition in question, -1 if calculation failed for any reason
     */
    @VisibleForTesting
    public long getLiveUncompressedSize(TableMetadataRef tableMetadataRef, DecoratedKey dk)
    {
        final int rateLimitMb = DatabaseDescriptor.getSelectSizeThroughputMebibytesPerSec();

        if (rateLimitMb == 0)
            return -1;

        RateLimiter rateLimiter = RateLimiter.create(DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND.toBytesPerSecond(rateLimitMb));

        long result = 0;

        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(tableMetadataRef.get(), FBUtilities.nowInSeconds(), dk);
        try (ReadExecutionController ec = cmd.executionController();
             // Need unfiltered to get EncodingStats Less Wrong estimates on delta encoding for ts etc.
             UnfilteredPartitionIterator unfilteredPartitions = cmd.executeLocally(ec))
        {
            UnfilteredRowIterator unfilteredRows = UnfilteredPartitionIterators.getOnlyElement(unfilteredPartitions, cmd);

            if (unfilteredRows.isEmpty())
                return result;

            // The Encodingstats in this UnfilteredRowIterator _definitely_ don't match the various different EncodingStats
            // used inside various SSTables for serializing data to disk. That said, using _something_ here is preferable
            // to just falling back on the epoch based NO_STATS form of EncodingStats (to the tune of 5-10% more optimistic
            // vs. pathologically pessimistic at 25%+ too high)
            EncodingStats stats = unfilteredRows.stats();
            RowIterator filteredRows = FilteredRows.filter(unfilteredRows, FBUtilities.nowInSeconds());

            result += TypeSizes.sizeofWithShortLength(filteredRows.partitionKey().getKey());
            result += DeletionTime.serializer.serializedSize(unfilteredRows.partitionLevelDeletion());

            SerializationHeader header = new SerializationHeader(true, tableMetadataRef.get(), tableMetadataRef.get().regularAndStaticColumns(), stats);
            SerializationHelper helper = new SerializationHelper(header);

            // Note: as of this writing (2023-02), the serializedSize
            // call here doesn't actually make use of the Version so we pass in latestVersion.
            int msVersion = BigFormat.getInstance().getLatestVersion().correspondingMessagingVersion();

            if (helper.header.hasStatic())
                result += UnfilteredSerializer.serializer.estimateSerializedSize(unfilteredRows.staticRow(), helper, msVersion);

            // Further, we kludge in a last size of zero here as we can't access the sizes of things from the UnfilteredSerializer
            // logic on serialization... /sigh
            long lastSize = 0;
            while (filteredRows.hasNext())
            {
                long startingSize = result;
                Row row = filteredRows.next();
                result += UnfilteredSerializer.serializer.serializedSize(row, helper, lastSize, msVersion);

                lastSize = result - startingSize;

                if (rateLimitMb > 0)
                    // We backpressure / throttle _after_ we've done a read since we don't really know the size of the row until
                    // we're here and have calculated it. Given we either a) have small rows that are predictable size
                    // and will burn through this, or b) have obscenely large rows that we won't know how large they are
                    // until it's too late, there's not much recourse.
                    rateLimiter.acquire(Ints.saturatedCast(lastSize));
            }
            result += UnfilteredSerializer.serializer.serializedSizeEndOfPartition();
        }
        return result;
    }

    public Message<SelectSizeCommand> getMessage()
    {
        return Message.outWithFlag(Verb.SELECT_SIZE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SelectSizeCommand that = (SelectSizeCommand) o;

        return key.equals(that.key) && keyspace.equals(that.keyspace) && table.equals(that.table) && selectSizeType == that.selectSizeType;
    }

    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + keyspace.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + selectSizeType.hashCode();
        return result;
    }

    private static class Serializer implements IVersionedSerializer<SelectSizeCommand>
    {
        public void serialize(SelectSizeCommand command, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(command.keyspace);
            out.writeUTF(command.table);
            ByteBufferUtil.writeWithShortLength(command.key, out);
            out.writeByte((byte)command.selectSizeType.ordinal());
        }

        public SelectSizeCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspaceName = in.readUTF();
            String tableName = in.readUTF();
            ByteBuffer partitionKey = ByteBufferUtil.readWithShortLength(in);
            SelectSizeStatement.Type selectSizeType = Type.values()[in.readByte()];
            return new SelectSizeCommand(keyspaceName, tableName, partitionKey, selectSizeType);
        }

        public long serializedSize(SelectSizeCommand command, int version)
        {
            return TypeSizes.sizeof(command.keyspace)
                   + TypeSizes.sizeof(command.table)
                   + ByteBufferUtil.serializedSizeWithShortLength(command.key)
                   + TypeSizes.sizeof((byte)command.selectSizeType.ordinal());
        }
    }
}
