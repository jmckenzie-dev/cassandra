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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.streamhist.LazyTombstoneHistogramBuilder;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder;
import org.apache.cassandra.utils.streamhist.TombstoneHistogramBuilder;

import static org.apache.cassandra.config.CassandraRelevantProperties.LAZY_TOMBSTONE_HISTOGRAMS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MetadataCollectorHistogramTest
{
    private static TableMetadata metadata;
    private static final UUID hostId = new UUID(1, 2);

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("histogram_test", "table")
                                .addPartitionKeyColumn("key", Int32Type.instance)
                                .addRegularColumn("value", Int32Type.instance)
                                .build();
    }

    @Test
    public void referenceRemainsTheDefaultAndSelectionOccursAtConstruction()
    {
        assertEquals("false", LAZY_TOMBSTONE_HISTOGRAMS.getDefaultValue());
        try (WithProperties properties = new WithProperties().set(LAZY_TOMBSTONE_HISTOGRAMS, false))
        {
            MetadataCollector reference = new MetadataCollector(metadata.comparator, hostId);
            assertEquals(StreamingTombstoneHistogramBuilder.class, reference.estimatedTombstoneDropTime.getClass());
            properties.set(LAZY_TOMBSTONE_HISTOGRAMS, true);
            MetadataCollector candidate = new MetadataCollector(metadata.comparator, hostId);
            assertEquals(LazyTombstoneHistogramBuilder.class, candidate.estimatedTombstoneDropTime.getClass());
            assertEquals(StreamingTombstoneHistogramBuilder.class, reference.estimatedTombstoneDropTime.getClass());
            reference.release();
            candidate.release();
        }
    }

    @Test
    public void liveCellsAndEmptyMetadataRemainIdentical() throws IOException
    {
        MetadataCollector reference = collector(false);
        MetadataCollector candidate = collector(true);
        assertEquivalent(reference, candidate);
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes("value"));
        for (int i = 0; i < 100; i++)
        {
            Cell<?> cell = BufferCell.live(column, i, ByteBufferUtil.bytes(i));
            reference.update(cell);
            candidate.update(cell);
            reference.update(LivenessInfo.create(i));
            candidate.update(LivenessInfo.create(i));
        }
        reference.update(DeletionTime.LIVE);
        candidate.update(DeletionTime.LIVE);
        reference.addCellPerPartitionCount();
        candidate.addCellPerPartitionCount();
        assertEquivalent(reference, candidate);
        assertEquals(0, stats(candidate).estimatedTombstoneDropTime.size());
        reference.release();
        candidate.release();
        assertEquivalent(reference, candidate);
    }

    @Test
    public void deletionAndExpirationMetadataRemainIdentical() throws IOException
    {
        for (boolean cursor : new boolean[]{ false, true })
        {
            MetadataCollector reference = collector(false);
            MetadataCollector candidate = collector(true);
            ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes("value"));
            for (int i = 0; i < 300; i++)
            {
                long deletionTime = 1000 + i * 60L;
                Cell<?> cell = i % 2 == 0
                               ? BufferCell.tombstone(column, i, deletionTime)
                               : new BufferCell(column, i, 300, deletionTime, ByteBufferUtil.bytes(i), null);
                if (cursor)
                {
                    reference.updateCellLiveness(cell);
                    candidate.updateCellLiveness(cell);
                }
                else
                {
                    reference.update(cell);
                    candidate.update(cell);
                }
                DeletionTime deletion = DeletionTime.build(i, deletionTime + 1);
                reference.updatePartitionDeletion(deletion);
                candidate.updatePartitionDeletion(deletion);
                LivenessInfo liveness = LivenessInfo.withExpirationTime(i, 300, deletionTime + 2);
                reference.update(liveness);
                candidate.update(liveness);
                if (i % 41 == 0)
                    assertEquivalent(reference, candidate);
            }
            reference.addCellPerPartitionCount();
            candidate.addCellPerPartitionCount();
            assertEquivalent(reference, candidate);
            assertEquals(900, reference.totalTombstones);
            reference.release();
            candidate.release();
            assertEquivalent(reference, candidate);
        }
    }

    private static MetadataCollector collector(boolean lazy)
    {
        TombstoneHistogramBuilder builder = lazy
                                            ? new LazyTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS)
                                            : new StreamingTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                     SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                     SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
        return new MetadataCollector(metadata.comparator, hostId, builder);
    }

    private static void assertEquivalent(MetadataCollector reference, MetadataCollector candidate) throws IOException
    {
        StatsMetadata expected = stats(reference);
        StatsMetadata actual = stats(candidate);
        assertEquals(expected, actual);
        assertEquals(expected.estimatedTombstoneDropTime.hashCode(), actual.estimatedTombstoneDropTime.hashCode());
        assertEquals(reference.totalTombstones, candidate.totalTombstones);
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            Version version = format.getLatestVersion();
            try (DataOutputBuffer expectedOutput = new DataOutputBuffer(); DataOutputBuffer actualOutput = new DataOutputBuffer())
            {
                StatsMetadata.serializer.serialize(version, expected, expectedOutput);
                StatsMetadata.serializer.serialize(version, actual, actualOutput);
                assertArrayEquals(expectedOutput.toByteArray(), actualOutput.toByteArray());
            }
        }
    }

    private static StatsMetadata stats(MetadataCollector collector)
    {
        return (StatsMetadata) collector.finalizeMetadata(metadata.partitioner.getClass().getCanonicalName(), 0.01, 0, null, false,
                                                          SerializationHeader.make(metadata, Collections.emptyList()),
                                                          ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(1)).get(MetadataType.STATS);
    }
}
