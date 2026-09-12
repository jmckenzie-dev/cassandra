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
package org.apache.cassandra.utils.streamhist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LazyTombstoneHistogramBuilderTest
{
    @Test
    public void emptySnapshotsAndFirstObservation() throws IOException
    {
        TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(5, 10, 60);
        TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(5, 10, 60);
        TombstoneHistogram emptyReference = reference.build();
        TombstoneHistogram emptyCandidate = candidate.build();
        assertEquivalent(emptyReference, emptyCandidate);
        reference.flushHistogram();
        candidate.flushHistogram();
        assertEquivalent(reference.build(), candidate.build());

        reference.update(61);
        candidate.update(61);
        assertEquivalent(reference.build(), candidate.build());
        assertEquals(0, emptyReference.size());
        assertEquals(0, emptyCandidate.size());
        assertEquivalent(emptyReference, emptyCandidate);
        assertEquals(1, candidate.build().size());
    }

    @Test
    public void zeroSpoolAndSaturatingWeights() throws IOException
    {
        for (int spoolSize : new int[]{ 0, 1, 10 })
        {
            TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(3, spoolSize, 60);
            TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(3, spoolSize, 60);
            for (long point : new long[]{ 0, 1, 59, 60, 61, Integer.MAX_VALUE, Cell.MAX_DELETION_TIME })
            {
                for (int weight : new int[]{ 0, 1, Integer.MAX_VALUE, 2 })
                {
                    reference.update(point, weight);
                    candidate.update(point, weight);
                    assertEquivalent(reference.build(), candidate.build());
                }
            }
        }
    }

    @Test
    public void spoolCapacityAndCollisionDraining() throws IOException
    {
        for (int spoolSize : new int[]{ 1, 3, 8, 128 })
        {
            TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(5, spoolSize, 1);
            TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(5, spoolSize, 1);
            for (int i = 0; i < 1000; i++)
            {
                long point = i * 256L;
                reference.update(point);
                candidate.update(point);
                if (i == spoolSize || i == spoolSize + 1 || i == 99 || i == 100 || i == 101)
                    assertEquivalent(reference.build(), candidate.build());
            }
            assertEquivalent(reference.build(), candidate.build());
        }
    }

    @Test
    public void defaultSpoolDrainsWithoutChangingTheApproximation() throws IOException
    {
        TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                     SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                     SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
        TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
        for (int i = 0; i < 140000; i++)
        {
            long point = i * (long) SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS;
            reference.update(point);
            candidate.update(point);
        }
        assertEquivalent(reference.build(), candidate.build());
        reference.update(123);
        candidate.update(123);
        assertEquivalent(reference.build(), candidate.build());
    }

    @Test
    public void releasePreservesSnapshotsAndRejectsUpdates() throws IOException
    {
        for (boolean populated : new boolean[]{ false, true })
        {
            TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(5, 10, 1);
            TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(5, 10, 1);
            if (populated)
            {
                reference.update(10, 3);
                candidate.update(10, 3);
            }
            reference.releaseBuffers();
            candidate.releaseBuffers();
            assertEquivalent(reference.build(), candidate.build());
            reference.releaseBuffers();
            candidate.releaseBuffers();
            reference.flushHistogram();
            candidate.flushHistogram();
            assertEquivalent(reference.build(), candidate.build());
            assertSameFailure(() -> reference.update(100), () -> candidate.update(100));
            assertSameFailure(() -> reference.update(100, 2), () -> candidate.update(100, 2));
            assertEquivalent(reference.build(), candidate.build());
        }
    }

    @Test
    public void rejectsTheSameInvalidConstructorArguments()
    {
        for (int[] arguments : new int[][]{ { 0, 10, 60 }, { -1, 10, 60 }, { 5, -1, 60 }, { 5, 10, 0 }, { 5, 10, -1 } })
            assertSameFailure(() -> new StreamingTombstoneHistogramBuilder(arguments[0], arguments[1], arguments[2]),
                              () -> new LazyTombstoneHistogramBuilder(arguments[0], arguments[1], arguments[2]));
    }

    @Test
    public void unrepresentableSpoolSizesKeepReferenceConstructorBehavior() throws IOException
    {
        for (int spoolSize : new int[]{ (1 << 29) + 1, 1 << 30 })
        {
            Throwable expected = failure(() -> new StreamingTombstoneHistogramBuilder(5, spoolSize, 1));
            Throwable actual = failure(() -> new LazyTombstoneHistogramBuilder(5, spoolSize, 1));
            assertEquals(NegativeArraySizeException.class, expected.getClass());
            assertEquals(expected.getClass(), actual.getClass());
            assertEquals(expected.getMessage(), actual.getMessage());
        }
        for (int spoolSize : new int[]{ (1 << 30) + 1, Integer.MAX_VALUE })
        {
            TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(5, spoolSize, 1);
            TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(5, spoolSize, 1);
            assertEquivalent(reference.build(), candidate.build());
            reference.update(100);
            candidate.update(100);
            assertEquivalent(reference.build(), candidate.build());
        }
    }

    static void assertEquivalent(TombstoneHistogram reference, TombstoneHistogram candidate) throws IOException
    {
        assertEquals(reference, candidate);
        assertEquals(reference.hashCode(), candidate.hashCode());
        assertEquals(reference.size(), candidate.size());
        List<Long> referenceEntries = new ArrayList<>();
        List<Long> candidateEntries = new ArrayList<>();
        reference.forEach((point, value) -> { referenceEntries.add(point); referenceEntries.add((long) value); });
        candidate.forEach((point, value) -> { candidateEntries.add(point); candidateEntries.add((long) value); });
        assertEquals(referenceEntries, candidateEntries);
        for (double bound : new double[]{ -1, 0, 1, 59, 60, 61, 1000, Integer.MAX_VALUE, Cell.MAX_DELETION_TIME })
            assertEquals(Double.doubleToLongBits(reference.sum(bound)), Double.doubleToLongBits(candidate.sum(bound)));
        for (int i = 0; i < referenceEntries.size(); i += 2)
        {
            long point = referenceEntries.get(i);
            for (long bound : new long[]{ point - 1, point, point + 1 })
                assertEquals(Double.doubleToLongBits(reference.sum(bound)), Double.doubleToLongBits(candidate.sum(bound)));
        }
        for (TombstoneHistogram.HistogramSerializer serializer : new TombstoneHistogram.HistogramSerializer[]{ TombstoneHistogram.HistogramSerializer.instance,
                                                                                                            TombstoneHistogram.LegacyHistogramSerializer.instance })
        {
            byte[] expected = serialize(serializer, reference);
            byte[] actual = serialize(serializer, candidate);
            assertArrayEquals(expected, actual);
            assertEquals(serializer.serializedSize(reference), serializer.serializedSize(candidate));
            try (DataInputBuffer expectedInput = new DataInputBuffer(expected);
                 DataInputBuffer actualInput = new DataInputBuffer(actual))
            {
                assertEquals(serializer.deserialize(expectedInput), serializer.deserialize(actualInput));
            }
        }
    }

    private static byte[] serialize(TombstoneHistogram.HistogramSerializer serializer, TombstoneHistogram histogram) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            serializer.serialize(histogram, output);
            return output.toByteArray();
        }
    }

    private static void assertSameFailure(Runnable reference, Runnable candidate)
    {
        Throwable expected = failure(reference);
        Throwable actual = failure(candidate);
        assertTrue(expected instanceof AssertionError);
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getMessage(), actual.getMessage());
    }

    private static Throwable failure(Runnable operation)
    {
        try
        {
            operation.run();
        }
        catch (Throwable failure)
        {
            return failure;
        }
        throw new AssertionError("Expected operation to fail");
    }
}
