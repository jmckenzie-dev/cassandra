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
package org.apache.cassandra.metrics;

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class AdaptiveHistogramHistoryTest
{
    @Test
    public void emptyAndZeroSnapshotsNeedNoHistory()
    {
        assertNull(AdaptiveHistogramHistory.pack(new long[0], null));
        assertNull(AdaptiveHistogramHistory.pack(new long[165], null));
        assertNull(AdaptiveHistogramHistory.pack(new long[3], new long[]{ Long.MAX_VALUE, 1, 2 }));
        assertArrayEquals(new long[]{ 3, -7 }, AdaptiveHistogramHistory.delta(new long[]{ 3, -7 }, null));
    }

    @Test
    public void choosesSmallestSignedWidthAtBothBoundaries()
    {
        assertWidth(byte[].class, -128, -1, 0, 127);
        assertWidth(short[].class, -129, 0, 127);
        assertWidth(short[].class, 0, 128);
        assertWidth(short[].class, Short.MIN_VALUE, 0, Short.MAX_VALUE);
        assertWidth(int[].class, (long) Short.MIN_VALUE - 1, 0);
        assertWidth(int[].class, 0, (long) Short.MAX_VALUE + 1);
        assertWidth(int[].class, Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
        assertWidth(long[].class, (long) Integer.MIN_VALUE - 1, 0);
        assertWidth(long[].class, 0, (long) Integer.MAX_VALUE + 1);
        assertWidth(long[].class, Long.MIN_VALUE, 0, Long.MAX_VALUE);
    }

    @Test
    public void reusesMatchingNarrowArraysAndReplacesChangedLengths()
    {
        for (long value : new long[]{ 10, 200, 40000 })
        {
            Object original = AdaptiveHistogramHistory.pack(new long[]{ value, -value }, null);
            long[] next = { value + 1, -value - 1 };
            Object reused = AdaptiveHistogramHistory.pack(next, original);
            assertSame(original, reused);
            assertArrayEquals(new long[2], AdaptiveHistogramHistory.delta(next, reused));

            Object resized = AdaptiveHistogramHistory.pack(new long[]{ value }, reused);
            assertNotSame(reused, resized);
            assertArrayEquals(new long[1], AdaptiveHistogramHistory.delta(new long[]{ value }, resized));
        }
    }

    @Test
    public void historyShrinksThroughEveryWidthAndDisappearsAfterReset()
    {
        Object history = null;
        long[][] snapshots = { { Long.MAX_VALUE, Long.MIN_VALUE }, { 40000, -40000 }, { 200, -200 }, { 1, -1 }, { 0, 0 } };
        Class<?>[] widths = { long[].class, int[].class, short[].class, byte[].class };
        long[] previous = null;
        for (int step = 0; step < snapshots.length; step++)
        {
            long[] now = snapshots[step];
            assertArrayEquals(CassandraMetricsRegistry.delta(now, previous), AdaptiveHistogramHistory.delta(now, history));
            history = AdaptiveHistogramHistory.pack(now, history);
            if (step < widths.length)
                assertEquals(widths[step], history.getClass());
            else
                assertNull(history);
            previous = now;
        }
        assertWidth(byte[].class, 1, 0);
    }

    @Test
    public void matchesLegacyForNegativeDeltasOverflowAndLengthChanges()
    {
        long[][] previousValues = { { 0, 0 }, { 127, -128 }, { 32767, -32768 },
                                   { Integer.MAX_VALUE, Integer.MIN_VALUE }, { Long.MAX_VALUE, Long.MIN_VALUE },
                                   { Long.MIN_VALUE, Long.MAX_VALUE }, {}, { 1, 2, 3, 4, 5 } };
        long[][] currentValues = { {}, { 0 }, { 0, 0 }, { Long.MIN_VALUE, Long.MAX_VALUE },
                                  { Long.MAX_VALUE, Long.MIN_VALUE }, { 4, -5, 6, -7, 8, -9 } };
        for (long[] previous : previousValues)
        {
            Object history = AdaptiveHistogramHistory.pack(previous.clone(), null);
            for (long[] now : currentValues)
            {
                long[] before = now.clone();
                assertArrayEquals(CassandraMetricsRegistry.delta(now, previous), AdaptiveHistogramHistory.delta(now, history));
                assertArrayEquals(before, now);
            }
        }
    }

    @Test
    public void returnedDeltasCannotChangeSnapshotsOrHistory()
    {
        for (long value : new long[]{ 1, 128, 32768, Long.MAX_VALUE })
        {
            long[] previous = { value, -value };
            Object history = AdaptiveHistogramHistory.pack(previous.clone(), null);
            long[] now = { 7, -8 };
            long[] actual = AdaptiveHistogramHistory.delta(now, history);
            assertNotSame(now, actual);
            assertNotSame(history, actual);
            Arrays.fill(actual, Long.MIN_VALUE);
            assertArrayEquals(new long[]{ 7, -8 }, now);
            assertArrayEquals(CassandraMetricsRegistry.delta(now, previous), AdaptiveHistogramHistory.delta(now, history));
        }
        long[] now = { 1, 2 };
        long[] first = AdaptiveHistogramHistory.delta(now, null);
        assertNotSame(now, first);
        Arrays.fill(first, -1);
        assertArrayEquals(new long[]{ 1, 2 }, AdaptiveHistogramHistory.delta(now, null));
    }

    private static void assertWidth(Class<?> expected, long... values)
    {
        Object packed = AdaptiveHistogramHistory.pack(values.clone(), null);
        assertEquals(expected, packed.getClass());
        assertArrayEquals(new long[values.length], AdaptiveHistogramHistory.delta(values, packed));
        assertArrayEquals(CassandraMetricsRegistry.delta(new long[values.length], values),
                          AdaptiveHistogramHistory.delta(new long[values.length], packed));
    }
}
