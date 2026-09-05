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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import com.codahale.metrics.Snapshot;

import org.junit.Test;

import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactDecayingEstimatedHistogramReservoirTest
{
    @Test
    public void emptyObservationAndLandmarkChangesAllocateNoCounterStorage()
    {
        TestClock clock = new TestClock();
        Pair pair = new Pair(false, 127, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
        for (long time : new long[]{ 0, TimeUnit.SECONDS.toNanos(60), LANDMARK_RESET_INTERVAL_IN_NS,
                                    LANDMARK_RESET_INTERVAL_IN_NS + 1, 2 * LANDMARK_RESET_INTERVAL_IN_NS + 2 })
        {
            clock.time = time;
            pair.assertEquivalent();
            assertEquals(0, pair.candidate.allocatedCounterCells());
        }
        pair.clear();
        pair.assertEquivalent();
        assertEquals(0, pair.candidate.allocatedCounterCells());
        clock.time += TimeUnit.SECONDS.toNanos(59);
        pair.update(100);
        pair.assertEquivalent();
        assertTrue(pair.candidate.allocatedCounterCells() > 0);
        assertTrue(pair.candidate.allocatedCounterCells() <= 32);
    }

    @Test
    public void bucketEdgesSparseAndFullyPopulatedStorageMatch()
    {
        for (boolean zeroes : new boolean[]{ false, true })
        {
            for (int bucketCount : new int[]{ 1, 17, 127, 164 })
            {
                TestClock clock = new TestClock();
                Pair pair = new Pair(zeroes, bucketCount, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
                for (long value : new long[]{ Long.MIN_VALUE, -1, 0, 1, 2 })
                    pair.update(value);
                pair.assertEquivalent();
                for (long boundary : pair.reference.buckets(pair.reference.size() - 1))
                {
                    pair.update(boundary - 1);
                    pair.update(boundary);
                    pair.update(boundary + 1);
                }
                pair.update(Long.MAX_VALUE);
                pair.assertEquivalent();
                clock.time += LANDMARK_RESET_INTERVAL_IN_NS + 1;
                pair.assertEquivalent();
                pair.clear();
                pair.assertEquivalent();
            }
        }
    }

    @Test
    public void sharedBucketDefinitionsCannotBeChangedThroughExports()
    {
        for (int buckets : new int[]{ 127, 164 })
        {
            for (boolean zeroes : new boolean[]{ false, true })
            {
                TestClock clock = new TestClock();
                Pair first = new Pair(zeroes, buckets, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
                Pair second = new Pair(zeroes, buckets, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
                long[] export = first.candidate.buckets(first.candidate.size() - 1);
                Arrays.fill(export, -1);
                assertArrayEquals(first.reference.buckets(first.reference.size() - 1),
                                  first.candidate.buckets(first.candidate.size() - 1));
                first.update(37);
                second.update(37);
                first.assertEquivalent();
                second.assertEquivalent();
                assertArrayEquals(first.reference.buckets(10), first.candidate.buckets(10));
            }
        }
    }

    @Test
    public void cumulativeHistorySurvivesDecayAndSnapshotsRemainIndependent()
    {
        TestClock clock = new TestClock();
        Pair pair = new Pair(false, 164, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
        pair.update(100);
        Snapshot referenceSnapshot = pair.reference.getSnapshot();
        Snapshot candidateSnapshot = pair.candidate.getSnapshot();
        clock.time = TimeUnit.MINUTES.toNanos(30) + 1;
        pair.assertEquivalent();
        assertEquals(0, pair.candidate.getSnapshot().size());
        assertEquals(1, Arrays.stream(pair.candidate.getSnapshot().getValues()).sum());
        pair.update(1000);
        pair.assertEquivalent();
        assertEquivalent(referenceSnapshot, candidateSnapshot, true);
        assertEquals(1, candidateSnapshot.size());
    }

    @Test
    public void hotSparseStoragePromotesWithoutChangingCounts()
    {
        TestClock clock = new TestClock();
        Pair pair = new Pair(false, 127, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
        for (int i = 0; i < 63; i++)
            pair.update(100);
        pair.assertEquivalent();
        assertTrue(pair.candidate.allocatedCounterCells() <= 32);
        pair.update(100);
        pair.assertEquivalent();
        assertEquals(pair.reference.size() * 2 * 2, pair.candidate.allocatedCounterCells());
        for (int i = 0; i < 1000; i++)
            pair.update(i);
        pair.assertEquivalent();
    }

    @Test
    public void legacyAndCompactSnapshotsMergeAndRebaseInBothDirections()
    {
        for (boolean compactParent : new boolean[]{ false, true })
        {
            for (boolean compactChild : new boolean[]{ false, true })
            {
                TestClock clock = new TestClock();
                ClearableReservoir parent = reservoir(compactParent, clock);
                DecayingEstimatedHistogramReservoir expectedParent = (DecayingEstimatedHistogramReservoir) reservoir(false, clock);
                parent.update(5);
                expectedParent.update(5);
                clock.time = TimeUnit.SECONDS.toNanos(61);
                ClearableReservoir child = reservoir(compactChild, clock);
                DecayingEstimatedHistogramReservoir expectedChild = (DecayingEstimatedHistogramReservoir) reservoir(false, clock);
                for (int i = 0; i < 100; i++)
                {
                    child.update(i);
                    expectedChild.update(i);
                }
                EstimatedHistogramReservoirSnapshot actual = (EstimatedHistogramReservoirSnapshot) parent.getSnapshot();
                EstimatedHistogramReservoirSnapshot expected = (EstimatedHistogramReservoirSnapshot) expectedParent.getSnapshot();
                actual.add(child.getSnapshot());
                expected.add(expectedChild.getSnapshot());
                assertEquivalent(expected, actual, true);
                actual.rebaseReservoir();
                expected.rebaseReservoir();
                assertEquivalent(expectedParent.getSnapshot(), parent.getSnapshot(), true);
                clock.time += TimeUnit.SECONDS.toNanos(17);
                parent.update(500);
                expectedParent.update(500);
                assertEquivalent(expectedParent.getSnapshot(), parent.getSnapshot(), true);
            }
        }
    }

    @Test
    public void clearableHistogramResetsCountsAndBothReservoirs()
    {
        TestClock clock = new TestClock();
        ClearableHistogram reference = new ClearableHistogram(reservoir(false, clock));
        ClearableHistogram candidate = new ClearableHistogram(reservoir(true, clock));
        for (int i = 0; i < 100; i++)
        {
            reference.update(i);
            candidate.update(i);
        }
        assertEquals(reference.getCount(), candidate.getCount());
        assertEquivalent(reference.getSnapshot(), candidate.getSnapshot(), true);
        reference.clear();
        candidate.clear();
        assertEquals(0, candidate.getCount());
        assertEquivalent(reference.getSnapshot(), candidate.getSnapshot(), true);
        reference.update(123);
        candidate.update(123);
        assertEquivalent(reference.getSnapshot(), candidate.getSnapshot(), true);
    }

    @Test
    public void concurrentFirstUpdatesSnapshotsAndRescalePreserveCounts() throws Exception
    {
        TestClock clock = new TestClock();
        Pair pair = new Pair(false, 127, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try
        {
            for (int round = 0; round < 3; round++)
            {
                clock.time = round * (LANDMARK_RESET_INTERVAL_IN_NS + 1);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int writer = 0; writer < 4; writer++)
                    futures.add(executor.submit(() -> {
                        start.await();
                        for (int i = 0; i < 2000; i++)
                            pair.update(i % 100);
                        return null;
                    }));
                futures.add(executor.submit(() -> {
                    start.await();
                    long[] previous = new long[pair.candidate.size()];
                    for (int i = 0; i < 100; i++)
                    {
                        Snapshot snapshot = pair.candidate.getSnapshot();
                        assertTrue(snapshot.getMin() >= 0);
                        assertTrue(snapshot.getMax() <= 103);
                        long[] current = snapshot.getValues();
                        for (int bucket = 0; bucket < current.length; bucket++)
                            assertTrue("Cumulative bucket regressed during promotion", current[bucket] >= previous[bucket]);
                        previous = current;
                    }
                    return null;
                }));
                start.countDown();
                for (Future<?> future : futures)
                    future.get(30, TimeUnit.SECONDS);
                pair.assertEquivalent();
                assertEquals((round + 1) * 8000L, Arrays.stream(pair.candidate.getSnapshot().getValues()).sum());
            }
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static ClearableReservoir reservoir(boolean compact, TestClock clock)
    {
        return compact ? new CompactDecayingEstimatedHistogramReservoir(false, 127, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS)
                       : new DecayingEstimatedHistogramReservoir(false, 127, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
    }

    static void assertEquivalent(Snapshot expected, Snapshot actual, boolean cumulative)
    {
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.getMin(), actual.getMin());
        assertEquals(expected.getMax(), actual.getMax());
        assertSameDouble(expected::getMean, actual::getMean);
        assertSameDouble(expected::getStdDev, actual::getStdDev);
        for (double quantile : new double[]{ 0, 0.5, 0.9, 0.99, 0.9999, 1 })
            assertSameDouble(() -> expected.getValue(quantile), () -> actual.getValue(quantile));
        ByteArrayOutputStream expectedDump = new ByteArrayOutputStream();
        ByteArrayOutputStream actualDump = new ByteArrayOutputStream();
        expected.dump(expectedDump);
        actual.dump(actualDump);
        assertArrayEquals(expectedDump.toByteArray(), actualDump.toByteArray());
        if (cumulative)
        {
            assertArrayEquals(expected.getValues(), actual.getValues());
            assertEquals(((EstimatedHistogramReservoirSnapshot) expected).getSnapshotLandmark(),
                         ((EstimatedHistogramReservoirSnapshot) actual).getSnapshotLandmark());
        }
        else
        {
            try
            {
                actual.getValues();
                fail("Percentile-only snapshot must reject cumulative values");
            }
            catch (UnsupportedOperationException expectedException)
            {
                // The reference percentile snapshot does not expose cumulative values.
            }
        }
    }

    private static void assertSameDouble(DoubleSupplier expected, DoubleSupplier actual)
    {
        double value;
        try
        {
            value = expected.getAsDouble();
        }
        catch (IllegalStateException failure)
        {
            try
            {
                actual.getAsDouble();
                fail("Expected " + failure);
            }
            catch (IllegalStateException actualFailure)
            {
                assertEquals(failure.getMessage(), actualFailure.getMessage());
            }
            return;
        }
        assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(actual.getAsDouble()));
    }

    static final class Pair
    {
        final DecayingEstimatedHistogramReservoir reference;
        final CompactDecayingEstimatedHistogramReservoir candidate;

        Pair(boolean zeroes, int bucketCount, int stripes, TestClock clock, long resetInterval)
        {
            reference = new DecayingEstimatedHistogramReservoir(zeroes, bucketCount, stripes, clock, resetInterval);
            candidate = new CompactDecayingEstimatedHistogramReservoir(zeroes, bucketCount, stripes, clock, resetInterval);
        }

        void update(long value)
        {
            reference.update(value);
            candidate.update(value);
        }

        void clear()
        {
            reference.clear();
            candidate.clear();
        }

        void assertEquivalent()
        {
            assertEquals(reference.size(), candidate.size());
            assertEquals(reference.stripeCount(), candidate.stripeCount());
            assertEquals(reference.bucketStrategy(), candidate.bucketStrategy());
            assertArrayEquals(reference.buckets(reference.size() - 1), candidate.buckets(candidate.size() - 1));
            CompactDecayingEstimatedHistogramReservoirTest.assertEquivalent(reference.getSnapshot(), candidate.getSnapshot(), true);
            CompactDecayingEstimatedHistogramReservoirTest.assertEquivalent(reference.getPercentileSnapshot(), candidate.getPercentileSnapshot(), false);
        }
    }

    static final class TestClock implements MonotonicClock
    {
        volatile long time;

        public long now() { return time; }
        public long error() { return 0; }
        public MonotonicClockTranslation translate() { throw new UnsupportedOperationException(); }
        public boolean isAfter(long instant) { return time > instant; }
        public boolean isAfter(long now, long instant) { return now > instant; }
    }
}
