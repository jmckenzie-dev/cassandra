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
import java.util.concurrent.FutureTask;
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
import static org.junit.Assert.assertFalse;
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
            assertEquals(0, pair.candidate.allocatedStripeCount());
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
        assertEquals(pair.reference.size() * 2, pair.candidate.allocatedCounterCells());
        for (int i = 0; i < 1000; i++)
            pair.update(i);
        pair.assertEquivalent();
    }

    @Test
    public void moderatelyOccupiedStorageStaysSparseThroughRebase()
    {
        for (int[] testCase : new int[][]{ { 127, 6 }, { 164, 8 } })
        {
            int bucketCount = testCase[0];
            int promotionPages = testCase[1];
            TestClock clock = new TestClock();
            Pair pair = new Pair(false, bucketCount, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
            long[] offsets = pair.reference.buckets(bucketCount);
            for (int pages = 1; pages <= promotionPages; pages++)
            {
                pair.update(offsets[pages - 1]);
                pair.assertEquivalent();
                if (pages < promotionPages)
                {
                    assertEquals(pages * 16 * 2, pair.candidate.allocatedCounterCells());
                    ((EstimatedHistogramReservoirSnapshot) pair.reference.getSnapshot()).rebaseReservoir();
                    ((EstimatedHistogramReservoirSnapshot) pair.candidate.getSnapshot()).rebaseReservoir();
                    pair.assertEquivalent();
                    assertEquals(pages * 16 * 2, pair.candidate.allocatedCounterCells());
                }
                else
                {
                    assertEquals(pair.candidate.size() * 2, pair.candidate.allocatedCounterCells());
                }
            }
        }
    }

    @Test
    public void serialWriterHandoffsKeepOnlyPrimaryStorage() throws Exception
    {
        for (int stripes : new int[]{ 1, 2, 4 })
        {
            TestClock clock = new TestClock();
            Pair pair = new Pair(false, 127, stripes, clock, LANDMARK_RESET_INTERVAL_IN_NS);
            for (int writer = 0; writer < 4; writer++)
            {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try
                {
                    executor.submit(() -> {
                        for (int i = 0; i < 1000; i++)
                            pair.update(i);
                    }).get(30, TimeUnit.SECONDS);
                }
                finally
                {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                }
                pair.assertEquivalent();
                assertEquals(1, pair.candidate.allocatedStripeCount());
                assertEquals(pair.candidate.size() * 2, pair.candidate.allocatedCounterCells());
                assertFalse(pair.candidate.isContended());
            }
            clock.time += LANDMARK_RESET_INTERVAL_IN_NS + 1;
            pair.assertEquivalent();
            assertEquals(1, pair.candidate.allocatedStripeCount());
            assertFalse(pair.candidate.isContended());
        }
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
                assertTrue(pair.candidate.allocatedStripeCount() <= pair.candidate.stripeCount());
            }
            ((EstimatedHistogramReservoirSnapshot) pair.reference.getSnapshot()).rebaseReservoir();
            ((EstimatedHistogramReservoirSnapshot) pair.candidate.getSnapshot()).rebaseReservoir();
            pair.assertEquivalent();
            pair.clear();
            pair.assertEquivalent();
            pair.update(17);
            pair.assertEquivalent();
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test
    public void contentionActivatesStripesAndRescaleRoundsEachStripe() throws Exception
    {
        TestClock clock = new TestClock();
        CompactDecayingEstimatedHistogramReservoir candidate =
            new CompactDecayingEstimatedHistogramReservoir(false, 127, 2, clock, TimeUnit.SECONDS.toNanos(59));
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try
        {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> writers = new ArrayList<>();
            for (int writer = 0; writer < 8; writer++)
                writers.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < 100000; i++)
                        candidate.update(100);
                    return null;
                }));
            start.countDown();
            for (Future<?> writer : writers)
                writer.get(30, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
        assertTrue("Concurrent writers must activate contention routing", candidate.isContended());
        assertEquals(2, candidate.allocatedStripeCount());
        assertEquals(800000L, Arrays.stream(candidate.getSnapshot().getValues()).sum());

        // Odd counts in both stripes distinguish per-stripe rounding from rounding their sum.
        for (int attempt = 0; attempt < 16; attempt++)
        {
            long[] counts = candidate.decayingStripeValues(100);
            if ((counts[0] & 1) != 0 && (counts[1] & 1) != 0)
                break;
            FutureTask<Void> correction = new FutureTask<>(() -> {
                int stripe = (int) (Thread.currentThread().getId() & 1);
                if ((candidate.decayingStripeValues(100)[stripe] & 1) == 0)
                    candidate.update(100);
                return null;
            });
            Thread writer = new Thread(correction);
            writer.start();
            correction.get(30, TimeUnit.SECONDS);
            writer.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(writer.isAlive());
        }
        long[] original = candidate.decayingStripeValues(100);
        assertEquals(1, original[0] & 1);
        assertEquals(1, original[1] & 1);
        long cumulative = original[0] + original[1];
        assertTrue(cumulative >= 800000 && cumulative <= 800002);
        long[] expected = { Math.round(original[0] / 2.0), Math.round(original[1] / 2.0) };
        assertTrue(expected[0] + expected[1] != Math.round(cumulative / 2.0));

        clock.time = TimeUnit.SECONDS.toNanos(60);
        EstimatedHistogramReservoirSnapshot snapshot = (EstimatedHistogramReservoirSnapshot) candidate.getSnapshot();
        assertArrayEquals(expected, candidate.decayingStripeValues(100));
        assertEquals(expected[0] + expected[1], snapshot.size());
        assertEquals(cumulative, Arrays.stream(snapshot.getValues()).sum());
        snapshot.rebaseReservoir();
        assertArrayEquals(new long[]{ expected[0] + expected[1], 0 }, candidate.decayingStripeValues(100));
        assertEquals(cumulative, Arrays.stream(candidate.getSnapshot().getValues()).sum());
        candidate.clear();
        assertEquals(0, candidate.getSnapshot().size());
        assertEquals(0, Arrays.stream(candidate.getSnapshot().getValues()).sum());
        candidate.update(100);
        assertEquals(1, candidate.getSnapshot().size());
        assertEquals(1, Arrays.stream(candidate.getSnapshot().getValues()).sum());
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
        final int stripes;

        Pair(boolean zeroes, int bucketCount, int stripes, TestClock clock, long resetInterval)
        {
            // Uncontended histories use one stripe, including after a snapshot rebase.
            reference = new DecayingEstimatedHistogramReservoir(zeroes, bucketCount, 1, clock, resetInterval);
            candidate = new CompactDecayingEstimatedHistogramReservoir(zeroes, bucketCount, stripes, clock, resetInterval);
            this.stripes = stripes;
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
            assertEquals(stripes, candidate.stripeCount());
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
