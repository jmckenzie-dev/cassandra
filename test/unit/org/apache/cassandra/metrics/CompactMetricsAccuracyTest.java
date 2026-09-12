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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;

import org.junit.Test;

import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompactMetricsAccuracyTest
{
    private static final int OBSERVATIONS = 1_000_000;
    private static final int STRIPES = 2;
    private static final long[] OFFSETS = EstimatedHistogram.newOffsets(164, false);

    @Test
    public void millionRawEventsPreserveMedianAndRareTailBuckets() throws Exception
    {
        long[] events = events();
        long[] sorted = events.clone();
        Arrays.sort(sorted);
        long[] expected = cumulativeBuckets(events);
        for (int threads : new int[]{ 1, 4 })
        {
            TestClock clock = new TestClock();
            ClearableReservoir[] reservoirs = reservoirs(clock, TimeUnit.MINUTES.toNanos(30));
            record(events, reservoirs, threads);
            for (ClearableReservoir reservoir : reservoirs)
            {
                Snapshot snapshot = reservoir.getSnapshot();
                assertArrayEquals(expected, snapshot.getValues());
                assertArrayEquals(expected, decayedBuckets(snapshot));
                assertEquals(OBSERVATIONS, snapshot.size());
                for (double quantile : new double[]{ 0.5, 0.99, 0.9999 })
                {
                    int rank = (int) Math.ceil(OBSERVATIONS * quantile) - 1;
                    long upper = OFFSETS[bucket(sorted[rank])];
                    assertEquals("threads=" + threads + " quantile=" + quantile, (double) upper, snapshot.getValue(quantile), 0.0);
                }
                assertEquals(OFFSETS[bucket(sorted[sorted.length - 1])], snapshot.getMax());
                assertTrue("The 200 rare observations must remain visible at p99.99", snapshot.getValue(0.9999) >= 1_000_000_000L);
                assertRawMomentsWithinBucketError(events, snapshot);
            }
        }
    }

    @Test
    public void halfLifeRoundingIsBoundedWhileLifetimeCountsAndRecentTailsRemainExact() throws Exception
    {
        long[] events = events();
        long[] expectedLifetime = cumulativeBuckets(events);
        TestClock clock = new TestClock();
        ClearableReservoir[] reservoirs = reservoirs(clock, TimeUnit.SECONDS.toNanos(59));
        record(events, reservoirs, 4);
        clock.time = TimeUnit.SECONDS.toNanos(60);

        long[][] decayed = new long[reservoirs.length][];
        for (int implementation = 0; implementation < reservoirs.length; implementation++)
        {
            Snapshot snapshot = reservoirs[implementation].getSnapshot();
            assertArrayEquals(expectedLifetime, snapshot.getValues());
            decayed[implementation] = decayedBuckets(snapshot);
            assertHalvedCounts(expectedLifetime, decayed[implementation], new long[expectedLifetime.length]);
            assertEquals(OFFSETS[bucket(1_000_000_000L + 199)], snapshot.getMax());
        }
        assertStripeRoundingDifference(decayed[0], decayed[1]);

        long recentTail = 1_000_000_000_000L;
        long[] fresh = new long[expectedLifetime.length];
        fresh[bucket(recentTail)] = 1000;
        for (ClearableReservoir reservoir : reservoirs)
            for (int event = 0; event < 1000; event++)
                reservoir.update(recentTail);

        long[] updatedLifetime = expectedLifetime.clone();
        updatedLifetime[bucket(recentTail)] += 1000;
        for (int implementation = 0; implementation < reservoirs.length; implementation++)
        {
            Snapshot snapshot = reservoirs[implementation].getSnapshot();
            assertArrayEquals(updatedLifetime, snapshot.getValues());
            decayed[implementation] = decayedBuckets(snapshot);
            assertHalvedCounts(expectedLifetime, decayed[implementation], fresh);
            assertEquals(OFFSETS[bucket(recentTail)], snapshot.getMax());
            assertEquals((double) OFFSETS[bucket(recentTail)], snapshot.getValue(0.9999), 0.0);
        }
        assertStripeRoundingDifference(decayed[0], decayed[1]);
    }

    private static void assertHalvedCounts(long[] original, long[] actual, long[] fresh)
    {
        assertEquals(original.length, actual.length);
        for (int bucket = 0; bucket < original.length; bucket++)
        {
            // Halving integer counters rounds each stripe upward by at most half an observation.
            long twiceRoundingError = 2 * (actual[bucket] - fresh[bucket]) - original[bucket];
            assertTrue("bucket=" + bucket + " doubled rounding error=" + twiceRoundingError,
                       twiceRoundingError >= 0 && twiceRoundingError <= STRIPES);
        }
    }

    private static void assertRawMomentsWithinBucketError(long[] events, Snapshot snapshot)
    {
        long sum = 0;
        long totalError = 0;
        double squaredError = 0;
        for (long event : events)
        {
            sum += event;
            long error = OFFSETS[bucket(event)] - event;
            totalError += error;
            squaredError += (double) error * error;
        }
        double rawMean = (double) sum / events.length;
        double largestMean = (double) (sum + totalError) / events.length;
        assertTrue("Mean must stay inside the raw-to-bucket-upper-bound interval",
                   snapshot.getMean() >= rawMean && snapshot.getMean() <= largestMean);

        double squaredDeviations = 0;
        for (long event : events)
        {
            double deviation = event - rawMean;
            squaredDeviations += deviation * deviation;
        }
        double rawStdDev = Math.sqrt(squaredDeviations / (events.length - 1));
        // Centering cannot increase the norm of the per-event quantization errors.
        double maximumStdDevError = Math.sqrt(squaredError / (events.length - 1));
        assertTrue("Standard deviation must stay within the observed bucket quantization error",
                   Math.abs(snapshot.getStdDev() - rawStdDev) <= maximumStdDevError);
    }

    private static void assertStripeRoundingDifference(long[] reference, long[] candidate)
    {
        for (int bucket = 0; bucket < reference.length; bucket++)
            assertTrue("bucket=" + bucket + " differs beyond the per-stripe rounding bound",
                       2 * Math.abs(reference[bucket] - candidate[bucket]) <= STRIPES);
    }

    private static long[] events()
    {
        long[] values = new long[OBSERVATIONS];
        for (int i = 0; i < values.length; i++)
        {
            long value;
            if (i < 970_000)
                value = 1000 + i % 101;
            else if (i < 999_000)
                value = 100_000 + i % 1001;
            else if (i < 999_800)
                value = 10_000_000 + i % 10_001;
            else
                value = 1_000_000_000L + i - 999_800;
            values[(int) ((i * 524287L) % values.length)] = value;
        }
        return values;
    }

    private static long[] cumulativeBuckets(long[] events)
    {
        long[] counts = new long[OFFSETS.length + 1];
        for (long event : events)
            counts[bucket(event)]++;
        return counts;
    }

    private static int bucket(long value)
    {
        int index = Arrays.binarySearch(OFFSETS, value);
        return index >= 0 ? index : -index - 1;
    }

    private static long[] decayedBuckets(Snapshot snapshot)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        snapshot.dump(output);
        return new String(output.toByteArray(), StandardCharsets.UTF_8).lines().mapToLong(Long::parseLong).toArray();
    }

    private static ClearableReservoir[] reservoirs(TestClock clock, long resetInterval)
    {
        return new ClearableReservoir[]{ new DecayingEstimatedHistogramReservoir(false, 164, STRIPES, clock, resetInterval),
                                         new CompactDecayingEstimatedHistogramReservoir(false, 164, STRIPES, clock, resetInterval) };
    }

    private static void record(long[] events, ClearableReservoir[] reservoirs, int threads) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try
        {
            List<Future<?>> results = new ArrayList<>();
            for (int worker = 0; worker < threads; worker++)
            {
                int offset = worker;
                results.add(executor.submit(() -> {
                    if (!start.await(30, TimeUnit.SECONDS))
                        throw new IllegalStateException("Writer start timed out");
                    for (int i = offset; i < events.length; i += threads)
                        for (ClearableReservoir reservoir : reservoirs)
                            reservoir.update(events[i]);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results)
                result.get(30, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static final class TestClock implements MonotonicClock
    {
        private volatile long time;

        public long now() { return time; }
        public long error() { return 0; }
        public boolean isAfter(long instant) { return time > instant; }
        public boolean isAfter(long now, long instant) { return now > instant; }
        public MonotonicClockTranslation translate() { throw new UnsupportedOperationException(); }
    }
}
