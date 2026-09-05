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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static org.apache.cassandra.config.CassandraRelevantProperties.GEOMETRIC_METER_ARRAYS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeometricThreadLocalMeterTest
{
    static final long TICK = TimeUnit.SECONDS.toNanos(5);

    @BeforeClass
    public static void disableTickers()
    {
        ThreadLocalMeter.disableBackgroundTicking();
        GeometricThreadLocalMeter.disableBackgroundTicking();
    }

    @Test
    public void factorySelectsRegistryAndTimerMeters()
    {
        for (boolean geometric : new boolean[]{ false, true })
        {
            try (WithProperties properties = new WithProperties().set(GEOMETRIC_METER_ARRAYS, geometric))
            {
                Class<?> expected = geometric ? GeometricThreadLocalMeter.class : ThreadLocalMeter.class;
                assertEquals(expected, Meter.create().getClass());
                ThreadLocalTimer timer = new ThreadLocalTimer();
                assertEquals(expected, timer.meter.getClass());
                timer.update(7, TimeUnit.MILLISECONDS);
                assertEquals(1, timer.getCount());

                CassandraMetricsRegistry.MetricName name = new CassandraMetricsRegistry.MetricName(
                    "org.apache.cassandra.metrics", "Table", "GeometricMeterTest", Boolean.toString(geometric));
                try
                {
                    com.codahale.metrics.Meter meter = CassandraMetricsRegistry.Metrics.meter(name);
                    assertEquals(expected, meter.getClass());
                    meter.mark(9);
                    assertEquals(9, CassandraMetricsRegistry.Metrics.meter(name).getCount());
                }
                finally
                {
                    CassandraMetricsRegistry.Metrics.remove(name);
                }
            }
        }
    }

    @Test
    public void exactTickBoundariesAndLongIdle()
    {
        TestClock clock = new TestClock();
        Pair pair = new Pair(clock);
        pair.assertEquivalent("empty");
        pair.mark(7);
        pair.assertEquivalent("first mark at creation time");
        for (long time : new long[]{ TICK - 1, TICK, TICK + 1, 2 * TICK, 2 * TICK + 1 })
        {
            clock.time = time;
            tickBoth();
            pair.assertEquivalent("time=" + time);
        }
        for (long count : new long[]{ 0, -1, Long.MAX_VALUE, 1, Long.MIN_VALUE })
        {
            pair.mark(count);
            clock.time += TICK + 1;
            tickBoth();
            pair.assertEquivalent("count=" + count);
        }
        clock.time = Long.MAX_VALUE;
        tickBoth();
        pair.assertEquivalent("long idle reset");
        assertBits("reset", Double.MIN_NORMAL * TimeUnit.SECONDS.toNanos(1), pair.candidate.getOneMinuteRate());
    }

    @Test
    public void growthPreservesEarlierRates()
    {
        TestClock clock = new TestClock();
        List<Pair> pairs = new ArrayList<>();
        Pair first = new Pair(clock);
        pairs.add(first);
        first.mark(31);
        clock.time = TICK + 1;
        tickBoth();
        int initialCapacity = GeometricThreadLocalMeter.rateCapacity();
        int previousCapacity = initialCapacity;
        int growths = 0;
        while (growths < 2)
        {
            Pair pair = new Pair(clock);
            pairs.add(pair);
            pair.mark(pairs.size());
            int capacity = GeometricThreadLocalMeter.rateCapacity();
            if (capacity != previousCapacity)
            {
                assertEquals(previousCapacity * 2, capacity);
                previousCapacity = capacity;
                growths++;
            }
            first.assertEquivalent("allocation=" + pairs.size());
        }
        clock.time += 2 * TICK;
        tickBoth();
        for (Pair pair : pairs)
            pair.assertEquivalent("after growth");
        assertEquals(initialCapacity * 4, GeometricThreadLocalMeter.rateCapacity());
    }

    @Test
    public void growthArithmeticDoesNotOverflow()
    {
        assertEquals(96, GeometricThreadLocalMeter.grownCapacity(48, 51));
        assertEquals(200, GeometricThreadLocalMeter.grownCapacity(48, 200));
        assertEquals(Integer.MAX_VALUE, GeometricThreadLocalMeter.grownCapacity(1 << 30, (1 << 30) + 3));
        assertEquals(Integer.MAX_VALUE, GeometricThreadLocalMeter.grownCapacity(Integer.MAX_VALUE - 3, Integer.MAX_VALUE));
    }

    @Test
    public void collectedMeterReleasesAndReusesRateStorage()
    {
        TestClock clock = new TestClock();
        GeometricThreadLocalMeter meter = new GeometricThreadLocalMeter(clock);
        meter.mark(73);
        clock.time = TICK + 1;
        GeometricThreadLocalMeter.tickAll();
        int offset = meter.rateGroupOffset();
        assertFalse(GeometricThreadLocalMeter.isRateGroupAvailable(offset));
        WeakReference<GeometricThreadLocalMeter> reference = new WeakReference<>(meter);
        meter = null;
        Util.spinAssertEquals(true, () -> {
            System.gc();
            return reference.get() == null && GeometricThreadLocalMeter.isRateGroupAvailable(offset);
        }, 20);
        int allocatedThrough = GeometricThreadLocalMeter.rateGroupIdGenerator.get();
        List<GeometricThreadLocalMeter> keepAlive = new ArrayList<>();
        GeometricThreadLocalMeter replacement;
        do
        {
            replacement = new GeometricThreadLocalMeter(clock);
            keepAlive.add(replacement);
            assertEquals(allocatedThrough, GeometricThreadLocalMeter.rateGroupIdGenerator.get());
        }
        while (replacement.rateGroupOffset() != offset);
        ThreadLocalMeter referenceMeter = new ThreadLocalMeter(clock);
        assertEquivalent(referenceMeter, replacement, "recycled empty slot");
        referenceMeter.mark(3);
        replacement.mark(3);
        clock.time += TICK + 1;
        tickBoth();
        assertEquivalent(referenceMeter, replacement, "recycled updated slot");
        assertTrue(keepAlive.contains(replacement));
    }

    @Test
    public void concurrentRegistrationMarkingAndTicking() throws Exception
    {
        TestClock clock = new TestClock();
        Pair shared = new Pair(clock);
        List<Pair> pairs = new ArrayList<>();
        pairs.add(shared);
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try
        {
            for (int round = 0; round < 3; round++)
            {
                List<Future<?>> writers = new ArrayList<>();
                for (int writer = 0; writer < 4; writer++)
                    writers.add(executor.submit(() -> {
                        for (int mark = 0; mark < 1000; mark++)
                            shared.mark(1);
                    }));
                for (Future<?> writer : writers)
                    writer.get(30, TimeUnit.SECONDS);

                clock.time += TICK + 1;
                CountDownLatch start = new CountDownLatch(1);
                List<Future<List<Pair>>> registrations = new ArrayList<>();
                for (int writer = 0; writer < 4; writer++)
                    registrations.add(executor.submit(() -> {
                        start.await();
                        List<Pair> created = new ArrayList<>();
                        for (int i = 0; i < 64; i++)
                        {
                            Pair pair = new Pair(clock);
                            pair.mark(i + 1);
                            created.add(pair);
                        }
                        return created;
                    }));
                Future<?> ticker = executor.submit(() -> {
                    start.await();
                    tickBoth();
                    return null;
                });
                start.countDown();
                ticker.get(30, TimeUnit.SECONDS);
                for (Future<List<Pair>> registration : registrations)
                    pairs.addAll(registration.get(30, TimeUnit.SECONDS));
                for (Pair pair : pairs)
                    pair.assertEquivalent("concurrent round=" + round);
                clock.time += TICK + 1;
                tickBoth();
                for (Pair pair : pairs)
                    pair.assertEquivalent("settled round=" + round);
            }
            assertEquals(12_000, shared.candidate.getCount());
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    static void tickBoth()
    {
        ThreadLocalMeter.tickAll();
        GeometricThreadLocalMeter.tickAll();
    }

    static void assertEquivalent(Meter reference, Meter candidate, String context)
    {
        assertEquals(context + " count", reference.getCount(), candidate.getCount());
        assertBits(context + " mean", reference.getMeanRate(), candidate.getMeanRate());
        assertBits(context + " m1", reference.getOneMinuteRate(), candidate.getOneMinuteRate());
        assertBits(context + " m5", reference.getFiveMinuteRate(), candidate.getFiveMinuteRate());
        assertBits(context + " m15", reference.getFifteenMinuteRate(), candidate.getFifteenMinuteRate());
    }

    private static void assertBits(String context, double reference, double candidate)
    {
        assertEquals(context, Double.doubleToRawLongBits(reference), Double.doubleToRawLongBits(candidate));
    }

    static final class Pair
    {
        final ThreadLocalMeter reference;
        final GeometricThreadLocalMeter candidate;

        Pair(TestClock clock)
        {
            reference = new ThreadLocalMeter(clock);
            candidate = new GeometricThreadLocalMeter(clock);
        }

        void mark(long count)
        {
            reference.mark(count);
            candidate.mark(count);
        }

        void assertEquivalent(String context)
        {
            GeometricThreadLocalMeterTest.assertEquivalent(reference, candidate, context);
        }
    }

    static final class TestClock implements MonotonicClock
    {
        volatile long time;

        public long now()
        {
            return time;
        }

        public long error()
        {
            return 0;
        }

        public MonotonicClockTranslation translate()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isAfter(long instant)
        {
            return time > instant;
        }

        public boolean isAfter(long now, long instant)
        {
            return now > instant;
        }
    }
}
