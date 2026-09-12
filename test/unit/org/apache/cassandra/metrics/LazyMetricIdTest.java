/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.metrics;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.TestClock;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.LAZY_METRIC_IDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LazyMetricIdTest
{
    @BeforeClass
    public static void disableTickers()
    {
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(new Config());
        ThreadLocalMeter.disableBackgroundTicking();
        GeometricThreadLocalMeter.disableBackgroundTicking();
    }

    @Test
    public void untouchedReadsResetsSnapshotsAndTicksDoNotAllocate()
    {
        TestClock clock = new TestClock();
        ThreadLocalCounter counter = ThreadLocalCounter.create(true);
        ThreadLocalHistogram histogram = ThreadLocalHistogram.create(new DecayingEstimatedHistogramReservoir(), true);
        ThreadLocalMeter meter = ThreadLocalMeter.create(clock, true);
        GeometricThreadLocalMeter geometric = GeometricThreadLocalMeter.create(clock, true);
        int allocated = ThreadLocalMetrics.getAllocatedMetricsCount();
        int references = ThreadLocalMetrics.getCleanupReferenceCount();
        int capacity = ThreadLocalMetrics.getSummaryCapacity();
        for (int i = 0; i < 10; i++)
        {
            counter.reset();
            histogram.reset();
            histogram.getSnapshot();
            assertEquals(0, counter.getCount());
            assertEquals(0, histogram.getCount());
            assertEquals(0, meter.getCount());
            assertEquals(0, geometric.getCount());
            assertEquals(0, meter.getMeanRate(), 0);
            assertEquals(0, geometric.getMeanRate(), 0);
            clock.time += i == 9 ? TimeUnit.DAYS.toNanos(1) : TimeUnit.SECONDS.toNanos(6);
            ThreadLocalMeter.tickAll();
            GeometricThreadLocalMeter.tickAll();
            assertEquals(meter.getOneMinuteRate(), geometric.getOneMinuteRate(), 0);
        }
        assertEquals(allocated, ThreadLocalMetrics.getAllocatedMetricsCount());
        assertEquals(references, ThreadLocalMetrics.getCleanupReferenceCount());
        assertEquals(capacity, ThreadLocalMetrics.getSummaryCapacity());
        assertEquals(-1, counter.metricIdForTesting());
    }

    @Test
    public void configurationIsCapturedAtConstruction()
    {
        String previous = LAZY_METRIC_IDS.getString();
        try
        {
            LAZY_METRIC_IDS.setBoolean(true);
            ThreadLocalCounter lazy = ThreadLocalCounter.create();
            LAZY_METRIC_IDS.setBoolean(false);
            ThreadLocalCounter eager = ThreadLocalCounter.create();
            assertEquals(-1, lazy.metricIdForTesting());
            assertTrue(eager.metricIdForTesting() >= 0);
            lazy.inc(0);
            assertTrue(lazy.metricIdForTesting() >= 0);
            int id = lazy.metricIdForTesting();
            lazy.inc(Long.MAX_VALUE);
            lazy.inc();
            assertEquals(Long.MIN_VALUE, lazy.getCount());
            assertEquals(id, lazy.metricIdForTesting());
        }
        finally
        {
            LAZY_METRIC_IDS.setString(previous);
        }
    }

    @Test
    public void lazyClearableHistogramClearsCountAndReservoir()
    {
        String previous = LAZY_METRIC_IDS.getString();
        try
        {
            LAZY_METRIC_IDS.setBoolean(true);
            ClearableHistogram histogram = ClearableHistogram.create(new DecayingEstimatedHistogramReservoir());
            assertTrue(histogram instanceof LazyClearableHistogram);
            histogram.clear();
            assertEquals(0, histogram.getCount());
            histogram.update(10);
            histogram.update(20L);
            assertEquals(2, histogram.getCount());
            histogram.clear();
            assertEquals(0, histogram.getCount());
            assertEquals(0, java.util.Arrays.stream(histogram.getSnapshot().getValues()).sum());
            histogram.update(30);
            assertEquals(1, histogram.getCount());
            assertEquals(1, java.util.Arrays.stream(histogram.getSnapshot().getValues()).sum());
        }
        finally
        {
            LAZY_METRIC_IDS.setString(previous);
        }
    }

    @Test
    public void lazyLatencyChildrenPreserveReleaseAggregation()
    {
        String previous = LAZY_METRIC_IDS.getString();
        try
        {
            for (boolean lazy : new boolean[] { false, true })
            {
                LAZY_METRIC_IDS.setBoolean(lazy);
                LatencyMetrics parent = new LatencyMetrics(ClientRequestMetrics.TYPE_NAME, "id-parent-" + lazy);
                MetricNameFactory factory = name -> new CassandraMetricsRegistry.MetricName(
                DefaultNameFactory.GROUP_NAME, ClientRequestMetrics.TYPE_NAME, name, "id-child-" + lazy);
                LatencyMetrics first = new LatencyMetrics(factory, "first", parent);
                LatencyMetrics second = new LatencyMetrics(factory, "second", parent);
                try
                {
                    assertEquals(lazy, parent.totalLatency instanceof LazyThreadLocalCounter);
                    assertEquals(lazy, first.totalLatency instanceof LazyThreadLocalCounter);
                    first.addNano(1000);
                    second.addNano(1000000);
                    second.addNano(1000000);
                    long[] values = parent.latency.getSnapshot().getValues();
                    first.release();
                    first.release();
                    assertEquals(3, parent.latency.getCount());
                    assertEquals(2001, parent.totalLatency.getCount());
                    assertArrayEquals(values, parent.latency.getSnapshot().getValues());
                    second.release();
                    assertEquals(3, parent.latency.getCount());
                    assertEquals(2001, parent.totalLatency.getCount());
                    assertArrayEquals(values, parent.latency.getSnapshot().getValues());
                }
                finally
                {
                    first.release();
                    second.release();
                    parent.release();
                }
            }
        }
        finally
        {
            LAZY_METRIC_IDS.setString(previous);
        }
    }

    @Test
    public void racingFirstUseGrowthResetAndWorkerExit() throws Exception
    {
        for (boolean lazy : new boolean[] { false, true })
        {
            ThreadLocalCounter counter = ThreadLocalCounter.create(lazy);
            ThreadLocalHistogram histogram = ThreadLocalHistogram.create(new DecayingEstimatedHistogramReservoir(), lazy);
            TestClock clock = new TestClock();
            ThreadLocalMeter meter = ThreadLocalMeter.create(clock, lazy);
            GeometricThreadLocalMeter geometric = GeometricThreadLocalMeter.create(clock, lazy);
            LocalAwareExecutorPlus executor = executorFactory().localAware().pooled("lazy-id-race", 8);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> writers = new ArrayList<>();
            List<ThreadLocalCounter> growth = new ArrayList<>();
            for (int i = 0; i < 2048; i++)
                growth.add(ThreadLocalCounter.create(lazy));
            try
            {
                for (int worker = 0; worker < 8; worker++)
                {
                    final int index = worker;
                    writers.add(executor.submit(() -> {
                        start.await();
                        for (int i = 0; i < 1000; i++)
                        {
                            counter.inc();
                            histogram.update(1);
                            meter.mark();
                            geometric.mark();
                            if (i < growth.size() / 8)
                                growth.get(index * (growth.size() / 8) + i).inc();
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> writer : writers)
                    writer.get(30, TimeUnit.SECONDS);
                assertEquals(8000, counter.getCount());
                assertEquals(8000, histogram.getCount());
                assertEquals(8000, meter.getCount());
                assertEquals(8000, geometric.getCount());
                counter.reset();
                histogram.reset();
                assertEquals(0, counter.getCount());
                assertEquals(0, histogram.getCount());
                for (int i = 0; i < 8; i++)
                    executor.submit(() -> { counter.inc(-7); histogram.update(2); }).get(30, TimeUnit.SECONDS);
            }
            finally
            {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
            assertEquals(-56, counter.getCount());
            assertEquals(8, histogram.getCount());
            assertEquals(8000, meter.getCount());
            assertEquals(8000, geometric.getCount());
            for (ThreadLocalCounter item : growth)
                assertEquals(1, item.getCount());
        }
    }

    @Test
    public void resetSubtractionPreservesConcurrentUpdates() throws Exception
    {
        for (boolean lazy : new boolean[] { false, true })
        {
            ThreadLocalCounter counter = ThreadLocalCounter.create(lazy);
            counter.inc(0);
            LocalAwareExecutorPlus executor = executorFactory().localAware().pooled("lazy-id-reset", 2);
            CountDownLatch start = new CountDownLatch(1);
            try
            {
                Future<?> writer = executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < 100000; i++)
                        counter.inc();
                    return null;
                });
                Future<Long> resetter = executor.submit(() -> {
                    start.await();
                    long reset = 0;
                    for (int i = 0; i < 1000; i++)
                        reset += ThreadLocalMetrics.getCountAndReset(counter.metricIdForTesting());
                    return reset;
                });
                start.countDown();
                writer.get(30, TimeUnit.SECONDS);
                assertEquals(100000, resetter.get(30, TimeUnit.SECONDS) + counter.getCount());
                Reference.reachabilityFence(counter);
            }
            finally
            {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void collectedIdsAreClearedBeforeReuse() throws Exception
    {
        for (boolean lazy : new boolean[] { false, true })
        {
            ThreadLocalCounter counter = ThreadLocalCounter.create(lazy);
            counter.inc(Long.MIN_VALUE);
            int id = counter.metricIdForTesting();
            assertNotEquals(0, ThreadLocalMetrics.getCount(id));
            WeakReference<ThreadLocalCounter> reference = new WeakReference<>(counter);
            counter = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while ((reference.get() != null || ThreadLocalMetrics.getCount(id) != 0) && System.nanoTime() < deadline)
            {
                System.gc();
                Thread.sleep(10);
            }
            assertEquals(null, reference.get());
            assertEquals(0, ThreadLocalMetrics.getCount(id));
            ThreadLocalMetrics.freeMetricIdSetTracker.triggerRecycling();
            ThreadLocalMetrics.freeMetricIdSetTracker.triggerRecycling();
            List<ThreadLocalCounter> replacements = new ArrayList<>();
            boolean reused = false;
            int limit = ThreadLocalMetrics.idGenerator.get() + 1;
            for (int i = 0; i < limit; i++)
            {
                ThreadLocalCounter replacement = ThreadLocalCounter.create(false);
                replacements.add(replacement);
                assertEquals(0, replacement.getCount());
                if (replacement.metricIdForTesting() == id)
                {
                    replacement.inc(17);
                    assertEquals(17, replacement.getCount());
                    reused = true;
                    break;
                }
            }
            assertTrue("collected ID must become reusable", reused);
            Reference.reachabilityFence(replacements);
        }
    }

    @Test
    public void collectedMetersReleaseBothIdsAndRateGroup() throws Exception
    {
        for (boolean geometric : new boolean[] { false, true })
        {
            for (boolean lazy : new boolean[] { false, true })
            {
                LocalAwareExecutorPlus executor = executorFactory().localAware().pooled("meter-id-cleanup", 1);
                MeterLease lease;
                try
                {
                    lease = executor.submit(() -> new MeterLease(geometric, lazy)).get(30, TimeUnit.SECONDS);
                }
                finally
                {
                    executor.shutdown();
                    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (!lease.released() && System.nanoTime() < deadline)
                {
                    System.gc();
                    Thread.sleep(10);
                }
                assertTrue("both IDs and rate group must release", lease.released());
                ThreadLocalMetrics.freeMetricIdSetTracker.triggerRecycling();
                ThreadLocalMetrics.freeMetricIdSetTracker.triggerRecycling();
                List<ThreadLocalCounter> replacements = new ArrayList<>();
                int reused = 0;
                int limit = ThreadLocalMetrics.idGenerator.get() + 1;
                for (int i = 0; i < limit && reused != 2; i++)
                {
                    ThreadLocalCounter replacement = ThreadLocalCounter.create(false);
                    replacements.add(replacement);
                    assertEquals(0, replacement.getCount());
                    if (replacement.metricIdForTesting() == lease.ids[0] || replacement.metricIdForTesting() == lease.ids[1])
                    {
                        replacement.inc(-19);
                        assertEquals(-19, replacement.getCount());
                        reused++;
                    }
                }
                assertEquals("both meter IDs must be reusable", 2, reused);
                TestClock clock = new TestClock();
                List<Meter> meters = new ArrayList<>();
                boolean rateReused = false;
                int ratesLimit = geometric ? GeometricThreadLocalMeter.rateGroupIdGenerator.get() : ThreadLocalMeter.rateGroupIdGenerator.get();
                for (int i = 0; i <= ratesLimit / 3 && !rateReused; i++)
                {
                    Meter replacement = geometric ? GeometricThreadLocalMeter.create(clock, true) : ThreadLocalMeter.create(clock, true);
                    meters.add(replacement);
                    int offset = geometric ? ((GeometricThreadLocalMeter) replacement).rateGroupOffset() : ((ThreadLocalMeter) replacement).rateGroupOffset();
                    assertEquals(0, replacement.getCount());
                    assertEquals(0, replacement.getOneMinuteRate(), 0);
                    assertEquals(0, replacement.getFiveMinuteRate(), 0);
                    assertEquals(0, replacement.getFifteenMinuteRate(), 0);
                    if (offset == lease.offset)
                    {
                        replacement.mark(3);
                        clock.time += TimeUnit.SECONDS.toNanos(6);
                        ThreadLocalMeter.tickAll();
                        GeometricThreadLocalMeter.tickAll();
                        assertEquals(3, replacement.getCount());
                        assertEquals(0.6, replacement.getOneMinuteRate(), 0.000001);
                        rateReused = true;
                    }
                }
                assertTrue("rate group must be reusable", rateReused);
                Reference.reachabilityFence(replacements);
                Reference.reachabilityFence(meters);
            }
        }
    }

    private static final class MeterLease
    {
        final boolean geometric;
        final WeakReference<Meter> owner;
        final int[] ids;
        final int offset;

        MeterLease(boolean geometric, boolean lazy)
        {
            this.geometric = geometric;
            TestClock clock = new TestClock();
            Meter meter = geometric ? GeometricThreadLocalMeter.create(clock, lazy) : ThreadLocalMeter.create(clock, lazy);
            meter.mark(73);
            ids = geometric ? ((GeometricThreadLocalMeter) meter).counterIds() : ((ThreadLocalMeter) meter).counterIds();
            offset = geometric ? ((GeometricThreadLocalMeter) meter).rateGroupOffset() : ((ThreadLocalMeter) meter).rateGroupOffset();
            assertNotEquals(ids[0], ids[1]);
            assertEquals(73, ThreadLocalMetrics.getCount(ids[0]));
            assertEquals(73, ThreadLocalMetrics.getCount(ids[1]));
            owner = new WeakReference<>(meter);
            Reference.reachabilityFence(meter);
        }

        boolean released()
        {
            return owner.get() == null && ThreadLocalMetrics.getCount(ids[0]) == 0 && ThreadLocalMetrics.getCount(ids[1]) == 0 &&
                   (geometric ? GeometricThreadLocalMeter.isRateGroupAvailable(offset) : ThreadLocalMeter.isRateGroupAvailable(offset));
        }
    }
}
