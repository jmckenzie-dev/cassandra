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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Metered;
import com.codahale.metrics.Snapshot;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NoOpMetricsTest
{
    @Test
    public void recognizesOnlySharedDisabledMetrics()
    {
        assertTrue(NoOpMetrics.isNoOp(NoOpMetrics.COUNTER));
        assertTrue(NoOpMetrics.isNoOp(NoOpMetrics.METER));
        assertTrue(NoOpMetrics.isNoOp(NoOpMetrics.HISTOGRAM));
        assertTrue(NoOpMetrics.isNoOp(NoOpMetrics.TIMER));
        assertFalse(NoOpMetrics.isNoOp(new com.codahale.metrics.Counter()));
        assertFalse(NoOpMetrics.isNoOp(null));
        assertTrue(NoOpMetrics.COUNTER instanceof Counter);
        assertTrue(NoOpMetrics.METER instanceof Meter);
    }

    @Test
    public void snapshotsStayEmptyAfterEveryUpdateOverload()
    {
        for (long value : new long[]{ Long.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE, Long.MAX_VALUE })
            updateAll(value);
        NoOpMetrics.RESERVOIR.clear();
        assertEmpty();

        Snapshot snapshot = NoOpMetrics.HISTOGRAM.getSnapshot();
        assertSame(snapshot, NoOpMetrics.HISTOGRAM.getSnapshot());
        assertSame(snapshot, NoOpMetrics.TIMER.getSnapshot());
        assertSame(snapshot, NoOpMetrics.TIMER.getPercentileSnapshot());
        assertSame(snapshot, NoOpMetrics.RESERVOIR.getSnapshot());
        assertSame(snapshot, NoOpMetrics.RESERVOIR.getPercentileSnapshot());
        assertEquals(0, NoOpMetrics.RESERVOIR.size());
        assertEquals(0, NoOpMetrics.RESERVOIR.buckets(100).length);
        assertEquals(CassandraReservoir.BucketStrategy.none, NoOpMetrics.HISTOGRAM.bucketStrategy());
        assertEquals(CassandraReservoir.BucketStrategy.none, NoOpMetrics.TIMER.bucketStrategy());
        assertEquals(0, NoOpMetrics.HISTOGRAM.bucketStarts(100).length);
        assertEquals(0, NoOpMetrics.TIMER.bucketStarts(100).length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        snapshot.dump(output);
        assertEquals(0, output.size());
    }

    @Test
    public void timerCallbacksExecuteOnceAndReturnTheirValues() throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        Object value = new Object();
        assertSame(value, NoOpMetrics.TIMER.time((Callable<Object>) () -> {
            calls.incrementAndGet();
            return value;
        }));
        assertSame(value, NoOpMetrics.TIMER.timeSupplier(() -> {
            calls.incrementAndGet();
            return value;
        }));
        NoOpMetrics.TIMER.time((Runnable) calls::incrementAndGet);
        assertEquals(3, calls.get());
        assertEmpty();
    }

    @Test
    public void timerCallbacksPreserveExceptions() throws Exception
    {
        Exception checked = new Exception("callback failure");
        try
        {
            NoOpMetrics.TIMER.time((Callable<Object>) () -> { throw checked; });
            fail("Expected callable exception");
        }
        catch (Exception e)
        {
            assertSame(checked, e);
        }

        RuntimeException unchecked = new IllegalStateException("callback failure");
        try
        {
            NoOpMetrics.TIMER.timeSupplier(() -> { throw unchecked; });
            fail("Expected supplier exception");
        }
        catch (RuntimeException e)
        {
            assertSame(unchecked, e);
        }
        try
        {
            NoOpMetrics.TIMER.time((Runnable) () -> { throw unchecked; });
            fail("Expected runnable exception");
        }
        catch (RuntimeException e)
        {
            assertSame(unchecked, e);
        }
        assertEmpty();
    }

    @Test
    public void timerContextsReturnElapsedTimeWithoutRecording()
    {
        try (com.codahale.metrics.Timer.Context context = NoOpMetrics.TIMER.time())
        {
            assertTrue(context.stop() >= 0);
        }
        try (Timer.Context context = NoOpMetrics.TIMER.startTime())
        {
            assertTrue(context.stop() >= 0);
        }
        assertEmpty();
    }

    @Test
    public void disabledLatencyPreservesItsConcreteTimerApi() throws Exception
    {
        LatencyMetrics latency = LatencyMetrics.noop();
        assertSame(latency, LatencyMetrics.noop());
        assertFalse(latency.isRecording());
        latency.addNano(1_000_000);
        latency.latency.update(20, TimeUnit.MILLISECONDS);
        latency.latency.update(Duration.ofSeconds(1));
        latency.totalLatency.inc(1000);
        Object value = new Object();
        assertSame(value, latency.latency.time((Callable<Object>) () -> value));
        assertSame(value, latency.latency.timeSupplier(() -> value));
        AtomicInteger calls = new AtomicInteger();
        latency.latency.time((Runnable) calls::incrementAndGet);
        assertEquals(1, calls.get());
        assertEquals(0, latency.totalLatency.getCount());
        assertEquals(0, latency.latency.getCount());
        assertEquals(0, latency.latency.getMeanRate(), 0);
        assertEquals(0, latency.latency.getOneMinuteRate(), 0);
        assertEquals(0, latency.latency.getFiveMinuteRate(), 0);
        assertEquals(0, latency.latency.getFifteenMinuteRate(), 0);
        assertSame(NoOpMetrics.HISTOGRAM.getSnapshot(), latency.latency.getSnapshot());
        latency.release();
        latency.release();
        assertEquals(0, latency.latency.getSnapshot().size());
    }

    @Test
    public void disabledLatencyParentDoesNotRetainChildren()
    {
        LatencyMetrics parent = LatencyMetrics.noop();
        MetricNameFactory factory = new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, "noop_parent_test");
        LatencyMetrics child = new LatencyMetrics(factory, "Child", parent);
        try
        {
            assertTrue(child.isRecording());
            child.addNano(1_000_000);
            assertEquals(1, child.latency.getCount());
            assertEquals(1000, child.totalLatency.getCount());
            assertEquals(0, parent.latency.getCount());
            assertEquals(0, parent.totalLatency.getCount());
            assertEquals(0, parent.latency.getSnapshot().size());
            child.release();
            assertEquals(0, parent.latency.getSnapshot().size());
        }
        finally
        {
            child.release();
        }
    }

    private static void updateAll(long value)
    {
        NoOpMetrics.COUNTER.inc();
        NoOpMetrics.COUNTER.dec();
        NoOpMetrics.COUNTER.inc(value);
        NoOpMetrics.COUNTER.dec(value);
        NoOpMetrics.METER.mark();
        NoOpMetrics.METER.mark(value);
        NoOpMetrics.HISTOGRAM.update((int) value);
        NoOpMetrics.HISTOGRAM.update(value);
        NoOpMetrics.TIMER.update(value, TimeUnit.NANOSECONDS);
        NoOpMetrics.TIMER.update(Duration.ofNanos(value));
        NoOpMetrics.RESERVOIR.update(value);
    }

    private static void assertEmpty()
    {
        assertEquals(0, NoOpMetrics.COUNTER.getCount());
        assertEquals(0, NoOpMetrics.HISTOGRAM.getCount());
        for (Metered metric : new Metered[]{ NoOpMetrics.METER, NoOpMetrics.TIMER })
        {
            assertEquals(0, metric.getCount());
            assertEquals(0, metric.getMeanRate(), 0);
            assertEquals(0, metric.getOneMinuteRate(), 0);
            assertEquals(0, metric.getFiveMinuteRate(), 0);
            assertEquals(0, metric.getFifteenMinuteRate(), 0);
        }
        Snapshot snapshot = NoOpMetrics.HISTOGRAM.getSnapshot();
        assertEquals(0, snapshot.size());
        assertEquals(0, snapshot.getValues().length);
        assertEquals(0, snapshot.getMin());
        assertEquals(0, snapshot.getMax());
        assertEquals(0, snapshot.getMean(), 0);
        assertEquals(0, snapshot.getStdDev(), 0);
        for (double quantile : new double[]{ 0, 0.5, 0.99, 0.9999, 1 })
            assertEquals(0, snapshot.getValue(quantile), 0);
    }

    public static class Properties
    {
        @Test
        public void generatedUpdatesStayEmpty()
        {
            for (long seed : new long[]{ 1, 42, 1009, 8675309 })
            {
                Random random = new Random(seed);
                for (int operation = 0; operation < 10000; operation++)
                {
                    updateAll(random.nextLong());
                    if (random.nextBoolean())
                        NoOpMetrics.RESERVOIR.clear();
                    assertEmpty();
                }
            }
        }

        @Test
        public void concurrentCallersShareNoRecordingState() throws Exception
        {
            ExecutorService executor = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            try
            {
                for (int worker = 0; worker < 8; worker++)
                {
                    final long seed = worker;
                    futures.add(executor.submit(() -> {
                        start.await();
                        Random random = new Random(seed);
                        for (int operation = 0; operation < 10000; operation++)
                        {
                            updateAll(random.nextLong());
                            assertEmpty();
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> future : futures)
                    future.get(30, TimeUnit.SECONDS);
                assertEmpty();
            }
            finally
            {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }
}
