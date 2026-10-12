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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Each thread must hold storage only for the metric ids it updates. A single array sized to the highest id a thread
 * touched gave every such thread a slot for every metric: 85 MB per thread at 80k tables.
 */
public class ThreadLocalMetricsPagingTest
{
    private final List<Integer> ids = new ArrayList<>();

    @After
    public void recycle()
    {
        ids.forEach(ThreadLocalMetrics::recycleMetricId);
        ids.clear();
    }

    private int[] allocate(int count)
    {
        int[] out = new int[count];
        for (int i = 0; i < count; i++)
        {
            out[i] = ThreadLocalMetrics.allocateMetricId();
            ids.add(out[i]);
        }
        return out;
    }

    @Test
    public void aThreadThatUpdatesOneNewMetricAllocatesOnePage() throws InterruptedException
    {
        int[] metrics = allocate(20_000);
        int newest = metrics[metrics.length - 1];
        AtomicInteger pages = new AtomicInteger();
        AtomicInteger capacity = new AtomicInteger();
        Thread thread = new Thread(() -> {
            ThreadLocalMetrics.add(newest, 5);
            pages.set(ThreadLocalMetrics.get().getAllocatedPageCount());
            capacity.set(ThreadLocalMetrics.get().getCounterCapacity());
        });
        thread.start();
        thread.join();

        assertEquals("pages for one updated id", 1, pages.get());
        assertTrue("the index covers the id: " + capacity.get(), capacity.get() > newest);
        assertEquals(5, ThreadLocalMetrics.getCount(newest));
    }

    @Test
    public void valuesAcrossPagesAndThreadsAddUpAndSurviveThreadExit() throws InterruptedException
    {
        int[] metrics = allocate(3 * ThreadLocalMetrics.PAGE_SIZE + 7);
        int threads = 8;
        int rounds = 1000;
        LocalAwareExecutorPlus executor = executorFactory().localAware().pooled("paging-test", threads);
        for (int t = 0; t < threads * 4; t++)
        {
            executor.submit(() -> {
                for (int r = 0; r < rounds; r++)
                    for (int i = 0; i < metrics.length; i += 3)
                        ThreadLocalMetrics.add(metrics[i], 1);
            });
        }
        long expected = (long) threads * 4 * rounds;
        for (int i = 0; i < metrics.length; i += 3)
            assertEventually(expected, metrics[i]);
        for (int i = 1; i < metrics.length; i += 3)
            assertEquals("untouched id " + i, 0, ThreadLocalMetrics.getCount(metrics[i]));

        // FastThreadLocal.onRemoval moves each exiting thread's values into the summary
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        for (int i = 0; i < metrics.length; i += 3)
            assertEquals("after thread exit, id " + i, expected, ThreadLocalMetrics.getCount(metrics[i]));
    }

    @Test
    public void recyclingZeroesTheIdInEveryThread() throws InterruptedException
    {
        int[] metrics = allocate(2 * ThreadLocalMetrics.PAGE_SIZE);
        int id = metrics[metrics.length - 1];
        Thread a = new Thread(() -> ThreadLocalMetrics.add(id, 3));
        a.start();
        a.join();
        ThreadLocalMetrics.add(id, 4);
        assertEquals(7, ThreadLocalMetrics.getCount(id));

        ThreadLocalMetrics.recycleMetricId(id);
        ids.remove(Integer.valueOf(id));
        assertEquals(0, ThreadLocalMetrics.getCount(id));
    }

    private static void assertEventually(long expected, int id) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (ThreadLocalMetrics.getCount(id) != expected && System.nanoTime() < deadline)
            Thread.sleep(10);
        assertEquals(expected, ThreadLocalMetrics.getCount(id));
    }
}
