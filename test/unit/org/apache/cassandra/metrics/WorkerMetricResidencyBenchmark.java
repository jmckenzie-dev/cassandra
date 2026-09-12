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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.concurrent.FastThreadLocalThread;

/** Run through ai-profile-many-tables so stdout/stderr and exit status are logged. */
public class WorkerMetricResidencyBenchmark
{
    private static final int GROUPS = 1000;
    private static final int COUNTERS = 58;
    private static final int TOUCHED = 6;
    private static final int UPDATES = 2_000_000;
    private static final int ROUNDS = 7;

    public static void main(String[] args) throws Exception
    {
        if (args.length != 3)
            throw new IllegalArgumentException("workers shared|partitioned original|eager|lazy");
        int workers = Integer.parseInt(args[0]);
        boolean shared = args[1].equals("shared");
        boolean original = args[2].equals("original");
        if ((workers != 8 && workers != 64) || (!shared && !args[1].equals("partitioned")))
            throw new IllegalArgumentException("Use 8 or 64 workers and shared or partitioned access");
        com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocations.isThreadAllocatedMemorySupported())
            throw new IllegalStateException("Thread allocation counters required");
        allocations.setThreadAllocatedMemoryEnabled(true);
        ThreadLocalCounter warmup = original ? new ThreadLocalCounter() : ThreadLocalCounter.create();
        warmup.inc();
        warmup.getCount();
        long constructStart = System.nanoTime();
        long constructAllocation = allocations.getThreadAllocatedBytes(Thread.currentThread().getId());
        ThreadLocalCounter[][] counters = new ThreadLocalCounter[GROUPS][COUNTERS];
        for (ThreadLocalCounter[] group : counters)
            for (int i = 0; i < group.length; i++)
                group[i] = original ? new ThreadLocalCounter() : ThreadLocalCounter.create();
        long constructionBytes = allocations.getThreadAllocatedBytes(Thread.currentThread().getId()) - constructAllocation;
        System.out.println(String.format(Locale.ROOT, "construction,%s,%d,%s,%d,%d,%d", args[2], workers, args[1],
                                         System.nanoTime() - constructStart, constructionBytes, ThreadLocalMetrics.getAllocatedMetricsCount()));

        CyclicBarrier barrier = new CyclicBarrier(workers + 1);
        CountDownLatch stop = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        long[][] times = new long[workers][ROUNDS + 1];
        long[][] bytes = new long[workers][ROUNDS + 1];
        int[] capacities = new int[workers];
        List<Thread> threads = new ArrayList<>();
        for (int worker = 0; worker < workers; worker++)
        {
            int index = worker;
            Thread thread = new FastThreadLocalThread(() -> {
                try
                {
                    List<ThreadLocalCounter> selected = new ArrayList<>();
                    for (int group = 0; group < GROUPS; group++)
                        if (shared || group % workers == index)
                            for (int metric = 0; metric < TOUCHED; metric++)
                                selected.add(counters[group][metric]);
                    ThreadLocalCounter[] active = selected.toArray(new ThreadLocalCounter[0]);
                    barrier.await(30, TimeUnit.SECONDS);
                    long before = allocations.getThreadAllocatedBytes(Thread.currentThread().getId());
                    long start = System.nanoTime();
                    for (ThreadLocalCounter counter : active)
                        counter.inc();
                    times[index][0] = System.nanoTime() - start;
                    bytes[index][0] = allocations.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;
                    capacities[index] = original ? -1 : ThreadLocalMetrics.get().getCounterCapacity();
                    barrier.await(30, TimeUnit.SECONDS);
                    for (int round = 1; round <= ROUNDS; round++)
                    {
                        barrier.await(30, TimeUnit.SECONDS);
                        before = allocations.getThreadAllocatedBytes(Thread.currentThread().getId());
                        start = System.nanoTime();
                        int position = 0;
                        for (int i = 0; i < UPDATES; i++)
                        {
                            active[position].inc();
                            if (++position == active.length)
                                position = 0;
                        }
                        times[index][round] = System.nanoTime() - start;
                        bytes[index][round] = allocations.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;
                        barrier.await(30, TimeUnit.SECONDS);
                    }
                    stop.await(30, TimeUnit.SECONDS);
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                    barrier.reset();
                }
            }, "metric-benchmark-" + worker);
            thread.start();
            threads.add(thread);
        }
        try
        {
            barrier.await(30, TimeUnit.SECONDS);
            barrier.await(30, TimeUnit.SECONDS);
            System.gc();
            long slots = 0;
            for (int capacity : capacities)
                slots += capacity;
            Runtime runtime = Runtime.getRuntime();
            System.out.println(String.format(Locale.ROOT, "residency,%s,%d,%s,worker_slots=%d,summary_slots=%d,cleanup_refs=%d,allocated_ids=%d,heap_bytes=%d",
                              args[2], workers, args[1], slots,
                              original ? -1 : ThreadLocalMetrics.getSummaryCapacity(),
                              original ? -1 : ThreadLocalMetrics.getCleanupReferenceCount(),
                              ThreadLocalMetrics.getAllocatedMetricsCount(), runtime.totalMemory() - runtime.freeMemory()));
            report(args, 0, shared ? (long) workers * GROUPS * TOUCHED : GROUPS * TOUCHED, times, bytes, 0);
            for (int round = 1; round <= ROUNDS; round++)
            {
                long start = System.nanoTime();
                barrier.await(30, TimeUnit.SECONDS);
                barrier.await(30, TimeUnit.SECONDS);
                report(args, round, (long) workers * UPDATES, times, bytes, System.nanoTime() - start);
            }
        }
        finally
        {
            stop.countDown();
            for (Thread thread : threads)
                thread.join(30000);
        }
        if (failure.get() != null)
            throw new AssertionError(failure.get());
        long count = 0;
        for (ThreadLocalCounter[] group : counters)
            for (ThreadLocalCounter counter : group)
                count += counter.getCount();
        long expected = (shared ? (long) workers : 1) * GROUPS * TOUCHED + (long) workers * UPDATES * ROUNDS;
        if (count != expected)
            throw new AssertionError("count=" + count + " expected=" + expected);
        System.out.println("verified_count=" + count);
        java.lang.ref.Reference.reachabilityFence(warmup);
    }

    private static void report(String[] args, int round, long operations, long[][] times, long[][] bytes, long wall)
    {
        long totalTime = 0;
        long totalBytes = 0;
        for (int i = 0; i < times.length; i++)
        {
            totalTime += times[i][round];
            totalBytes += bytes[i][round];
        }
        System.out.println(String.format(Locale.ROOT, "recording,%s,%s,%s,round=%d,operations=%d,thread_ns_per_op=%.3f,bytes_per_op=%.6f,wall_ns=%d",
                          args[2], args[0], args[1], round, operations, (double) totalTime / operations,
                          (double) totalBytes / operations, wall));
    }
}
