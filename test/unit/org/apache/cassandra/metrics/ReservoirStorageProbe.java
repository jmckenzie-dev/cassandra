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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.sun.management.ThreadMXBean;

import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.ObjectSizes;

/** Standalone probe run through ai-profile-many-tables with PROFILE_MAIN_CLASS set to this class. */
public class ReservoirStorageProbe
{
    private static final int BUCKETS = 164;
    private static final int DEFAULT_STRIPES = 2;
    private static final int STORAGE_BATCH = 100;
    private static final long[] VALUES = EstimatedHistogram.newOffsets(BUCKETS, false);
    private static volatile Object sink;

    private final MethodHandle constructor;
    private final ThreadMXBean allocation;
    private final ExecutorService workers = Executors.newFixedThreadPool(4);
    private final int samples;
    private final int warmups;
    private final int updates;
    private final int stripes;

    private ReservoirStorageProbe(String implementation, int samples, int warmups, int updates, int stripes) throws Exception
    {
        Class<? extends Reservoir> type = Class.forName(implementation).asSubclass(Reservoir.class);
        MethodHandle unbound = MethodHandles.publicLookup().unreflectConstructor(type.getConstructor(boolean.class, int.class, int.class,
                                                                                                    MonotonicClock.class, long.class));
        constructor = MethodHandles.insertArguments(unbound, 0, false, BUCKETS, stripes, new FixedClock(), TimeUnit.MINUTES.toNanos(30))
                                   .asType(MethodType.methodType(Reservoir.class));
        allocation = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemorySupported())
            throw new IllegalStateException("Thread allocation counters are unavailable");
        allocation.setThreadAllocatedMemoryEnabled(true);
        this.samples = samples;
        this.warmups = warmups;
        this.updates = updates;
        this.stripes = stripes;
    }

    public static void main(String[] args) throws Throwable
    {
        if (args.length != 1 && args.length != 4 && args.length != 5)
            throw new IllegalArgumentException("Expected reservoir-class [samples warmups updates-per-sample [stripes]]");
        int samples = args.length >= 4 ? Integer.parseInt(args[1]) : 5;
        int warmups = args.length >= 4 ? Integer.parseInt(args[2]) : 3;
        int updates = args.length >= 4 ? Integer.parseInt(args[3]) : 200_000;
        int stripes = args.length == 5 ? Integer.parseInt(args[4]) : DEFAULT_STRIPES;
        if (samples < 1 || warmups < 1 || updates < 4 || updates % 4 != 0 || (stripes != 1 && stripes != 2))
            throw new IllegalArgumentException("Positive samples/warmups, updates divisible by four, and one or two stripes are required");
        ReservoirStorageProbe probe = new ReservoirStorageProbe(args[0], samples, warmups, updates, stripes);
        try
        {
            System.out.println(String.format("reservoir_probe class=%s buckets=%d stripes=%d clock=fixed_zero samples=%d warmups=%d updates=%d",
                                             args[0], BUCKETS, probe.stripes, samples, warmups, updates));
            probe.run();
        }
        finally
        {
            sink = null;
            probe.workers.shutdownNow();
            if (!probe.workers.awaitTermination(30, TimeUnit.SECONDS))
                throw new IllegalStateException("Probe workers did not stop");
        }
    }

    private Reservoir create() throws Throwable
    {
        return (Reservoir) constructor.invokeExact();
    }

    private void run() throws Throwable
    {
        for (int occupied : new int[]{ 0, 1, 4, 64, BUCKETS })
            storage(occupied);

        for (int iteration = -warmups; iteration < samples; iteration++)
        {
            boolean report = iteration >= 0;
            construction(iteration, report, false);
            construction(iteration, report, true);
            for (int occupied : new int[]{ 0, 1, BUCKETS })
                snapshots(iteration, report, occupied);
            for (int threads : new int[]{ 1, 4 })
                for (int occupied : new int[]{ 1, BUCKETS })
                    updates(iteration, report, threads, occupied);
        }
    }

    private void populate(Reservoir reservoir, int occupied)
    {
        for (int i = 0; i < occupied; i++)
            reservoir.update(VALUES[i]);
    }

    private void storage(int occupied) throws Throwable
    {
        Reservoir[] batch = new Reservoir[STORAGE_BATCH];
        for (int i = 0; i < batch.length; i++)
        {
            batch[i] = create();
            populate(batch[i], occupied);
        }
        long single = ObjectSizes.measureDeep(batch[0]);
        long group = ObjectSizes.measureDeep(batch) - ObjectSizes.sizeOfReferenceArray(batch.length);
        for (Reservoir reservoir : batch)
            requirePopulation(reservoir.getSnapshot(), occupied);
        long afterSnapshot = ObjectSizes.measureDeep(batch) - ObjectSizes.sizeOfReferenceArray(batch.length);
        System.out.println(String.format("reservoir_storage occupied=%d count=%d single_reachable_bytes=%d group_reachable_bytes=%d amortized_bytes=%.2f after_snapshot_group_bytes=%d",
                                         occupied, batch.length, single, group, (double) group / batch.length, afterSnapshot));
        sink = batch;
    }

    private void construction(int sample, boolean report, boolean firstUpdate) throws Throwable
    {
        int count = 1000;
        long beforeBytes = allocated();
        long before = System.nanoTime();
        for (int i = 0; i < count; i++)
        {
            Reservoir reservoir = create();
            if (firstUpdate)
                reservoir.update(VALUES[0]);
            sink = reservoir;
        }
        long elapsed = System.nanoTime() - before;
        long bytes = allocated() - beforeBytes;
        if (report)
            report(sample, firstUpdate ? "construct_first_update" : "construct", 1, firstUpdate ? 1 : 0, count, elapsed, bytes);
    }

    private void snapshots(int sample, boolean report, int occupied) throws Throwable
    {
        Reservoir reservoir = create();
        populate(reservoir, occupied);
        int count = 1000;
        long beforeBytes = allocated();
        long before = System.nanoTime();
        for (int i = 0; i < count; i++)
            sink = reservoir.getSnapshot();
        long elapsed = System.nanoTime() - before;
        long bytes = allocated() - beforeBytes;
        requirePopulation((Snapshot) sink, occupied);
        if (report)
            report(sample, "snapshot", 1, occupied, count, elapsed, bytes);
    }

    private void updates(int sample, boolean report, int threads, int occupied) throws Throwable
    {
        Reservoir reservoir = create();
        populate(reservoir, occupied);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Long>> results = new ArrayList<>();
        for (int worker = 0; worker < threads; worker++)
        {
            final int offset = worker;
            results.add(workers.submit(() -> {
                ready.countDown();
                if (!start.await(30, TimeUnit.SECONDS))
                    throw new IllegalStateException("Update start timed out");
                long beforeBytes = allocated();
                for (int i = 0; i < updates / threads; i++)
                    reservoir.update(VALUES[(i + offset) % occupied]);
                return allocated() - beforeBytes;
            }));
        }
        if (!ready.await(30, TimeUnit.SECONDS))
            throw new IllegalStateException("Update workers did not become ready");
        long before = System.nanoTime();
        start.countDown();
        long bytes = 0;
        for (Future<Long> result : results)
            bytes += result.get(30, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - before;
        requirePopulation(reservoir.getSnapshot(), updates + occupied);
        sink = reservoir;
        if (report)
            report(sample, "update", threads, occupied, updates, elapsed, bytes);
    }

    private long allocated()
    {
        long bytes = allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
        if (bytes < 0)
            throw new IllegalStateException("Thread allocation counter became unavailable");
        return bytes;
    }

    private static void requirePopulation(Snapshot snapshot, long expected)
    {
        long count = 0;
        for (long value : snapshot.getValues())
            count += value;
        if (count != expected)
            throw new AssertionError("Expected cumulative population " + expected + " but got " + count);
    }

    private static void report(int sample, String operation, int threads, int occupied, int count, long nanos, long bytes)
    {
        System.out.println(String.format("reservoir_timing sample=%d operation=%s threads=%d occupied=%d operations=%d nanos=%d ns_per_op=%.3f allocated_bytes=%d bytes_per_op=%.3f",
                                         sample, operation, threads, occupied, count, nanos, (double) nanos / count, bytes, (double) bytes / count));
    }

    private static final class FixedClock implements MonotonicClock
    {
        public long now() { return 0; }
        public long error() { return 0; }
        public boolean isAfter(long instant) { return 0 > instant; }
        public boolean isAfter(long now, long instant) { return now > instant; }
        public MonotonicClockTranslation translate() { throw new UnsupportedOperationException(); }
    }
}
