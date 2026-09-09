/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import com.sun.management.ThreadMXBean;
import io.opentelemetry.sdk.metrics.internal.aggregator.OtelArrayAccess;

import org.apache.cassandra.utils.ObjectSizes;

/** Single-owner storage microbenchmark. The ai-* launcher owns logging and JVM configuration. */
public final class OtelStorageBenchmark
{
    private static volatile Object observed;
    private static final long[] SEEDS = { 1, 128, 32768, 2147483648L };
    private static final String[] WIDTHS = { "byte", "short", "int", "long" };

    public static void main(String[] args)
    {
        Options options = Options.parse(args);
        if (options.weightedOverflowCheck)
        {
            weightedOverflowCheck(options.buckets);
            return;
        }
        if (options.propertyOnly)
        {
            properties(options.buckets);
            return;
        }
        verify(options.buckets);
        if (options.verifyOnly)
            return;
        ThreadMXBean allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocations.isThreadAllocatedMemorySupported())
            throw new UnsupportedOperationException("Thread allocation counters required");
        allocations.setThreadAllocatedMemoryEnabled(true);
        System.out.printf(Locale.ROOT, "# java=%s cpus=%d heap_bytes=%d buckets=%d population=%d iterations=%d warmup=%d rounds=%d%n",
                          Runtime.version(), Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory(),
                          options.buckets, options.population, options.iterations, options.warmup, options.rounds);
        System.out.println("# jvm_args=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println("# Single owner; Cassandra uses atomics, other paths do not. No decay, collection, or contention.");
        System.out.println("# Steady batches contain at most 64 updates per object, with clearing and preseed outside timing.");
        System.out.println("# Full final bucket checksums are outside timing. All measured samples retained.");
        System.out.println("memory,implementation,case,buckets,population,graph_bytes,marginal_bytes");
        memory(options);
        System.out.println("sample,implementation,case,round,order,operations,ns_per_op,bytes_per_op,checksum");
        for (int width = 0; width < WIDTHS.length; width++)
            for (boolean spread : new boolean[]{ false, true })
                benchmark(options, allocations, (spread ? "spread-" : "hot-") + WIDTHS[width], SEEDS[width], true, spread);
        benchmark(options, allocations, "construction", 0, false, false);
        benchmark(options, allocations, "first-touch", 0, false, false);
        benchmark(options, allocations, "widen-byte", Byte.MAX_VALUE, false, false);
        benchmark(options, allocations, "widen-short", Short.MAX_VALUE, false, false);
        benchmark(options, allocations, "widen-int", Integer.MAX_VALUE, false, false);
        observed = null;
        System.out.println("# benchmark=PASS");
    }

    private static void memory(Options options)
    {
        for (Kind kind : Kind.values())
            for (int state = 0; state < 6; state++)
            {
                long seed = state == 0 ? 0 : SEEDS[Math.min(state - 1, 3)];
                Object[] full = population(kind, options.population, options.buckets, seed);
                Object[] less = population(kind, options.population - 1, options.buckets, seed);
                if (state == 5)
                {
                    reset(kind, full, options.buckets, 0);
                    reset(kind, less, options.buckets, 0);
                }
                observed = new Object[]{ full, less };
                long fullBytes = ObjectSizes.measureDeep(full) - ObjectSizes.measure(full);
                long lessBytes = ObjectSizes.measureDeep(less) - ObjectSizes.measure(less);
                System.out.printf(Locale.ROOT, "memory,%s,%s,%d,%d,%d,%d%n", kind,
                                  state == 0 ? "empty" : state == 5 ? "reset-long" : WIDTHS[state - 1],
                                  options.buckets, options.population, fullBytes, fullBytes - lessBytes);
            }
    }

    private static void benchmark(Options options, ThreadMXBean allocations, String name, long seed, boolean steady, boolean spread)
    {
        for (int round = -options.warmup; round < options.rounds; round++)
        {
            Sample[] samples = new Sample[3];
            for (int order = 0; order < 3; order++)
            {
                Kind kind = Kind.values()[Math.floorMod(round + order, 3)];
                Sample sample = measure(options, allocations, kind, name, seed, steady, spread);
                samples[kind.ordinal()] = sample;
                if (round >= 0)
                    System.out.printf(Locale.ROOT, "sample,%s,%s,%d,%d,%d,%.6f,%.6f,%d%n", kind, name,
                                      round, order, sample.operations, (double) sample.nanos / sample.operations,
                                      (double) sample.bytes / sample.operations, sample.checksum);
            }
            if (samples[0].checksum != samples[1].checksum || samples[0].checksum != samples[2].checksum)
                throw new AssertionError("Different checksums for " + name + " round=" + round);
        }
    }

    private static Sample measure(Options options, ThreadMXBean allocations, Kind kind, String name,
                                  long seed, boolean steady, boolean spread)
    {
        int count = steady ? options.iterations : Math.min(options.iterations, 10000);
        boolean construction = name.equals("construction");
        Object[] arrays = population(kind, options.population, options.buckets, seed);
        int limit = options.population * (steady ? 64 : 1);
        int[] indexes = new int[limit];
        int[] buckets = new int[limit];
        int stride = options.buckets % 73 == 0 ? 1 : 73;
        for (int i = 0; i < limit; i++)
        {
            indexes[i] = i % options.population;
            buckets[i] = spread ? ((i / options.population) * stride) % options.buckets : 0;
        }
        long nanos = 0, bytes = 0, checksum = 1;
        long thread = Thread.currentThread().threadId();
        for (int completed = 0; completed < count;)
        {
            int batch = Math.min(limit, count - completed);
            if (construction)
                Arrays.fill(arrays, null);
            else if (steady)
                reset(kind, arrays, options.buckets, seed);
            else
                arrays = population(kind, options.population, options.buckets, seed);
            long beforeBytes = allocations.getThreadAllocatedBytes(thread);
            long beforeTime = System.nanoTime();
            if (construction)
                for (int i = 0; i < batch; i++)
                    arrays[i] = kind.create(options.buckets);
            else
                update(kind, arrays, indexes, buckets, batch);
            nanos += System.nanoTime() - beforeTime;
            bytes += allocations.getThreadAllocatedBytes(thread) - beforeBytes;
            observed = arrays;
            checksum = 31 * checksum + checksum(kind, arrays, options.buckets);
            completed += batch;
        }
        return new Sample(count, nanos, bytes, checksum);
    }

    private static void update(Kind kind, Object[] arrays, int[] indexes, int[] buckets, int count)
    {
        switch (kind)
        {
            case LONG:
                for (int i = 0; i < count; i++)
                    ((long[]) arrays[indexes[i]])[buckets[i]]++;
                break;
            case CASSANDRA:
                for (int i = 0; i < count; i++)
                    ((AdaptiveCounterArray) arrays[indexes[i]]).addAndGet(buckets[i], 1);
                break;
            case OTEL:
                for (int i = 0; i < count; i++)
                    OtelArrayAccess.add(arrays[indexes[i]], buckets[i], 1);
                break;
        }
    }

    private static Object[] population(Kind kind, int count, int buckets, long seed)
    {
        Object[] arrays = new Object[count];
        for (int i = 0; i < count; i++)
        {
            arrays[i] = kind.create(buckets);
            if (seed != 0)
                kind.add(arrays[i], 0, seed);
        }
        return arrays;
    }

    private static void reset(Kind kind, Object[] arrays, int buckets, long seed)
    {
        for (Object array : arrays)
        {
            kind.clear(array, buckets);
            if (seed != 0)
                kind.add(array, 0, seed);
        }
    }

    private static long checksum(Kind kind, Object[] arrays, int buckets)
    {
        long result = 1;
        for (Object array : arrays)
            if (array != null)
                for (int bucket = 0; bucket < buckets; bucket++)
                    result = 31 * result + kind.get(array, bucket);
        return result;
    }

    private static void verify(int buckets)
    {
        for (Kind kind : Kind.values())
        {
            Object array = kind.create(buckets);
            long previous = 0;
            for (long value : new long[]{ 0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, 2147483648L, Long.MAX_VALUE })
            {
                kind.add(array, buckets - 1, value - previous);
                if (kind.get(array, buckets - 1) != value)
                    throw new AssertionError(kind + " failed boundary " + value);
                previous = value;
            }
            kind.add(array, buckets - 1, 1);
            if (kind.get(array, buckets - 1) != Long.MIN_VALUE)
                throw new AssertionError(kind + " failed long overflow");
            kind.clear(array, buckets);
            for (int i = 0; i < buckets; i++)
                if (kind.get(array, i) != 0)
                    throw new AssertionError(kind + " clear failed");
        }
        Object original = OtelArrayAccess.create(buckets);
        for (long seed : SEEDS)
        {
            OtelArrayAccess.add(original, 0, seed);
            Object copy = OtelArrayAccess.copy(original);
            long value = OtelArrayAccess.get(original, 0);
            OtelArrayAccess.add(copy, 0, 1);
            if (OtelArrayAccess.length(copy) != buckets || OtelArrayAccess.get(original, 0) != value
                || OtelArrayAccess.get(copy, 0) != value + 1)
                throw new AssertionError("OTel copy independence failed");
        }
        System.out.println("# verify=PASS boundaries=byte-short-int-long-overflow-clear-copy");
    }

    private static void weightedOverflowCheck(int buckets)
    {
        boolean equivalent = true;
        for (long seed : SEEDS)
        {
            Object otel = OtelArrayAccess.create(buckets);
            AdaptiveCounterArray cassandra = new AdaptiveCounterArray(buckets);
            OtelArrayAccess.add(otel, 0, seed);
            cassandra.addAndGet(0, seed);
            OtelArrayAccess.add(otel, 0, Long.MAX_VALUE);
            long actual = OtelArrayAccess.get(otel, 0);
            long expected = seed + Long.MAX_VALUE;
            long reference = cassandra.addAndGet(0, Long.MAX_VALUE);
            System.out.printf(Locale.ROOT, "weighted-overflow,seed=%d,expected=%d,cassandra=%d,otel=%d%n",
                              seed, expected, reference, actual);
            if (reference != expected)
                throw new AssertionError("Cassandra differs from long addition");
            equivalent &= actual == expected;
        }
        if (!equivalent)
            throw new AssertionError("OTel narrow weighted addition does not preserve long overflow");
    }

    private static void properties(int buckets)
    {
        long operations = 0;
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(0x6f74656cL + seed);
            Object cassandra = Kind.CASSANDRA.create(buckets);
            Object otel = Kind.OTEL.create(buckets);
            long[] oracle = new long[buckets];
            for (int step = 0; step < 2000; step++)
            {
                int operation = random.nextInt(32);
                if (operation == 0)
                {
                    Arrays.fill(oracle, 0);
                    Kind.CASSANDRA.clear(cassandra, buckets);
                    OtelArrayAccess.clear(otel);
                }
                else if (operation == 1)
                {
                    Object copy = OtelArrayAccess.copy(otel);
                    OtelArrayAccess.clear(otel);
                    otel = copy;
                }
                else
                {
                    int index = random.nextInt(buckets);
                    long delta = new long[]{ 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, 2147483648L }[random.nextInt(7)];
                    oracle[index] += delta;
                    Kind.CASSANDRA.add(cassandra, index, delta);
                    OtelArrayAccess.add(otel, index, delta);
                }
                for (int i = 0; i < buckets; i++)
                    if (oracle[i] != Kind.CASSANDRA.get(cassandra, i) || oracle[i] != OtelArrayAccess.get(otel, i))
                        throw new AssertionError("Property failed seed=" + seed + " step=" + step + " bucket=" + i);
                operations++;
            }
        }
        System.out.println("# properties=PASS seeds=16 operations=" + operations + " buckets=" + buckets);
    }

    private enum Kind
    {
        LONG, CASSANDRA, OTEL;

        Object create(int buckets)
        {
            switch (this)
            {
                case LONG: return new long[buckets];
                case CASSANDRA: return new AdaptiveCounterArray(buckets);
                case OTEL: return OtelArrayAccess.create(buckets);
                default: throw new AssertionError(this);
            }
        }

        void add(Object array, int index, long delta)
        {
            switch (this)
            {
                case LONG: ((long[]) array)[index] += delta; break;
                case CASSANDRA: ((AdaptiveCounterArray) array).addAndGet(index, delta); break;
                case OTEL: OtelArrayAccess.add(array, index, delta); break;
            }
        }

        long get(Object array, int index)
        {
            switch (this)
            {
                case LONG: return ((long[]) array)[index];
                case CASSANDRA: return ((AdaptiveCounterArray) array).get(index);
                case OTEL: return OtelArrayAccess.get(array, index);
                default: throw new AssertionError(this);
            }
        }

        void clear(Object array, int buckets)
        {
            switch (this)
            {
                case LONG: Arrays.fill((long[]) array, 0); break;
                case CASSANDRA:
                    for (int i = 0; i < buckets; i++)
                        ((AdaptiveCounterArray) array).set(i, 0);
                    break;
                case OTEL: OtelArrayAccess.clear(array); break;
            }
        }
    }

    private record Sample(int operations, long nanos, long bytes, long checksum) {}

    private static final class Options
    {
        int iterations = 1000000, warmup = 5, rounds = 9, buckets = 165, population = 1000;
        boolean verifyOnly, propertyOnly, weightedOverflowCheck;

        static Options parse(String[] args)
        {
            Options options = new Options();
            for (int i = 0; i < args.length; i++)
            {
                String key = args[i];
                if (key.equals("--verify-only")) { options.verifyOnly = true; continue; }
                if (key.equals("--property-only")) { options.propertyOnly = true; continue; }
                if (key.equals("--weighted-overflow-check")) { options.weightedOverflowCheck = true; continue; }
                if (++i == args.length)
                    throw new IllegalArgumentException("Missing value for " + key);
                int value = Integer.parseInt(args[i]);
                if (value <= 0)
                    throw new IllegalArgumentException("Positive value required for " + key);
                switch (key)
                {
                    case "--iterations": options.iterations = value; break;
                    case "--warmup-rounds": options.warmup = value; break;
                    case "--rounds": options.rounds = value; break;
                    case "--buckets": options.buckets = value; break;
                    case "--population": options.population = value; break;
                    default: throw new IllegalArgumentException("Unknown option " + key);
                }
            }
            if (options.population < 2 || options.population > 1000 || options.buckets > 10000)
                throw new IllegalArgumentException("Population must be 2..1000; buckets must be <=10000");
            if ((options.verifyOnly ? 1 : 0) + (options.propertyOnly ? 1 : 0) + (options.weightedOverflowCheck ? 1 : 0) > 1)
                throw new IllegalArgumentException("Choose only one verification mode");
            return options;
        }
    }
}
