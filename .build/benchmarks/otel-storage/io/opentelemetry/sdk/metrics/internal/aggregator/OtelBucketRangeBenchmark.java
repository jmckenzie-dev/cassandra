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
package io.opentelemetry.sdk.metrics.internal.aggregator;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import org.github.jamm.MemoryMeter;

/** Fixed-scale capacity and quantile bounds, using the actual OTel indexer and circular counter. */
public final class OtelBucketRangeBenchmark
{
    private static final MemoryMeter METER = MemoryMeter.builder().build();

    private OtelBucketRangeBenchmark()
    {
    }

    public static void main(String[] args)
    {
        if (args.length != 0)
            throw new IllegalArgumentException("--bucket-ranges takes no further arguments");
        System.out.println("# Fixed-scale storage experiment; no runtime downscaling, locking, or SDK aggregator.");
        System.out.println("# Each synthetic positive-integer corpus has 200000 observations; scale selection is offline.");
        System.out.println("# Quantile intervals are bucket bounds, not an OTel percentile estimator.");
        System.out.println("range,workload,budget,scale,used_buckets,marginal_counter_bytes,plain_array_bytes,bucket_width_percent");
        System.out.println("quantile,workload,budget,scale,quantile,exact,lower_exclusive,upper_inclusive");
        for (String workload : new String[]{ "narrow", "broad", "rare-tail", "one-outlier" })
        {
            long[] values = corpus(workload);
            long[] sorted = values.clone();
            Arrays.sort(sorted);
            for (int budget : new int[]{ 64, 165, 256 })
                measure(workload, values, sorted, budget);
        }
        System.out.println("# verified: exact bin counts, capacity rejection, clear/copy independence, quantile containment");
    }

    private static void measure(String workload, long[] values, long[] sorted, int budget)
    {
        int scale = 20;
        Base2ExponentialHistogramIndexer indexer;
        int start;
        int end;
        while (true)
        {
            indexer = Base2ExponentialHistogramIndexer.get(scale);
            start = indexer.computeIndex(sorted[0]);
            end = indexer.computeIndex(sorted[sorted.length - 1]);
            if ((long) end - start + 1 <= budget)
                break;
            --scale;
        }

        AdaptingCircularBufferCounter counts = new AdaptingCircularBufferCounter(budget);
        long[] exactBins = new long[budget];
        for (long value : values)
        {
            int index = indexer.computeIndex(value);
            if (!counts.increment(index, 1))
                throw new AssertionError("Offline-selected range must fit");
            exactBins[index - start]++;
        }
        for (int i = 0; i < budget; ++i)
        {
            if (counts.get(start + i) != exactBins[i])
                throw new AssertionError("Counter differs from exact bins");
        }
        if (counts.increment(start + budget, 1))
            throw new AssertionError("Out-of-capacity recording must fail");

        Object[] population = new Object[1000];
        for (int i = 0; i < population.length; ++i)
            population[i] = new AdaptingCircularBufferCounter(counts);
        Object[] half = Arrays.copyOf(population, 500);
        long bytes = METER.measureDeep(population) - METER.measureDeep(half)
                     - METER.measure(population) + METER.measure(half);
        if (bytes % 500 != 0)
            throw new AssertionError("Nonintegral marginal size");
        double step = Math.scalb(1.0, -scale);
        System.out.printf(Locale.ROOT, "range,%s,%d,%d,%d,%d,%d,%.6f%n",
                          workload, budget, scale, end - start + 1, bytes / 500,
                          METER.measure(new long[budget]), (Math.pow(2, step) - 1) * 100);

        for (double quantile : new double[]{ 0.5, 0.99, 0.9999, 1.0 })
        {
            int rank = (int) Math.ceil(quantile * values.length);
            long total = 0;
            int bucket = start;
            for (; bucket <= end; ++bucket)
            {
                total += counts.get(bucket);
                if (total >= rank)
                    break;
            }
            long exact = sorted[rank - 1];
            double lower = Math.pow(2, bucket * step);
            double upper = Math.pow(2, (bucket + 1.0) * step);
            if (exact < lower * (1 - 1e-12) || exact > upper * (1 + 1e-12))
                throw new AssertionError("Exact quantile outside exponential bucket");
            System.out.printf(Locale.ROOT, "quantile,%s,%d,%d,%.4f,%d,%.6f,%.6f%n",
                              workload, budget, scale, quantile, exact, lower, upper);
        }

        AdaptingCircularBufferCounter copy = new AdaptingCircularBufferCounter(counts);
        counts.clear();
        if (!counts.isEmpty() || copy.isEmpty() || copy.get(start) != exactBins[0])
            throw new AssertionError("Clear affected independent copy");
    }

    private static long[] corpus(String workload)
    {
        Random random = new Random(0x07e1L);
        long[] values = new long[200000];
        for (int i = 0; i < values.length; ++i)
        {
            switch (workload)
            {
                case "narrow":
                    values[i] = 1000 + random.nextInt(1000);
                    break;
                case "broad":
                    values[i] = Math.max(1, (long) Math.pow(10, random.nextDouble() * 12));
                    break;
                case "rare-tail":
                    values[i] = i % 10000 < 2 ? 1000000000L + random.nextInt(1000000)
                                             : 1000 + random.nextInt(1000);
                    break;
                case "one-outlier":
                    values[i] = i == 0 ? 1000000000000L : 1000 + random.nextInt(1000);
                    break;
                default:
                    throw new IllegalArgumentException(workload);
            }
        }
        return values;
    }
}
