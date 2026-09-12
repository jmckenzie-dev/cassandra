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

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.ObjectSizes;

/** Measures retained storage after four distinct workers update each reservoir without overlap. */
public class ReservoirStripeProbe
{
    private static final int RESERVOIRS = 100;
    private static final int BUCKETS = 164;
    private static final int STRIPES = 2;
    private static final long[] VALUES = EstimatedHistogram.newOffsets(BUCKETS, false);

    public static void main(String[] args) throws Exception
    {
        if ((args.length != 1 && args.length != 2)
            || (!args[0].equals("legacy") && !args[0].equals("compact"))
            || (args.length == 2 && !args[1].equals("aged")))
            throw new IllegalArgumentException("Expected legacy or compact, optionally followed by aged");
        boolean compact = args[0].equals("compact");
        AgedClock agedClock = args.length == 2 ? new AgedClock() : null;
        MonotonicClock clock = agedClock == null ? new FixedClock() : agedClock;
        if (agedClock != null)
            System.out.println("reservoir_stripe_age age_seconds=1799 reset_seconds=1800");
        ExecutorService[] workers = new ExecutorService[4];
        try
        {
            for (int worker = 0; worker < workers.length; worker++)
                workers[worker] = Executors.newSingleThreadExecutor();
            for (int occupied : new int[]{ 1, 4, BUCKETS })
            {
                if (agedClock != null)
                    agedClock.time = 0;
                ClearableReservoir[] reservoirs = new ClearableReservoir[RESERVOIRS];
                for (int i = 0; i < reservoirs.length; i++)
                    reservoirs[i] = compact
                                    ? new CompactDecayingEstimatedHistogramReservoir(false, BUCKETS, STRIPES, clock, TimeUnit.MINUTES.toNanos(30))
                                    : new DecayingEstimatedHistogramReservoir(false, BUCKETS, STRIPES, clock, TimeUnit.MINUTES.toNanos(30));
                if (agedClock != null)
                    agedClock.time = TimeUnit.MINUTES.toNanos(30) - TimeUnit.SECONDS.toNanos(1);

                int observations = Math.max(128, occupied);
                long[] threadIds = new long[workers.length];
                for (int worker = 0; worker < workers.length; worker++)
                {
                    threadIds[worker] = workers[worker].submit(() -> {
                        for (ClearableReservoir reservoir : reservoirs)
                            for (int observation = 0; observation < observations; observation++)
                                reservoir.update(VALUES[observation % occupied]);
                        return Thread.currentThread().getId();
                    }).get(30, TimeUnit.SECONDS);
                }
                if (Arrays.stream(threadIds).distinct().count() != workers.length)
                    throw new AssertionError("The probe requires four distinct worker threads");

                long graph = ObjectSizes.measureDeep(reservoirs) - ObjectSizes.sizeOfReferenceArray(reservoirs.length);
                long expected = (long) observations * workers.length;
                for (ClearableReservoir reservoir : reservoirs)
                {
                    long actual = Arrays.stream(reservoir.getSnapshot().getValues()).sum();
                    if (actual != expected)
                        throw new AssertionError("Expected " + expected + " cumulative observations but got " + actual);
                }
                long afterSnapshot = ObjectSizes.measureDeep(reservoirs) - ObjectSizes.sizeOfReferenceArray(reservoirs.length);
                System.out.println(String.format("reservoir_stripes mode=%s occupied=%d reservoirs=%d configured_stripes=%d workers=%d thread_ids=%s observations_per_reservoir=%d graph_bytes=%d amortized_bytes=%.2f after_snapshot_graph_bytes=%d",
                                                 args[0], occupied, reservoirs.length, STRIPES, workers.length, Arrays.toString(threadIds),
                                                 expected, graph, (double) graph / reservoirs.length, afterSnapshot));
            }
        }
        finally
        {
            for (ExecutorService worker : workers)
                if (worker != null)
                    worker.shutdownNow();
            for (ExecutorService worker : workers)
                if (worker != null && !worker.awaitTermination(30, TimeUnit.SECONDS))
                    throw new IllegalStateException("Probe worker did not stop");
        }
    }

    private static class FixedClock implements MonotonicClock
    {
        public long now() { return 0; }
        public long error() { return 0; }
        public boolean isAfter(long instant) { return now() > instant; }
        public boolean isAfter(long now, long instant) { return now > instant; }
        public MonotonicClockTranslation translate() { throw new UnsupportedOperationException(); }
    }

    private static final class AgedClock extends FixedClock
    {
        private volatile long time;

        public long now() { return time; }
    }
}
