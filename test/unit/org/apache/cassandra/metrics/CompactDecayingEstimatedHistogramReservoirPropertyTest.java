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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.metrics.CompactDecayingEstimatedHistogramReservoirTest.Pair;
import org.apache.cassandra.metrics.CompactDecayingEstimatedHistogramReservoirTest.TestClock;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;

public class CompactDecayingEstimatedHistogramReservoirPropertyTest
{
    @Test
    public void generatedTracesPreserveBucketsDecayClearAndRebase()
    {
        for (long seed : new long[]{ 1, 42, 1009, 8675309 })
        {
            Random random = new Random(seed);
            for (int example = 0; example < 30; example++)
            {
                TestClock clock = new TestClock();
                boolean zeroes = random.nextBoolean();
                int bucketCount = new int[]{ 17, 127, 164 }[random.nextInt(3)];
                int stripes = 1 << random.nextInt(3);
                long interval = TimeUnit.SECONDS.toNanos(1 + random.nextInt(1800));
                Pair pair = new Pair(zeroes, bucketCount, stripes, clock, interval);
                int operation = -1;
                try
                {
                    pair.assertEquivalent();
                    for (operation = 0; operation < 300; operation++)
                    {
                        switch (random.nextInt(20))
                        {
                            case 0:
                                pair.clear();
                                break;
                            case 1:
                                ((EstimatedHistogramReservoirSnapshot) pair.reference.getSnapshot()).rebaseReservoir();
                                ((EstimatedHistogramReservoirSnapshot) pair.candidate.getSnapshot()).rebaseReservoir();
                                break;
                            case 2:
                            case 3:
                                clock.time += TimeUnit.SECONDS.toNanos(random.nextInt(1801));
                                break;
                            default:
                                pair.update(random.nextInt(10000) - 100);
                        }
                        if (operation % 10 == 0)
                            pair.assertEquivalent();
                    }
                    pair.assertEquivalent();
                }
                catch (AssertionError failure)
                {
                    throw new AssertionError("seed=" + seed + ", example=" + example + ", operation=" + operation
                                             + ", zeroes=" + zeroes + ", buckets=" + bucketCount
                                             + ", stripes=" + stripes + ", interval=" + interval, failure);
                }
            }
        }
    }
}
