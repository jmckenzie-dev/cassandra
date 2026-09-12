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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.TestClock;

import static org.junit.Assert.assertEquals;

public class LazyMetricIdPropertyTest
{
    @BeforeClass
    public static void disableTickers()
    {
        LazyMetricIdTest.disableTickers();
    }

    @Test
    public void generatedSignedUpdatesResetsAndZeroCountsPreserveIds()
    {
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(seed);
            ThreadLocalCounter[] lazy = new ThreadLocalCounter[128];
            ThreadLocalCounter[] eager = new ThreadLocalCounter[128];
            long[] expected = new long[128];
            for (int i = 0; i < lazy.length; i++)
            {
                lazy[i] = ThreadLocalCounter.create(true);
                eager[i] = ThreadLocalCounter.create(false);
            }
            for (int step = 0; step < 5000; step++)
            {
                int index = random.nextInt(lazy.length);
                long[] boundaries = { 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, random.nextLong() };
                long value = boundaries[random.nextInt(boundaries.length)];
                switch (random.nextInt(4))
                {
                    case 0:
                        lazy[index].inc(value);
                        eager[index].inc(value);
                        expected[index] += value;
                        break;
                    case 1:
                        lazy[index].dec(value);
                        eager[index].dec(value);
                        expected[index] -= value;
                        break;
                    case 2:
                        lazy[index].reset();
                        eager[index].reset();
                        expected[index] = 0;
                        break;
                    default:
                        break;
                }
                String context = "seed=" + seed + " step=" + step + " index=" + index;
                assertEquals(context, expected[index], eager[index].getCount());
                assertEquals(context, expected[index], lazy[index].getCount());
            }
        }
    }

    @Test
    public void generatedMeterTicksMatchEveryRateBit()
    {
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(seed);
            TestClock clock = new TestClock();
            Meter[] meters = { ThreadLocalMeter.create(clock, false), ThreadLocalMeter.create(clock, true),
                               GeometricThreadLocalMeter.create(clock, false), GeometricThreadLocalMeter.create(clock, true) };
            for (int step = 0; step < 2000; step++)
            {
                switch (random.nextInt(3))
                {
                    case 0:
                        long[] counts = { 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, random.nextLong() };
                        long count = counts[random.nextInt(counts.length)];
                        for (Meter meter : meters)
                            meter.mark(count);
                        break;
                    case 1:
                        long[] advances = { 0, 1, TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(5) + 1,
                                            TimeUnit.DAYS.toNanos(1) };
                        clock.time += advances[random.nextInt(advances.length)];
                        break;
                    default:
                        ThreadLocalMeter.tickAll();
                        GeometricThreadLocalMeter.tickAll();
                        break;
                }
                String context = "seed=" + seed + " step=" + step;
                for (Meter meter : meters)
                {
                    assertEquals(context, meters[0].getCount(), meter.getCount());
                    assertEquals(context, Double.doubleToLongBits(meters[0].getOneMinuteRate()), Double.doubleToLongBits(meter.getOneMinuteRate()));
                    assertEquals(context, Double.doubleToLongBits(meters[0].getFiveMinuteRate()), Double.doubleToLongBits(meter.getFiveMinuteRate()));
                    assertEquals(context, Double.doubleToLongBits(meters[0].getFifteenMinuteRate()), Double.doubleToLongBits(meter.getFifteenMinuteRate()));
                    assertEquals(context, Double.doubleToLongBits(meters[0].getMeanRate()), Double.doubleToLongBits(meter.getMeanRate()));
                }
            }
        }
    }
}
