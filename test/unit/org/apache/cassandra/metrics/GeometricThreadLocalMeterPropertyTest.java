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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.Pair;
import org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.TestClock;

import static org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.TICK;
import static org.apache.cassandra.metrics.GeometricThreadLocalMeterTest.tickBoth;

public class GeometricThreadLocalMeterPropertyTest
{
    @BeforeClass
    public static void disableTickers()
    {
        GeometricThreadLocalMeterTest.disableTickers();
    }

    @Test
    public void generatedTracesPreserveEveryRateBit()
    {
        for (long seed : new long[]{ 1, 42, 19332, 0x5eed })
        {
            Random random = new Random(seed);
            TestClock clock = new TestClock();
            List<Pair> pairs = new ArrayList<>();
            pairs.add(new Pair(clock));
            for (int step = 0; step < 1024; step++)
            {
                int operation = random.nextInt(5);
                String context = "seed=" + seed + " step=" + step + " operation=" + operation;
                switch (operation)
                {
                    case 0:
                        if (pairs.size() < 128)
                            pairs.add(new Pair(clock));
                        break;
                    case 1:
                        long[] counts = { 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, random.nextLong() };
                        pairs.get(random.nextInt(pairs.size())).mark(counts[random.nextInt(counts.length)]);
                        break;
                    case 2:
                        long[] advances = { 0, 1, TICK - 1, TICK, TICK + 1, TimeUnit.DAYS.toNanos(1) };
                        clock.time += advances[random.nextInt(advances.length)];
                        break;
                    case 3:
                        tickBoth();
                        break;
                    case 4:
                        pairs.get(random.nextInt(pairs.size())).mark(random.nextInt(1000));
                        break;
                    default:
                        throw new AssertionError(operation);
                }
                for (Pair pair : pairs)
                    pair.assertEquivalent(context);
            }
            clock.time += TICK + 1;
            tickBoth();
            for (Pair pair : pairs)
                pair.assertEquivalent("seed=" + seed + " final tick");
        }
    }
}
