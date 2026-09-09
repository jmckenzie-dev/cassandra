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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.metrics;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class AdaptiveHistogramHistoryPropertyTest
{
    @Test
    public void generatedScrapeSequencesMatchLegacyAfterPackingAndReuse()
    {
        for (long seed : new long[]{ 1, 42, 1009, 8675309 })
        {
            Random random = new Random(seed);
            for (int example = 0; example < 40; example++)
            {
                Object history = null;
                long[] previous = null;
                for (int step = 0; step < 100; step++)
                {
                    int length = new int[]{ 0, 1, 2, 17, 165 }[random.nextInt(5)];
                    long[] now = new long[length];
                    int width = random.nextInt(6);
                    for (int i = 0; i < length; i++)
                        now[i] = value(random, width);
                    String context = "seed=" + seed + ", example=" + example + ", step=" + step;
                    long[] expected = CassandraMetricsRegistry.delta(now, previous);
                    long[] actual = AdaptiveHistogramHistory.delta(now, history);
                    assertArrayEquals(context, expected, actual);

                    history = AdaptiveHistogramHistory.pack(now, history);
                    previous = now.clone();
                    Arrays.fill(actual, random.nextLong());
                    assertArrayEquals(context, new long[length], AdaptiveHistogramHistory.delta(now, history));
                    assertArrayEquals(context, previous, now);
                }
            }
        }
    }

    private static long value(Random random, int width)
    {
        switch (width)
        {
            case 0:
                return 0;
            case 1:
                return (byte) random.nextInt();
            case 2:
                return (short) random.nextInt();
            case 3:
                return random.nextInt();
            case 4:
                return random.nextLong();
            default:
                long[] boundaries = { Long.MIN_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                      (long) Integer.MIN_VALUE - 1, (long) Integer.MAX_VALUE + 1,
                                      -32769, -32768, 32767, 32768, -129, -128, 127, 128, -1, 0, 1 };
                return boundaries[random.nextInt(boundaries.length)];
        }
    }
}
