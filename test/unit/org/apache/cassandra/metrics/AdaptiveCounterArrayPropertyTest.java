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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLongArray;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AdaptiveCounterArrayPropertyTest
{
    @Test
    public void generatedOperationsMatchAtomicLongArray()
    {
        for (long seed : new long[]{ 1, 42, 1009, 8675309 })
        {
            Random random = new Random(seed);
            for (int example = 0; example < 40; example++)
            {
                int length = new int[]{ 1, 16, 17, 128, 165 }[random.nextInt(5)];
                AdaptiveCounterArray actual = new AdaptiveCounterArray(length);
                AtomicLongArray expected = new AtomicLongArray(length);
                int operation = -1;
                try
                {
                    for (operation = 0; operation < 500; operation++)
                    {
                        int index = random.nextInt(length);
                        // Start with a narrow history, then force one transition and exercise signed longs.
                        boolean narrow = operation < 100;
                        long value = narrow ? random.nextInt(1000) : value(random);
                        if (operation == 100)
                        {
                            actual.set(index, Integer.MAX_VALUE);
                            expected.set(index, Integer.MAX_VALUE);
                            assertEquals(expected.addAndGet(index, 1), actual.addAndGet(index, 1));
                        }
                        else
                        {
                            switch (random.nextInt(4))
                            {
                                case 0:
                                    actual.set(index, value);
                                    expected.set(index, value);
                                    break;
                                case 1:
                                    assertEquals(expected.addAndGet(index, value), actual.addAndGet(index, value));
                                    break;
                                case 2:
                                    long compare = random.nextBoolean() ? expected.get(index) : value(random);
                                    assertEquals(expected.compareAndSet(index, compare, value),
                                                 actual.compareAndSet(index, compare, value));
                                    break;
                                default:
                                    assertEquals(expected.get(index), actual.get(index));
                            }
                        }
                        if (narrow)
                            assertFalse(actual.isWide());
                        assertEquals(expected.length(), actual.length());
                        for (int i = 0; i < length; i++)
                            assertEquals("index=" + i, expected.get(i), actual.get(i));
                    }
                }
                catch (AssertionError failure)
                {
                    throw new AssertionError("seed=" + seed + ", example=" + example + ", operation=" + operation
                                             + ", length=" + length, failure);
                }
            }
        }
    }

    private static long value(Random random)
    {
        long[] boundaries = { Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE, -1, 0, 1,
                              Integer.MAX_VALUE - 1L, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L,
                              Long.MAX_VALUE - 1, Long.MAX_VALUE };
        return random.nextBoolean() ? boundaries[random.nextInt(boundaries.length)] : random.nextLong();
    }
}
