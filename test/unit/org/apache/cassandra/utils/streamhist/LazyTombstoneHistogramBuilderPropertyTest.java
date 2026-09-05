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
package org.apache.cassandra.utils.streamhist;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.db.rows.Cell;

import static org.apache.cassandra.utils.streamhist.LazyTombstoneHistogramBuilderTest.assertEquivalent;

public class LazyTombstoneHistogramBuilderPropertyTest
{
    @Test
    public void generatedEventSequencesPreserveEverySnapshot() throws IOException
    {
        for (long seed : new long[]{ 1, 42, 1009, 8675309 })
        {
            Random random = new Random(seed);
            for (int example = 0; example < 50; example++)
            {
                int bins = 1 + random.nextInt(32);
                int spool = random.nextInt(65);
                int round = 1 + random.nextInt(60);
                TombstoneHistogramBuilder reference = new StreamingTombstoneHistogramBuilder(bins, spool, round);
                TombstoneHistogramBuilder candidate = new LazyTombstoneHistogramBuilder(bins, spool, round);
                int operation = -1;
                try
                {
                    assertEquivalent(reference.build(), candidate.build());
                    for (operation = 0; operation < 500; operation++)
                    {
                        switch (random.nextInt(10))
                        {
                            case 0:
                                reference.flushHistogram();
                                candidate.flushHistogram();
                                break;
                            case 1:
                                assertEquivalent(reference.build(), candidate.build());
                                break;
                            default:
                                long point = random.nextBoolean() ? random.nextInt(500) : random.nextLong() & Cell.MAX_DELETION_TIME;
                                int weight = random.nextInt(20) == 0 ? Integer.MAX_VALUE : 1 + random.nextInt(100);
                                reference.update(point, weight);
                                candidate.update(point, weight);
                        }
                    }
                    reference.releaseBuffers();
                    candidate.releaseBuffers();
                    assertEquivalent(reference.build(), candidate.build());
                }
                catch (AssertionError failure)
                {
                    throw new AssertionError("seed=" + seed + ", example=" + example + ", operation=" + operation
                                             + ", bins=" + bins + ", spool=" + spool + ", round=" + round, failure);
                }
            }
        }
    }
}
