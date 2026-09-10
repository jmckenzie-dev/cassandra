/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.memtable;

import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.db.memtable.IdleMemtableFlusher.AdmissionBudget;

import static org.junit.Assert.assertTrue;

public class IdleFlushAdmissionBudgetPropertyTest
{
    @Test
    public void generatedTracesRespectEnvelopesAndRecover()
    {
        for (int seed = 0; seed < 100; seed++)
        {
            Random random = new Random(seed);
            int rate = 1 + random.nextInt(1000);
            int byteRate = 1 + random.nextInt(10000);
            long now = 0;
            long bytes = 0;
            long largest = 0;
            long count = 0;
            AdmissionBudget budget = new AdmissionBudget(rate, byteRate, now);
            for (int step = 0; step < 1000; step++)
            {
                now += random.nextInt(100_000_000);
                if (budget.available(now) && random.nextBoolean())
                {
                    int size = random.nextInt(10 * byteRate);
                    budget.charge(size);
                    bytes += size;
                    largest = Math.max(largest, size);
                    count++;
                }
                double seconds = now / 1_000_000_000.0;
                assertTrue("count envelope, seed=" + seed, count <= rate * (1 + seconds) + 0.000001);
                assertTrue("byte envelope, seed=" + seed, bytes <= byteRate * (1 + seconds) + largest + 0.000001);
            }
            assertTrue("eventual admission, seed=" + seed, budget.available(now + 11_000_000_000L));
        }
    }
}
