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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TableMetricBookkeepingTest
{
    @Test
    public void lateGrowthReplacementNoOpsAndClear() throws Exception
    {
        for (boolean compact : new boolean[] { false, true })
        {
            TableMetrics.OwnedMetrics owned = new TableMetrics.OwnedMetrics(compact);
            Counter first = new Counter();
            Counter replacement = new Counter();
            owned.put("first", first);
            for (int i = 0; i < 200; i++)
                owned.put("late" + i, NoOpMetrics.COUNTER);
            owned.put("first", replacement);
            assertSame(replacement, owned.get("first"));
            assertNull(owned.get("missing"));
            Map<String, Metric> contents = new HashMap<>();
            owned.forEach(contents::put);
            assertEquals(201, contents.size());
            assertSame(NoOpMetrics.COUNTER, contents.get("late199"));
            owned.clear();
            assertNull(owned.get("first"));
            contents.clear();
            owned.forEach(contents::put);
            assertEquals(0, contents.size());
            owned.put("again", first);
            assertSame(first, owned.get("again"));
        }
    }

    public static class Properties
    {
        @Test
        public void generatedLifecycleSequencesMatchMap()
        {
            for (int seed = 0; seed < 16; seed++)
            {
                Random random = new Random(seed);
                Map<String, Metric> expected = new HashMap<>();
                TableMetrics.OwnedMetrics compact = new TableMetrics.OwnedMetrics(true);
                TableMetrics.OwnedMetrics control = new TableMetrics.OwnedMetrics(false);
                for (int step = 0; step < 2000; step++)
                {
                    String name = "metric" + random.nextInt(200);
                    if (random.nextInt(100) == 0)
                    {
                        expected.clear();
                        compact.clear();
                        control.clear();
                    }
                    else if (random.nextBoolean())
                    {
                        Metric value = random.nextBoolean() ? new Counter() : NoOpMetrics.COUNTER;
                        expected.put(name, value);
                        compact.put(name, value);
                        control.put(name, value);
                    }
                    assertSame(expected.get(name), compact.get(name));
                    assertSame(expected.get(name), control.get(name));
                    Map<String, Metric> actual = new HashMap<>();
                    compact.forEach(actual::put);
                    assertEquals(expected, actual);
                    actual.clear();
                    control.forEach(actual::put);
                    assertEquals(expected, actual);
                }
            }
        }
    }

    public static void main(String[] args)
    {
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        Counter value = new Counter();
        String[] names = new String[64];
        for (int i = 0; i < names.length; i++)
            names[i] = "metric" + i;
        System.out.println("round,compact,tables,construct_ns_per_table,construct_bytes_per_table,release_ns_per_table");
        for (int round = 0; round < 30; round++)
        {
            for (boolean compact : round % 2 == 0 ? new boolean[] { false, true } : new boolean[] { true, false })
            {
                int count = 1000;
                TableMetrics.OwnedMetrics[] tables = new TableMetrics.OwnedMetrics[count];
                long bytes = allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
                long start = System.nanoTime();
                for (int i = 0; i < count; i++)
                {
                    TableMetrics.OwnedMetrics owned = new TableMetrics.OwnedMetrics(compact);
                    for (String name : names)
                    {
                        assertNull(owned.get(name));
                        owned.put(name, value);
                    }
                    tables[i] = owned;
                }
                long creation = System.nanoTime() - start;
                bytes = allocation.getThreadAllocatedBytes(Thread.currentThread().getId()) - bytes;
                long[] released = { 0 };
                start = System.nanoTime();
                for (TableMetrics.OwnedMetrics owned : tables)
                {
                    owned.forEach((name, metric) -> released[0]++);
                    owned.clear();
                }
                long release = System.nanoTime() - start;
                assertEquals(count * names.length, released[0]);
                System.out.println(String.format(java.util.Locale.ROOT, "%d,%s,%d,%.3f,%.3f,%.3f", round, compact, count,
                                                 (double) creation / count, (double) bytes / count, (double) release / count));
            }
        }
    }
}
