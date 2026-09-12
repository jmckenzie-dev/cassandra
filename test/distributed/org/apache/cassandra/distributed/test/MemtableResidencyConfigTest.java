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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemtableResidencyConfigTest
{
    @Test
    public void hierarchyOptionsRequireUcsAndPreserveDefault()
    {
        MemtableResidencyProfileHarness.Config defaults = MemtableResidencyProfileHarness.Config.parse(new String[] { "--ucs-scaling", "T4" });
        assertTrue(!defaults.compactionOptions().contains("min_hierarchy_size"));
        assertTrue(!defaults.finalHeapDump);
        assertTrue(MemtableResidencyProfileHarness.Config.parse(new String[] { "--final-heap-dump" }).finalHeapDump);
        for (String size : new String[] { "1B", "1KiB", "4KiB", "64KiB", "1MiB" })
        {
            MemtableResidencyProfileHarness.Config config = MemtableResidencyProfileHarness.Config.parse(new String[] {
                "--ucs-scaling", "T4", "--ucs-min-hierarchy", size, "--ucs-trace"
            });
            assertTrue(config.ucsTrace);
            assertTrue(config.compactionOptions().contains("'min_hierarchy_size':'" + size + "'"));
        }
        for (String[] args : new String[][] { { "--ucs-trace" }, { "--ucs-min-hierarchy", "1KiB" },
                                             { "--ucs-scaling", "T4", "--ucs-min-hierarchy", "x'" } })
            assertThatThrownBy(() -> MemtableResidencyProfileHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void subnetPreservesWorkloadAndProvisionsAllAddresses() throws Exception
    {
        MemtableResidencyProfileHarness.Config defaults = MemtableResidencyProfileHarness.Config.parse(new String[0]);
        assertEquals(0, defaults.subnet);
        for (int subnet : new int[] { 0, 37, 255 })
        {
            MemtableResidencyProfileHarness.Config config = MemtableResidencyProfileHarness.Config.parse(new String[] {
                "--subnet", Integer.toString(subnet)
            });
            assertEquals(subnet, config.subnet);
            assertArrayEquals(defaults.tableOrder(), config.tableOrder());
            assertEquals(defaults.operationsPerCycle(), config.operationsPerCycle());
            assertEquals(defaults.payload(3, 7), config.payload(3, 7));
            MemtableResidencyProfileHarness harness = new MemtableResidencyProfileHarness(config);
            assertEquals(subnet, harness.runParameters().get("subnet"));
            Cluster.Builder builder = Cluster.build(1);
            harness.configureCluster(builder);
            try (Cluster cluster = builder.createWithoutStarting())
            {
                IInstanceConfig node = cluster.get(1).config();
                String address = "127.0." + subnet + ".1";
                for (String key : new String[] { "listen_address", "broadcast_address", "rpc_address", "broadcast_rpc_address" })
                    assertEquals(address, node.get(key));
                assertEquals(address, node.broadcastAddress().getAddress().getHostAddress());
                assertEquals(7012, node.getInt("storage_port"));
                assertEquals(9042, node.getInt("native_transport_port"));
            }
        }
    }

    @Test
    public void generatedSchedulesPreserveDatasetAndRate()
    {
        Random random = new Random(7361);
        for (int trial = 0; trial < 500; trial++)
        {
            int tables = 1 + random.nextInt(100);
            int active = 1 + random.nextInt(tables);
            int rows = 1 + random.nextInt(10);
            int rate = 1 + random.nextInt(10000);
            MemtableResidencyProfileHarness.Config c = MemtableResidencyProfileHarness.Config.parse(new String[] {
                "--scenario", "rotating-bursts", "--tables", Integer.toString(tables),
                "--active-tables", Integer.toString(active), "--rows-per-table", Integer.toString(rows),
                "--rate", Integer.toString(rate), "--seed", Long.toString(random.nextLong())
            });
            int[] order = c.tableOrder();
            assertArrayEquals(order, c.tableOrder());
            String payload = c.payload(trial, trial + 1);
            assertEquals(c.payloadBytes, payload.length());
            assertEquals(payload, c.payload(trial, trial + 1));
            for (int i = 0; i < payload.length(); i++)
                assertTrue(payload.charAt(i) >= '!' && payload.charAt(i) <= '~');
            Set<Integer> permutation = new HashSet<>();
            for (int table : order)
            {
                assertTrue(table >= 0 && table < tables);
                permutation.add(table);
            }
            assertEquals(tables, permutation.size());
            Set<Integer> visited = new HashSet<>();
            for (int cycle = 0; cycle < tables; cycle++)
            {
                Set<String> keys = new HashSet<>();
                Set<Integer> subset = new HashSet<>();
                for (int op = 0; op < c.operationsPerCycle(); op++)
                {
                    int table = c.tableFor(order, cycle, op);
                    visited.add(table);
                    subset.add(table);
                    assertTrue(keys.add(table + ":" + (cycle * rows + op / active)));
                    assertEquals((long) op * 1000000000L / rate, c.offsetNanos(op));
                    if (op > 0)
                        assertTrue(c.offsetNanos(op) > c.offsetNanos(op - 1));
                }
                assertEquals(active, subset.size());
                assertEquals(active * rows, keys.size());
                for (int position = 0; position < tables; position++)
                    assertEquals(subset.contains(order[position]), c.activeInCycle(position, cycle));
            }
            assertEquals(tables, visited.size());
        }
    }

    @Test
    public void rejectsInvalidArgumentsAndOverflow()
    {
        String[][] cases = {
            { "--idle-flush-ms", "-1" }, { "--idle-flush-ms", "100" },
            { "--memtable-heap-mib", "-1" },
            { "--idle-flush-max-concurrent", "0" }, { "--ucs-scaling", "T4'; DROP TABLE foo" },
            { "--ucs-scaling", "T4", "--idle-flush-ms", "100", "--eager-memtable" },
            { "--ucs-scaling", "T4", "--idle-flush-ms", "100", "--memtable", "SkipListMemtable" },
            { "--subnet" }, { "--subnet", "-1" }, { "--subnet", "256" }, { "--subnet", "x" },
            { "--tables" }, { "--unknown", "1" }, { "--tables", "0" }, { "--tables", "x" },
            { "--tables", "2", "--active-tables", "3" }, { "--sample-ms", "0" }, { "--rate", "0" },
            { "--idle-ms", "-1" }, { "--scenario", "evict" }, { "--format", "invalid" },
            { "--memtable", "invalid" }, { "--payload-bytes", "0" }, { "--cycles", "0" },
            { "--memtable", "SkipListMemtable", "--eager-memtable" },
            { "--eager-memtable", "--memtable", "ShardedSkipListMemtable" },
            { "--explicit-retirement" },
            { "--scenario", "written-flushed", "--explicit-retirement" },
            { "--explicit-retirement", "--scenario", "trickle" },
            { "--tables", "2147483647", "--rows-per-table", "2" },
            { "--tables", "1", "--rows-per-table", "2147483647", "--cycles", "2" }
        };
        for (String[] args : cases)
            assertThatThrownBy(() -> MemtableResidencyProfileHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void idleFlushOptionsKeepTheBaselineAvailable()
    {
        MemtableResidencyProfileHarness.Config baseline = MemtableResidencyProfileHarness.Config.parse(new String[0]);
        assertEquals(0, baseline.idleFlushMillis);
        assertEquals(2, baseline.idleFlushMaxConcurrent);
        MemtableResidencyProfileHarness.Config idle = MemtableResidencyProfileHarness.Config.parse(new String[] {
            "--scenario", "idle-reactivate", "--ucs-scaling", "T4,L10", "--idle-flush-ms", "30000",
            "--overwrite", "--settle-each-cycle", "--cursor-compaction", "--idle-flush-max-concurrent", "4",
            "--memtable-heap-mib", "256"
        });
        assertEquals(30000, idle.idleFlushMillis);
        assertEquals(4, idle.idleFlushMaxConcurrent);
        assertEquals(256, idle.memtableHeapMiB);
        assertTrue(idle.overwrite);
        assertTrue(idle.settleEachCycle);
        assertTrue(idle.cursorCompaction);
        assertTrue(idle.compactionOptions().contains("T4,L10"));
        assertArrayEquals(baseline.tableOrder(), idle.tableOrder());
    }

    @Test
    public void initializationControlDoesNotChangeWorkload()
    {
        for (int seed = 0; seed < 100; seed++)
        {
            String[] args = { "--tables", "10", "--active-tables", "3", "--seed", Integer.toString(seed) };
            MemtableResidencyProfileHarness.Config lazy = MemtableResidencyProfileHarness.Config.parse(args);
            String[] eagerArgs = Arrays.copyOf(args, args.length + 1);
            eagerArgs[args.length] = "--eager-memtable";
            MemtableResidencyProfileHarness.Config eager = MemtableResidencyProfileHarness.Config.parse(eagerArgs);
            assertEquals(false, lazy.eagerMemtable);
            assertEquals(true, eager.eagerMemtable);
            assertArrayEquals(lazy.tableOrder(), eager.tableOrder());
            assertEquals(lazy.operationsPerCycle(), eager.operationsPerCycle());
            assertEquals(lazy.payload(seed, seed), eager.payload(seed, seed));
        }
    }

    @Test
    public void allocationControlsPreserveGeneratedWorkload()
    {
        for (int seed = 0; seed < 100; seed++)
        {
            String[] args = { "--tables", "10", "--active-tables", "3", "--seed", Integer.toString(seed) };
            MemtableResidencyProfileHarness.Config reference = MemtableResidencyProfileHarness.Config.parse(args);
            assertEquals(false, reference.lazyTombstoneHistograms);
            assertEquals(false, reference.geometricMeterArrays);
            assertEquals(false, reference.legacyMetrics);
            for (String flag : new String[] { "--lazy-tombstone-histograms", "--geometric-meter-arrays", "--legacy-metrics" })
            {
                String[] candidateArgs = Arrays.copyOf(args, args.length + 1);
                candidateArgs[args.length] = flag;
                MemtableResidencyProfileHarness.Config candidate = MemtableResidencyProfileHarness.Config.parse(candidateArgs);
                assertEquals(flag.equals("--lazy-tombstone-histograms"), candidate.lazyTombstoneHistograms);
                assertEquals(flag.equals("--geometric-meter-arrays"), candidate.geometricMeterArrays);
                assertEquals(flag.equals("--legacy-metrics"), candidate.legacyMetrics);
                assertArrayEquals(reference.tableOrder(), candidate.tableOrder());
                assertEquals(reference.operationsPerCycle(), candidate.operationsPerCycle());
                assertEquals(reference.payload(seed, seed), candidate.payload(seed, seed));
            }
        }
    }

    @Test
    public void fixedSubsetDoesNotRotate()
    {
        MemtableResidencyProfileHarness.Config c = MemtableResidencyProfileHarness.Config.parse(new String[] {
            "--scenario", "idle-reactivate", "--tables", "10", "--active-tables", "3"
        });
        int[] order = c.tableOrder();
        for (int cycle = 1; cycle < 20; cycle++)
            for (int op = 0; op < c.operationsPerCycle(); op++)
                assertEquals(c.tableFor(order, 0, op), c.tableFor(order, cycle, op));
    }

    @Test
    public void retirementControlPreservesGeneratedWrites()
    {
        Random random = new Random(19837);
        for (String scenario : new String[] { "idle-reactivate", "rotating-bursts" })
        {
            for (int trial = 0; trial < 100; trial++)
            {
                int tables = 1 + random.nextInt(100);
                String[] args = {
                    "--scenario", scenario, "--tables", Integer.toString(tables),
                    "--active-tables", Integer.toString(1 + random.nextInt(tables)),
                    "--rows-per-table", Integer.toString(1 + random.nextInt(4)),
                    "--cycles", Integer.toString(1 + random.nextInt(4)),
                    "--rate", Integer.toString(1 + random.nextInt(1000)),
                    "--seed", Long.toString(random.nextLong())
                };
                MemtableResidencyProfileHarness.Config control = MemtableResidencyProfileHarness.Config.parse(args);
                String[] retiredArgs = Arrays.copyOf(args, args.length + 1);
                retiredArgs[args.length] = "--explicit-retirement";
                MemtableResidencyProfileHarness.Config retired = MemtableResidencyProfileHarness.Config.parse(retiredArgs);
                assertEquals(false, control.explicitRetirement);
                assertEquals(true, retired.explicitRetirement);
                int[] controlOrder = control.tableOrder();
                int[] retiredOrder = retired.tableOrder();
                assertArrayEquals(controlOrder, retiredOrder);
                assertEquals(control.operationsPerCycle(), retired.operationsPerCycle());
                for (int cycle = 0; cycle < control.cycles; cycle++)
                {
                    for (int op = 0; op < control.operationsPerCycle(); op++)
                    {
                        int table = control.tableFor(controlOrder, cycle, op);
                        assertEquals(table, retired.tableFor(retiredOrder, cycle, op));
                        int row = cycle * control.rows + op / control.activeTables;
                        assertEquals(control.payload(table, row), retired.payload(table, row));
                        assertEquals(control.offsetNanos(op), retired.offsetNanos(op));
                    }
                }
            }
        }
    }
}
