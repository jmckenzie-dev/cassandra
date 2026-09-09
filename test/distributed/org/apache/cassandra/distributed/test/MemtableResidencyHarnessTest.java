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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.utils.JsonUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MemtableResidencyHarnessTest
{
    @Test
    public void scenariosVerifyDataAndProduceDistinctCheckpoints() throws Throwable
    {
        for (String scenario : new String[] { "never-written", "written-flushed", "idle-reactivate", "rotating-bursts", "trickle" })
            verifyScenario(scenario, false, false);
    }

    @Test
    public void eagerControlPreservesEmptyAndFlushedData() throws Throwable
    {
        verifyScenario("never-written", true, false);
        verifyScenario("written-flushed", true, false);
    }

    @Test
    public void explicitRetirementReclaimsAndReactivatesTables() throws Throwable
    {
        verifyScenario("idle-reactivate", false, true);
        verifyScenario("rotating-bursts", false, true);
    }

    private void verifyScenario(String scenario, boolean eager, boolean retirement) throws Throwable
    {
        verifyScenario(scenario, eager, retirement, false, false);
    }

    @Test
    public void allocationCandidatesPreserveRetirementData() throws Throwable
    {
        verifyScenario("idle-reactivate", false, true, true, false);
        verifyScenario("idle-reactivate", false, true, false, true);
        verifyScenario("idle-reactivate", false, true, true, true);
    }

    @Test
    public void legacyMetricsPreserveEmptyAndFlushedData() throws Throwable
    {
        verifyScenario("never-written", false, false, false, false, true);
        verifyScenario("written-flushed", false, false, false, false, true);
    }

    private void verifyScenario(String scenario, boolean eager, boolean retirement, boolean lazyHistograms, boolean geometricMeters) throws Throwable
    {
        verifyScenario(scenario, eager, retirement, lazyHistograms, geometricMeters, false);
    }

    private void verifyScenario(String scenario, boolean eager, boolean retirement,
                                boolean lazyHistograms, boolean geometricMeters, boolean legacyMetrics) throws Throwable
    {
        Files.createDirectories(Paths.get("logs"));
        Path output = Files.createTempDirectory(Paths.get("logs"), "residency-tests-");
        List<String> args = new ArrayList<>(Arrays.asList(
                "--scenario", scenario, "--tables", "3", "--active-tables", "2", "--rows-per-table", "2",
                "--cycles", "2", "--rate", "100", "--idle-ms", "30", "--hold-ms", "30",
                "--sample-ms", "10", "--subnet", "143", "--no-profile", "--out", output.toString()
        ));
        if (eager)
            args.add("--eager-memtable");
        if (retirement)
            args.add("--explicit-retirement");
        if (lazyHistograms)
            args.add("--lazy-tombstone-histograms");
        if (geometricMeters)
            args.add("--geometric-meter-arrays");
        if (legacyMetrics)
            args.add("--legacy-metrics");
        MemtableResidencyProfileHarness.main(args.toArray(new String[0]));
        Path run;
        try (Stream<Path> paths = Files.list(output))
        {
            run = paths.filter(p -> p.getFileName().toString().contains("residency-" + scenario + '-')).findFirst().orElseThrow();
        }
        Map<?, ?> summary = JsonUtils.fromJsonMap(Files.readString(run.resolve("summary.json")));
        assertNull(summary.get("failure"));
        assertEquals(143, ((Number) summary.get("subnet")).intValue());
        assertEquals(scenario.equals("never-written") ? 0L : 8L, ((Number) summary.get("completedWrites")).longValue());
        assertEquals(0L, ((Number) summary.get("failedWriteRequests")).longValue());
        assertEquals(eager ? "eager" : "lazy", summary.get("memtableInitialization"));
        assertEquals(retirement, summary.get("explicitRetirement"));
        assertEquals(lazyHistograms, summary.get("lazyTombstoneHistograms"));
        assertEquals(geometricMeters, summary.get("geometricMeterArrays"));
        assertEquals(!legacyMetrics, summary.get("optimizedMetricsEnabled"));
        assertEquals(retirement ? 4L : 0L, ((Number) summary.get("completedRetirementRequests")).longValue());
        Map<?, ?> effective = (Map<?, ?>) summary.get("effectiveConfiguration");
        assertEquals(lazyHistograms, effective.get("lazyTombstoneHistograms"));
        assertEquals(geometricMeters, effective.get("geometricMeterArrays"));
        assertEquals(!legacyMetrics, effective.get("optimizedMetricsEnabled"));
        Map<?, ?> parameters = (Map<?, ?>) effective.get("memtableParameters");
        assertEquals(Boolean.toString(!eager), parameters.get("lazy_initialization"));
        Map<?, ?> checkpoints = (Map<?, ?>) summary.get("checkpoints");
        assertNotNull(checkpoints.get("created"));
        Map<?, ?> created = (Map<?, ?>) checkpoints.get("created");
        assertEquals(eager ? 3L : 0L, ((Number) created.get("initialized_trie_memtables")).longValue());
        Map<?, ?> policy = (Map<?, ?>) checkpoints.get("policy");
        Map<?, ?> settled = (Map<?, ?>) checkpoints.get("settled");
        assertEquals(false, policy.get("settledPostGc"));
        assertEquals(true, settled.get("settledPostGc"));
        assertEquals(3L, ((Number) settled.get("live_memtables")).longValue());
        assertEquals(0L, ((Number) settled.get("pending_flushes")).longValue());
        if (scenario.equals("written-flushed") || retirement)
        {
            assertEquals(0L, ((Number) settled.get("dirty_memtables")).longValue());
            assertTrue(((Number) settled.get("sstables")).longValue() > 0);
        }
        if (scenario.equals("never-written"))
            assertEquals(0L, ((Number) settled.get("sstables")).longValue());
        long initialized = eager ? 3L : scenario.equals("never-written") || scenario.equals("written-flushed") || retirement
                                       ? 0L : scenario.equals("rotating-bursts") ? 3L : 2L;
        assertEquals(initialized, ((Number) settled.get("initialized_trie_memtables")).longValue());
        Map<?, ?> verified = (Map<?, ?>) checkpoints.get("verified");
        assertEquals(initialized, ((Number) verified.get("initialized_trie_memtables")).longValue());
        if (retirement)
        {
            for (int cycle = 0; cycle < 2; cycle++)
            {
                String name = "03-cycle-00" + cycle;
                for (String stage : new String[] { "-written", "-pre-retire" })
                {
                    Map<?, ?> dirty = (Map<?, ?>) checkpoints.get(name + stage);
                    assertEquals(2L, ((Number) dirty.get("dirty_memtables")).longValue());
                    assertEquals(2L, ((Number) dirty.get("initialized_trie_memtables")).longValue());
                    assertTrue(((Number) dirty.get("memtable_accounted_heap_bytes")).longValue() > 0);
                }
                for (String stage : new String[] { "-reclaimed", "-read" })
                {
                    Map<?, ?> dormant = (Map<?, ?>) checkpoints.get(name + stage);
                    assertEquals(false, dormant.get("settledPostGc"));
                    assertEquals(0L, ((Number) dormant.get("dirty_memtables")).longValue());
                    assertEquals(0L, ((Number) dormant.get("initialized_trie_memtables")).longValue());
                    assertEquals(0L, ((Number) dormant.get("memtable_accounted_heap_bytes")).longValue());
                    assertEquals(0L, ((Number) dormant.get("flushing_memtables")).longValue());
                    assertEquals(0L, ((Number) dormant.get("pending_flushes")).longValue());
                    assertEquals(0L, ((Number) dormant.get("node_pool_reclaiming_heap_bytes")).longValue());
                    assertTrue(((Number) dormant.get("sstables")).longValue() > 0);
                }
                try (Stream<String> lines = Files.lines(run.resolve(name + "-retire-samples.csv")))
                {
                    assertTrue(lines.count() >= 2);
                }
            }
        }
        try (Stream<String> lines = Files.lines(run.resolve("04-hold-samples.csv")))
        {
            assertTrue(lines.count() >= 2);
        }
    }
}
