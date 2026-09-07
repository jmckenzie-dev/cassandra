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
package org.apache.cassandra.distributed.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.metrics.ReadRepairMetrics;

import static org.apache.cassandra.distributed.util.TombstonePagingTestUtil.readPages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Bounded native-query measurements; latency is reported, while read counts and results are asserted. */
public class PagingAcrossTombstonesPerformanceTest extends TestBaseImpl
{
    private static final String TABLE = "paging_performance";
    private static final String[] MODES = { "disabled_clean", "enabled_clean", "matching_checkpoint", "reconciliation" };
    private static final int WARMUP = 20;
    private static final int ROUNDS = 4;
    private static final int SAMPLES = 25;

    @Test(timeout = 180000)
    public void compareNativePagingReadCosts() throws Exception
    {
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(Instant.now());
        Path log = Files.createDirectories(Paths.get("logs")).resolve("paging-performance-" + stamp + ".log");
        try (BufferedWriter measurements = Files.newBufferedWriter(log);
             Cluster cluster = init(Cluster.build(3).withSubnet(19)
                                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                      .set("dynamic_snitch", false)
                                                                      .set("hinted_handoff_enabled", false)
                                                                      .set("tombstone_paging_threshold", 50)
                                                                      .set("tombstone_failure_threshold", 50))
                                          .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                 " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            for (int node = 1; node <= 3; node++)
                for (int pk = 0; pk < 3; pk++)
                    for (int ck = 0; ck < 192; ck++)
                    {
                        cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE + '.' + TABLE +
                                                          " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1", pk, ck, ck);
                        if (ck < 64 && (pk == 1 || (pk == 2 && node == 3)))
                            cluster.get(node).executeInternal("DELETE FROM " + KEYSPACE + '.' + TABLE +
                                                              " USING TIMESTAMP 2 WHERE pk = ? AND ck = ?", pk, ck);
                    }
            String host = cluster.get(1).broadcastAddress().getHostString();
            try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                         .addContactPoint(host)
                                                                                         .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                                                         .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                                                                     Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                         .build();
                 Session session = client.connect())
            {
                for (boolean flushed : new boolean[]{ false, true })
                {
                    if (flushed)
                        cluster.forEach(node -> node.flush(KEYSPACE));
                    List<List<Long>> timings = new ArrayList<>();
                    for (int mode = 0; mode < MODES.length; mode++)
                    {
                        timings.add(new ArrayList<>());
                        configure(cluster, mode);
                        for (int i = 0; i < WARMUP; i++)
                            read(session, mode);
                    }
                    for (int round = 0; round < ROUNDS; round++)
                        for (int offset = 0; offset < MODES.length; offset++)
                        {
                            int mode = (round + offset) % MODES.length;
                            configure(cluster, mode);
                            long readsBefore = replicaReads(cluster);
                            long repairsBefore = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount());
                            for (int i = 0; i < SAMPLES; i++)
                                timings.get(mode).add(read(session, mode));
                            long reads = replicaReads(cluster) - readsBefore;
                            long repairs = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount()) - repairsBefore;
                            if (mode < 2)
                                assertEquals(6L * SAMPLES, reads);
                            else if (mode == 2)
                                assertEquals(9L * SAMPLES, reads);
                            else
                                assertTrue("Expected extra reads for reconciliation", reads > 6L * SAMPLES);
                            assertEquals("Unexpected reconciliation path", mode == 3, repairs > 0);
                            report(measurements, "PAGING_COST storage=%s mode=%s round=%d queries=%d reads=%d reconciliations=%d",
                                   flushed ? "sstable" : "memtable", MODES[mode], round, SAMPLES, reads, repairs);
                        }
                    for (int mode = 0; mode < MODES.length; mode++)
                    {
                        List<Long> samples = timings.get(mode);
                        Collections.sort(samples);
                        report(measurements, "PAGING_LATENCY storage=%s mode=%s samples=%d min_us=%.3f median_us=%.3f p95_us=%.3f max_us=%.3f",
                               flushed ? "sstable" : "memtable", MODES[mode], samples.size(), samples.get(0) / 1000.0,
                               samples.get(samples.size() / 2) / 1000.0, samples.get(samples.size() * 95 / 100) / 1000.0,
                               samples.get(samples.size() - 1) / 1000.0);
                    }
                }
            }
        }
    }

    private static void report(BufferedWriter measurements, String format, Object... values) throws IOException
    {
        String line = String.format(Locale.ROOT, format, values);
        System.out.println(line);
        measurements.write(line);
        measurements.newLine();
        measurements.flush();
    }

    private static long read(Session session, int mode)
    {
        int pk = mode < 2 ? 0 : mode - 1;
        String query = "SELECT ck, v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = " + pk + " LIMIT 101";
        long start = System.nanoTime();
        List<Row> rows = readPages(session, query, 100, 192);
        long elapsed = System.nanoTime() - start;
        assertEquals(101, rows.size());
        for (int i = 0; i < rows.size(); i++)
        {
            int expected = i + (pk == 0 ? 0 : 64);
            assertEquals(expected, rows.get(i).getInt("ck"));
            assertEquals(expected, rows.get(i).getInt("v"));
        }
        return elapsed;
    }

    private static void configure(Cluster cluster, int mode)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            DatabaseDescriptor.setTombstonePagingThreshold(mode == 0 ? -1 : 50);
            DatabaseDescriptor.setTombstoneFailureThreshold(mode == 0 ? 10000 : 50);
        }));
    }

    private static long replicaReads(Cluster cluster)
    {
        return cluster.stream().mapToLong(node -> node.callOnInstance(() ->
            Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metric.readLatency.latency.getCount())).sum();
    }
}
