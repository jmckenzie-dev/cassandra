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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.util.TombstonePagingTestUtil.readPages;
import static org.junit.Assert.assertTrue;

/** Compares paged reconciliation of divergent replicas with a timestamp-ordered mutation model. */
public class PagingAcrossTombstonesFuzzTest extends TestBaseImpl
{
    private static final String TABLE = "paging_fuzz";
    private static final int ROWS = 48;

    @Test
    public void divergentReplicasMatchModel() throws IOException
    {
        run(true);
    }

    @Test
    public void mutationModelMatchesCassandra() throws IOException
    {
        run(false);
    }

    @Test
    public void partitionKeyInMatchesModel() throws IOException
    {
        long seed = TEST_SEED.getLong(18424L);
        Random random = new Random(seed);
        try (Cluster cluster = init(Cluster.build(3)
                                          .withSubnet(16)
                                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                      .set("hinted_handoff_enabled", false)
                                                                      .set("tombstone_paging_threshold", 2)
                                                                      .set("tombstone_failure_threshold", 2))
                                          .start());
             com.datastax.driver.core.Cluster client = client(cluster, 1);
             Session session = client.connect())
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                 " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            for (int example = 0; example < 12; example++)
            {
                List<String> history = new ArrayList<>();
                Map<Integer, TreeMap<Integer, Integer>> model = new TreeMap<>();
                int partitions = 2 + random.nextInt(3);
                for (int p = 0; p < partitions; p++)
                {
                    int pk = example * 4 + p;
                    TreeMap<Integer, Integer> live = new TreeMap<>();
                    model.put(pk, live);
                    int deletedPrefix = example % 3 == 0 ? 6 : 3;
                    for (int ck = 0; ck < 12; ck++)
                    {
                        int value = random.nextInt(1000);
                        applyInMutation(cluster, history, "INSERT INTO " + KEYSPACE + '.' + TABLE +
                                                         " (pk, ck, v) VALUES (" + pk + ", " + ck + ", " + value + ") USING TIMESTAMP 1");
                        live.put(ck, value);
                        if (example % 6 == 0 || (p == 0 && example % 4 == 0) || ck < deletedPrefix || random.nextBoolean())
                        {
                            applyInMutation(cluster, history, "DELETE FROM " + KEYSPACE + '.' + TABLE +
                                                             " USING TIMESTAMP 2 WHERE pk = " + pk + " AND ck = " + ck);
                            live.remove(ck);
                        }
                        else if (random.nextBoolean())
                        {
                            value = random.nextInt(1000);
                            applyInMutation(cluster, history, "UPDATE " + KEYSPACE + '.' + TABLE + " USING TIMESTAMP 3 SET v = " + value +
                                                             " WHERE pk = " + pk + " AND ck = " + ck);
                            live.put(ck, value);
                        }
                    }
                }
                Object[][] expected = model.entrySet().stream()
                                           .flatMap(partition -> partition.getValue().entrySet().stream()
                                                                          .map(entry -> row(partition.getKey(), entry.getKey(), entry.getValue())))
                                           .toArray(Object[][]::new);
                String keys = model.keySet().stream().map(String::valueOf).collect(Collectors.joining(", "));
                String base = "SELECT pk, ck, v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk IN (" + keys + ')';
                for (boolean flushed : new boolean[]{ false, true })
                {
                    if (flushed)
                        cluster.forEach(node -> node.flush(KEYSPACE));
                    String context = "seed=" + seed + ", example=" + example + ", flushed=" + flushed + ", mutations=" + history;
                    configure(cluster, 3);
                    Object[][] ordered;
                    try
                    {
                        ordered = inRows(session, base, 1);
                        Comparator<Object[]> comparator = Comparator.<Object[]>comparingInt(result -> (Integer) result[0])
                                                                   .thenComparingInt(result -> (Integer) result[1]);
                        assertRows(Arrays.stream(ordered).sorted(comparator).toArray(Object[][]::new), expected);
                    }
                    catch (AssertionError | RuntimeException failure)
                    {
                        throw new AssertionError("IN model control failed: " + context, failure);
                    }
                    for (int limit : Arrays.stream(new int[]{ 0, 3, expected.length, expected.length + 1 }).distinct().toArray())
                        for (int fetchSize : new int[]{ 1, 2 })
                        {
                            if (limit != 0 && fetchSize >= limit)
                                continue;
                            String cql = base + (limit == 0 ? "" : " LIMIT " + limit);
                            Object[][] limited = Arrays.stream(ordered)
                                                       .limit(limit == 0 ? Long.MAX_VALUE : limit)
                                                       .toArray(Object[][]::new);
                            configure(cluster, 3);
                            try
                            {
                                assertRows(inRows(session, cql, fetchSize), limited);
                            }
                            catch (AssertionError | RuntimeException failure)
                            {
                                throw new AssertionError("IN LIMIT control failed: " + context + ", query=" + cql + ", fetchSize=" + fetchSize, failure);
                            }
                            configure(cluster, 0);
                            long before = thresholdCrossings(cluster);
                            try
                            {
                                assertRows(inRows(session, cql, fetchSize), limited);
                                assertTrue("No checkpoint was exercised", thresholdCrossings(cluster) > before);
                            }
                            catch (AssertionError | RuntimeException failure)
                            {
                                throw new AssertionError("IN paging failed: " + context + ", query=" + cql + ", fetchSize=" + fetchSize, failure);
                            }
                        }
                }
            }
        }
    }

    private static void applyInMutation(Cluster cluster, List<String> history, String cql)
    {
        history.add(cql);
        cluster.forEach(node -> node.executeInternal(cql));
    }

    private static Object[][] inRows(Session session, String cql, int fetchSize)
    {
        return readPages(session, cql, fetchSize, 48).stream()
                                                   .map(result -> row(result.getInt("pk"), result.getInt("ck"), result.getInt("v")))
                                                   .toArray(Object[][]::new);
    }

    private void run(boolean exerciseTombstonePaging) throws IOException
    {
        long seed = TEST_SEED.getLong(18424L);
        int examples = 6;
        Random random = new Random(seed);
        List<AssertionError> failures = new ArrayList<>();
        try (Cluster cluster = init(Cluster.build(3)
                                          .withSubnet(10)
                                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                      .set("hinted_handoff_enabled", false)
                                                                      .set("tombstone_paging_threshold", 2)
                                                                      .set("tombstone_failure_threshold", 2))
                                          .start());
             com.datastax.driver.core.Cluster client1 = client(cluster, 1);
             com.datastax.driver.core.Cluster client2 = client(cluster, 2);
             com.datastax.driver.core.Cluster client3 = client(cluster, 3);
             Session session1 = client1.connect();
             Session session2 = client2.connect();
             Session session3 = client3.connect())
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                 " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            for (int example = 0; example < examples; example++)
            {
                TreeMap<Integer, Integer> model = populate(cluster, random, example);
                for (boolean flushed : new boolean[]{ false, true })
                {
                    if (flushed)
                        cluster.forEach(node -> node.flush(KEYSPACE));
                    configure(cluster, 3);
                    Session[] sessions = new Session[]{ session1, session2, session3 };
                    verifyModel(cluster, sessions, model, example);
                    List<AssertionError> controlFailures = new ArrayList<>();
                    verify(sessions, model, example, false, "disabled control, example=" + example + ", flushed=" + flushed,
                           controlFailures);
                    assertTrue(controlFailures.toString(), controlFailures.isEmpty());
                    for (int mode = 0; exerciseTombstonePaging && mode < 3; mode++)
                    {
                        configure(cluster, mode);
                        long before = thresholdCrossings(cluster);
                        String context = "seed=" + seed + ", example=" + example +
                                         ", flushed=" + flushed + ", config=" + mode;
                        verify(sessions, model, example, true, context, failures);
                        assertTrue(context + ": no replica crossed the tombstone threshold",
                                   thresholdCrossings(cluster) > before);
                    }
                }
            }
        }
        if (!failures.isEmpty())
            throw new AssertionError(failures.size() + " queries failed:\n" +
                                     failures.stream().map(failure -> failure.getMessage() + ": " + failure.getCause())
                                             .collect(Collectors.joining("\n")), failures.get(0));
    }

    private static TreeMap<Integer, Integer> populate(Cluster cluster, Random random, int pk)
    {
        TreeMap<Integer, Integer> model = new TreeMap<>();
        for (int ck = 0; ck < ROWS; ck++)
        {
            for (int node = 1; node <= 3; node++)
                insert(cluster, node, pk, ck, ck, 1);
            insert(cluster, 1, -pk - 1, ck, ck, 1);
            model.put(ck, ck);
        }
        // Keep a deleted prefix on one replica to force checkpointed responses alongside stale rows.
        for (int ck = 0; ck < 12; ck++)
        {
            delete(cluster, 1, pk, ck, ck, 2);
            delete(cluster, 1, -pk - 1, ck, ck, 2);
            model.remove(ck);
        }
        for (int operation = 0; operation < 64; operation++)
        {
            int node = 1 + random.nextInt(3);
            int ck = 12 + random.nextInt(ROWS - 12);
            long timestamp = 3L + operation;
            switch (random.nextInt(3))
            {
                case 0:
                    int value = random.nextInt(1000);
                    insert(cluster, node, pk, ck, value, timestamp);
                    insert(cluster, 1, -pk - 1, ck, value, timestamp);
                    model.put(ck, value);
                    break;
                case 1:
                    delete(cluster, node, pk, ck, ck, timestamp);
                    delete(cluster, 1, -pk - 1, ck, ck, timestamp);
                    model.remove(ck);
                    break;
                case 2:
                    int end = Math.min(ROWS - 1, ck + random.nextInt(5));
                    delete(cluster, node, pk, ck, end, timestamp);
                    delete(cluster, 1, -pk - 1, ck, end, timestamp);
                    model.subMap(ck, true, end, true).clear();
                    break;
                default:
                    throw new AssertionError();
            }
        }
        return model;
    }

    private static void verifyModel(Cluster cluster, Session[] sessions, TreeMap<Integer, Integer> model, int pk)
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            Map<Integer, Integer> ordered = reversed ? model.descendingMap() : model;
            Object[][] expected = ordered.entrySet().stream()
                                         .map(entry -> new Object[]{ entry.getKey(), entry.getValue() })
                                         .toArray(Object[][]::new);
            String query = "SELECT ck, v FROM " + KEYSPACE + '.' + TABLE +
                           " WHERE pk = ? ORDER BY ck " + (reversed ? "DESC" : "ASC");
            // Negative partitions replay every mutation on one replica, using Cassandra's own reconciliation.
            assertRows(cluster.get(1).executeInternal(query, -pk - 1), expected);
            for (Session session : sessions)
            {
                Object[][] actual = readPages(session, query.replace("?", Integer.toString(pk)), Integer.MAX_VALUE, ROWS).stream()
                                           .map(row -> new Object[]{ row.getInt("ck"), row.getInt("v") })
                                           .toArray(Object[][]::new);
                assertRows(actual, expected);
            }
        }
    }

    private static void insert(Cluster cluster, int node, int pk, int ck, int value, long timestamp)
    {
        cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE + '.' + TABLE +
                                          " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, value, timestamp);
    }

    private static void delete(Cluster cluster, int node, int pk, int start, int end, long timestamp)
    {
        if (start == end)
            cluster.get(node).executeInternal("DELETE FROM " + KEYSPACE + '.' + TABLE +
                                              " USING TIMESTAMP ? WHERE pk = ? AND ck = ?", timestamp, pk, start);
        else
            cluster.get(node).executeInternal("DELETE FROM " + KEYSPACE + '.' + TABLE +
                                              " USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck <= ?",
                                              timestamp, pk, start, end);
    }

    private static void configure(Cluster cluster, int mode)
    {
        for (int node = 1; node <= 3; node++)
        {
            int threshold = mode == 0 ? 2 : node + 1;
            boolean enabled = mode != 3 && (mode != 2 || node != 2);
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.setTombstonePagingThreshold(enabled ? threshold : -1);
                DatabaseDescriptor.setTombstoneFailureThreshold(enabled ? threshold + 3 : 10000);
            });
        }
    }

    private static com.datastax.driver.core.Cluster client(Cluster cluster, int node)
    {
        String host = cluster.get(node).broadcastAddress().getHostString();
        return com.datastax.driver.core.Cluster.builder()
                                              .addContactPoint(host)
                                              .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                          Collections.singletonList(new InetSocketAddress(host, 9042))))
                                              .build();
    }

    private static void verify(Session[] sessions, TreeMap<Integer, Integer> model, int pk, boolean requirePager,
                               String context, List<AssertionError> failures)
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            Map<Integer, Integer> ordered = reversed ? model.descendingMap() : model;
            for (int limit : new int[]{ 1, 5, ROWS + 1 })
            {
                Object[][] expected = ordered.entrySet().stream()
                                             .limit(limit)
                                             .map(entry -> new Object[]{ entry.getKey(), entry.getValue() })
                                             .toArray(Object[][]::new);
                String query = "SELECT ck, v FROM " + KEYSPACE + '.' + TABLE +
                               " WHERE pk = ? ORDER BY ck " + (reversed ? "DESC" : "ASC") + " LIMIT " + limit;
                for (int coordinator = 1; coordinator <= 3; coordinator++)
                    for (int pageSize : new int[]{ 1, 4, 16 })
                    {
                        if (requirePager && pageSize >= limit)
                            continue;
                        try
                        {
                            Object[][] actual = readPages(sessions[coordinator - 1], query.replace("?", Integer.toString(pk)), pageSize, ROWS).stream()
                                                                         .map(row -> new Object[]{ row.getInt("ck"), row.getInt("v") })
                                                                         .toArray(Object[][]::new);
                            assertRows(actual, expected);
                        }
                        catch (AssertionError | RuntimeException failure)
                        {
                            failures.add(new AssertionError(context + ", " + query + ", coordinator=" + coordinator +
                                                            ", pageSize=" + pageSize, failure));
                        }
                    }
            }
        }
    }

    private static long thresholdCrossings(Cluster cluster)
    {
        return cluster.stream().mapToLong(node -> node.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                                   .getColumnFamilyStore(TABLE)
                                                                                   .metric.tombstonePages.getCount()))
                      .sum();
    }
}
