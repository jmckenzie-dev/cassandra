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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.util.TombstonePagingTestUtil.readPages;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PagingAcrossTombstonesInteractionTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster client;
    private static Session session;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withSubnet(13)
                              .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP, Feature.NETWORK)
                                                          .set("dynamic_snitch", false)
                                                          .set("hinted_handoff_enabled", false)
                                                          .set("read_request_timeout", "10s")
                                                          .set("tombstone_paging_threshold", 2)
                                                          .set("tombstone_failure_threshold", 2))
                              .start());
        String host = cluster.get(1).broadcastAddress().getHostString();
        client = com.datastax.driver.core.Cluster.builder()
                                                .addContactPoint(host)
                                                .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                            Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                .build();
        session = client.connect();
    }

    @AfterClass
    public static void teardown()
    {
        if (session != null)
            session.close();
        if (client != null)
            client.close();
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void acknowledgedWritesDuringCheckpointReadAppearOnLaterPages() throws Exception
    {
        String table = createRows("concurrent_checkpoint", "NONE", 6);
        String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 4";
        CountDownLatch checkpointReady = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean held = new AtomicBoolean();
        cluster.filters().outbound().verbs(Verb.READ_RSP.id).from(2).to(1).messagesMatching((from, to, message) -> {
            if (held.compareAndSet(false, true))
            {
                checkpointReady.countDown();
                try
                {
                    assertTrue("Checkpoint response was not released", release.await(20, TimeUnit.SECONDS));
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return false;
        }).drop();
        try
        {
            ResultSetFuture reading = session.executeAsync(statement(cql, ConsistencyLevel.ALL, 1, null));
            assertTrue("Replica did not return a response", checkpointReady.await(20, TimeUnit.SECONDS));
            write("UPDATE " + table + " USING TIMESTAMP 3 SET v = 99 WHERE pk = 0 AND ck = 3");
            write("DELETE FROM " + table + " USING TIMESTAMP 3 WHERE pk = 0 AND ck = 4");
            write("INSERT INTO " + table + " (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 3");
            release.countDown();
            PagingState state = checkpoint(reading.get(20, TimeUnit.SECONDS));
            assertRows(rows(session.execute(statement(cql, ConsistencyLevel.ALL, 1, state))),
                       row(3, 99), row(5, 5), row(6, 6));
        }
        finally
        {
            release.countDown();
            cluster.filters().reset();
        }
    }

    @Test
    public void mutationsBehindConsumedCheckpointAreNotRevisited()
    {
        String table = createRows("behind_checkpoint", "NONE", 5);
        String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 3";
        PagingState state = checkpoint(session.execute(statement(cql, ConsistencyLevel.ALL, 1, null)));
        write("INSERT INTO " + table + " (pk, ck, v) VALUES (0, 1, 99) USING TIMESTAMP 3");
        write("UPDATE " + table + " USING TIMESTAMP 3 SET v = 77 WHERE pk = 0 AND ck = 4");
        assertRows(rows(session.execute(statement(cql, ConsistencyLevel.ALL, 1, state))), row(3, 3), row(4, 77));
    }

    @Test
    public void speculationReplacesMissingCheckpointResponse()
    {
        String table = createRows("speculative_checkpoint", "10ms", 5);
        cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("speculative_checkpoint").updateSpeculationThreshold());
        AtomicInteger droppedReplica = new AtomicInteger();
        cluster.filters().outbound().verbs(Verb.READ_RSP.id).to(1).messagesMatching((from, to, message) -> {
            droppedReplica.compareAndSet(0, from);
            return from == droppedReplica.get();
        }).drop();
        try
        {
            String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 3";
            ResultSet results = session.execute(statement(cql, ConsistencyLevel.QUORUM, 1, null));
            assertNotNull(checkpoint(results));
            assertRows(rows(results), row(3, 3), row(4, 4));
            assertTrue(droppedReplica.get() > 0);
            assertTrue("Expected a server speculative read",
                       cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                  .getColumnFamilyStore("speculative_checkpoint")
                                                                  .metric.speculativeRetries.getCount()) > 0);
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    @Test
    public void timedOutContinuationCanBeRetried()
    {
        String table = createRows("timeout_checkpoint", "NONE", 5);
        String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 3";
        PagingState state = checkpoint(session.execute(statement(cql, ConsistencyLevel.ALL, 1, null)));
        cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setReadRpcTimeout(1000));
        cluster.filters().outbound().verbs(Verb.READ_RSP.id).from(2, 3).to(1).drop();
        try
        {
            assertThatThrownBy(() -> session.execute(statement(cql, ConsistencyLevel.ALL, 1, state)))
            .isInstanceOf(ReadTimeoutException.class);
        }
        finally
        {
            cluster.filters().reset();
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setReadRpcTimeout(10000));
        }
        assertRows(rows(session.execute(statement(cql, ConsistencyLevel.ALL, 1, state))), row(3, 3), row(4, 4));
    }

    @Test
    public void replicaShutdownBetweenPagesPreservesConsistency() throws Exception
    {
        String table = createRows("shutdown_checkpoint", "NONE", 5);
        String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 3";
        PagingState quorumState = checkpoint(session.execute(statement(cql, ConsistencyLevel.QUORUM, 1, null)));
        PagingState allState = checkpoint(session.execute(statement(cql, ConsistencyLevel.ALL, 1, null)));
        InetSocketAddress endpoint = cluster.get(3).broadcastAddress();
        cluster.get(3).shutdown().get(30, TimeUnit.SECONDS);
        try
        {
            await().atMost(30, TimeUnit.SECONDS).until(() -> cluster.get(1).callOnInstance(() ->
                !FailureDetector.instance.isAlive(toCassandraInetAddressAndPort(endpoint))));
            assertRows(rows(session.execute(statement(cql, ConsistencyLevel.QUORUM, 1, quorumState))), row(3, 3), row(4, 4));
            assertThatThrownBy(() -> session.execute(statement(cql, ConsistencyLevel.ALL, 1, allState)))
            .isInstanceOf(UnavailableException.class);
        }
        finally
        {
            cluster.get(3).startup();
            await().atMost(30, TimeUnit.SECONDS).until(() -> cluster.get(1).callOnInstance(() ->
                FailureDetector.instance.isAlive(toCassandraInetAddressAndPort(endpoint))));
        }
        assertRows(rows(session.execute(statement(cql, ConsistencyLevel.ALL, 1, allState))), row(3, 3), row(4, 4));
    }

    @Test
    public void expiredRowsProduceResumableCheckpoints()
    {
        String table = KEYSPACE + ".expired_checkpoint";
        createTable(table, "(pk int, ck int, v int, PRIMARY KEY (pk, ck))", "NONE");
        for (int ck = 0; ck < 5; ck++)
            write("INSERT INTO " + table + " (pk, ck, v) VALUES (0, " + ck + ", " + ck + ")" +
                  (ck < 3 ? " USING TTL 1" : ""));
        awaitExpiry();
        cluster.forEach(node -> node.flush(KEYSPACE));
        ResultSet results = session.execute(statement("SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 3",
                                                      ConsistencyLevel.ALL, 1, null));
        assertNotNull(checkpoint(results));
        assertRows(rows(results), row(3, 3), row(4, 4));
    }

    @Test
    public void ttlExpiryBetweenPagesUsesTheNewReadTime()
    {
        String table = createRows("expiry_between_pages", "NONE", 6);
        write("INSERT INTO " + table + " (pk, ck, v) VALUES (0, 3, 99) USING TIMESTAMP 3 AND TTL 1");
        String cql = "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 4";
        PagingState state = checkpoint(session.execute(statement(cql, ConsistencyLevel.ALL, 1, null)));
        awaitExpiry();
        assertRows(rows(session.execute(statement(cql, ConsistencyLevel.ALL, 1, state))), row(4, 4), row(5, 5));
    }

    @Test
    public void collectionTombstonesPreserveTheirLiveRow()
    {
        for (boolean divergent : new boolean[]{ false, true })
        {
            String table = KEYSPACE + ".collection_checkpoint_" + divergent;
            createTable(table, "(pk int, ck int, m map<int, int>, PRIMARY KEY (pk, ck))", "NONE");
            for (int node = 1; node <= 3; node++)
            {
                for (int ck = 0; ck < 2; ck++)
                    cluster.get(node).executeInternal("INSERT INTO " + table +
                                                     " (pk, ck, m) VALUES (0, ?, {0:0, 1:1, 2:2}) USING TIMESTAMP 1", ck);
                if (!divergent || node == 1)
                    cluster.get(node).executeInternal("DELETE m[0], m[1], m[2] FROM " + table +
                                                     " USING TIMESTAMP 2 WHERE pk = 0 AND ck = 0");
                cluster.get(node).flush(KEYSPACE);
            }
            long before = thresholdCrossings(table);
            ResultSet results = session.execute(statement("SELECT ck, m FROM " + table + " WHERE pk = 0 LIMIT 3",
                                                          ConsistencyLevel.ALL, 1, null));
            List<com.datastax.driver.core.Row> rows = results.all();
            assertEquals(2, rows.size());
            assertEquals(0, rows.get(0).getInt("ck"));
            assertTrue(rows.get(0).getMap("m", Integer.class, Integer.class).isEmpty());
            assertEquals(1, rows.get(1).getInt("ck"));
            assertEquals(3, rows.get(1).getMap("m", Integer.class, Integer.class).size());
            assertTrue("Expected collection cells to cross the threshold", thresholdCrossings(table) > before);
        }
    }

    @Test
    public void groupedReadContinuesAcrossEmptyCheckpoint()
    {
        String table = KEYSPACE + ".grouped_checkpoint";
        createTable(table, "(pk int, g int, ck int, PRIMARY KEY (pk, g, ck))", "NONE");
        for (int node = 1; node <= 3; node++)
            for (int g = 0; g < 3; g++)
                for (int ck = 0; ck < 3; ck++)
                {
                    cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, g, ck) VALUES (0, ?, ?) USING TIMESTAMP 1", g, ck);
                    if (g == 0)
                        cluster.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE pk = 0 AND g = 0 AND ck = ?", ck);
                }
        long before = thresholdCrossings(table);
        String cql = "SELECT g, count(*) FROM " + table + " WHERE pk = 0 GROUP BY pk, g LIMIT 3";
        configurePaging(false);
        try
        {
            assertRows(groupedRows(cql), row(1, 3L), row(2, 3L));
        }
        finally
        {
            configurePaging(true);
        }
        assertRows(groupedRows(cql), row(1, 3L), row(2, 3L));
        assertTrue(thresholdCrossings(table) > before);
    }

    @Test
    public void groupedCheckpointsPreserveStaticRowsAndLimits()
    {
        verifyGroupedCheckpoints(false);
    }

    @Test
    public void groupedCheckpointsReconcileReplicaDifferences()
    {
        verifyGroupedCheckpoints(true);
    }

    private static void verifyGroupedCheckpoints(boolean divergent)
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            String table = KEYSPACE + ".grouped_combinations_" + divergent + '_' + reversed;
            cluster.schemaChange("CREATE TABLE " + table + " (pk int, g int, ck int, s int static, PRIMARY KEY (pk, g, ck))" +
                                 " WITH CLUSTERING ORDER BY (g " + (reversed ? "DESC" : "ASC") + ", ck " + (reversed ? "DESC" : "ASC") + ')' +
                                 " AND read_repair = 'NONE' AND speculative_retry = 'NONE'");
            List<Object[]> expected = new ArrayList<>();
            for (int pk = 0; pk < 3; pk++)
            {
                for (int node = 1; node <= 3; node++)
                {
                    cluster.get(node).executeInternal("UPDATE " + table + " USING TIMESTAMP 1 SET s = ? WHERE pk = ?", pk + 10, pk);
                    for (int g = 0; g < 3; g++)
                        for (int ck = 0; ck < 8; ck++)
                        {
                            cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, g, ck) VALUES (?, ?, ?) USING TIMESTAMP 1", pk, g, ck);
                            if ((pk == 2 || g == 0 || (ck > 0 && ck < 6)) && (!divergent || node == 1))
                                cluster.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE pk = ? AND g = ? AND ck = ?", pk, g, ck);
                        }
                }
                if (pk == 2)
                    expected.add(row(pk, null, pk + 10, 0L));
                else
                    for (int g = 1; g < 3; g++)
                        expected.add(row(pk, g, pk + 10, 3L));
            }

            String base = "SELECT pk, g, s, count(ck) FROM " + table + " WHERE pk IN (0, 1, 2) GROUP BY pk, g";
            Comparator<Object[]> order = Comparator.<Object[], Integer>comparing(result -> (Integer) result[0])
                                                  .thenComparing(result -> (Integer) result[1], Comparator.nullsFirst(Comparator.naturalOrder()));
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    cluster.forEach(node -> node.flush(KEYSPACE));
                List<Object[]> ordered;
                configurePaging(false);
                try
                {
                    ordered = groupedStaticRows(base, 1);
                    assertRows(ordered.stream().sorted(order).toArray(Object[][]::new), expected.toArray(Object[][]::new));
                }
                finally
                {
                    configurePaging(true);
                }
                long reconciliations = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount());
                for (int partitionLimit : new int[]{ 0, 1 })
                    for (int limit : new int[]{ 3, 6 })
                        for (int fetchSize : new int[]{ 1, 2 })
                        {
                            String cql = base + (partitionLimit == 0 ? "" : " PER PARTITION LIMIT " + partitionLimit) + " LIMIT " + limit;
                            Map<Integer, Integer> counts = new HashMap<>();
                            Object[][] limited = ordered.stream()
                                                        .filter(result -> partitionLimit == 0 || counts.merge((Integer) result[0], 1, Integer::sum) <= partitionLimit)
                                                        .limit(limit).toArray(Object[][]::new);
                            configurePaging(false);
                            try
                            {
                                assertRows(groupedStaticRows(cql, fetchSize).toArray(Object[][]::new), limited);
                            }
                            finally
                            {
                                configurePaging(true);
                            }
                            long before = thresholdCrossings(table);
                            try
                            {
                                assertRows(groupedStaticRows(cql, fetchSize).toArray(Object[][]::new), limited);
                            }
                            catch (AssertionError e)
                            {
                                throw new AssertionError(cql + ", fetchSize=" + fetchSize + ", flushed=" + flushed, e);
                            }
                            assertTrue(cql + ": expected tombstone paging", thresholdCrossings(table) > before);
                        }
                if (divergent)
                    assertTrue("Expected full-data reconciliation", cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount()) > reconciliations);
            }
        }
    }

    private static List<Object[]> groupedStaticRows(String cql, int fetchSize)
    {
        List<Object[]> rows = new ArrayList<>();
        for (com.datastax.driver.core.Row result : readPages(session, cql, fetchSize, 72))
            rows.add(row(result.getInt(0), result.getObject(1), result.getInt(2), result.getLong(3)));
        return rows;
    }

    @Test
    public void partitionKeyInContinuesEachPartition()
    {
        String table = createRows("in_checkpoint", "NONE", 5);
        for (int node = 1; node <= 3; node++)
            for (int pk = 1; pk < 3; pk++)
                for (int ck = 0; ck < 5; ck++)
                {
                    cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1", pk, ck, ck);
                    if (ck < 3)
                        cluster.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE pk = ? AND ck = ?", pk, ck);
                }
        String cql = "SELECT pk, ck FROM " + table + " WHERE pk IN (0, 1, 2) LIMIT 7";
        Object[][] expected = { row(0, 3), row(0, 4), row(1, 3), row(1, 4), row(2, 3), row(2, 4) };
        configurePaging(false);
        try
        {
            assertRows(partitionRows(cql), expected);
        }
        finally
        {
            configurePaging(true);
        }
        assertRows(partitionRows(cql), expected);
    }

    @Test
    public void partitionKeyInRespectsLimitsInBothClusteringOrders()
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            String table = KEYSPACE + ".in_limits_" + reversed;
            createInTable(table, reversed);
            List<Object[]> expected = new ArrayList<>();
            for (int pk = 0; pk < 3; pk++)
                for (int i = 0; i < 5; i++)
                {
                    int ck = reversed ? 4 - i : i;
                    populateInRow(table, pk, ck, i < 3, false);
                    if (i >= 3)
                        expected.add(row(pk, ck, null));
                }
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    cluster.forEach(node -> node.flush(KEYSPACE));
                verifyInLimits(table, expected, false);
            }
        }
    }

    @Test
    public void partitionKeyInSkipsDeletedPartitionsAndRepeatedCheckpoints()
    {
        String table = KEYSPACE + ".in_repeated";
        createInTable(table, false);
        List<Object[]> expected = new ArrayList<>();
        for (int pk = 0; pk < 3; pk++)
            for (int ck = 0; ck < 12; ck++)
            {
                boolean deleted = pk == 0 || (ck != 6 && ck != 10 && ck != 11);
                populateInRow(table, pk, ck, deleted, false);
                if (!deleted)
                    expected.add(row(pk, ck, null));
            }
        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                cluster.forEach(node -> node.flush(KEYSPACE));
            verifyInLimits(table, expected, true);
        }
    }

    @Test
    public void partitionKeyInPreservesStaticRowsDuringReconciliation()
    {
        for (boolean divergent : new boolean[]{ false, true })
        {
            String table = KEYSPACE + ".in_static_" + divergent;
            createInTable(table, false);
            List<Object[]> expected = new ArrayList<>();
            for (int pk = 0; pk < 3; pk++)
            {
                for (int node = 1; node <= 3; node++)
                    cluster.get(node).executeInternal("UPDATE " + table + " USING TIMESTAMP 1 SET s = ? WHERE pk = ?", pk + 10, pk);
                for (int ck = 0; ck < 5; ck++)
                {
                    boolean deleted = pk == 0 || ck < 3;
                    populateInRow(table, pk, ck, deleted, divergent);
                    if (!deleted)
                        expected.add(row(pk, ck, pk + 10));
                }
            }
            expected.add(row(0, null, 10));
            long reconciliations = verifyInLimits(table, expected, false);
            cluster.forEach(node -> node.flush(KEYSPACE));
            reconciliations += verifyInLimits(table, expected, false);
            if (divergent)
                assertTrue("Expected full-data reconciliation with paging enabled", reconciliations > 0);
        }
    }

    private static void createInTable(String table, boolean reversed)
    {
        cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))" +
                             " WITH CLUSTERING ORDER BY (ck " + (reversed ? "DESC" : "ASC") + ')' +
                             " AND read_repair = 'NONE' AND speculative_retry = 'NONE'");
    }

    private static void populateInRow(String table, int pk, int ck, boolean deleted, boolean divergent)
    {
        for (int node = 1; node <= 3; node++)
        {
            cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1", pk, ck, ck);
            if (deleted && (!divergent || node == 1))
                cluster.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE pk = ? AND ck = ?", pk, ck);
        }
    }

    private static long verifyInLimits(String table, List<Object[]> expected, boolean perPartition)
    {
        String base = "SELECT pk, ck, s FROM " + table + " WHERE pk IN (0, 1, 2)";
        List<Object[]> ordered;
        configurePaging(false);
        try
        {
            ordered = inRows(base, 1);
            Comparator<Object[]> comparator = Comparator.<Object[], Integer>comparing(result -> (Integer) result[0])
                                                       .thenComparing(result -> (Integer) result[1], Comparator.nullsFirst(Comparator.naturalOrder()));
            assertRows(ordered.stream().sorted(comparator).toArray(Object[][]::new),
                       expected.stream().sorted(comparator).toArray(Object[][]::new));
        }
        finally
        {
            configurePaging(true);
        }
        long reconciliations = 0;
        for (int partitionLimit : perPartition ? new int[]{ 0, 1, 2 } : new int[]{ 0 })
            for (int limit : new int[]{ 0, 3, expected.size(), expected.size() + 1 })
                for (int fetchSize : new int[]{ 1, 2 })
                {
                    String cql = base + (partitionLimit == 0 ? "" : " PER PARTITION LIMIT " + partitionLimit) +
                                 (limit == 0 ? "" : " LIMIT " + limit);
                    Map<Integer, Integer> counts = new HashMap<>();
                    Object[][] limited = ordered.stream()
                                                .filter(result -> partitionLimit == 0 || counts.merge((Integer) result[0], 1, Integer::sum) <= partitionLimit)
                                                .limit(limit == 0 ? Long.MAX_VALUE : limit)
                                                .toArray(Object[][]::new);
                    configurePaging(false);
                    try
                    {
                        assertRows(inRows(cql, fetchSize).toArray(Object[][]::new), limited);
                    }
                    finally
                    {
                        configurePaging(true);
                    }
                    long before = thresholdCrossings(table);
                    long repairedBefore = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount());
                    assertRows(inRows(cql, fetchSize).toArray(Object[][]::new), limited);
                    assertTrue(cql + ": expected tombstone paging", thresholdCrossings(table) > before);
                    reconciliations += cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount()) - repairedBefore;
                }
        return reconciliations;
    }

    private static List<Object[]> inRows(String cql, int fetchSize)
    {
        List<Object[]> rows = new ArrayList<>();
        for (com.datastax.driver.core.Row result : readPages(session, cql, fetchSize, 36))
            rows.add(row(result.getInt("pk"), result.getObject("ck"), result.getObject("s")));
        return rows;
    }

    @Test
    public void unsupportedRangeCheckpointFailsSafely()
    {
        String table = createRows("range_checkpoint", "NONE", 5);
        assertThatThrownBy(() -> session.execute(statement("SELECT ck, v FROM " + table + " LIMIT 3",
                                                          ConsistencyLevel.ALL, 1, null)).all())
        .isInstanceOf(ReadFailureException.class);
    }

    private static void awaitExpiry()
    {
        long expiry = System.currentTimeMillis() + 2000;
        await().atMost(10, TimeUnit.SECONDS).until(() -> System.currentTimeMillis() >= expiry);
    }

    private static void configurePaging(boolean enabled)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            DatabaseDescriptor.setTombstonePagingThreshold(enabled ? 2 : -1);
            DatabaseDescriptor.setTombstoneFailureThreshold(enabled ? 2 : 10000);
        }));
    }

    private static Object[][] groupedRows(String cql)
    {
        return readPages(session, cql, 1, 9).stream()
                      .map(result -> row(result.getInt(0), result.getLong(1))).toArray(Object[][]::new);
    }

    private static Object[][] partitionRows(String cql)
    {
        List<Object[]> rows = new ArrayList<>();
        session.execute(statement(cql, ConsistencyLevel.ALL, 1, null)).all()
               .forEach(result -> rows.add(row(result.getInt("pk"), result.getInt("ck"))));
        rows.sort(Comparator.<Object[]>comparingInt(result -> (Integer) result[0]).thenComparingInt(result -> (Integer) result[1]));
        return rows.toArray(new Object[0][]);
    }

    private static String createRows(String name, String speculation, int count)
    {
        String table = KEYSPACE + '.' + name;
        createTable(table, "(pk int, ck int, v int, PRIMARY KEY (pk, ck))", speculation);
        for (int node = 1; node <= 3; node++)
            for (int ck = 0; ck < count; ck++)
            {
                cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                if (ck < 3)
                    cluster.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
            }
        return table;
    }

    private static void createTable(String table, String columns, String speculation)
    {
        cluster.schemaChange("CREATE TABLE " + table + ' ' + columns +
                             " WITH read_repair = 'NONE' AND speculative_retry = '" + speculation + "'");
    }

    private static void write(String cql)
    {
        session.execute(new SimpleStatement(cql).setConsistencyLevel(ConsistencyLevel.ALL));
    }

    private static SimpleStatement statement(String cql, ConsistencyLevel consistency, int fetchSize, PagingState state)
    {
        SimpleStatement statement = new SimpleStatement(cql);
        statement.setConsistencyLevel(consistency);
        statement.setFetchSize(fetchSize);
        if (state != null)
            statement.setPagingState(state);
        return statement;
    }

    private static PagingState checkpoint(ResultSet results)
    {
        assertEquals("Expected an empty checkpoint page", 0, results.getAvailableWithoutFetching());
        PagingState state = results.getExecutionInfo().getPagingState();
        assertNotNull("Expected continuation state", state);
        return state;
    }

    private static Object[][] rows(ResultSet results)
    {
        return results.all().stream().map(result -> row(result.getInt("ck"), result.getInt("v"))).toArray(Object[][]::new);
    }

    private static long thresholdCrossings(String qualified)
    {
        String table = qualified.substring(qualified.indexOf('.') + 1);
        long count = 0;
        for (int node = 1; node <= 3; node++)
            count += cluster.get(node).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstonePages.getCount());
        return count;
    }
}
