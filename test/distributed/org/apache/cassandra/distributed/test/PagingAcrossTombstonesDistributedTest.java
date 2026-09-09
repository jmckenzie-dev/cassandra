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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PagingAcrossTombstonesDistributedTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster client;
    private static Session session;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withSubnet(11)
                              .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                          .set("dynamic_snitch", false)
                                                          .set("hinted_handoff_enabled", false)
                                                          .set("tombstone_paging_threshold", 2)
                                                          .set("tombstone_failure_threshold", 2))
                              .start());
        String host = cluster.get(1).broadcastAddress().getHostString();
        client = com.datastax.driver.core.Cluster.builder()
                                                .addContactPoint(host)
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
    public void initialCheckpointCompletesAtAllInMemtable()
    {
        verify("initial_memtable", 3, true, false, ConsistencyLevel.ALL);
    }

    @Test
    public void initialCheckpointCompletesAtAllAfterFlush()
    {
        verify("initial_flushed", 3, true, true, ConsistencyLevel.ALL);
    }

    @Test
    public void belowThresholdCompletesAtAll()
    {
        verify("below_threshold", 2, true, false, ConsistencyLevel.ALL);
    }

    @Test
    public void localCheckpointCompletesAtOne()
    {
        verify("local_checkpoint", 3, true, false, ConsistencyLevel.ONE);
    }

    @Test
    public void disabledPagingCompletesAtAll()
    {
        verify("disabled_paging", 3, false, false, ConsistencyLevel.ALL);
    }

    @Test
    public void matchingCheckpointsAtQuorum()
    {
        verify("initial_quorum", 3, true, false, ConsistencyLevel.QUORUM);
    }

    @Test
    public void remoteDataCheckpointResumesAtNonReplicaCoordinator() throws IOException
    {
        try (Cluster remote = init(Cluster.build(4)
                                          .withSubnet(12)
                                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                      .set("tombstone_paging_threshold", 2)
                                                                      .set("tombstone_failure_threshold", 2))
                                          .start(), 3))
        {
            String qualified = KEYSPACE + ".remote_checkpoint";
            remote.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            int coordinator = 0;
            for (int node = 1; node <= 4; node++)
            {
                boolean replica = remote.get(node).callOnInstance(() -> StorageService.instance
                                                                                     .getNaturalReplicasForToken(KEYSPACE, ByteBufferUtil.bytes(0))
                                                                                     .containsSelf());
                if (!replica)
                {
                    coordinator = node;
                    continue;
                }
                for (int ck = 0; ck < 5; ck++)
                {
                    remote.get(node).executeInternal("INSERT INTO " + qualified +
                                                     " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                    if (ck < 3)
                        remote.get(node).executeInternal("DELETE FROM " + qualified +
                                                         " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
                }
            }
            assertTrue("Expected one non-replica coordinator", coordinator > 0);
            String host = remote.get(coordinator).broadcastAddress().getHostString();
            try (com.datastax.driver.core.Cluster remoteClient = com.datastax.driver.core.Cluster.builder()
                                                                                               .addContactPoint(host)
                                                                                               .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                               .build();
                 Session remoteSession = remoteClient.connect())
            {
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                {
                    SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + qualified + " WHERE pk = 0 LIMIT 2");
                    statement.setConsistencyLevel(consistency);
                    statement.setFetchSize(1);
                    ResultSet results = remoteSession.execute(statement);
                    assertEquals(0, results.getAvailableWithoutFetching());
                    assertNotNull(results.getExecutionInfo().getPagingState());
                    Object[][] rows = results.all().stream()
                                            .map(result -> row(result.getInt("ck"), result.getInt("v")))
                                            .toArray(Object[][]::new);
                    assertRows(rows, row(3, 3), row(4, 4));
                }
            }
        }
    }

    @Test
    public void equalContinuationDifferentDataReconciles()
    {
        verifyConflictingValue("checkpoint_conflict", "NONE");
    }

    @Test
    public void emptyMismatchedCheckpointsRefillToCompletion()
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
        {
            String table = KEYSPACE + ".empty_mismatch_" + repair.toLowerCase(java.util.Locale.ROOT);
            cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = '" + repair + "' AND speculative_retry = 'NONE'");
            for (int node = 1; node <= 3; node++)
            {
                configure(node, 2);
                for (int ck = 0; ck < 12; ck++)
                    cluster.get(node).executeInternal("DELETE FROM " + table +
                                                      " USING TIMESTAMP ? WHERE pk = 0 AND ck = ?", (long) node + 1, ck);
                for (int ck = 12; ck < 15; ck++)
                    cluster.get(node).executeInternal("INSERT INTO " + table +
                                                      " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
            }
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                {
                    cluster.get(1).executeInternal("DELETE FROM " + table +
                                                  " USING TIMESTAMP 10 WHERE pk = 0 AND ck = 0");
                    cluster.forEach(node -> node.flush(KEYSPACE));
                }
                long before = reconciliations();
                Object[][] actual = org.apache.cassandra.distributed.util.TombstonePagingTestUtil
                                    .readPages(session, "SELECT ck, v FROM " + table + " WHERE pk = 0 LIMIT 4", 2, 3)
                                    .stream().map(result -> row(result.getInt("ck"), result.getInt("v"))).toArray(Object[][]::new);
                assertRows(actual, row(12, 12), row(13, 13), row(14, 14));
                assertTrue("Expected digest mismatch repair", reconciliations() > before);
            }
        }
    }

    @Test
    public void equalContinuationDifferentDataRepairs()
    {
        verifyConflictingValue("checkpoint_repair", "BLOCKING");
    }

    private static void verifyConflictingValue(String table, String repair)
    {
        String qualified = KEYSPACE + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = '" + repair + "' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            configure(node, 2);
            for (int ck = 0; ck < 6; ck++)
                cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                  " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
            for (int ck = 1; ck <= 3; ck++)
                cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                  " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
        }
        cluster.get(2).executeInternal("INSERT INTO " + qualified + " (pk, ck, v) VALUES (0, 0, 99) USING TIMESTAMP 3");
        cluster.forEach(node -> node.flush(KEYSPACE));

        long before = reconciliations();
        assertRows(query("SELECT ck, v FROM " + qualified + " WHERE pk = 0 LIMIT 3", 2),
                   row(0, 99), row(4, 4), row(5, 5));
        assertTrue("Expected full-data reconciliation", reconciliations() > before);
        assertRows(cluster.get(1).executeInternal("SELECT v FROM " + qualified + " WHERE pk = 0 AND ck = 0"),
                   row(repair.equals("BLOCKING") ? 99 : 0));
    }

    @Test
    public void differentCheckpointsReconcileInBothDirections()
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            String qualified = KEYSPACE + ".different_checkpoints_" + reversed;
            cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            for (int node = 1; node <= 3; node++)
            {
                configure(node, node + 1);
                for (int ck = 0; ck < 8; ck++)
                {
                    cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                      " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                    if (reversed ? ck >= 3 : ck < 5)
                        cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                          " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
                }
            }
            long before = reconciliations();
            Object[][] expected = reversed ? new Object[][]{ row(2, 2), row(1, 1), row(0, 0) }
                                           : new Object[][]{ row(5, 5), row(6, 6), row(7, 7) };
            assertRows(query("SELECT ck, v FROM " + qualified + " WHERE pk = 0 ORDER BY ck " +
                             (reversed ? "DESC" : "ASC") + " LIMIT 3", 2), expected);
            assertTrue("Expected full-data reconciliation", reconciliations() > before);
        }
    }

    @Test
    public void repeatedMatchingCheckpointsWithCompositeClustering()
    {
        String qualified = KEYSPACE + ".composite_checkpoints";
        cluster.schemaChange("CREATE TABLE " + qualified +
                             " (pk int, ck int, suffix text, v int, PRIMARY KEY (pk, ck, suffix))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            configure(node, 2);
            for (int ck = 0; ck < 9; ck++)
            {
                cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                  " (pk, ck, suffix, v) VALUES (0, ?, '', ?) USING TIMESTAMP 1", ck, ck);
                if (ck >= 3)
                    cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                      " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ? AND suffix = ''", ck);
            }
            cluster.get(node).flush(KEYSPACE);
        }
        long before = reconciliations();
        assertRows(query("SELECT ck, v FROM " + qualified + " WHERE pk = 0 ORDER BY ck DESC LIMIT 3", 1),
                   row(2, 2), row(1, 1), row(0, 0));
        assertEquals("Matching checkpoints should not reconcile", before, reconciliations());
    }

    private static void configure(int node, int threshold)
    {
        cluster.get(node).runOnInstance(() -> {
            DatabaseDescriptor.setTombstonePagingThreshold(threshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
        });
    }

    @Test
    public void checkpointAndExhaustionWithEqualDigestReconcile()
    {
        String qualified = KEYSPACE + ".checkpoint_and_exhaustion";
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            configure(node, node == 2 ? 10000 : 2);
            for (int ck = 0; ck < 3; ck++)
                cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                  " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
        }
        long before = reconciliations();
        assertRows(query("SELECT ck, v FROM " + qualified + " WHERE pk = 0 LIMIT 2", 1));
        assertTrue("Different completion states must reconcile even when digests match", reconciliations() > before);
    }

    @Test
    public void staticDataDoesNotCreateRowsAtEmptyCheckpoints()
    {
        String qualified = KEYSPACE + ".static_checkpoints";
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, s int static, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            configure(node, 2);
            cluster.get(node).executeInternal("INSERT INTO " + qualified + " (pk, s) VALUES (0, 77) USING TIMESTAMP 1");
            for (int ck = 0; ck < 5; ck++)
            {
                cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                  " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                if (ck < 3)
                    cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                      " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
            }
        }
        SimpleStatement statement = new SimpleStatement("SELECT ck, v, s FROM " + qualified + " WHERE pk = 0 LIMIT 2");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setFetchSize(1);
        ResultSet results = session.execute(statement);
        assertEquals("A static row alone does not complete a checkpointed partition", 0, results.getAvailableWithoutFetching());
        assertNotNull(results.getExecutionInfo().getPagingState());
        Object[][] rows = results.all().stream()
                                .map(result -> row(result.getInt("ck"), result.getInt("v"), result.getInt("s")))
                                .toArray(Object[][]::new);
        assertRows(rows, row(3, 3, 77), row(4, 4, 77));

        for (int node = 1; node <= 3; node++)
            for (int ck = 3; ck < 5; ck++)
                cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                  " USING TIMESTAMP 3 WHERE pk = 0 AND ck = ?", ck);
        rows = session.execute(statement).all().stream()
                      .map(result -> row(result.getObject("ck"), result.getObject("v"), result.getInt("s")))
                      .toArray(Object[][]::new);
        assertRows(rows, row(null, null, 77));
    }

    private static Object[][] query(String cql, int fetchSize)
    {
        SimpleStatement statement = new SimpleStatement(cql);
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setFetchSize(fetchSize);
        return session.execute(statement).all().stream()
                      .map(result -> row(result.getInt("ck"), result.getInt("v")))
                      .toArray(Object[][]::new);
    }

    private static long reconciliations()
    {
        return cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount() + ReadRepairMetrics.repairedBlocking.getCount());
    }

    private static void verify(String table, int deletedRows, boolean paging, boolean flushed, ConsistencyLevel consistency)
    {
        String qualified = KEYSPACE + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.setTombstonePagingThreshold(paging ? 2 : -1);
                DatabaseDescriptor.setTombstoneFailureThreshold(paging ? 2 : 10000);
            });
            for (int ck = 0; ck < 5; ck++)
                cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                  " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
            for (int ck = 0; ck < deletedRows; ck++)
                cluster.get(node).executeInternal("DELETE FROM " + qualified +
                                                  " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
            if (flushed)
                cluster.get(node).flush(KEYSPACE);
        }

        Queue<String> responses = new ConcurrentLinkedQueue<>();
        cluster.filters().outbound().verbs(Verb.READ_RSP.id).to(1).messagesMatching((from, to, message) -> {
            responses.add(cluster.get(to).unsafeCallOnThisThread(() -> {
                try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
                {
                    Message<?> decoded = Message.serializer.deserialize(in, toCassandraInetAddressAndPort(message.from()), message.version());
                    return "replica=" + from + ", digest=" + ((ReadResponse) decoded.payload).isDigestResponse() +
                           ", params=" + decoded.header.params();
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            }));
            return false;
        }).drop();
        try
        {
            long before = reconciliations();
            SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + qualified +
                                                            " WHERE pk = 0 ORDER BY ck ASC LIMIT 2");
            statement.setConsistencyLevel(consistency);
            statement.setFetchSize(1);
            ResultSet resultSet = session.execute(statement);
            if (paging && deletedRows == 3)
            {
                assertEquals(0, resultSet.getAvailableWithoutFetching());
                assertNotNull(resultSet.getExecutionInfo().getPagingState());
            }
            Object[][] actual = resultSet.all().stream()
                                       .map(result -> row(result.getInt("ck"), result.getInt("v")))
                                       .toArray(Object[][]::new);
            assertRows(actual, row(deletedRows, deletedRows), row(deletedRows + 1, deletedRows + 1));
            assertEquals("Matching replicas should not reconcile", before, reconciliations());
        }
        catch (RuntimeException failure)
        {
            throw new AssertionError("Read failed; replica responses: " + responses, failure);
        }
        finally
        {
            cluster.filters().reset();
        }
    }
}
