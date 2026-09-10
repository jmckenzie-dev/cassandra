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
package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.reads.AbstractReadExecutor;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PagingCheckpointOwnershipTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster client;
    private static Session session;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build(3).withSubnet(19)
                              .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                          .set("dynamic_snitch", false)
                                                          .set("hinted_handoff_enabled", false)
                                                          .set("concurrent_reads", 2)
                                                          .set("read_request_timeout", "30s")
                                                          .set("tombstone_paging_threshold", 2)
                                                          .set("tombstone_failure_threshold", 10000))
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

    @Test(timeout = 120000)
    public void rejectedLocalCheckpointDoesNotAdvancePager()
    {
        String table = "rejected_local_checkpoint";
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table +
                             " (pk int PRIMARY KEY, a int, b int, c int, v int)" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        cluster.forEach(node -> {
            node.executeInternal("INSERT INTO " + KEYSPACE + '.' + table + " (pk, v) VALUES (0, 42) USING TIMESTAMP 1");
            node.executeInternal("DELETE a, b, c FROM " + KEYSPACE + '.' + table + " USING TIMESTAMP 2 WHERE pk = 0");
        });
        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                cluster.forEach(node -> node.flush(KEYSPACE));
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.db.ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                QueryPager pager = Util.cmd(cfs, 0).withLimit(2).build().getPager(null, ProtocolVersion.CURRENT);
                long before = cfs.metric.tombstonePages.getCount();
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    assertThatThrownBy(() -> {
                        CoordinatorWarnings.init();
                        try
                        {
                            try (PartitionIterator page = pager.fetchPage(1, org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                         ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
                            {
                                page.hasNext();
                            }
                        }
                        finally
                        {
                            CoordinatorWarnings.done();
                        }
                    }).isInstanceOf(org.apache.cassandra.exceptions.ReadFailureException.class)
                      .satisfies(error -> assertTrue(((org.apache.cassandra.exceptions.ReadFailureException) error)
                                                    .failureReasonByEndpoint.containsValue(RequestFailureReason.READ_TOO_MANY_TOMBSTONES)));
                    assertNull("Rejected local checkpoint advanced the cursor", pager.state());
                    assertEquals(2, pager.maxRemaining());
                    assertFalse(pager.isExhausted());
                }
                assertTrue("Expected coordinator-local checkpoints", cfs.metric.tombstonePages.getCount() >= before + 2);
            });
            assertNativeFailure(() -> session.execute(statement("SELECT * FROM " + KEYSPACE + '.' + table + " WHERE pk = 0 LIMIT 2",
                                                                 ConsistencyLevel.ONE, 1)), ConsistencyLevel.ONE);
        }
    }

    @Test(timeout = 180000)
    public void unsupportedRepairCheckpointReturnsTypedNativeFailure() throws Exception
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
        {
            String table = "unsupported_repair_" + repair.toLowerCase(java.util.Locale.ROOT);
            String qualified = KEYSPACE + '.' + table;
            cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = '" + repair + "' AND speculative_retry = 'NONE'");
            for (int pk = 0; pk < 2; pk++)
            {
                int key = pk;
                for (int node = 1; node <= 3; node++)
                    cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                      " (pk, ck, v) VALUES (?, 0, ?) USING TIMESTAMP ?", pk, node, (long) node);
                if (pk == 1)
                    cluster.forEach(node -> node.flush(KEYSPACE));
                long reads = localReads(table);
                long repairs = repairs();
                long checkpoints = checkpoints(table);
                Map<Integer, IMessage> initialResponses = new ConcurrentHashMap<>();
                CountDownLatch ready = new CountDownLatch(2);
                cluster.filters().outbound().verbs(Verb.READ_RSP.id).to(1).messagesMatching((from, to, message) -> {
                    boolean initialResponse = cluster.get(from).unsafeCallOnThisThread(() -> {
                        org.apache.cassandra.net.Message<?> response = Instance.deserializeMessage(message);
                        return !response.header.params().containsKey(ParamType.TOMBSTONE_PAGE_CHECKPOINT);
                    });
                    if (!initialResponse)
                        return false;
                    if (initialResponses.putIfAbsent(from, message) == null)
                    {
                        ready.countDown();
                        return true;
                    }
                    return false;
                }).drop();
                try
                {
                    // No LIMIT and no native paging: the repair resolver cannot resume checkpoints.
                    SimpleStatement statement = statement("SELECT ck, v FROM " + qualified + " WHERE pk = " + pk,
                                                          ConsistencyLevel.ALL, Integer.MAX_VALUE);
                    ResultSetFuture result = session.executeAsync(statement);
                    assertTrue("Expected both initial remote responses", ready.await(10, TimeUnit.SECONDS));
                    await().atMost(10, TimeUnit.SECONDS).until(() -> localReads(table) > reads);
                    assertEquals("Initial reads must finish before tombstones are added", checkpoints, checkpoints(table));
                    cluster.forEach(node -> {
                        for (int ck = -3; ck < 0; ck++)
                            node.executeInternal("DELETE FROM " + qualified + " USING TIMESTAMP 10 WHERE pk = ? AND ck = ?", key, ck);
                        if (key == 1)
                            node.flush(KEYSPACE);
                    });
                    for (int node = 2; node <= 3; node++)
                    {
                        IMessage response = initialResponses.get(node);
                        cluster.get(1).runOnInstance(() -> MessagingService.instance().inboundSink.accept(Instance.deserializeMessage(response)));
                    }
                    assertNativeFailure(() -> result.getUninterruptibly(10, TimeUnit.SECONDS), ConsistencyLevel.ALL);
                    assertTrue("Expected digest mismatch to start full-data repair", repairs() > repairs);
                    assertTrue("Expected repair reads to hit the paging threshold", checkpoints(table) > checkpoints);

                    long afterRepair = repairs();
                    assertNativeFailure(() -> session.execute(statement), ConsistencyLevel.ALL);
                    assertEquals("Initial checkpoint rejection must not start repair", afterRepair, repairs());
                }
                finally
                {
                    cluster.filters().reset();
                }
            }
        }
    }

    @Test(timeout = 120000)
    public void localCopyPublishesCheckpointOnlyAfterResponseAcceptance()
    {
        String table = "accepted_local_copy";
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table +
                             " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        cluster.forEach(node -> {
            for (int ck = 0; ck < 3; ck++)
                node.executeInternal("DELETE FROM " + KEYSPACE + '.' + table + " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
            node.executeInternal("INSERT INTO " + KEYSPACE + '.' + table + " (pk, ck, v) VALUES (0, 3, 42) USING TIMESTAMP 1");
        });
        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                cluster.forEach(node -> node.flush(KEYSPACE));
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.db.ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                SinglePartitionReadCommand command = (SinglePartitionReadCommand) Util.cmd(cfs, 0).withLimit(1).build();
                command.setTombstonePageCheckpointOwner();
                long before = cfs.metric.readLatency.latency.getCount();
                AbstractReadExecutor executor = AbstractReadExecutor.getReadExecutor(ClusterMetadata.current(), command,
                                                                                     org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                                     ReadCoordinator.DEFAULT, Dispatcher.RequestTime.forImmediateExecution());
                assertTrue(executor.hasLocalRead());
                executor.executeAsync();
                await().atMost(10, TimeUnit.SECONDS).until(() -> cfs.metric.readLatency.latency.getCount() > before);
                assertNull("Local execution must not write directly to the owned command", command.tombstoneLimitedRow());
                assertEquals(0, command.tombstoneLimitedCount());
                executor.awaitResponses();
                try (PartitionIterator result = executor.getResult())
                {
                    assertFalse("Expected an empty checkpoint page", result.hasNext());
                }
                assertNotNull("Accepted local response lost its checkpoint", command.tombstoneLimitedRow());
                assertEquals(3, command.tombstoneLimitedCount());
                assertEquals(org.apache.cassandra.utils.ByteBufferUtil.bytes(2), command.tombstoneLimitedRow().clustering().bufferAt(0));
            });
        }
    }

    @Test(timeout = 180000)
    public void rejectedLocalCheckpointWithRemoteDigestMismatchReturnsTypedFailure() throws Exception
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
            for (boolean flushed : new boolean[]{ false, true })
                verifyRejectedLocalRepair(repair, flushed, LocalResponseOrder.BEFORE_QUORUM, false);
    }

    @Test(timeout = 180000)
    public void rejectedLocalCheckpointWithReversedRemoteResponsesReturnsTypedFailure() throws Exception
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
            for (boolean flushed : new boolean[]{ false, true })
                verifyRejectedLocalRepair(repair, flushed, LocalResponseOrder.BEFORE_QUORUM, true);
    }

    @Test(timeout = 180000)
    public void remoteQuorumCompletesBeforeLateLocalCheckpoint() throws Exception
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
            for (boolean flushed : new boolean[]{ false, true })
                for (boolean reverseRemote : new boolean[]{ false, true })
                    verifyRejectedLocalRepair(repair, flushed, LocalResponseOrder.AFTER_RESULT, reverseRemote);
    }

    @Test(timeout = 180000)
    public void lateLocalCheckpointDuringRepairDoesNotPoisonRemoteQuorum() throws Exception
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
            for (boolean flushed : new boolean[]{ false, true })
                for (boolean reverseRemote : new boolean[]{ false, true })
                    verifyRejectedLocalRepair(repair, flushed, LocalResponseOrder.DURING_REPAIR, reverseRemote);
    }

    private enum LocalResponseOrder
    {
        BEFORE_QUORUM, DURING_REPAIR, AFTER_RESULT
    }

    private static void verifyRejectedLocalRepair(String repair, boolean flushed, LocalResponseOrder localOrder, boolean reverseRemote) throws Exception
    {
        boolean lateLocal = localOrder != LocalResponseOrder.BEFORE_QUORUM;
        String table = "local_order_" + repair.toLowerCase(java.util.Locale.ROOT) + '_' + flushed + '_' +
                       localOrder.name().toLowerCase(java.util.Locale.ROOT) + '_' + reverseRemote;
        String qualified = KEYSPACE + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = '" + repair + "' AND speculative_retry = 'ALWAYS'");
        for (int node = 1; node <= 3; node++)
            cluster.get(node).executeInternal("INSERT INTO " + qualified + " (pk, ck, v) VALUES (0, 0, ?) USING TIMESTAMP ?", node, (long) node);
        for (int ck = -3; ck < 0; ck++)
            cluster.get(1).executeInternal("DELETE FROM " + qualified + " USING TIMESTAMP 10 WHERE pk = 0 AND ck = ?", ck);
        if (flushed)
            cluster.forEach(node -> node.flush(KEYSPACE));

        Map<Integer, IMessage> responses = new ConcurrentHashMap<>();
        Map<Integer, IMessage> repairResponses = new ConcurrentHashMap<>();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch repairReady = new CountDownLatch(2);
        cluster.filters().outbound().verbs(Verb.READ_RSP.id).to(1).messagesMatching((from, to, message) -> {
            if (responses.putIfAbsent(from, message) == null)
            {
                ready.countDown();
                return true;
            }
            if (lateLocal && repairResponses.putIfAbsent(from, message) == null)
            {
                repairReady.countDown();
                return true;
            }
            return false;
        }).drop();
        try
        {
            if (lateLocal)
                cluster.get(1).runOnInstance(ReadStageGate::start);
            long reads = localReads(table);
            long repairs = repairs();
            long checkpoints = checkpoints(table);
            ResultSetFuture result = session.executeAsync(statement("SELECT ck, v FROM " + qualified + " WHERE pk = 0",
                                                                    ConsistencyLevel.QUORUM, Integer.MAX_VALUE));
            assertTrue("Expected both remote responses", ready.await(10, TimeUnit.SECONDS));
            if (lateLocal)
            {
                assertEquals("Local read escaped the gate", reads, localReads(table));
                assertEquals(checkpoints, checkpoints(table));
            }
            else
            {
                await().atMost(10, TimeUnit.SECONDS).until(() -> localReads(table) > reads);
                assertTrue("Expected the local replica to checkpoint before remote quorum", checkpoints(table) > checkpoints);
            }
            for (int node : reverseRemote ? new int[]{ 3, 2 } : new int[]{ 2, 3 })
            {
                IMessage response = responses.get(node);
                cluster.get(1).runOnInstance(() -> MessagingService.instance().inboundSink.accept(Instance.deserializeMessage(response)));
            }
            if (lateLocal)
            {
                assertTrue("Expected full-data repair responses", repairReady.await(10, TimeUnit.SECONDS));
                assertTrue("Expected repair to start before local reads", repairs() > repairs);
                assertEquals(reads, localReads(table));
                assertEquals(checkpoints, checkpoints(table));
                if (localOrder == LocalResponseOrder.DURING_REPAIR)
                {
                    cluster.get(1).runOnInstance(ReadStageGate::releaseAndDrain);
                    assertTrue("Expected late initial and repair reads to checkpoint", checkpoints(table) >= checkpoints + 2);
                    assertFalse("Rejected local responses must not finish the pending remote repair", result.isDone());
                }
                for (int node : reverseRemote ? new int[]{ 3, 2 } : new int[]{ 2, 3 })
                {
                    IMessage response = repairResponses.get(node);
                    cluster.get(1).runOnInstance(() -> MessagingService.instance().inboundSink.accept(Instance.deserializeMessage(response)));
                }
                ResultSet rows = result.getUninterruptibly(10, TimeUnit.SECONDS);
                assertTrue("Unpaged remote quorum must return a complete result", rows.isFullyFetched());
                assertNull(rows.getExecutionInfo().getPagingState());
                assertEquals(1, rows.getAvailableWithoutFetching());
                com.datastax.driver.core.Row row = rows.one();
                assertEquals(0, row.getInt("ck"));
                assertEquals(3, row.getInt("v"));
                assertEquals(0, rows.getAvailableWithoutFetching());
                if (localOrder == LocalResponseOrder.AFTER_RESULT)
                {
                    assertEquals("Local reads ran before the remote result completed", reads, localReads(table));
                    assertEquals(checkpoints, checkpoints(table));
                    cluster.get(1).runOnInstance(ReadStageGate::releaseAndDrain);
                    assertTrue("Expected late initial and repair reads to checkpoint", checkpoints(table) >= checkpoints + 2);
                }
            }
            else
            {
                assertNativeFailure(() -> result.getUninterruptibly(10, TimeUnit.SECONDS), ConsistencyLevel.QUORUM);
            }
            assertTrue("Expected remote digest mismatch to start repair", repairs() > repairs);
        }
        finally
        {
            try
            {
                if (lateLocal)
                    cluster.get(1).runOnInstance(ReadStageGate::releaseAndDrain);
            }
            finally
            {
                cluster.filters().reset();
            }
        }
    }

    private static final class ReadStageGate
    {
        private static CountDownLatch release;
        private static List<Future<?>> blockers;

        private static void start()
        {
            int threads = Stage.READ.getMaximumPoolSize();
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch gate = new CountDownLatch(1);
            release = gate;
            blockers = new ArrayList<>();
            for (int i = 0; i < threads; i++)
                blockers.add(Stage.READ.submit(() -> {
                    ready.countDown();
                    assertTrue("Local read gate was not released", gate.await(30, TimeUnit.SECONDS));
                    return null;
                }));
            assertTrue("Read workers did not enter the gate", awaitUninterruptibly(ready, 10, TimeUnit.SECONDS));
        }

        private static void releaseAndDrain()
        {
            if (release == null)
                return;
            release.countDown();
            try
            {
                for (Future<?> blocker : blockers)
                    blocker.get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            catch (ExecutionException | TimeoutException e)
            {
                throw new AssertionError(e);
            }
            await().atMost(10, TimeUnit.SECONDS).until(() -> Stage.READ.executor().getPendingTaskCount() == 0
                                                           && Stage.READ.executor().getActiveTaskCount() == 0);
            release = null;
            blockers = null;
        }
    }

    private static SimpleStatement statement(String cql, ConsistencyLevel consistency, int fetchSize)
    {
        SimpleStatement statement = new SimpleStatement(cql);
        statement.setConsistencyLevel(consistency);
        statement.setFetchSize(fetchSize);
        statement.setReadTimeoutMillis(40000);
        return statement;
    }

    private static void assertNativeFailure(org.assertj.core.api.ThrowableAssert.ThrowingCallable read, ConsistencyLevel consistency)
    {
        assertThatThrownBy(read).isInstanceOf(ReadFailureException.class).satisfies(error -> {
            ReadFailureException failure = (ReadFailureException) error;
            assertEquals(consistency, failure.getConsistencyLevel());
            assertFalse(failure.getFailuresMap().isEmpty());
            assertTrue(failure.getFailuresMap().values().stream().allMatch(reason -> reason == RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        });
    }

    private static long localReads(String table)
    {
        return cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.readLatency.latency.getCount());
    }

    private static long repairs()
    {
        return cluster.get(1).callOnInstance(() -> ReadRepairMetrics.reconcileRead.getCount() + ReadRepairMetrics.repairedBlocking.getCount());
    }

    private static long checkpoints(String table)
    {
        return cluster.stream().mapToLong(node -> node.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstonePages.getCount())).sum();
    }
}
