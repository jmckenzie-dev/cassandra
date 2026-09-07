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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PagingAcrossTombstonesTransientTest extends TestBaseImpl
{
    @Test(timeout = 120000)
    public void speculativeTransientCheckpointReturnsTypedFailureAtOne() throws Exception
    {
        String keyspace = "transient_checkpoint_one";
        try (Cluster cluster = Cluster.build(3).withSubnet(18)
                                      .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                  .set("transient_replication_enabled", true)
                                                                  .set("hinted_handoff_enabled", false)
                                                                  .set("read_request_timeout", "30s")
                                                                  .set("tombstone_paging_threshold", 2)
                                                                  .set("tombstone_failure_threshold", 2))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace +
                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2/1'}");
            cluster.schemaChange("CREATE TABLE " + keyspace +
                                 ".checkpoint (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'ALWAYS'");
            int fullNode = 0;
            int transientNode = 0;
            int coordinator = 0;
            for (int node = 1; node <= 3; node++)
            {
                int role = cluster.get(node).callOnInstance(() -> {
                    org.apache.cassandra.locator.Replica replica = StorageService.instance
                                                                                .getNaturalReplicasForToken(keyspace, ByteBufferUtil.bytes(0))
                                                                                .selfIfPresent();
                    return replica == null ? 0 : replica.isFull() ? 1 : 2;
                });
                if (role == 0)
                    coordinator = node;
                else if (role == 1)
                    fullNode = node;
                else
                    transientNode = node;
                for (int ck = 0; ck < 5; ck++)
                {
                    cluster.get(node).executeInternal("INSERT INTO " + keyspace +
                                                      ".checkpoint (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                    if (role == 1 && ck < 3)
                        cluster.get(node).executeInternal("DELETE FROM " + keyspace +
                                                          ".checkpoint USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
                }
            }
            assertTrue(fullNode > 0 && transientNode > 0 && coordinator > 0);
            String host = cluster.get(coordinator).broadcastAddress().getHostString();
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
                        cluster.forEach(node -> node.flush(keyspace));
                    Map<Integer, IMessage> responses = new ConcurrentHashMap<>();
                    CountDownLatch ready = new CountDownLatch(2);
                    cluster.filters().outbound().verbs(Verb.READ_RSP.id).to(coordinator).messagesMatching((from, to, message) -> {
                        if (responses.putIfAbsent(from, message) == null)
                            ready.countDown();
                        return true;
                    }).drop();
                    try
                    {
                        SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + keyspace + ".checkpoint WHERE pk = 0 LIMIT 3");
                        statement.setConsistencyLevel(ConsistencyLevel.ONE);
                        statement.setFetchSize(1);
                        statement.setReadTimeoutMillis(40000);
                        ResultSetFuture result = session.executeAsync(statement);
                        assertTrue("Expected full and transient responses", ready.await(10, TimeUnit.SECONDS));
                        // Deliver the real transient response before the full response can satisfy ONE.
                        IMessage transientResponse = responses.get(transientNode);
                        IMessage fullResponse = responses.get(fullNode);
                        cluster.get(coordinator).runOnInstance(() -> MessagingService.instance().inboundSink.accept(Instance.deserializeMessage(transientResponse)));
                        cluster.get(coordinator).runOnInstance(() -> MessagingService.instance().inboundSink.accept(Instance.deserializeMessage(fullResponse)));
                        assertThatThrownBy(() -> result.getUninterruptibly(10, TimeUnit.SECONDS))
                        .isInstanceOf(com.datastax.driver.core.exceptions.ReadFailureException.class)
                        .satisfies(error -> {
                            com.datastax.driver.core.exceptions.ReadFailureException failure = (com.datastax.driver.core.exceptions.ReadFailureException) error;
                            assertEquals(ConsistencyLevel.ONE, failure.getConsistencyLevel());
                            assertTrue(failure.getFailuresMap().containsValue(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
                        });
                    }
                    finally
                    {
                        cluster.filters().reset();
                    }
                }
            }
        }
    }

    @Test
    public void transientDataReconcilesWithFullReplicaCheckpoints() throws IOException
    {
        String keyspace = "transient_paging";
        try (Cluster cluster = Cluster.build(3).withSubnet(16)
                                      .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                  .set("transient_replication_enabled", true)
                                                                  .set("hinted_handoff_enabled", false)
                                                                  .set("tombstone_paging_threshold", 2)
                                                                  .set("tombstone_failure_threshold", 2))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace +
                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'}");
            cluster.schemaChange("CREATE TABLE " + keyspace +
                                 ".checkpoint (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            int transientNode = 0;
            int fullReplicas = 0;
            for (int node = 1; node <= 3; node++)
            {
                boolean isTransient = cluster.get(node).callOnInstance(() -> StorageService.instance
                                                                                         .getNaturalReplicasForToken(keyspace, ByteBufferUtil.bytes(0))
                                                                                         .selfIfPresent().isTransient());
                if (isTransient)
                    transientNode = node;
                else
                    fullReplicas++;
                for (int ck = 0; ck < 5; ck++)
                {
                    cluster.get(node).executeInternal("INSERT INTO " + keyspace +
                                                      ".checkpoint (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                    if (!isTransient && ck < 3)
                        cluster.get(node).executeInternal("DELETE FROM " + keyspace +
                                                          ".checkpoint USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
                }
            }
            assertEquals(2, fullReplicas);
            assertTrue(transientNode > 0);
            cluster.get(transientNode).executeInternal("UPDATE " + keyspace +
                                                      ".checkpoint USING TIMESTAMP 3 SET v = 99 WHERE pk = 0 AND ck = 3");
            AtomicInteger responses = new AtomicInteger();
            cluster.filters().outbound().verbs(Verb.READ_RSP.id).from(transientNode).messagesMatching((from, to, message) -> {
                responses.incrementAndGet();
                return false;
            }).drop();
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    cluster.forEach(node -> node.flush(keyspace));
                for (int coordinator = 1; coordinator <= 3; coordinator++)
                {
                    String host = cluster.get(coordinator).broadcastAddress().getHostString();
                    try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                                 .addContactPoint(host)
                                                                                                 .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                                                                             Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                                 .build();
                         Session session = client.connect())
                    {
                        SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + keyspace + ".checkpoint WHERE pk = 0 LIMIT 3");
                        statement.setConsistencyLevel(ConsistencyLevel.ALL);
                        statement.setFetchSize(1);
                        Object[][] rows = session.execute(statement).all().stream()
                                                 .map(result -> row(result.getInt("ck"), result.getInt("v")))
                                                 .toArray(Object[][]::new);
                        assertRows(rows, row(3, 99), row(4, 4));
                    }
                }
            }
            assertTrue("Expected a response from the transient replica", responses.get() > 0);
            long checkpoints = cluster.stream().mapToLong(node -> node.callOnInstance(() ->
                Keyspace.open(keyspace).getColumnFamilyStore("checkpoint").metric.tombstoneFailures.getCount())).sum();
            assertTrue("Expected full-replica tombstone checkpoints", checkpoints > 0);
        }
    }
}
