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
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.util.TombstonePagingTestUtil.readPages;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

public class ShortReadProtectionTombstonePagingTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(3)
                              .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                          .set("hinted_handoff_enabled", false)
                                                          .set("dynamic_snitch", false)
                                                          .set("tombstone_failure_threshold", 2)
                                                          .set("tombstone_paging_threshold", 2))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void checkpointWithoutClusteringReturnsTypedFailureAtOne()
    {
        for (String repair : new String[]{ "NONE", "BLOCKING" })
        {
            String table = "srp_no_clustering_" + repair.toLowerCase(java.util.Locale.ROOT);
            cluster.schemaChange("CREATE TABLE " + qualified(table) +
                                 " (pk int PRIMARY KEY, m map<int, int>) WITH read_repair = '" + repair +
                                 "' AND speculative_retry = 'NONE'");
            cluster.forEach(node -> {
                node.executeInternal("INSERT INTO " + qualified(table) + " (pk, m) VALUES (0, {9: 9}) USING TIMESTAMP 1");
                node.executeInternal("DELETE m[0], m[1], m[2] FROM " + qualified(table) + " USING TIMESTAMP 2 WHERE pk = 0");
            });
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    flush();
                for (int coordinator : new int[]{ 1, 3 })
                {
                    String host = cluster.get(coordinator).broadcastAddress().getHostString();
                    try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                                   .addContactPoint(host)
                                                                                                   .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                                                                   .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                                                                               Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                                   .build();
                         Session session = client.connect())
                    {
                        for (com.datastax.driver.core.ConsistencyLevel consistency : new com.datastax.driver.core.ConsistencyLevel[]{ com.datastax.driver.core.ConsistencyLevel.ONE,
                                                                                                                                    com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE })
                        {
                            SimpleStatement statement = new SimpleStatement("SELECT m FROM " + qualified(table) + " WHERE pk = 0 LIMIT 2");
                            statement.setConsistencyLevel(consistency);
                            statement.setFetchSize(1);
                            statement.setReadTimeoutMillis(15000);
                            assertThatThrownBy(() -> session.execute(statement))
                            .as("repair=%s, coordinator=%s, flushed=%s, consistency=%s", repair, coordinator, flushed, consistency)
                            .isInstanceOf(com.datastax.driver.core.exceptions.ReadFailureException.class)
                            .satisfies(error -> {
                                com.datastax.driver.core.exceptions.ReadFailureException failure = (com.datastax.driver.core.exceptions.ReadFailureException) error;
                                org.junit.Assert.assertEquals(consistency, failure.getConsistencyLevel());
                                assertTrue(failure.getFailuresMap().containsValue(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
                            });
                        }
                    }
                }
            }
        }
    }

    @Test
    public void staticTombstoneCheckpointFailsBeforeReturningRows()
    {
        String table = "srp_static_checkpoint";
        cluster.schemaChange("CREATE TABLE " + qualified(table) +
                             " (pk int, ck int, s1 int static, s2 int static, s3 int static, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
        {
            for (int pk = 0; pk < 2; pk++)
            {
                for (int ck = 0; ck < 2; ck++)
                    cluster.get(node).executeInternal("INSERT INTO " + qualified(table) +
                                                      " (pk, ck, s1, s2, s3, v) VALUES (?, ?, 1, 2, 3, ?) USING TIMESTAMP 1", pk, ck, ck);
                if (pk == 0 || node == 3)
                    cluster.get(node).executeInternal("DELETE s1, s2, s3 FROM " + qualified(table) + " USING TIMESTAMP 2 WHERE pk = ?", pk);
            }
        }

        try
        {
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    flush();
                for (int coordinator : new int[]{ 1, 3 })
                {
                    String host = cluster.get(coordinator).broadcastAddress().getHostString();
                    try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                                   .addContactPoint(host)
                                                                                                   .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                                                                   .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                                                                               Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                                   .build();
                         Session session = client.connect())
                    {
                        for (int pk = 0; pk < 2; pk++)
                        {
                            String cql = "SELECT ck, v, s1, s2, s3 FROM " + qualified(table) + " WHERE pk = " + pk + " LIMIT 3";
                            cluster.forEach(node -> node.runOnInstance(() -> {
                                DatabaseDescriptor.setTombstonePagingThreshold(-1);
                                DatabaseDescriptor.setTombstoneFailureThreshold(100);
                            }));
                            assertRows(readPages(session, cql, 1, 2).stream()
                                                                   .map(result -> row(result.getInt("ck"), result.getInt("v"),
                                                                                      result.getObject("s1"), result.getObject("s2"), result.getObject("s3")))
                                                                   .toArray(Object[][]::new),
                                       row(0, 0, null, null, null), row(1, 1, null, null, null));
                            cluster.forEach(node -> node.runOnInstance(() -> {
                                DatabaseDescriptor.setTombstonePagingThreshold(2);
                                DatabaseDescriptor.setTombstoneFailureThreshold(2);
                            }));
                            long before = cluster.stream().mapToLong(node -> node.callOnInstance(() ->
                                Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstonePages.getCount())).sum();
                            assertThatThrownBy(() -> readPages(session, cql, 1, 2))
                            .as("coordinator=%s, pk=%s, flushed=%s", coordinator, pk, flushed)
                            .isInstanceOf(com.datastax.driver.core.exceptions.ReadFailureException.class)
                            .satisfies(error -> assertTrue(((com.datastax.driver.core.exceptions.ReadFailureException) error)
                                                          .getFailuresMap().containsValue(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code)));
                            long after = cluster.stream().mapToLong(node -> node.callOnInstance(() ->
                                Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstonePages.getCount())).sum();
                            assertTrue("Expected a static-row tombstone cutoff", after > before);
                        }
                    }
                }
            }
        }
        finally
        {
            cluster.forEach(node -> node.runOnInstance(() -> {
                DatabaseDescriptor.setTombstonePagingThreshold(2);
                DatabaseDescriptor.setTombstoneFailureThreshold(2);
            }));
        }
    }

    @Test
    public void remoteZeroLiveRefillContinuesAfterTombstoneCheckpoint()
    {
        String table = "srp_zero_live";
        setupZeroLiveRefill(table);
        assertUnpaged(table, false, row(1, 1), row(5, 5));
    }

    @Test(timeout = 60000)
    public void markerOnlyRefillTerminates()
    {
        String table = "srp_marker_only";
        createTable(table);
        insert(table, 1, 0, 10);
        insert(table, 1, 1, 10);
        delete(table, 2, 0, 30);
        delete(table, 2, 1, 30);
        for (int ck = 2; ck < 26; ck += 2)
            cluster.get(1).executeInternal("DELETE FROM " + qualified(table) +
                                          " USING TIMESTAMP 20 WHERE pk = 0 AND ck >= ? AND ck < ?", ck, ck + 1);
        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                flush();
            long before = shortReadProtectionRequests(table);
            assertRows(cluster.coordinator(3).execute(query(table, false), ALL));
            long requests = shortReadProtectionRequests(table) - before;
            assertTrue("Expected a refill", requests > 0);
            assertTrue("Refill did not terminate within its request budget: " + requests, requests <= 6);
        }
    }

    @Test
    public void remoteZeroLiveRefillContinuesWithProtocolPaging()
    {
        String table = "srp_zero_live_paged";
        setupZeroLiveRefill(table);
        assertPaged(table, row(1, 1), row(5, 5));
    }

    @Test
    public void fullDataShortReadDoesNotRewindOuterPager()
    {
        String table = "srp_full_data_paged";
        createTable(table);
        delete(table, 3, 0, 20);
        delete(table, 3, 1, 20);
        delete(table, 3, 2, 20);
        for (int node = 1; node <= 3; node++)
        {
            insert(table, node, 3, 10);
            insert(table, node, 4, 10);
            insert(table, node, 5, 10);
            insert(table, node, 6, 10);
        }
        flush();

        String host = cluster.get(3).broadcastAddress().getHostString();
        InetSocketAddress address = new InetSocketAddress(host, 9042);
        long requestsBefore = shortReadProtectionRequests(table);
        try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                       .addContactPoint(host)
                                                                                       .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(address)))
                                                                                       .build();
             Session session = client.connect())
        {
            SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + qualified(table) +
                                                            " WHERE pk = 0 ORDER BY ck ASC LIMIT 4");
            statement.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
            statement.setFetchSize(2);
            List<com.datastax.driver.core.Row> rows = session.execute(statement).all();
            Object[][] actual = new Object[rows.size()][];
            for (int i = 0; i < rows.size(); i++)
                actual[i] = row(rows.get(i).getInt("ck"), rows.get(i).getInt("v"));
            assertRows(actual, row(3, 3), row(4, 4), row(5, 5), row(6, 6));
        }
        long requestsAfter = shortReadProtectionRequests(table);

        assertTrue("Expected full-data reconciliation to issue a refill request", requestsAfter > requestsBefore);
    }

    @Test
    public void directReadWithoutPagerFailsSafelyAtTombstoneCheckpoint()
    {
        String table = "srp_direct_unlimited";
        createTable(table);
        delete(table, 3, 0, 20);
        delete(table, 3, 1, 20);
        delete(table, 3, 2, 20);
        insert(table, 3, 3, 10);
        flush();

        cluster.get(3).runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
            SinglePartitionReadCommand unlimited = SinglePartitionReadCommand.fullPartitionRead(metadata,
                                                                                                 FBUtilities.nowInSeconds(),
                                                                                                 metadata.partitioner.decorateKey(Int32Type.instance.decompose(0)));
            SinglePartitionReadCommand limited = unlimited.withUpdatedLimit(DataLimits.cqlLimits(1));
            CoordinatorWarnings.init();
            try
            {
                for (SinglePartitionReadCommand command : new SinglePartitionReadCommand[]{ unlimited, limited })
                {
                    try
                    {
                        int rowCount = 0;
                        try (PartitionIterator partitions = StorageProxy.fetchRows(Collections.singletonList(command),
                                                                                    org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                                    ReadCoordinator.DEFAULT,
                                                                                    Dispatcher.RequestTime.forImmediateExecution()))
                        {
                            while (partitions.hasNext())
                            {
                                try (RowIterator rows = partitions.next())
                                {
                                    while (rows.hasNext())
                                    {
                                        rows.next();
                                        rowCount++;
                                    }
                                }
                            }
                        }
                        throw new AssertionError("Expected direct " + (command.limits().isUnlimited() ? "unlimited" : "finite") +
                                                 " read to fail safely, but it returned " + rowCount + " rows");
                    }
                    catch (ReadFailureException expected)
                    {
                    }
                    catch (Exception e)
                    {
                        throw new AssertionError("Expected ReadFailureException", e);
                    }
                }
            }
            finally
            {
                CoordinatorWarnings.done();
            }
        });
    }

    @Test
    public void shortenedNonzeroRefillContinuesAfterTombstoneCheckpoint()
    {
        String table = "srp_short_refill";
        createTable(table);
        insert(table, 1, 0, 10);
        insert(table, 1, 1, 10);
        insert(table, 1, 2, 10);
        delete(table, 1, 3, 20);
        delete(table, 1, 4, 20);
        delete(table, 1, 5, 20);
        insert(table, 1, 6, 10);
        delete(table, 2, 0, 30);
        delete(table, 2, 1, 30);
        flush();

        assertUnpaged(table, false, row(2, 2), row(6, 6));
    }

    @Test
    public void checkpointTombstoneStillShadowsStaleReplicaRow()
    {
        String table = "srp_stale_row";
        createTable(table);
        insert(table, 1, 0, 10);
        insert(table, 1, 1, 10);
        delete(table, 1, 2, 20);
        delete(table, 1, 3, 20);
        delete(table, 1, 4, 20);
        delete(table, 1, 5, 30);
        insert(table, 1, 6, 10);
        delete(table, 2, 0, 30);
        insert(table, 2, 5, 10);
        flush();

        assertUnpaged(table, false, row(1, 1), row(6, 6));
    }

    @Test
    public void repeatedTombstoneCheckpointsContinueUntilLimitIsFilled()
    {
        String table = "srp_repeated_checkpoints";
        createTable(table);
        insert(table, 1, 0, 10);
        insert(table, 1, 1, 10);
        delete(table, 1, 2, 20);
        delete(table, 1, 3, 20);
        delete(table, 1, 4, 20);
        insert(table, 1, 5, 10);
        delete(table, 1, 6, 20);
        delete(table, 1, 7, 20);
        delete(table, 1, 8, 20);
        insert(table, 1, 9, 10);
        delete(table, 2, 0, 30);
        delete(table, 2, 1, 30);
        flush();

        assertUnpaged(table, false, row(5, 5), row(9, 9));
    }

    @Test
    public void descendingRefillContinuesAfterTombstoneCheckpoint()
    {
        String table = "srp_descending";
        createTable(table);
        insert(table, 1, 5, 10);
        insert(table, 1, 4, 10);
        delete(table, 1, 3, 20);
        delete(table, 1, 2, 20);
        delete(table, 1, 1, 20);
        insert(table, 1, 0, 10);
        delete(table, 2, 5, 30);
        flush();

        assertUnpaged(table, true, row(4, 4), row(0, 0));
    }

    private static void setupZeroLiveRefill(String table)
    {
        createTable(table);
        insert(table, 1, 0, 10);
        insert(table, 1, 1, 10);
        delete(table, 1, 2, 20);
        delete(table, 1, 3, 20);
        delete(table, 1, 4, 20);
        insert(table, 1, 5, 10);
        delete(table, 2, 0, 30);
        flush();
    }

    private static void createTable(String table)
    {
        cluster.schemaChange("CREATE TABLE " + qualified(table) +
                             " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair='NONE' AND speculative_retry='NONE'");
    }

    private static void insert(String table, int node, int ck, long timestamp)
    {
        cluster.get(node).executeInternal("INSERT INTO " + qualified(table) +
                                          " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP ?",
                                          ck, ck, timestamp);
    }

    private static void delete(String table, int node, int ck, long timestamp)
    {
        cluster.get(node).executeInternal("DELETE FROM " + qualified(table) +
                                          " USING TIMESTAMP ? WHERE pk = 0 AND ck = ?",
                                          timestamp, ck);
    }

    private static void flush()
    {
        cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
    }

    private static void assertUnpaged(String table, boolean reversed, Object[]... expectedRows)
    {
        long requestsBefore = shortReadProtectionRequests(table);
        Object[][] rows = cluster.coordinator(3).execute(query(table, reversed), ALL);
        long requestsAfter = shortReadProtectionRequests(table);

        assertTrue("Expected short read protection to issue a refill request", requestsAfter > requestsBefore);
        assertRows(rows, expectedRows);
    }

    private static void assertPaged(String table, Object[]... expectedRows)
    {
        long requestsBefore = shortReadProtectionRequests(table);
        Iterator<Object[]> rows = cluster.coordinator(3).executeWithPaging(query(table, false), ALL, 2);
        assertRows(rows, expectedRows);
        long requestsAfter = shortReadProtectionRequests(table);

        assertTrue("Expected short read protection to issue a refill request", requestsAfter > requestsBefore);
    }

    private static String query(String table, boolean reversed)
    {
        return "SELECT ck, v FROM " + qualified(table) + " WHERE pk = 0 ORDER BY ck " +
               (reversed ? "DESC" : "ASC") + " LIMIT 2";
    }

    private static String qualified(String table)
    {
        return KEYSPACE + '.' + table;
    }

    private static long shortReadProtectionRequests(String table)
    {
        return cluster.get(3).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(table)
                                                           .metric.shortReadProtectionRequests.getCount());
    }
}
