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
package org.apache.cassandra.distributed.upgrade.storage.paging;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.upgrade.UpgradeTestBase;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.util.TombstonePagingTestUtil.readPages;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

public class MixedVersionPagingAcrossTombstonesTest extends UpgradeTestBase
{
    @Test
    public void upgradedCoordinatorReconcilesOldFullResponses() throws Throwable
    {
        upgrade(10000).runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int coordinator = 1; coordinator <= upgraded; coordinator++)
                assertRows(read(cluster, coordinator), row(3, 3), row(4, 4));
        }).run();
    }

    @Test
    public void oldCoordinatorRejectsNewCheckpoint() throws Throwable
    {
        upgrade(10000).runAfterNodeUpgrade((cluster, upgraded) ->
            assertThatThrownBy(() -> read(cluster, 3)).isInstanceOf(ReadFailureException.class)).run();
    }

    @Test
    public void oldReplicaFailureReachesUpgradedCoordinator() throws Throwable
    {
        upgrade(2).runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int coordinator = 1; coordinator <= upgraded; coordinator++)
            {
                int node = coordinator;
                assertThatThrownBy(() -> read(cluster, node)).isInstanceOf(ReadFailureException.class);
            }
        }).run();
    }

    @Test
    public void featureCombinationsWithOneAndTwoUpgradedNodes() throws Throwable
    {
        upgradeCase(10000).setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE +
                                 ".combinations (pk int, g int, ck int, s int static, PRIMARY KEY (pk, g, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE +
                                 ".expired (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                 " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
            for (int node = 1; node <= 3; node++)
            {
                for (int pk = 0; pk < 3; pk++)
                {
                    cluster.get(node).executeInternal("UPDATE " + KEYSPACE +
                                                     ".combinations USING TIMESTAMP 1 SET s = ? WHERE pk = ?", pk + 10, pk);
                    for (int g = 0; g < 3; g++)
                        for (int ck = 0; ck < 8; ck++)
                        {
                            cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE +
                                                             ".combinations (pk, g, ck) VALUES (?, ?, ?) USING TIMESTAMP 1", pk, g, ck);
                            if (pk == 2 || g == 0 || (ck > 0 && ck < 6))
                                cluster.get(node).executeInternal("DELETE FROM " + KEYSPACE +
                                                                 ".combinations USING TIMESTAMP 2 WHERE pk = ? AND g = ? AND ck = ?", pk, g, ck);
                        }
                }
                for (int ck = 0; ck < 5; ck++)
                    cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE +
                                                     ".expired (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1" +
                                                     (ck < 3 ? " AND TTL 1" : ""), ck, ck);
            }
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int node = 1; node <= 3; node++)
                    assertRows(cluster.get(node).executeInternal("SELECT ck, v FROM " + KEYSPACE + ".expired WHERE pk = 0"),
                               row(3, 3), row(4, 4));
            });
        }).runAfterNodeUpgrade((cluster, upgraded) -> {
            for (boolean flushed : new boolean[]{ false, true })
            {
                if (flushed)
                    cluster.forEach(node -> node.flush(KEYSPACE));
                for (int coordinator = 1; coordinator <= 3; coordinator++)
                    try (com.datastax.driver.core.Cluster client = client(cluster, coordinator);
                         Session session = client.connect())
                    {
                        String context = "upgraded=" + upgraded + ", coordinator=" + coordinator + ", flushed=" + flushed;
                        try
                        {
                            assertCombination(cluster, upgraded, coordinator, session, "combinations",
                                              "SELECT g, count(*) FROM " + KEYSPACE + ".combinations WHERE pk = 0 GROUP BY pk, g",
                                              new Object[][]{ row(1, 3L), row(2, 3L) });
                            List<Object[]> regular = new ArrayList<>();
                            for (int g = 1; g < 3; g++)
                                for (int ck : new int[]{ 0, 6, 7 })
                                    regular.add(row(g, ck, 10));
                            assertCombination(cluster, upgraded, coordinator, session, "combinations",
                                              "SELECT g, ck, s FROM " + KEYSPACE + ".combinations WHERE pk = 0",
                                              regular.toArray(Object[][]::new));
                            List<Object[]> partitions = new ArrayList<>();
                            for (int pk = 0; pk < 2; pk++)
                                for (int g = 1; g < 3; g++)
                                    for (int ck : new int[]{ 0, 6, 7 })
                                        partitions.add(row(pk, g, ck, pk + 10));
                            partitions.add(row(2, null, null, 12));
                            assertCombination(cluster, upgraded, coordinator, session, "combinations",
                                              "SELECT pk, g, ck, s FROM " + KEYSPACE + ".combinations WHERE pk IN (0, 1, 2)",
                                              partitions.toArray(Object[][]::new));
                            String grouped = "SELECT pk, g, s, count(ck) FROM " + KEYSPACE +
                                             ".combinations WHERE pk IN (0, 1, 2) GROUP BY pk, g";
                            assertCombination(cluster, upgraded, coordinator, session, "combinations", grouped,
                                              new Object[][]{ row(0, 1, 10, 3L), row(0, 2, 10, 3L),
                                                              row(1, 1, 11, 3L), row(1, 2, 11, 3L), row(2, null, 12, 0L) });
                            assertCombination(cluster, upgraded, coordinator, session, "combinations", grouped + " PER PARTITION LIMIT 1",
                                              new Object[][]{ row(0, 1, 10, 3L), row(1, 1, 11, 3L), row(2, null, 12, 0L) });
                            assertCombination(cluster, upgraded, coordinator, session, "expired",
                                              "SELECT ck, v FROM " + KEYSPACE + ".expired WHERE pk = 0",
                                              new Object[][]{ row(3, 3), row(4, 4) });
                        }
                        catch (AssertionError | RuntimeException failure)
                        {
                            throw new AssertionError(context, failure);
                        }
                    }
            }
        }).run();
    }

    private static void assertCombination(UpgradeableCluster cluster, int upgraded, int coordinator, Session session,
                                          String table, String base, Object[][] expected)
    {
        configurePaging(cluster, upgraded, false);
        Object[][] ordered;
        try
        {
            ordered = rows(session, base, 2);
            Comparator<Object[]> comparator = Comparator.comparing(Arrays::deepToString);
            assertRows(Arrays.stream(ordered).sorted(comparator).toArray(Object[][]::new),
                       Arrays.stream(expected).sorted(comparator).toArray(Object[][]::new));
        }
        finally
        {
            configurePaging(cluster, upgraded, true);
        }
        for (int limit : new int[]{ 0, 3, expected.length + 1 })
            for (int fetchSize : new int[]{ 1, 2 })
            {
                String cql = base + (limit == 0 ? "" : " LIMIT " + limit);
                Object[][] limited = Arrays.copyOf(ordered, limit == 0 ? ordered.length : Math.min(limit, ordered.length));
                configurePaging(cluster, upgraded, false);
                try
                {
                    assertRows(rows(session, cql, fetchSize), limited);
                }
                finally
                {
                    configurePaging(cluster, upgraded, true);
                }
                long before = thresholdCrossings(cluster, upgraded, table);
                try
                {
                    if (coordinator <= upgraded)
                        assertRows(rows(session, cql, fetchSize), limited);
                    else
                        assertThatThrownBy(() -> rows(session, cql, fetchSize)).isInstanceOf(ReadFailureException.class);
                    assertTrue(cql + ": expected tombstone paging", thresholdCrossings(cluster, upgraded, table) > before);
                }
                catch (AssertionError | RuntimeException failure)
                {
                    throw new AssertionError(cql + ", fetchSize=" + fetchSize, failure);
                }
            }
    }

    private TestCase upgradeCase(int failureThreshold)
    {
        return new TestCase().nodes(3)
                             .singleUpgradeToCurrentFrom(v50)
                             .nodesToUpgradeOrdered(1, 2)
                             .withBuilder(builder -> builder.withSubnet(14))
                             .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                         .set("hinted_handoff_enabled", false)
                                                         .set("tombstone_failure_threshold", failureThreshold))
                             .runBeforeNodeRestart((cluster, node) -> {
                                 cluster.get(node).config().set("tombstone_paging_threshold", 2);
                                 cluster.get(node).config().set("tombstone_failure_threshold", 2);
                             });
    }

    private TestCase upgrade(int failureThreshold)
    {
        return upgradeCase(failureThreshold)
                             .setup(cluster -> {
                                 cluster.schemaChange("CREATE TABLE " + KEYSPACE +
                                                      ".checkpoint (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                                      " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
                                 for (int node = 1; node <= 3; node++)
                                     for (int ck = 0; ck < 5; ck++)
                                     {
                                         cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE +
                                                                          ".checkpoint (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
                                         if (ck < 3)
                                             cluster.get(node).executeInternal("DELETE FROM " + KEYSPACE +
                                                                              ".checkpoint USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
                                     }
                             });
    }

    private static Object[][] read(UpgradeableCluster cluster, int coordinator)
    {
        try (com.datastax.driver.core.Cluster client = client(cluster, coordinator);
             Session session = client.connect())
        {
            return rows(session, "SELECT ck, v FROM " + KEYSPACE + ".checkpoint WHERE pk = 0 LIMIT 3", 1);
        }
    }

    static com.datastax.driver.core.Cluster client(UpgradeableCluster cluster, int coordinator)
    {
        String host = cluster.get(coordinator).broadcastAddress().getHostString();
        return com.datastax.driver.core.Cluster.builder()
                                              .addContactPoint(host)
                                              .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                              .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                          Collections.singletonList(new InetSocketAddress(host, 9042))))
                                              .build();
    }

    static Object[][] rows(Session session, String cql, int fetchSize)
    {
        return readPages(session, cql, fetchSize, 72).stream().map(result -> {
            Object[] values = new Object[result.getColumnDefinitions().size()];
            for (int i = 0; i < values.length; i++)
                values[i] = result.getObject(i);
            return values;
        }).toArray(Object[][]::new);
    }

    static void configurePaging(UpgradeableCluster cluster, int upgraded, boolean enabled)
    {
        for (int node = 1; node <= upgraded; node++)
            ((IInvokableInstance) cluster.get(node)).runOnInstance(() -> {
                DatabaseDescriptor.setTombstonePagingThreshold(enabled ? 2 : -1);
                DatabaseDescriptor.setTombstoneFailureThreshold(enabled ? 2 : 10000);
            });
    }

    static long thresholdCrossings(UpgradeableCluster cluster, int upgraded, String table)
    {
        long count = 0;
        for (int node = 1; node <= upgraded; node++)
            count += ((IInvokableInstance) cluster.get(node)).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstonePages.getCount());
        return count;
    }
}
