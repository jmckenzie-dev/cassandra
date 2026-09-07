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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadFailureException;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.upgrade.UpgradeTestBase;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.client;
import static org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.configurePaging;
import static org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.rows;
import static org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.thresholdCrossings;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/** Checks generated replica divergence against mutation replay before enabling mixed-version paging. */
public class MixedVersionPagingAcrossTombstonesFuzzTest extends UpgradeTestBase
{
    private static final String TABLE = "mixed_paging_fuzz";
    private static final int ROWS = 24;
    private static final int EXAMPLES = 6;

    @Test
    public void divergentReplicasMatchModelThroughUpgrade() throws Throwable
    {
        long seed = TEST_SEED.getLong(18424L);
        List<TreeMap<Integer, Integer>> models = new ArrayList<>();
        List<String> history = new ArrayList<>();
        new TestCase().nodes(3)
                      .singleUpgradeToCurrentFrom(v50)
                      .nodesToUpgradeOrdered(1, 2)
                      .withBuilder(builder -> builder.withSubnet(17))
                      .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                  .set("hinted_handoff_enabled", false)
                                                  .set("tombstone_failure_threshold", 10000))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                               " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                                               " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
                          Random random = new Random(seed);
                          for (int pk = 0; pk < EXAMPLES; pk++)
                              models.add(populate(cluster, random, history, pk));
                      })
                      .runBeforeNodeRestart((cluster, node) -> {
                          cluster.get(node).config().set("tombstone_paging_threshold", 2);
                          cluster.get(node).config().set("tombstone_failure_threshold", 2);
                      })
                      .runAfterNodeUpgrade((cluster, upgraded) -> {
                          for (boolean flushed : new boolean[]{ false, true })
                          {
                              if (flushed)
                                  cluster.forEach(node -> node.flush(KEYSPACE));
                              String context = "seed=" + seed + ", upgraded=" + upgraded + ", flushed=" + flushed;
                              try
                              {
                                  configurePaging(cluster, upgraded, false);
                                  verifyReplay(cluster, models);
                                  for (int coordinator = 1; coordinator <= 3; coordinator++)
                                      try (com.datastax.driver.core.Cluster client = client(cluster, coordinator);
                                           Session session = client.connect())
                                      {
                                          verifyQueries(cluster, upgraded, coordinator, session, models);
                                      }
                              }
                              catch (AssertionError | RuntimeException failure)
                              {
                                  throw new AssertionError(context + ", mutations=" + history, failure);
                              }
                              finally
                              {
                                  configurePaging(cluster, upgraded, true);
                              }
                          }
                      }).run();
    }

    private static TreeMap<Integer, Integer> populate(UpgradeableCluster cluster, Random random, List<String> history, int pk)
    {
        TreeMap<Integer, Integer> model = new TreeMap<>();
        for (int ck = 0; ck < ROWS; ck++)
        {
            int value = random.nextInt(1000);
            String cql = insert(ck, value, 1);
            for (int node = 1; node <= 3; node++)
                apply(cluster, history, node, pk, cql);
            model.put(ck, value);
        }
        // Deleted ends force checkpoints in both directions while old replicas retain stale rows.
        for (int ck = 0; ck < ROWS; ck++)
            if (pk == 0 || ck < 4 || ck >= ROWS - 4)
            {
                apply(cluster, history, 1, pk, delete(ck, ck, 2));
                model.remove(ck);
            }
        if (pk == 0)
            return model;

        for (int operation = 0; operation < 32; operation++)
        {
            int node = 1 + random.nextInt(3);
            int ck = 4 + random.nextInt(ROWS - 8);
            long timestamp = 3L + operation;
            switch (random.nextInt(3))
            {
                case 0:
                    int value = random.nextInt(1000);
                    apply(cluster, history, node, pk, insert(ck, value, timestamp));
                    model.put(ck, value);
                    break;
                case 1:
                    apply(cluster, history, node, pk, delete(ck, ck, timestamp));
                    model.remove(ck);
                    break;
                case 2:
                    int end = Math.min(ROWS - 5, ck + random.nextInt(5));
                    apply(cluster, history, node, pk, delete(ck, end, timestamp));
                    model.subMap(ck, true, end, true).clear();
                    break;
                default:
                    throw new AssertionError();
            }
        }
        return model;
    }

    private static void apply(UpgradeableCluster cluster, List<String> history, int node, int pk, String cql)
    {
        history.add("node=" + node + ", pk=" + pk + ": " + cql);
        cluster.get(node).executeInternal(cql, pk);
        // Node 3 stays on 5.0 and replays every mutation into a separate partition.
        cluster.get(3).executeInternal(cql, -pk - 1);
    }

    private static String insert(int ck, int value, long timestamp)
    {
        return "INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (?, " + ck + ", " + value + ") USING TIMESTAMP " + timestamp;
    }

    private static String delete(int start, int end, long timestamp)
    {
        return "DELETE FROM " + KEYSPACE + '.' + TABLE + " USING TIMESTAMP " + timestamp + " WHERE pk = ? AND " +
               (start == end ? "ck = " + start : "ck >= " + start + " AND ck <= " + end);
    }

    private static void verifyReplay(UpgradeableCluster cluster, List<TreeMap<Integer, Integer>> models)
    {
        for (int pk = 0; pk < EXAMPLES; pk++)
            for (boolean reversed : new boolean[]{ false, true })
            {
                Map<Integer, Integer> ordered = reversed ? models.get(pk).descendingMap() : models.get(pk);
                Object[][] expected = ordered.entrySet().stream().map(entry -> row(entry.getKey(), entry.getValue())).toArray(Object[][]::new);
                assertRows(cluster.get(3).executeInternal("SELECT ck, v FROM " + KEYSPACE + '.' + TABLE +
                                                         " WHERE pk = ? ORDER BY ck " + (reversed ? "DESC" : "ASC"), -pk - 1), expected);
            }
    }

    private static void verifyQueries(UpgradeableCluster cluster, int upgraded, int coordinator, Session session,
                                      List<TreeMap<Integer, Integer>> models)
    {
        for (int pk = 0; pk < EXAMPLES; pk++)
            for (boolean reversed : new boolean[]{ false, true })
            {
                Map<Integer, Integer> ordered = reversed ? models.get(pk).descendingMap() : models.get(pk);
                Object[][] expected = ordered.entrySet().stream().map(entry -> row(entry.getKey(), entry.getValue())).toArray(Object[][]::new);
                String base = "SELECT ck, v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = " + pk + " ORDER BY ck " + (reversed ? "DESC" : "ASC");
                verifyQuery(cluster, upgraded, coordinator, session, base, expected);
            }
        for (int first : new int[]{ 0, 3 })
        {
            List<Object[]> expected = new ArrayList<>();
            for (int pk = first; pk < first + 3; pk++)
                for (Map.Entry<Integer, Integer> entry : models.get(pk).entrySet())
                    expected.add(row(pk, entry.getKey(), entry.getValue()));
            String base = "SELECT pk, ck, v FROM " + KEYSPACE + '.' + TABLE +
                          " WHERE pk IN (" + first + ", " + (first + 1) + ", " + (first + 2) + ')';
            configurePaging(cluster, upgraded, false);
            Object[][] ordered = rows(session, base, Integer.MAX_VALUE);
            Comparator<Object[]> order = Comparator.<Object[]>comparingInt(result -> (Integer) result[0]).thenComparingInt(result -> (Integer) result[1]);
            assertRows(Arrays.stream(ordered).sorted(order).toArray(Object[][]::new), expected.toArray(Object[][]::new));
            verifyQuery(cluster, upgraded, coordinator, session, base, ordered);
        }
    }

    private static void verifyQuery(UpgradeableCluster cluster, int upgraded, int coordinator, Session session, String base, Object[][] expected)
    {
        configurePaging(cluster, upgraded, false);
        assertRows(rows(session, base, Integer.MAX_VALUE), expected);
        for (int limit : new int[]{ 3, 11, 73 })
            for (int fetchSize : new int[]{ 1, 2 })
            {
                String cql = base + " LIMIT " + limit;
                Object[][] limited = Arrays.copyOf(expected, Math.min(limit, expected.length));
                try
                {
                    configurePaging(cluster, upgraded, false);
                    assertRows(rows(session, cql, fetchSize), limited);
                    configurePaging(cluster, upgraded, true);
                    long before = thresholdCrossings(cluster, upgraded, TABLE);
                    if (coordinator <= upgraded)
                        assertRows(rows(session, cql, fetchSize), limited);
                    else
                        assertThatThrownBy(() -> rows(session, cql, fetchSize)).isInstanceOf(ReadFailureException.class);
                    assertTrue("Expected tombstone paging", thresholdCrossings(cluster, upgraded, TABLE) > before);
                }
                catch (AssertionError | RuntimeException failure)
                {
                    throw new AssertionError(cql + ", coordinator=" + coordinator + ", fetchSize=" + fetchSize, failure);
                }
            }
    }
}
