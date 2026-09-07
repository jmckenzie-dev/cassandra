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
package org.apache.cassandra.distributed.test.storage.paging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MixedConfigPagingAcrossTombstonesTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static final List<com.datastax.driver.core.Cluster> clients = new ArrayList<>();
    private static final List<Session> sessions = new ArrayList<>();
    private final String name;
    private final int[] thresholds;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> clusters()
    {
        return Arrays.asList(new Object[][]{
            { "matching", new int[]{ 2, 2, 2 } },
            { "different", new int[]{ 2, 3, 4 } },
            { "one_disabled", new int[]{ 2, -1, 4 } },
            { "two_disabled", new int[]{ 2, -1, -1 } }
        });
    }

    public MixedConfigPagingAcrossTombstonesTest(String name, int[] thresholds)
    {
        this.name = name;
        this.thresholds = thresholds;
    }

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build(3).withSubnet(15)
                              .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                          .set("hinted_handoff_enabled", false))
                              .start());
        for (int node = 1; node <= 3; node++)
        {
            String host = cluster.get(node).broadcastAddress().getHostString();
            com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint(host)
                                                                                      .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(),
                                                                                                                                  Collections.singletonList(new InetSocketAddress(host, 9042))))
                                                                                      .build();
            clients.add(client);
            sessions.add(client.connect());
        }
    }

    @AfterClass
    public static void teardown()
    {
        sessions.forEach(Session::close);
        clients.forEach(com.datastax.driver.core.Cluster::close);
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void configure()
    {
        for (int node = 1; node <= 3; node++)
        {
            int threshold = thresholds[node - 1];
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.setTombstonePagingThreshold(threshold);
                DatabaseDescriptor.setTombstoneFailureThreshold(threshold == -1 ? 10000 : threshold);
                assertEquals(threshold, DatabaseDescriptor.getTombstonePagingThreshold());
            });
        }
    }

    @Test
    public void testAllLive()
    {
        verify("live", new int[0], new int[0][], false);
    }

    @Test
    public void testNonTriggeringTombstones()
    {
        verify("below_threshold", new int[]{ 0 }, new int[0][], false);
    }

    @Test
    public void testSimpleTombstoneShortCircuit()
    {
        verify("simple", IntStream.range(0, 6).toArray(), new int[0][], true);
    }

    @Test
    public void testMultiTombstoneShortCircuit()
    {
        verify("multiple", IntStream.range(0, 12).toArray(), new int[0][], true);
    }

    @Test
    public void testDisjointTombstoneShortCircuit()
    {
        verify("disjoint", IntStream.concat(IntStream.range(0, 6), IntStream.range(12, 18)).toArray(), new int[0][], true);
    }

    @Test
    public void testRangeTombstoneIntersection()
    {
        verify("range_intersection", new int[]{ 0, 1, 2 }, new int[][]{ { 3, 11 }, { 8, 14 } }, true);
    }

    @Test
    public void testTombstoneLimitOnRangeTombstoneStart()
    {
        verify("range_start", new int[]{ 0, 1 }, new int[][]{ { 2, 9 } }, true);
    }

    @Test
    public void testTombstoneLimitOnRangeTombstoneEnd()
    {
        verify("range_end", new int[]{ 10, 11, 12 }, new int[][]{ { 0, 9 } }, true);
    }

    private void verify(String scenario, int[] deletes, int[][] ranges, boolean expectCheckpoint)
    {
        String table = name + '_' + scenario;
        String qualified = KEYSPACE + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualified + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))" +
                             " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        for (int node = 1; node <= 3; node++)
            for (int ck = 0; ck < 24; ck++)
                cluster.get(node).executeInternal("INSERT INTO " + qualified +
                                                  " (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1", ck, ck);
        boolean[] deleted = new boolean[24];
        for (int ck : deletes)
        {
            cluster.get(1).executeInternal("DELETE FROM " + qualified + " USING TIMESTAMP 2 WHERE pk = 0 AND ck = ?", ck);
            deleted[ck] = true;
        }
        for (int[] range : ranges)
        {
            cluster.get(1).executeInternal("DELETE FROM " + qualified +
                                          " USING TIMESTAMP 3 WHERE pk = 0 AND ck >= ? AND ck <= ?", range[0], range[1]);
            Arrays.fill(deleted, range[0], range[1] + 1, true);
        }

        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                cluster.forEach(node -> node.flush(KEYSPACE));
            long before = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstoneFailures.getCount());
            for (boolean reversed : new boolean[]{ false, true })
            {
                List<Object[]> expected = new ArrayList<>();
                for (int ck = 0; ck < 24; ck++)
                    if (!deleted[ck])
                        expected.add(row(ck, ck));
                if (reversed)
                    Collections.reverse(expected);
                for (Session session : sessions)
                {
                    SimpleStatement statement = new SimpleStatement("SELECT ck, v FROM " + qualified +
                                                                    " WHERE pk = 0 ORDER BY ck " + (reversed ? "DESC" : "ASC") + " LIMIT 25");
                    statement.setConsistencyLevel(ConsistencyLevel.ALL);
                    statement.setFetchSize(1);
                    Object[][] actual = session.execute(statement).all().stream()
                                               .map(result -> row(result.getInt("ck"), result.getInt("v")))
                                               .toArray(Object[][]::new);
                    assertRows(actual, expected.toArray(new Object[0][]));
                }
            }
            if (expectCheckpoint)
                assertTrue("Expected a checkpoint for " + table + ", flushed=" + flushed,
                           cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.tombstoneFailures.getCount()) > before);
        }
    }
}
