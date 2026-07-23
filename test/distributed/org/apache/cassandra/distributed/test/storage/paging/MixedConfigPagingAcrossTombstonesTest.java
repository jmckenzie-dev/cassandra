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

package org.apache.cassandra.distributed.test.storage.paging;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;


/**
 * In a cluster that has the option to page across tombstones, we need to confirm that if the feature is enabled on some
 * nodes and disabled on others, the results are still correctly reconciled at the Coordinator level. There's a variety
 * of cases we need to confirm here that are relevant to our operation which we can also leverage for mixed-mode upgrade
 * testing.
 * <p>
 * This is the base set of tests we run our different clusters through. The generation and validation patterns are
 * consistent across all the different cluster types; cluster types are split out to different test files both to
 * aid in parallelization but also help in test reporting and troubleshooting.
 * <p>
 * TODO: When we move to JUnit5, consider being opinionated about single-threaded test execution:
 *      <a href="https://junit.org/junit5/docs/5.9.1/api/org.junit.jupiter.api/org/junit/jupiter/api/parallel/ExecutionMode.html#SAME_THREAD">annotation</a>
 */
@RunWith(Parameterized.class)
public class MixedConfigPagingAcrossTombstonesTest extends TestBaseImpl
{
    protected static final int TOTAL_ROWS = 1000;

    /** "Convenient" user-facing name to identify test **/
    protected final String testName;

    /** Parameterized per test run **/
    protected final ClusterConfig testClusterConfig;

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> clusters() throws IOException
    {
        // VMatch == version match
        // CMatch == config match
        // CMis == Config Mismatch -> different paging window on node
        // CDis == Config Disabled -> i.e. version turned off on N
        return Arrays.asList(new Object[][]{
        { "VMatchCMatch", new ClusterConfig(new String[]{"4.0.5", "4.0.5", "4.0.5"},
                                            new boolean[]{true, true, true},
                                            new int[]{5, 5, 5}) }
        /*
        { "VMatchCMis2", createCluster() },
        { "VMatchCDis1", createCluster() },
        { "VMatchCMis1CDis1", createCluster() },
        { "VMatchCDis2", createCluster() },
         */
        });
    }

    public MixedConfigPagingAcrossTombstonesTest(String name, ClusterConfig config)
    {
        testName = name;
        testClusterConfig = config;
    }

    /**
     * Rather than passing around some relatively unidentified [] of primitives, wrap them up a touch.
     */
    public static class ClusterConfig
    {
        public final String[] versions;
        public final boolean[] enabled;
        public final int[] limits;

        public ClusterConfig(String[] versions, boolean[] enabled, int[] limits)
        {
            this.versions = versions;
            this.enabled = enabled;
            this.limits = limits;
        }
    }

    protected static Cluster createCluster(ClusterConfig config) throws IOException
    {
        Cluster cluster = Cluster.build(3).createWithoutStarting();
        // TODO: Get separate config per cluster
        /*
        cluster.stream().forEach(instance -> {
            instance.config().set("tombstone_paging_enabled", "true");
            instance.config().set("tombstone_warn_threshold", "5");
            instance.config().set("tombstone_failure_threshold", "10");
        });
        cluster.startup();
         */
        cluster.startup();
        return cluster;
    }

    /**
     * For purposes of tests here, we hold a very simple array-based model of "seen/not seen, live/tombstoned" for the
     * results we get back. So long as the liveness of the data is correct, we trust the per-replica reconciliation
     * is correct.
     */
    protected static class SimpleClusterModel
    {
        private final Boolean[] _liveness = new Boolean[TOTAL_ROWS];

        public SimpleClusterModel()
        {
            Arrays.fill(_liveness, false);
        }

        public void addLiveCell(ICluster<IInvokableInstance> c, int index)
        {
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, index, index);
            _liveness[index] = true;
        }

        public void tombstoneCell(ICluster<IInvokableInstance> c, int index)
        {
            c.coordinator(1).execute(withKeyspace("DELETE FROM %s.tbl WHERE pk = 1 AND ck = ?"), QUORUM, index);
            _liveness[index] = false;
        }

        public void validateLiveness(ICluster<IInvokableInstance> c)
        {
            Object[][] results = c.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl"), QUORUM);

            // Rows come back as [pk, ck, v]; liveness is tracked per clustering (ck), which is column index 1.
            // (pk is always 1 and v mirrors ck, so folding every column into one set would falsely mark ck=1 live.)
            Set<Integer> seenLive = new HashSet<>();

            for (Object[] row : results)
                seenLive.add((Integer) row[1]);

            for (int i = 0; i < _liveness.length; i++)
            {
                if (_liveness[i] && !seenLive.contains(i))
                    Assert.fail(String.format("Expected to see live ck: %d but was not in ResultSet.", i));
                else if (!_liveness[i] && seenLive.contains(i))
                    Assert.fail(String.format("Expected to see tombstoned ck: %d but was live in ResultSet.", i));
            }
        }
    }

    /**
     * The pattern for all our tests is the same as it's kept as simple and stupid as possible to only exercise the cluster-
     * wide differences.
     */
    private void runTest(ICluster<IInvokableInstance> cluster, Consumer<SimpleClusterModel> dataGenerator)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
        SimpleClusterModel model = new SimpleClusterModel();
        dataGenerator.accept(model);
        model.validateLiveness(cluster);
    }

    /**
     * No tombstones at all.
     */
    @Test
    public void testAllLive() throws Exception
    {
        try(ICluster<IInvokableInstance> cluster = createCluster(testClusterConfig))
        {
            runTest(cluster, model -> {
                for (int i = 0; i < TOTAL_ROWS; i++)
                    model.addLiveCell(cluster, i);
            });
        }
    }

    /**
     * A couple sets of 5 tombstones sprinkled throughout
     */
    @Test
    public void testNonTriggeringTombstones() throws Exception
    {
        try(ICluster<IInvokableInstance> cluster = createCluster(testClusterConfig))
        {
            runTest(cluster, model -> {
                for (int i = 0; i < TOTAL_ROWS; i++)
                    model.addLiveCell(cluster, i);
                for (int i = 0; i < 5; i++)
                    model.tombstoneCell(cluster, i);
                for (int i = 273; i < 278; i++)
                    model.tombstoneCell(cluster, i);

            });
        }
    }

    /**
     * Single set of point tombstones in middle of data range at 15 tombstones
     */
    @Test
    public void testSimpleTombstoneShortCircuit()
    {
    }

    /**
     * Single set of point tombstones in middle of data range at 60 tombstones
     */
    @Test
    public void testMultiTombstoneShortCircuit()
    {
    }

    /**
     * Two sets of tombstones, 20 each, near start and end of range
     */
    @Test
    public void testDisjointTombstoneShortCircuit()
    {
    }

    /**
     * Range tombstone where boundary of paging for 10 tombstone limit would intersect but 20 would not
     */
    @Test
    public void testRangeTombstoneIntersection()
    {
    }

    /**
     * Range tombstones are... tricky. Check to ensure if we hit our limit on RT start things behave.
     */
    @Test
    public void testTombstoneLimitOnRangeTombstoneStart()
    {
    }

    /**
     * And if we hit our limit at the same as a RT close, we behave.
     */
    @Test
    public void testTombstoneLimitOnRangeTombstoneEnd()
    {
    }

    public void testPagingReference() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start());
             Cluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            singleNode.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1)
                           .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                    singleNode.coordinator(1)
                              .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                }
            }

            int[] pageSizes = new int[]{ 1, 2, 3, 5, 10, 20, 50, Integer.MAX_VALUE };
            String[] statements = new String[]{ withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl LIMIT 3"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10)"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10) LIMIT 2")
            };
            for (String statement : statements)
            {
                Object[][] noPagingRows = singleNode.coordinator(1).execute(statement, QUORUM);
                for (int pageSize : pageSizes)
                {
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(statement, QUORUM, pageSize);
                    assertRows(Iterators.toArray(pagingRows, Object[].class), noPagingRows);
                }
            }
        }
    }

    public void testPagingWithRangeTombstonesReference() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, regular int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 1 AND ck < 10", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,2,2)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,3,3)", ConsistencyLevel.ALL);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            Iterator<Object[]> iter = cluster.coordinator(1).executeWithPaging("SELECT pk,ck,regular FROM " + KEYSPACE + ".tbl " +
                                                                               "WHERE pk=? AND ck>=? ORDER BY ck DESC;",
                                                                               ConsistencyLevel.QUORUM, 1,
                                                                               1, 1);

            assertRows(iter,
                       row(1, 3, 3),
                       row(1, 2, 2),
                       row(1, 1, 1));
        }
    }
}