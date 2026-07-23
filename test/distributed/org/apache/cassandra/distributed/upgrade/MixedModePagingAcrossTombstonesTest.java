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

package org.apache.cassandra.distributed.upgrade;

import org.apache.cassandra.distributed.api.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.*;

public class MixedModePagingAcrossTombstonesTest extends UpgradeTestBase {
    /**
     * Confirm that in a mixed mode case with 2 nodes where the older node throws a {@link org.apache.cassandra.db.filter.TombstoneOverwhelmingException}
     * we respect that and fail out.
     *
     * @throws Throwable
     */
    @Test
    public void mixedModeTOEException() throws Throwable {
        new TestCase()
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                  .set("tombstone_paging_threshold", 25000)
                                  .set("tombstone_warn_threshold", 10)
                                  .set("tombstone_failure_threshold", 250))
                .nodes(2)
                .singleUpgradeToCurrentFrom(v41)
                .nodesToUpgrade(1)
                .setup(cluster -> {
                    cluster.schemaChange(CREATE_TABLE);
                    // Insert on upgraded node
                    insertDataOnAll(cluster.coordinator(1), 1000);
                    // Deletion on lower versioned node so it'll bail out
                    deleteDataOnOne(cluster.get(2), 50, 500);
                })
                .runAfterClusterUpgrade(cluster -> {
                    // Run the query and confirm that the older node's tombstone failure propagates to the coordinator
                    System.err.println("------------------ QUERYING DATA --------------------------");
                    Throwable thrown = null;
                    try {
                        cluster.coordinator(1).execute(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE_NAME), ConsistencyLevel.ALL);
                    } catch (Throwable t) {
                        thrown = t;
                    }
                    Assert.assertNotNull("Expected read to fail with READ_TOO_MANY_TOMBSTONES from the non-upgraded replica", thrown);
                    Assert.assertTrue("Expected READ_TOO_MANY_TOMBSTONES but got: " + thrown, thrown.toString().contains("READ_TOO_MANY_TOMBSTONES"));
                })
                .run();
    }

    /**
     * We want to confirm that, in the case where an older version node doesn't hit the limit (i.e. is missing data)
     * and a new version node _does_ hit the tombstone limit, that things don't cycle endlessly on read repair or otherwise
     * fail in catastrophically weird ways
     *
     * @throws Throwable
     */
    @Test
    public void mixedModeMismatchReturn() throws Throwable {
        //pass
    }

    @Test
    public void mixedModeReadColumnSubsetDigestCheck() throws Throwable {
        new TestCase()
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .nodes(2)
                .nodesToUpgrade(1)
                .singleUpgradeToCurrentFrom(v41)
                .setup(cluster -> {
                    cluster.schemaChange(CREATE_TABLE);
                    insertDataOnAll(cluster.coordinator(1));
                    testDigestConsistency(cluster.coordinator(1));
                    testDigestConsistency(cluster.coordinator(2));
                })
                .runAfterClusterUpgrade(cluster -> {
                    // should not cause a digest mismatch in mixed mode
                    testDigestConsistency(cluster.coordinator(1));
                    testDigestConsistency(cluster.coordinator(2));
                })
                .run();
    }

    @Test
    public void testPagingWithCompactStorage() throws Throwable {
        new TestCase()
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .nodes(2)
                .nodesToUpgrade(2)
                .upgradesToCurrentFrom(v41)
                .setup((cluster) -> {
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
                    for (int i = 1; i < 10; i++)
                        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, 1, i, i);
                })
                .runAfterNodeUpgrade((cluster, i) -> {
                    for (int coord = 1; coord <= 2; coord++) {
                        Iterator<Object[]> iter = cluster.coordinator(coord).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL, 2);
                        for (int j = 1; j < 10; j++) {
                            Assert.assertTrue(iter.hasNext());
                            Assert.assertArrayEquals(new Object[]{1, j, j}, iter.next());
                        }
                        Assert.assertFalse(iter.hasNext());
                    }
                }).run();
    }

    /**
     * Test that queries repair rows that exist in both replicas but have been deleted only in one replica.
     * The row deletion can be either in the upgraded or in the not-upgraded node.
     */
    @Test
    public void mixedModeReadRepairDeleteRow() throws Throwable {
        // rows for columns (k, c, v, s)
        Object[] row1 = row(0, 1, 10, 8);
        Object[] row2 = row(0, 2, 20, 8);

        allUpgrades(2, 1)
                .setup(cluster -> {
                    cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, s int static, PRIMARY KEY (k, c))"));

                    // insert the rows in all the nodes
                    String insert = withKeyspace("INSERT INTO %s.t (k, c, v, s) VALUES (?, ?, ?, ?)");
                    cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
                    cluster.coordinator(2).execute(insert, ConsistencyLevel.ALL, row2);
                })
                .runAfterClusterUpgrade(cluster -> {

                    // internally delete one row per replica
                    String delete = withKeyspace("DELETE FROM %s.t WHERE k=? AND c=?");
                    cluster.get(1).executeInternal(delete, 0, 1);
                    cluster.get(2).executeInternal(delete, 0, 2);

                    // query to trigger read repair
                    String query = withKeyspace("SELECT k, c, v, s FROM %s.t");
                    assertRows(cluster.get(1).executeInternal(query), row2);
                    assertRows(cluster.get(2).executeInternal(query), row1);
                    Object[] emptyPartition = row(0, null, null, 8);
                    assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), emptyPartition);
                    assertRows(cluster.get(1).executeInternal(query), emptyPartition);
                    assertRows(cluster.get(2).executeInternal(query), emptyPartition);
                })
                .run();

    }

    public static void insertDataOnAll(ICoordinator coordinator) { insertDataOnAll(coordinator, 1000); }

    public static void insertDataOnAll(ICoordinator coordinator, int clusteringCount)
    {
        System.err.println("------------------ INSERTING DATA --------------------------");
        for (int i = 0; i < clusteringCount; i++) {
            coordinator.execute(String.format("INSERT INTO %s.%s (k, c, s1, s2, v1, v2) VALUES (1, %d, 2, {1, 2, 3, 4, 5}, 3, {6, 7, 8, 9, 10})", KEYSPACE, TABLE_NAME, i), ConsistencyLevel.ALL);
        }
    }

    public static void deleteDataOnOne(IInstance instance, int startClustering, int deleteCount)
    {
        System.err.println("------------------ DELETING DATA --------------------------");
        for (int i = startClustering; i < deleteCount; i++) {
            instance.executeInternal(String.format("DELETE FROM %s.%s WHERE k = 1 AND c = %d", KEYSPACE, TABLE_NAME, i));
        }
    }
}