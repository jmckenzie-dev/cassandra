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
