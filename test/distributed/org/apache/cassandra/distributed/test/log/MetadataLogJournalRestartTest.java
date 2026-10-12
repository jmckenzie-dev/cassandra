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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * metadata_log_durability: journal. A node stopped without a drain (the in-JVM shutdown does not flush memtables)
 * has the newest log entries only in the journal and the commit log. Startup reads the table before commit log
 * replay, so it must recover them from the journal: every acknowledged DDL is present after the restart.
 */
public class MetadataLogJournalRestartTest extends TestBaseImpl
{
    private static final int THREADS = 4;
    private static final int TABLES_PER_THREAD = 6;
    private static final String GOSSIP_START = "Initializing from gossip";
    private static final String RECOVERED = "Recovered \\d+ metadata log entries";

    private static int journalFiles(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            Path dir = SystemKeyspaceStorage.journalDirectory(DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations()[0]);
            if (!Files.isDirectory(dir))
                return 0;
            try (Stream<Path> files = Files.list(dir))
            {
                return (int) files.count();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private static long epoch(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
    }

    @Test
    public void everyAcknowledgedDdlSurvivesAStopWithoutDrain() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("metadata_log_durability", "journal"))
                                           .start()))
        {
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++)
            {
                int thread = t;
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < TABLES_PER_THREAD; i++)
                        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t_" + thread + '_' + i + " (k int PRIMARY KEY, v int)"));
                }));
            }
            for (Future<?> future : futures)
                future.get(5, TimeUnit.MINUTES);
            executor.shutdown();

            IInvokableInstance node = cluster.get(1);
            // One more DDL, so that the newest journal file holds an entry for certain, whatever the 10 s flush did
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.last (k int PRIMARY KEY, v int)"));
            long acknowledged = epoch(node);
            assertTrue("the journal holds entries before the stop", journalFiles(node) > 0);

            stopUnchecked(node);
            node.startup();

            Object[][] tables = node.executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", KEYSPACE);
            assertEquals(THREADS * TABLES_PER_THREAD + 1, tables.length);
            assertTrue("epoch " + epoch(node) + " is at least the acknowledged " + acknowledged, epoch(node) >= acknowledged);
            assertFalse("startup logs the recovery", node.logs().grep(RECOVERED).getResult().isEmpty());
        }
    }

    @Test
    public void firstCmsNodeStoppedRightAfterInitializationRestartsAsNormal() throws Exception
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.set("metadata_log_durability", "journal"))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);
            long acknowledged = epoch(node);
            int gossipStarts = node.logs().grep(GOSSIP_START).getResult().size();
            assertTrue("the journal holds the initial entries", journalFiles(node) > 0);

            stopUnchecked(node);
            node.startup();

            assertTrue("epoch " + epoch(node) + " is at least " + acknowledged, epoch(node) >= acknowledged);
            assertTrue("the node is still a CMS member",
                       node.callOnInstance(() -> ClusterMetadataService.instance().isCurrentMember(FBUtilities.getBroadcastAddressAndPort())));
            assertEquals("startup must not fall back to gossip", gossipStarts, node.logs().grep(GOSSIP_START).getResult().size());
            assertFalse("startup logs the recovery", node.logs().grep(RECOVERED).getResult().isEmpty());
        }
    }
}
