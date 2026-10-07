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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.SCHEMA_TABLES_FULL_WRITE_ON_STARTUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Startup writes system_schema only when it does not already hold the schema, and a full write stays within
 * max_mutation_size. Before, every startup wrote each keyspace as one mutation, so a keyspace whose schema passed
 * max_mutation_size (about 24k tables at the default 16 MiB) could not restart.
 */
public class StartupSchemaWriteTest extends TestBaseImpl
{
    private static final String SKIPPED = "system_schema already holds the schema";

    @Test
    public void cleanRestartSkipsTheRewriteAndKeepsEveryTable() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTables(cluster, 20);
            IInvokableInstance node = cluster.get(1);

            restart(node, true);
            assertFalse("a clean restart must skip the system_schema rewrite", node.logs().grep(SKIPPED).getResult().isEmpty());
            assertEquals(20, tableCount(cluster));

            // A change after the restart is written as its own diff, and the next restart skips again.
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.after_restart (k int PRIMARY KEY, v int)"));
            long mark = node.logs().mark();
            restart(node, true);
            assertFalse(node.logs().grep(mark, SKIPPED).getResult().isEmpty());
            assertEquals(21, tableCount(cluster));
        }
    }

    /**
     * Without a drain, system.local's newest schema_version is only in the commit log, which is replayed after TCM
     * starts. The node then sees an older version and does the full write, which must also keep every table.
     */
    @Test
    public void restartWithoutDrainDoesTheFullWriteAndKeepsEveryTable() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTables(cluster, 20);
            IInvokableInstance node = cluster.get(1);

            long mark = node.logs().mark();
            restart(node, false);
            assertTrue(node.logs().grep(mark, SKIPPED).getResult().isEmpty());
            assertEquals(20, tableCount(cluster));
        }
    }

    @Test
    public void fullWriteOfAKeyspaceLargerThanMaxMutationSizeSucceeds() throws Exception
    {
        // 450 tables of system_schema rows are one ~269 KB mutation, about twice this max_mutation_size.
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("commitlog_segment_size", "1MiB")
                                                             .set("max_mutation_size", "128KiB"))
                                           .start()))
        {
            int tables = 450;
            createTables(cluster, tables);
            IInvokableInstance node = cluster.get(1);

            long mark = node.logs().mark();
            try (WithProperties ignored = new WithProperties().set(SCHEMA_TABLES_FULL_WRITE_ON_STARTUP, true))
            {
                restart(node, true);
            }
            LogAction logs = node.logs();
            assertTrue("the forced restart must do the full write", logs.grep(mark, SKIPPED).getResult().isEmpty());
            assertTrue(logs.grep(mark, "MutationExceededMaxSizeException").getResult().isEmpty());
            assertEquals(tables, tableCount(cluster));
        }
    }

    private static void createTables(Cluster cluster, int count)
    {
        for (int i = 0; i < count; i++)
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t" + i + " (pk int, c int, v text, PRIMARY KEY (pk, c))"));
    }

    // A production stop drains (nodetool drain or the shutdown hook); the in-JVM shutdown does not.
    private static void restart(IInvokableInstance node, boolean drain) throws IOException
    {
        if (drain)
            node.nodetoolResult("drain").asserts().success();
        try
        {
            node.shutdown().get();
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        node.startup();
    }

    private static int tableCount(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                                              ConsistencyLevel.ONE, KEYSPACE).length;
    }
}
