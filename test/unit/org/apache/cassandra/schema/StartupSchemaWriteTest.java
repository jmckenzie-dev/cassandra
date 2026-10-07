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

package org.apache.cassandra.schema;

import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Node startup used to write each keyspace's whole schema to system_schema as one mutation, which passes the 16 MiB
 * max_mutation_size at about 24k tables. Startup now skips the write when system.local shows system_schema already
 * holds the schema, and a full write (new node, upgrade) uses mutations within a byte budget.
 */
public class StartupSchemaWriteTest
{
    private static final int BUDGET = 64 * 1024;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void fullCreateIsSplitWithinTheBudgetAndReadsBackWhole()
    {
        KeyspaceMetadata keyspace = keyspace("bounded_ks", 2000, 0);
        List<Mutation> mutations = SchemaKeyspace.makeBoundedCreateKeyspaceMutations(keyspace, FBUtilities.timestampMicros(), BUDGET);

        assertTrue("2000 tables must not fit one " + BUDGET + " byte mutation: " + mutations.size(), mutations.size() > 1);
        for (Mutation m : mutations)
            assertTrue("mutation of " + size(m) + " bytes over the budget", size(m) <= BUDGET);
        assertNotNull("the first mutation holds the keyspace row",
                      mutations.get(0).getPartitionUpdate(SchemaKeyspace.metadata().tables.getNullable(SchemaKeyspaceTables.KEYSPACES)));

        mutations.forEach(Mutation::applyUnsafe);
        assertReadsBack(keyspace);
    }

    @Test
    public void aTableLargerThanTheBudgetGetsItsOwnMutation()
    {
        KeyspaceMetadata keyspace = keyspace("wide_ks", 50, 3000);
        List<Mutation> mutations = SchemaKeyspace.makeBoundedCreateKeyspaceMutations(keyspace, FBUtilities.timestampMicros(), BUDGET);

        int over = 0;
        for (Mutation m : mutations)
            if (size(m) > BUDGET)
                over++;
        assertEquals("only the one wide table may exceed the budget", 1, over);

        mutations.forEach(Mutation::applyUnsafe);
        assertReadsBack(keyspace);
    }

    @Test
    public void schemaTablesHoldOnlyForTheSameVersionAndRelease()
    {
        UUID version = new UUID(0, 4242);
        SystemKeyspace.updateSchemaVersion(version);
        setRelease(FBUtilities.getReleaseVersionString());
        assertTrue(SystemKeyspace.schemaTablesHold(version));
        assertFalse("another schema version", SystemKeyspace.schemaTablesHold(new UUID(0, 4243)));

        setRelease("1.0.0");
        assertFalse("written by another release (an upgrade)", SystemKeyspace.schemaTablesHold(version));
        setRelease(FBUtilities.getReleaseVersionString());
    }

    private static void setRelease(String release)
    {
        executeInternal(format("INSERT INTO system.%s (key, release_version) VALUES ('%s', ?)", SystemKeyspace.LOCAL, SystemKeyspace.LOCAL), release);
    }

    private static void assertReadsBack(KeyspaceMetadata keyspace)
    {
        KeyspaceMetadata fetched = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(keyspace.name);
        assertNotNull(fetched);
        assertEquals(keyspace.params, fetched.params);
        assertEquals(keyspace.tables.size(), fetched.tables.size());
        for (TableMetadata table : keyspace.tables)
        {
            TableMetadata read = fetched.tables.getNullable(table.name);
            assertNotNull(table.name, read);
            assertEquals(table.id, read.id);
            assertEquals(table.name, table.columns().size(), read.columns().size());
            assertEquals(table.params, read.params);
        }
    }

    private static long size(Mutation m)
    {
        return m.serializedSize(MessagingService.current_version);
    }

    private static KeyspaceMetadata keyspace(String name, int tables, int wideColumns)
    {
        Tables.Builder builder = Tables.builder();
        for (int i = 0; i < tables; i++)
        {
            TableMetadata.Builder table = TableMetadata.builder(name, "t" + i)
                                                       .addPartitionKeyColumn("pk", Int32Type.instance)
                                                       .addClusteringColumn("c", Int32Type.instance)
                                                       .addRegularColumn("v", UTF8Type.instance);
            if (i == 0)
                for (int c = 0; c < wideColumns; c++)
                    table.addRegularColumn("w" + c, Int32Type.instance);
            builder.add(table.build());
        }
        return KeyspaceMetadata.create(name, KeyspaceParams.simple(1), builder.build());
    }
}
