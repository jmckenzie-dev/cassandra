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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DistributedSchemaDiffCacheTest
{
    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Test
    public void sameFromReturnsTheCachedDiff()
    {
        Keyspaces before = Keyspaces.of(keyspace("ks", 3));
        DistributedSchema after = new DistributedSchema(withTable(before, "ks", 3));

        KeyspacesDiff first = after.diffFrom(before);
        assertSame(first, after.diffFrom(before));
        assertEquals(1, first.altered.size());
        assertEquals("table_3", first.altered.get(0).tables.created.iterator().next().name);
    }

    @Test
    public void otherFromIsRecomputed()
    {
        Keyspaces before = Keyspaces.of(keyspace("ks", 3));
        KeyspaceMetadata ksm = before.getNullable("ks");
        Keyspaces older = before.withAddedOrUpdated(ksm.withSwapped(ksm.tables.without("table_2")));
        DistributedSchema after = new DistributedSchema(withTable(before, "ks", 3));

        KeyspacesDiff fromBefore = after.diffFrom(before);
        KeyspacesDiff fromOlder = after.diffFrom(older);
        assertNotSame(fromBefore, fromOlder);
        assertEquals(2, fromOlder.altered.get(0).tables.created.size());
        assertSame("the new entry replaces the old one", fromOlder, after.diffFrom(older));
    }

    @Test
    public void diffThatDropsAKeyspaceIsNotCached()
    {
        Keyspaces before = Keyspaces.of(keyspace("ks", 3), keyspace("gone", 3));
        DistributedSchema after = new DistributedSchema(before.without("gone"));

        KeyspacesDiff first = after.diffFrom(before);
        assertTrue(first.dropped.containsKeyspace("gone"));
        assertNotSame(first, after.diffFrom(before));
    }

    private static Keyspaces withTable(Keyspaces keyspaces, String ks, int i)
    {
        KeyspaceMetadata ksm = keyspaces.getNullable(ks);
        return keyspaces.withAddedOrUpdated(ksm.withSwapped(ksm.tables.with(table(ks, i))));
    }

    private static KeyspaceMetadata keyspace(String name, int tables)
    {
        Tables.Builder builder = Tables.builder();
        for (int i = 0; i < tables; i++)
            builder.add(table(name, i));
        return KeyspaceMetadata.create(name, KeyspaceParams.simple(1), builder.build());
    }

    private static TableMetadata table(String ks, int i)
    {
        return TableMetadata.builder(ks, "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }
}
