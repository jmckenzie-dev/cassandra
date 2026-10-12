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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;

/**
 * {@link SystemKeyspace#listSnapshotsSince} serves from memory once loaded. It must list what the table holds, newest
 * first, whatever the order of loading, storing and truncating.
 */
public class SystemKeyspaceSnapshotEpochsTest
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @Before
    public void truncate()
    {
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME).truncateBlockingWithoutSnapshot();
        SystemKeyspace.invalidateSnapshotEpochs();
    }

    private static void store(long... epochs)
    {
        for (long epoch : epochs)
            SystemKeyspace.storeSnapshot(Epoch.create(epoch), ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    private static List<Epoch> fromTable(long since)
    {
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT epoch FROM %s.%s", SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME));
        List<Epoch> epochs = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            if (row.getLong("epoch") > since)
                epochs.add(Epoch.create(row.getLong("epoch")));
        return epochs.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
    }

    private static void assertMatchesTable(long... sinces)
    {
        for (long since : sinces)
            assertEquals("since " + since, fromTable(since), SystemKeyspace.listSnapshotsSince(Epoch.create(since)));
    }

    @Test
    public void emptyTable()
    {
        assertMatchesTable(0, 5);
    }

    @Test
    public void matchesTableForRecentAndOlderSearches()
    {
        store(100, 200, 300, 400);
        // The first search sets the lowest epoch served from memory; older searches read the table
        assertMatchesTable(350, 399, 400, 401, 0, 99, 100, 150, 399, 350);
    }

    @Test
    public void matchesTableWhenFirstSearchIsOldest()
    {
        store(100, 200, 300);
        assertMatchesTable(0, 99, 100, 150, 299, 300, 301);
    }

    @Test
    public void storedSnapshotsAppearWithoutReload()
    {
        store(100, 200);
        assertMatchesTable(50);
        store(300);
        store(400);
        assertMatchesTable(50, 250, 400);
    }

    @Test
    public void truncateDropsTheCachedEpochs()
    {
        store(100, 200);
        assertMatchesTable(50);
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME).truncateBlockingWithoutSnapshot();
        assertMatchesTable(50);
        store(500);
        assertMatchesTable(50, 400);
    }

    @Test
    public void servesFromMemoryAfterTheFirstSearch()
    {
        store(100, 200, 300);
        assertMatchesTable(50);
        QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE epoch = 300", SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME));
        // Hand-deleted rows are not seen until the set is dropped; this shows the table is not read
        assertEquals(3, SystemKeyspace.listSnapshotsSince(Epoch.create(50)).size());
        SystemKeyspace.invalidateSnapshotEpochs();
        assertMatchesTable(50);
        assertEquals(2, SystemKeyspace.listSnapshotsSince(Epoch.create(50)).size());
    }
}
