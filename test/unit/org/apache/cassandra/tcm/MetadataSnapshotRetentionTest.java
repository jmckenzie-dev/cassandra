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

package org.apache.cassandra.tcm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Storing a snapshot keeps only the newest {@code metadata_snapshots_retained} rows in system.metadata_snapshots, and
 * the in-memory epoch set used by {@link SystemKeyspace#listSnapshotsSince} follows the table.
 */
public class MetadataSnapshotRetentionTest
{
    private final MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots(Runnable::run);
    private int originalRetained;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @Before
    public void setup()
    {
        originalRetained = DatabaseDescriptor.getMetadataSnapshotsRetained();
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME).truncateBlockingWithoutSnapshot();
        SystemKeyspace.invalidateSnapshotEpochs();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(originalRetained);
    }

    private ClusterMetadata metadataAt(long epoch)
    {
        return ClusterMetadataTestHelper.minimalForTesting(Epoch.create(epoch), Murmur3Partitioner.instance, new DistributedSchema(Keyspaces.NONE));
    }

    private void store(long... epochs)
    {
        for (long epoch : epochs)
            snapshots.storeSnapshot(metadataAt(epoch));
    }

    // Newest first
    private static List<Long> tableEpochs()
    {
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT epoch FROM %s.%s", SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME));
        List<Long> epochs = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            epochs.add(row.getLong("epoch"));
        epochs.sort(Comparator.reverseOrder());
        return epochs;
    }

    private static List<Long> listedEpochs(long since)
    {
        return SystemKeyspace.listSnapshotsSince(Epoch.create(since)).stream().map(Epoch::getEpoch).collect(Collectors.toList());
    }

    @Test
    public void keepsOnlyTheNewestN()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(3);
        store(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

        assertEquals(Arrays.asList(11L, 10L, 9L), tableEpochs());
        assertEquals(tableEpochs(), listedEpochs(0));
        assertEquals(metadataAt(11), snapshots.getLatestSnapshot());
        assertEquals(metadataAt(10), snapshots.getSnapshotBefore(Epoch.create(10)));
        assertEquals(metadataAt(9), snapshots.getSnapshotBefore(Epoch.create(9)));
        assertEquals(null, snapshots.getSnapshotBefore(Epoch.create(8)));
        assertEquals(metadataAt(11), snapshots.getSnapshot(Epoch.create(11)));
    }

    @Test
    public void minusOneKeepsAll()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        store(2, 3, 4, 5, 6, 7, 8);

        assertEquals(Arrays.asList(8L, 7L, 6L, 5L, 4L, 3L, 2L), tableEpochs());
        assertEquals(tableEpochs(), listedEpochs(0));
    }

    @Test
    public void settingIsReadAtEachStore()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        store(2, 3, 4, 5);
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        store(6);
        assertEquals(Arrays.asList(6L, 5L), tableEpochs());
        DatabaseDescriptor.setMetadataSnapshotsRetained(4);
        store(7);
        assertEquals(Arrays.asList(7L, 6L, 5L), tableEpochs());
        store(8, 9);
        assertEquals(Arrays.asList(9L, 8L, 7L, 6L), tableEpochs());
        assertEquals(tableEpochs(), listedEpochs(0));
    }

    @Test
    public void rowsBelowTheLoadedFloorAreDeleted()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        store(2, 3, 4, 5, 6);
        // A search above 4 loads the set with a floor of 4
        SystemKeyspace.invalidateSnapshotEpochs();
        assertEquals(Arrays.asList(6L, 5L), listedEpochs(4));

        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        store(7);
        assertEquals(Arrays.asList(7L, 6L), tableEpochs());
        assertEquals(tableEpochs(), listedEpochs(0));
        assertEquals(tableEpochs(), listedEpochs(4));
    }

    @Test
    public void retentionAfterRestartDeletesRowsFromTheTable()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        store(2, 3, 4, 5, 6);
        SystemKeyspace.invalidateSnapshotEpochs();

        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        store(7);
        assertEquals(Arrays.asList(7L, 6L), tableEpochs());
    }

    @Test
    public void justStoredSnapshotSurvivesEvenWhenOlderThanTheKeptOnes()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        store(10, 11, 12);
        assertEquals(Arrays.asList(12L, 11L), tableEpochs());

        store(5);
        List<Long> epochs = tableEpochs();
        assertTrue("stored epoch must survive: " + epochs, epochs.contains(5L));
        assertTrue("newest must survive: " + epochs, epochs.contains(12L));

        store(13);
        assertEquals(Arrays.asList(13L, 12L), tableEpochs());
    }

    @Test
    public void validationRejectsZeroOneAndBelowMinusOne()
    {
        for (int bad : new int[]{ 0, 1, -2, Integer.MIN_VALUE })
        {
            try
            {
                DatabaseDescriptor.setMetadataSnapshotsRetained(bad);
                fail("accepted " + bad);
            }
            catch (ConfigurationException expected)
            {
                // expected
            }
            assertEquals(originalRetained, DatabaseDescriptor.getMetadataSnapshotsRetained());
        }
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        assertEquals(2, DatabaseDescriptor.getMetadataSnapshotsRetained());
    }

    @Test
    public void retentionRunsOnTheExecutorNotOnTheStoringThread()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        List<Runnable> queued = new ArrayList<>();
        MetadataSnapshots deferred = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots(queued::add);
        deferred.storeSnapshot(metadataAt(2));
        deferred.storeSnapshot(metadataAt(3));
        deferred.storeSnapshot(metadataAt(4));

        assertEquals("single flight: one task queued while it is pending", 1, queued.size());
        assertEquals(Arrays.asList(4L, 3L, 2L), tableEpochs());

        queued.get(0).run();
        assertEquals(Arrays.asList(4L, 3L), tableEpochs());

        deferred.storeSnapshot(metadataAt(5));
        assertEquals("a finished task allows the next store to queue another", 2, queued.size());
        queued.get(1).run();
        assertEquals(Arrays.asList(5L, 4L), tableEpochs());
    }

    @Test
    public void retentionReadsTheSettingWhenTheTaskRuns()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        List<Runnable> queued = new ArrayList<>();
        MetadataSnapshots deferred = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots(queued::add);
        deferred.storeSnapshot(metadataAt(2));
        deferred.storeSnapshot(metadataAt(3));
        deferred.storeSnapshot(metadataAt(4));
        DatabaseDescriptor.setMetadataSnapshotsRetained(-1);
        queued.get(0).run();
        assertEquals(Arrays.asList(4L, 3L, 2L), tableEpochs());
    }

    @Test
    public void rejectedTaskDoesNotFailTheStoreOrBlockLaterRetention()
    {
        DatabaseDescriptor.setMetadataSnapshotsRetained(2);
        AtomicInteger calls = new AtomicInteger();
        MetadataSnapshots rejecting = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots(r -> {
            if (calls.incrementAndGet() == 1)
                throw new RejectedExecutionException("test");
            r.run();
        });
        long before = TCMMetrics.instance.snapshotRetentionFailures.getCount();
        rejecting.storeSnapshot(metadataAt(2));
        rejecting.storeSnapshot(metadataAt(3));
        rejecting.storeSnapshot(metadataAt(4));
        assertEquals(before + 1, TCMMetrics.instance.snapshotRetentionFailures.getCount());
        assertEquals(Arrays.asList(4L, 3L), tableEpochs());
    }
}
