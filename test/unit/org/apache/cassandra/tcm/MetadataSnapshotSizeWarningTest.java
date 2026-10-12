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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.utils.Clock.Global;
import org.apache.cassandra.utils.NoSpamLogger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link MetadataSnapshots.SystemKeyspaceMetadataSnapshots#storeSnapshot} writes the serialised {@link ClusterMetadata}
 * without the commit log and flushes it, so {@code max_mutation_size} no longer bounds it. Single-mutation writes used
 * to fail with {@link MutationExceededMaxSizeException} at about 24k tables on the default 16 MiB. This test verifies:
 * <ul>
 *     <li>a small snapshot stores cleanly, with no warning-threshold side effects</li>
 *     <li>a snapshot larger than {@code max_mutation_size} stores and reads back</li>
 *     <li>{@link MetadataSnapshotListener#notify} swallows a store failure (so the log-processing thread survives)
 *     while still incrementing {@link TCMMetrics#snapshotStoreFailures}</li>
 *     <li>the size warning fires against the limits that still apply: max_value_size and internode_max_message_size</li>
 * </ul>
 * test/conf/cassandra.yaml sets {@code commitlog_segment_size: 5MiB}, which makes
 * {@code max_mutation_size = commitlog_segment_size / 2 = 2,621,440} bytes for this test JVM.
 */
public class MetadataSnapshotSizeWarningTest
{
    private static final String KEYSPACE = "metadata_snapshot_size_warning_test_ks";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    private static DistributedSchema schemaWithTables(int tableCount)
    {
        List<TableMetadata> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++)
        {
            tables.add(TableMetadata.builder(KEYSPACE, "t" + i)
                                     .addPartitionKeyColumn("k", Int32Type.instance)
                                     .addRegularColumn("v", UTF8Type.instance)
                                     .build());
        }
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(tables));
        return new DistributedSchema(Keyspaces.of(ksm));
    }

    private static void truncateSnapshotsTable()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME);
        if (cfs != null)
            cfs.truncateBlockingWithoutSnapshot();
    }

    @Test
    public void smallSnapshotStoresCleanlyWithNoWarningOrFailure()
    {
        truncateSnapshotsTable();
        try
        {
            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata small = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(2), Murmur3Partitioner.instance, schemaWithTables(10));
            snapshots.storeSnapshot(small);

            assertEquals(small, snapshots.getSnapshot(small.epoch));
            assertEquals("A small snapshot must not increment the failure counter",
                         failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());
            assertTrue("A small snapshot must be well under the warning threshold",
                       TCMMetrics.instance.lastSnapshotSize.getValue() < MetadataSnapshots.SystemKeyspaceMetadataSnapshots.snapshotSizeLimit() * 0.75);
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void snapshotLargerThanMaxMutationSizeStoresAndReadsBack() throws IOException
    {
        truncateSnapshotsTable();
        try
        {
            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();
            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata large = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(3), Murmur3Partitioner.instance, schemaWithTables(8000));
            assertTrue("Test setup: the snapshot must exceed max_mutation_size " + DatabaseDescriptor.getMaxMutationSize(),
                       MetadataSnapshots.toBytes(large).remaining() > DatabaseDescriptor.getMaxMutationSize());

            snapshots.storeSnapshot(large);

            assertEquals(large, snapshots.getSnapshot(large.epoch));
            assertEquals(large, snapshots.getLatestSnapshot());
            assertEquals(failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void listenerSwallowsStoreFailureButCountsIt()
    {
        truncateSnapshotsTable();
        try
        {
            MetadataSnapshots failingSnapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots()
            {
                @Override
                public void storeSnapshot(ClusterMetadata metadata)
                {
                    throw new RuntimeException("injected store failure");
                }
            };
            StubClusterMetadataService service = StubClusterMetadataService.builder(Murmur3Partitioner.instance)
                                                                            .withSnapshots(failingSnapshots)
                                                                            .build();
            ClusterMetadataService.unsetInstance();
            ClusterMetadataService.setInstance(service);

            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            ClusterMetadata snapshot = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(4), Murmur3Partitioner.instance, schemaWithTables(10));
            Entry entry = new Entry(Entry.Id.NONE, snapshot.epoch, new ForceSnapshot(snapshot));
            ClusterMetadata previous = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
            Transformation.Result result = entry.transform.execute(previous);

            MetadataSnapshotListener listener = new MetadataSnapshotListener();
            // Must not throw: the listener is required to swallow storeSnapshot failures so the log-processing
            // thread survives a snapshot that cannot be stored.
            listener.notify(entry, result);

            assertEquals("The listener must increment snapshotStoreFailures on a caught storeSnapshot failure",
                         failuresBefore + 1, TCMMetrics.instance.snapshotStoreFailures.getCount());
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    /**
     * Covers the CASSANDRA-21664 size warning: once the serialised snapshot reaches 75% of the snapshot size limit
     * (the smaller of max_value_size and internode_max_message_size),
     * {@link MetadataSnapshots.SystemKeyspaceMetadataSnapshots#storeSnapshot} logs a rate-limited WARN. This test
     * lowers max_value_size to 4 MiB so a snapshot in the [75%, 100%) band stays small, and checks that the warning
     * fires and the snapshot still stores and reads back.
     */
    @Test
    public void warningBandSnapshotStoresSuccessfullyAndLogsWarning() throws IOException
    {
        truncateSnapshotsTable();
        Logger logger = null;
        ListAppender<ILoggingEvent> listAppender = null;
        int maxValueSize = DatabaseDescriptor.getMaxValueSize();
        try
        {
            DatabaseDescriptor.setMaxValueSize(4 * 1024 * 1024);
            // Advance NoSpamLogger's clock past the 5-minute suppression window so a warning logged by an earlier
            // test does not suppress this one.
            NoSpamLogger.unsafeSetClock(() -> Global.nanoTime() + TimeUnit.MINUTES.toNanos(10));

            // Attach a ListAppender to capture log output from MetadataSnapshots
            logger = (Logger) org.slf4j.LoggerFactory.getLogger(MetadataSnapshots.class);
            listAppender = new ListAppender<>();
            listAppender.start();
            logger.addAppender(listAppender);

            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            // Calculate table count to land in the warning band [75%, 100%) of the snapshot size limit. The
            // per-table serialised cost is measured directly rather than assumed: a hardcoded 527-byte
            // estimate held for the plain _jdkNN test variants but was ~39% low against the latest_jdkNN
            // variants (different dependency versions), pushing the snapshot past 100% of the limit and
            // into the oversize-throw path instead of the warning-band success path this test exercises.
            // Two measurements at different table counts isolate the per-table slope from the fixed
            // overhead of the rest of ClusterMetadata (schema keyspace, node states, epoch, ...), so the
            // estimate self-corrects under any JDK, dependency profile, or future metadata format change.
            int limit = MetadataSnapshots.SystemKeyspaceMetadataSnapshots.snapshotSizeLimit();
            assertEquals("Test setup: max_value_size must set the limit", 4 * 1024 * 1024, limit);
            int calibrationLow = 200;
            int calibrationHigh = 1000;
            long sizeAtLow = MetadataSnapshots.toBytes(ClusterMetadataTestHelper.minimalForTesting(
                Epoch.create(1), Murmur3Partitioner.instance, schemaWithTables(calibrationLow))).remaining();
            long sizeAtHigh = MetadataSnapshots.toBytes(ClusterMetadataTestHelper.minimalForTesting(
                Epoch.create(1), Murmur3Partitioner.instance, schemaWithTables(calibrationHigh))).remaining();
            double bytesPerTable = (sizeAtHigh - sizeAtLow) / (double) (calibrationHigh - calibrationLow);
            double overhead = sizeAtLow - calibrationLow * bytesPerTable;

            double targetFraction = 0.80; // 80% of the limit
            int tableCount = (int) ((limit * targetFraction - overhead) / bytesPerTable);

            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata warningBand = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(5), Murmur3Partitioner.instance, schemaWithTables(tableCount));

            // Must not throw: the snapshot is below the limit, so the store should succeed
            snapshots.storeSnapshot(warningBand);

            // Verify the snapshot stored successfully and can be retrieved
            assertEquals(warningBand, snapshots.getSnapshot(warningBand.epoch));

            assertEquals("A warning-band snapshot must store successfully and not increment the failure counter",
                         failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());

            // Guard: verify the serialised size actually landed in the warning band [75%, 100%)
            long recordedSize = TCMMetrics.instance.lastSnapshotSize.getValue();
            assertTrue("Serialised size " + recordedSize + " must be at least 75% of the limit " + limit,
                       recordedSize >= limit * 0.75);
            assertTrue("Serialised size " + recordedSize + " must be below the limit " + limit + " (test setup error if not)",
                       recordedSize < limit);

            boolean foundWarning = listAppender.list.stream()
                                                     .anyMatch(event -> event.getLevel() == Level.WARN
                                                                        && event.getFormattedMessage().contains("Serialised cluster metadata snapshot")
                                                                        && event.getFormattedMessage().contains("of the snapshot size limit"));
            assertTrue("Expected a WARN log about snapshot size approaching the limit, but none was found. " +
                       "Captured log events: " + listAppender.list.size(), foundWarning);
        }
        finally
        {
            DatabaseDescriptor.setMaxValueSize(maxValueSize);
            if (logger != null && listAppender != null)
                logger.detachAppender(listAppender);
            NoSpamLogger.unsafeSetClock(Global::nanoTime);
            truncateSnapshotsTable();
        }
    }
}
