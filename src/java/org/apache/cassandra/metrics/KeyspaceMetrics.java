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
package org.apache.cassandra.metrics;

import java.util.function.ToLongFunction;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.resolveShortMetricName;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class KeyspaceMetrics
{
    public static final String TYPE_NAME = "keyspace";
    /** Sum of live data bytes in current table memtables, excluding data structure overhead. */
    public final Gauge<Long> memtableLiveDataSize;
    /**
     * Sum of on-heap bytes owned by current table memtables, including allocator overhead and overwritten data.
     */
    public final Gauge<Long> memtableOnHeapDataSize;
    /**
     * Sum of off-heap bytes owned by current table memtables, including allocator overhead and overwritten data.
     */
    public final Gauge<Long> memtableOffHeapDataSize;
    /**
     * Live data bytes in current memtables across tables and their backing secondary-index tables, excluding
     * overhead and memtables pending flush.
     */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /**
     * On-heap bytes owned by current memtables across tables and their backing secondary-index tables. Excludes
     * memtables pending flush.
     */
    public final Gauge<Long> allMemtablesOnHeapDataSize;
    /**
     * Off-heap bytes owned by current memtables across tables and their backing secondary-index tables. Excludes
     * memtables pending flush.
     */
    public final Gauge<Long> allMemtablesOffHeapDataSize;
    /**
     * Sum of operations accumulated in current table memtables, as counted by PartitionUpdate.operationCount();
     * this is not a count of distinct live columns.
     */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Gauge<Long> memtableSwitchCount;
    /** Flushes started but not yet finished with post-flush processing. */
    public final Gauge<Long> pendingFlushes;
    /** Estimated remaining compaction tasks. */
    public final Gauge<Long> pendingCompactions;
    /** Physical bytes used by live SSTables. */
    public final Gauge<Long> liveDiskSpaceUsed;
    /**
     * Sum of each table's live physical SSTable bytes divided by the keyspace's full replication factor. This is a
     * local estimate, not a measured cluster-wide unique data size.
     */
    public final Gauge<Long> unreplicatedLiveDiskSpaceUsed;
    /** Uncompressed logical bytes used by live SSTables. */
    public final Gauge<Long> uncompressedLiveDiskSpaceUsed;
    /** Sum of each table's live uncompressed SSTable bytes divided by the keyspace's full replication factor. */
    public final Gauge<Long> unreplicatedUncompressedLiveDiskSpaceUsed;
    /** Physical bytes used by SSTables, including obsolete SSTables awaiting deletion. */
    public final Gauge<Long> totalDiskSpaceUsed;
    /** Off-heap bytes used by compression metadata for live SSTables. */
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** Local replica execution time for single-partition reads. */
    public final LatencyMetrics readLatency;
    /** Local replica execution time for partition-range reads. */
    public final LatencyMetrics rangeLatency;
    /**
     * Time to apply partition updates locally, including memtable and index updates; excludes waiting for replica
     * acknowledgements.
     */
    public final LatencyMetrics writeLatency;
    /** Number of SSTable data files accessed per local single-partition read. */
    public final Histogram sstablesPerReadHistogram;
    /** Number of SSTable data files accessed per local partition-range read. */
    public final Histogram sstablesPerRangeReadHistogram;
    /** Tombstones scanned per local read, after any purgeable tombstones have been removed. */
    public final Histogram tombstoneScannedHistogram;
    /** Purgeable tombstones encountered during local reads. */
    public final Histogram purgeableTombstoneScannedHistogram;
    /** Live rows scanned per local read. */
    public final Histogram liveScannedHistogram;
    /** Affected rows per local partition update. */
    public final Histogram rowsMutatedPerWriteHistogram;
    /**
     * Minimum absolute timestamp difference between overwritten cells in a partition update, in microseconds.
     * Omits updates without a previous cell and caps samples at 18165375903306.
     */
    public final Histogram colUpdateTimeDeltaHistogram;
    /** Time to acquire the partition lock for a materialized-view update. Recorded on the base table. */
    public final Timer viewLockAcquireTime;
    /** Time for the local read needed to construct a materialized-view update. Recorded on the base table. */
    public final Timer viewReadTime;
    /** Time spent in the Paxos prepare phase of compare-and-set operations. */
    public final LatencyMetrics casPrepare;
    /** Time spent in the Paxos propose phase of compare-and-set operations. */
    public final LatencyMetrics casPropose;
    /** Time spent in the Paxos commit phase of compare-and-set operations. */
    public final LatencyMetrics casCommit;
    /** Time for locally run key migrations between consensus systems. */
    public final LatencyMetrics keyMigration;
    /** Time to obtain Accord maximum-conflict information during key migration. */
    public final LatencyMetrics accordGetMaxConflicts;
    /** Time for range migrations performed by locally coordinated Accord repairs. */
    public final LatencyMetrics accordRepair;
    /** Time for Accord range migration after receiving streamed data. */
    public final LatencyMetrics accordPostStreamRepair;
    /** Unexpected failures of locally coordinated Accord repair range migrations. */
    public final Meter rangeMigrationUnexpectedFailures;
    /** Mutation rejections caused by routing a request to the wrong consensus system during migration. */
    public final Meter mutationsRejectedOnWrongSystem;
    /** Read rejections caused by routing a request to the wrong consensus system during migration. */
    public final Meter readsRejectedOnWrongSystem;
    /**
     * Writes that achieved the requested consistency level but failed to achieve the configured ideal consistency
     * level.
     */
    public final Counter writeFailedIdealCL;
    /** Write latency measured against the configured ideal consistency level. */
    public final LatencyMetrics idealCLWriteLatency;
    /** Speculative read retries sent to additional replicas. */
    public final Counter speculativeRetries;
    /** Reads that timed out despite sending a speculative retry. */
    public final Counter speculativeFailedRetries;
    /** Reads that needed speculation but had no additional eligible replica. */
    public final Counter speculativeInsufficientReplicas;
    /**
     * Writes for which the coordinator contacted additional replicas after the additional-write latency threshold
     * elapsed.
     */
    public final Counter additionalWrites;
    /** Repair jobs started as coordinator. */
    public final Counter repairsStarted;
    /** Repair jobs that finished as coordinator, including failed jobs. */
    public final Counter repairsCompleted;
    /** Duration of repair operations coordinated by this node. */
    public final Timer repairTime;
    /** Duration of the preparation phase of repair operations coordinated by this node. */
    public final Timer repairPrepareTime;
    /** Duration of anticompaction to separate repaired and unrepaired data. */
    public final Timer anticompactionTime;
    /** Time spent building Merkle trees for repair validation. */
    public final Timer validationTime;
    /** Time spent synchronizing data during repair. */
    public final Timer repairSyncTime;
    /** Approximate bytes read per repair validation. */
    public final Histogram bytesValidated;
    /** Partitions read per repair validation. */
    public final Histogram partitionsValidated;
    /** Lifetime count of reads for keys outside the node's owned token ranges for this keyspace **/
    public final Counter outOfRangeTokenReads;
    /** Lifetime count of writes for keys outside the node's owned token ranges for this keyspace **/
    public final Counter outOfRangeTokenWrites;
    /** Lifetime count of paxos requests for keys outside the node's owned token ranges for this keyspace **/
    public final Counter outOfRangeTokenPaxosRequests;

    /*
     * Metrics for inconsistencies detected between repaired data sets across replicas. These
     * are tracked on the coordinator.
     */

    /**
     * Repaired-data mismatches detected by the coordinator with no pending repair sessions that could explain the
     * mismatch.
     */
    public final Meter confirmedRepairedInconsistencies;
    /**
     * Repaired-data mismatches detected by the coordinator while pending repair sessions could explain different
     * repaired sets.
     */
    public final Meter unconfirmedRepairedInconsistencies;

    /** Extra repaired rows read on a replica to compare repaired-data digests after a digest mismatch. */
    public final Histogram repairedDataTrackingOverreadRows;
    /** Time spent on replica overreads needed for repaired-data digest comparison. */
    public final Timer repairedDataTrackingOverreadTime;

    /**
     * Read commands for which the coordinator reports a replica tombstone warning to the client; counts commands,
     * not replicas.
     */
    public final Meter clientTombstoneWarnings;
    /**
     * Read commands for which the coordinator reports a replica tombstone abort to the client; counts commands,
     * not replicas.
     */
    public final Meter clientTombstoneAborts;

    /** Read results that exceed the coordinator result-size warning threshold. */
    public final Meter coordinatorReadSizeWarnings;
    /** Read results rejected by the coordinator result-size abort threshold. */
    public final Meter coordinatorReadSizeAborts;
    /**
     * Result bytes accumulated at the coordinator when read-size checks run, including samples at size-triggered
     * aborts.
     */
    public final Histogram coordinatorReadSize;

    /** Read commands for which the coordinator reports a replica local-read-size warning. */
    public final Meter localReadSizeWarnings;
    /** Read commands for which the coordinator reports a replica local-read-size abort. */
    public final Meter localReadSizeAborts;
    /** Estimated data bytes accumulated by local replica reads while local read-size tracking is enabled. */
    public final Histogram localReadSize;

    /** Read commands for which the coordinator reports a replica row-index-size warning. */
    public final Meter rowIndexSizeWarnings;
    /** Read commands for which the coordinator reports a replica row-index-size abort. */
    public final Meter rowIndexSizeAborts;
    /**
     * Estimated in-memory bytes for materialized Big-format row-index entries when the row-index size check runs.
     */
    public final Histogram rowIndexSize;

    /**
     * Read commands for which the coordinator reports a replica warning about the number of SSTable indexes
     * accessed.
     */
    public final Meter tooManySSTableIndexesReadWarnings;
    /**
     * Read commands for which the coordinator reports a replica abort due to the number of SSTable indexes
     * accessed.
     */
    public final Meter tooManySSTableIndexesReadAborts;

    /** Writes for which the coordinator reports a mutation-size threshold warning. */
    public final Meter writeSizeWarnings;
    /** Writes for which the coordinator reports a mutation tombstone-count threshold warning. */
    public final Meter writeTombstoneWarnings;

    /** Bytes processed by anticompaction to split SSTables along repair ranges. */
    public final Meter bytesAnticompacted;
    /**
     * Bytes in SSTables wholly contained in repair ranges, whose repair status could change without rewriting
     * their data.
     */
    public final Meter bytesMutatedAnticompaction;
    /** Bytes examined during preview repair. */
    public final Meter bytesPreviewed;
    /** Token ranges with mismatching data detected by preview repair. */
    public final Meter tokenRangesPreviewedDesynchronized;
    /** Estimated bytes associated with mismatching data detected by preview repair. */
    public final Meter bytesPreviewedDesynchronized;

    /** Time to build the SSTable interval tree for a new tracker view while holding the tracker lock. */
    public final LatencyMetrics viewSSTableIntervalTree;

    public final ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> formatSpecificGauges;

    private final KeyspaceMetricNameFactory factory;
    private final Keyspace keyspace;

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param ks Keyspace to measure metrics
     */
    public KeyspaceMetrics(final Keyspace ks)
    {
        factory = new KeyspaceMetricNameFactory(ks);
        keyspace = ks;
        memtableColumnsCount = createKeyspaceGauge("MemtableColumnsCount",
                metric -> metric.memtableColumnsCount.getValue());
        memtableLiveDataSize = createKeyspaceGauge("MemtableLiveDataSize",
                metric -> metric.memtableLiveDataSize.getValue());
        memtableOnHeapDataSize = createKeyspaceGauge("MemtableOnHeapDataSize",
                metric -> metric.memtableOnHeapDataSize.getValue());
        memtableOffHeapDataSize = createKeyspaceGauge("MemtableOffHeapDataSize",
                metric -> metric.memtableOffHeapDataSize.getValue());
        allMemtablesLiveDataSize = createKeyspaceGauge("AllMemtablesLiveDataSize",
                metric -> metric.allMemtablesLiveDataSize.getValue());
        allMemtablesOnHeapDataSize = createKeyspaceGauge("AllMemtablesOnHeapDataSize",
                metric -> metric.allMemtablesOnHeapDataSize.getValue());
        allMemtablesOffHeapDataSize = createKeyspaceGauge("AllMemtablesOffHeapDataSize",
                metric -> metric.allMemtablesOffHeapDataSize.getValue());
        memtableSwitchCount = createKeyspaceGauge("MemtableSwitchCount",
                metric -> metric.memtableSwitchCount.getCount());
        pendingCompactions = createKeyspaceGauge("PendingCompactions", metric -> metric.pendingCompactions.getValue());
        pendingFlushes = createKeyspaceGauge("PendingFlushes", metric -> metric.pendingFlushes.getCount());

        liveDiskSpaceUsed = createKeyspaceGauge("LiveDiskSpaceUsed", metric -> metric.liveDiskSpaceUsed.getCount());
        uncompressedLiveDiskSpaceUsed = createKeyspaceGauge("UncompressedLiveDiskSpaceUsed", metric -> metric.uncompressedLiveDiskSpaceUsed.getCount());
        unreplicatedLiveDiskSpaceUsed = createKeyspaceGauge("UnreplicatedLiveDiskSpaceUsed",
                                                            metric -> metric.liveDiskSpaceUsed.getCount() / keyspace.getReplicationStrategy().getReplicationFactor().fullReplicas);
        unreplicatedUncompressedLiveDiskSpaceUsed = createKeyspaceGauge("UnreplicatedUncompressedLiveDiskSpaceUsed",
                                                                        metric -> metric.uncompressedLiveDiskSpaceUsed.getCount() / keyspace.getReplicationStrategy().getReplicationFactor().fullReplicas);
        totalDiskSpaceUsed = createKeyspaceGauge("TotalDiskSpaceUsed", metric -> metric.totalDiskSpaceUsed.getCount());

        compressionMetadataOffHeapMemoryUsed = createKeyspaceGauge("CompressionMetadataOffHeapMemoryUsed",
                metric -> metric.compressionMetadataOffHeapMemoryUsed.getValue());

        // latency metrics for TableMetrics to update
        readLatency = createLatencyMetrics("Read");
        writeLatency = createLatencyMetrics("Write");
        rangeLatency = createLatencyMetrics("Range");

        // create histograms for TableMetrics to replicate updates to
        sstablesPerReadHistogram = createKeyspaceHistogram("SSTablesPerReadHistogram", true);
        sstablesPerRangeReadHistogram = createKeyspaceHistogram("SSTablesPerRangeReadHistogram", true);
        tombstoneScannedHistogram = createKeyspaceHistogram("TombstoneScannedHistogram", false);
        purgeableTombstoneScannedHistogram = createKeyspaceHistogram("PurgeableTombstoneScannedHistogram", false);
        liveScannedHistogram = createKeyspaceHistogram("LiveScannedHistogram", false);
        rowsMutatedPerWriteHistogram = createKeyspaceHistogram("RowsMutatedPerWriteHistogram", false);
        colUpdateTimeDeltaHistogram = createKeyspaceHistogram("ColUpdateTimeDeltaHistogram", false);
        viewLockAcquireTime = createKeyspaceTimer("ViewLockAcquireTime");
        viewReadTime = createKeyspaceTimer("ViewReadTime");

        casPrepare = createLatencyMetrics("CasPrepare");
        casPropose = createLatencyMetrics("CasPropose");
        casCommit = createLatencyMetrics("CasCommit");
        keyMigration = createLatencyMetrics("KeyMigration");
        accordGetMaxConflicts = createLatencyMetrics("AccordGetMaxConflicts");
        accordRepair = createLatencyMetrics("AccordRepair");
        accordPostStreamRepair = createLatencyMetrics("AccordPostStreamRepair");
        rangeMigrationUnexpectedFailures = createKeyspaceMeter("RangeMigrationUnexpectedFailures");
        mutationsRejectedOnWrongSystem = createKeyspaceMeter("MutationsRejectedOnWrongSystem");
        readsRejectedOnWrongSystem = createKeyspaceMeter("ReadsRejectedOnWrongSystem");
        writeFailedIdealCL = createKeyspaceCounter("WriteFailedIdealCL");
        idealCLWriteLatency = createLatencyMetrics("IdealCLWrite");

        speculativeRetries = createKeyspaceCounter("SpeculativeRetries", metric -> metric.speculativeRetries.getCount());
        speculativeFailedRetries = createKeyspaceCounter("SpeculativeFailedRetries", metric -> metric.speculativeFailedRetries.getCount());
        speculativeInsufficientReplicas = createKeyspaceCounter("SpeculativeInsufficientReplicas", metric -> metric.speculativeInsufficientReplicas.getCount());
        additionalWrites = createKeyspaceCounter("AdditionalWrites", metric -> metric.additionalWrites.getCount());
        repairsStarted = createKeyspaceCounter("RepairJobsStarted", metric -> metric.repairsStarted.getCount());
        repairsCompleted = createKeyspaceCounter("RepairJobsCompleted", metric -> metric.repairsCompleted.getCount());
        repairTime =createKeyspaceTimer("RepairTime");
        repairPrepareTime = createKeyspaceTimer("RepairPrepareTime");
        anticompactionTime = createKeyspaceTimer("AntiCompactionTime");
        validationTime = createKeyspaceTimer("ValidationTime");
        repairSyncTime = createKeyspaceTimer("RepairSyncTime");
        partitionsValidated = createKeyspaceHistogram("PartitionsValidated", false);
        bytesValidated = createKeyspaceHistogram("BytesValidated", false);

        confirmedRepairedInconsistencies = createKeyspaceMeter("RepairedDataInconsistenciesConfirmed");
        unconfirmedRepairedInconsistencies = createKeyspaceMeter("RepairedDataInconsistenciesUnconfirmed");

        repairedDataTrackingOverreadRows = createKeyspaceHistogram("RepairedDataTrackingOverreadRows", false);
        repairedDataTrackingOverreadTime = createKeyspaceTimer("RepairedDataTrackingOverreadTime");

        clientTombstoneWarnings = createKeyspaceMeter("ClientTombstoneWarnings");
        clientTombstoneAborts = createKeyspaceMeter("ClientTombstoneAborts");

        coordinatorReadSizeWarnings = createKeyspaceMeter("CoordinatorReadSizeWarnings");
        coordinatorReadSizeAborts = createKeyspaceMeter("CoordinatorReadSizeAborts");
        coordinatorReadSize = createKeyspaceHistogram("CoordinatorReadSize", false);

        localReadSizeWarnings = createKeyspaceMeter("LocalReadSizeWarnings");
        localReadSizeAborts = createKeyspaceMeter("LocalReadSizeAborts");
        localReadSize = createKeyspaceHistogram("LocalReadSize", false);

        rowIndexSizeWarnings = createKeyspaceMeter("RowIndexSizeWarnings");
        rowIndexSizeAborts = createKeyspaceMeter("RowIndexSizeAborts");
        rowIndexSize = createKeyspaceHistogram("RowIndexSize", false);

        tooManySSTableIndexesReadWarnings = createKeyspaceMeter("TooManySSTableIndexesReadWarnings");
        tooManySSTableIndexesReadAborts = createKeyspaceMeter("TooManySSTableIndexesReadAborts");

        writeSizeWarnings = createKeyspaceMeter("WriteSizeWarnings");
        writeTombstoneWarnings = createKeyspaceMeter("WriteTombstoneWarnings");

        formatSpecificGauges = createFormatSpecificGauges(keyspace);

        outOfRangeTokenReads = createKeyspaceCounter("ReadOutOfRangeToken");
        outOfRangeTokenWrites = createKeyspaceCounter("WriteOutOfRangeToken");
        outOfRangeTokenPaxosRequests = createKeyspaceCounter("PaxosOutOfRangeToken");

        viewSSTableIntervalTree = createLatencyMetrics("ViewSSTableIntervalTree");
        bytesAnticompacted =  createKeyspaceMeter("BytesAnticompacted");
        bytesMutatedAnticompaction = createKeyspaceMeter("BytesMutatedAnticompaction");
        bytesPreviewed = createKeyspaceMeter("BytesPreviewed");
        tokenRangesPreviewedDesynchronized = createKeyspaceMeter("TokenRangesPreviewedDesynchronized");
        bytesPreviewedDesynchronized = createKeyspaceMeter("BytesPreviewedDesynchronized");
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        Metrics.removeIfMatch(fullName -> resolveShortMetricName(fullName,
                                                                 KeyspaceMetricNameFactory.GROUP_NAME,
                                                                 TYPE_NAME,
                                                                 factory.scope()),
                              factory::createMetricName, m -> {});
    }

    private ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> createFormatSpecificGauges(Keyspace keyspace)
    {
        ImmutableMap.Builder<SSTableFormat<? ,?>, ImmutableMap<String, Gauge<? extends Number>>> builder = ImmutableMap.builder();
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            ImmutableMap.Builder<String, Gauge<? extends Number>> gauges = ImmutableMap.builder();
            for (GaugeProvider<?> gaugeProvider : format.getFormatSpecificMetricsProviders().getGaugeProviders())
            {
                String finalName = gaugeProvider.name;
                Gauge<? extends Number> gauge = Metrics.register(factory.createMetricName(finalName), gaugeProvider.getKeyspaceGauge(keyspace));
                gauges.put(gaugeProvider.name, gauge);
            }
            builder.put(format, gauges.build());
        }
        return builder.build();
    }

    /**
     * Creates a gauge that will sum the current value of a metric for all column families in this keyspace
     *
     * @param name the name of the metric being created
     * @param extractor a function that produces a specified metric value for a given table
     *
     * @return Gauge&gt;Long> that computes sum of MetricValue.getValue()
     */
    private Gauge<Long> createKeyspaceGauge(String name, final ToLongFunction<TableMetrics> extractor)
    {
        return Metrics.register(factory.createMetricName(name), new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : keyspace.getColumnFamilyStores())
                {
                    sum += extractor.applyAsLong(cf.metric);
                }
                return sum;
            }
        });
    }

    /**
     * Creates a counter that will sum the current value of a metric for all column families in this keyspace
     * @param name
     * @param extractor
     * @return Counter that computes sum of MetricValue.getValue()
     */
    private Counter createKeyspaceCounter(String name, final ToLongFunction<TableMetrics> extractor)
    {
        return Metrics.register(factory.createMetricName(name), new Counter()
        {
            @Override
            public long getCount()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : keyspace.getColumnFamilyStores())
                {
                    sum += extractor.applyAsLong(cf.metric);
                }
                return sum;
            }
        });
    }

    protected Counter createKeyspaceCounter(String name)
    {
        return Metrics.counter(factory.createMetricName(name));
    }

    protected Histogram createKeyspaceHistogram(String name, boolean considerZeroes)
    {
        return Metrics.histogram(factory.createMetricName(name), considerZeroes);
    }

    protected Timer createKeyspaceTimer(String name)
    {
        return Metrics.timer(factory.createMetricName(name));
    }

    protected Meter createKeyspaceMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }

    private LatencyMetrics createLatencyMetrics(String name)
    {
        return new LatencyMetrics(factory, name);
    }

    static class KeyspaceMetricNameFactory implements MetricNameFactory
    {
        public static final String GROUP_NAME = TableMetrics.class.getPackage().getName();
        private final String keyspaceName;

        KeyspaceMetricNameFactory(Keyspace ks)
        {
            this.keyspaceName = ks.getName();
        }

        public String scope()
        {
            return keyspaceName;
        }

        @Override
        public MetricName createMetricName(String metricName)
        {
            assert metricName.indexOf('.') == -1 : String.format("Metric name '%s' should not contain '.'", metricName);
            return new MetricName(GROUP_NAME, TYPE_NAME, metricName, scope(),
                                  GROUP_NAME + ':' +
                                  "type=" + "Keyspace" +
                                  ",keyspace=" + scope() +
                                  ",name=" + metricName);
        }
    }
}
