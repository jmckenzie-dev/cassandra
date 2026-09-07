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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.resolveShortMetricName;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class TableMetrics
{
    public static final String TYPE_NAME = "Table";
    public static final String INDEX_TYPE_NAME = "IndexTable";
    public static final String ALIAS_TYPE_NAME = "ColumnFamily";
    public static final String INDEX_ALIAS_TYPE_NAME = "IndexColumnFamily";
    /**
     * stores metrics that will be rolled into a single global metric
     */
    private static final ConcurrentMap<String, Set<Metric>> ALL_TABLE_METRICS = Maps.newConcurrentMap();
    public static final long[] EMPTY = new long[0];
    private static final MetricNameFactory GLOBAL_FACTORY = new AllTableMetricNameFactory(TYPE_NAME);
    private static final MetricNameFactory GLOBAL_ALIAS_FACTORY = new AllTableMetricNameFactory(ALIAS_TYPE_NAME);

    public final static LatencyMetrics GLOBAL_READ_LATENCY = new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Read");
    public final static LatencyMetrics GLOBAL_WRITE_LATENCY = new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Write");
    public final static LatencyMetrics GLOBAL_RANGE_LATENCY = new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Range");
    public final static LatencyMetrics GLOBAL_KEY_MIGRATION_LATENCY = new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "KeyMigration");
    public final static LatencyMetrics GLOBAL_RANGE_MIGRATION_LATENCY = new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "RangeMigration");

    /** On-heap bytes owned by the current memtable, including allocator overhead and overwritten data. */
    public final Gauge<Long> memtableOnHeapDataSize;
    /** Off-heap bytes owned by the current memtable, including allocator overhead and overwritten data. */
    public final Gauge<Long> memtableOffHeapDataSize;
    /** Live data bytes in the current memtable, excluding data structure overhead. */
    public final Gauge<Long> memtableLiveDataSize;
    /**
     * On-heap bytes owned by current memtables for the table and its backing secondary-index tables. Excludes
     * memtables pending flush.
     */
    public final Gauge<Long> allMemtablesOnHeapDataSize;
    /**
     * Off-heap bytes owned by current memtables for the table and its backing secondary-index tables. Excludes
     * memtables pending flush.
     */
    public final Gauge<Long> allMemtablesOffHeapDataSize;
    /**
     * Live data bytes in current memtables for the table and its backing secondary-index tables, excluding
     * overhead and memtables pending flush.
     */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /**
     * Operations accumulated in the current memtable, as counted by PartitionUpdate.operationCount(); this is not
     * a count of distinct live columns.
     */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Counter memtableSwitchCount;
    /**
     * Compressed data bytes divided by uncompressed data bytes across compressed canonical SSTables. Returns -1
     * when no eligible data exists.
     */
    public final Gauge<Double> compressionRatio;
    /**
     * Combined SSTable metadata histogram of estimated partition sizes in bytes. Excludes memtables; repeated
     * partitions in different SSTables contribute separately.
     */
    public final Gauge<long[]> estimatedPartitionSizeHistogram;
    /**
     * Approximate partition count from canonical SSTables plus all memtables. Overlapping partitions can make this
     * differ from the distinct logical key count.
     */
    public final Gauge<Long> estimatedPartitionCount;
    /** Combined SSTable metadata histogram of estimated cells per partition. Excludes memtables. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Number of SSTable data files accessed per local single-partition read. */
    public final TableHistogram sstablesPerReadHistogram;
    /** Number of SSTable data files accessed per local partition-range read. */
    public final TableHistogram sstablesPerRangeReadHistogram;
    /** Local replica execution time for single-partition reads. */
    public final LatencyMetrics readLatency;
    /** Local replica execution time for partition-range reads. */
    public final LatencyMetrics rangeLatency;
    /**
     * Time to apply partition updates locally, including memtable and index updates; excludes waiting for replica
     * acknowledgements.
     */
    public final LatencyMetrics writeLatency;
    /** Flushes started but not yet finished with post-flush processing. */
    public final Counter pendingFlushes;
    /** Total number of bytes flushed since server [re]start */
    public final Counter bytesFlushed;
    /** The average on-disk flushed size for sstables. */
    public final MovingAverage flushSizeOnDisk;
    /** Total number of bytes written by compaction since server [re]start */
    public final Counter compactionBytesWritten;
    /** Estimated remaining compaction tasks. */
    public final Gauge<Integer> pendingCompactions;
    /** Number of live SSTables. */
    public final Gauge<Integer> liveSSTableCount;
    /** Live SSTables whose format version is older than the latest version. */
    public final Gauge<Integer> oldVersionSSTableCount;
    /**
     * Largest difference between maximum and minimum timestamps in a live SSTable, in milliseconds. Returns
     * zero when no eligible SSTable exists.
     */
    public final Gauge<Long> maxSSTableDuration;
    /** Largest physical size of a live SSTable, including all its components, in bytes. */
    public final Gauge<Long> maxSSTableSize;
    /** Physical bytes used by live SSTables. */
    public final Counter liveDiskSpaceUsed;
    /** Uncompressed logical bytes used by live SSTables. */
    public final Counter uncompressedLiveDiskSpaceUsed;
    /** Physical bytes used by SSTables, including obsolete SSTables awaiting deletion. */
    public final Counter totalDiskSpaceUsed;
    /**
     * Smallest estimated partition size in canonical SSTable metadata, in bytes; zero when no SSTables exist.
     */
    public final Gauge<Long> minPartitionSize;
    /** Largest estimated partition size in canonical SSTable metadata, in bytes; zero when no SSTables exist. */
    public final Gauge<Long> maxPartitionSize;
    /**
     * Mean estimated partition size in canonical SSTables, weighted by each SSTable histogram's sample count, in
     * bytes; zero when no samples exist.
     */
    public final Gauge<Long> meanPartitionSize;
    /** Bytes occupied by cached compression dictionaries. */
    public final Gauge<Long> compressionDictionariesMemoryUsed;
    /** Off-heap bytes used by compression metadata for live SSTables. */
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** Tombstones scanned per local read, after any purgeable tombstones have been removed. */
    public final TableHistogram tombstoneScannedHistogram;
    /** Purgeable tombstones encountered during local reads. */
    public final TableHistogram purgeableTombstoneScannedHistogram;
    /** Live rows scanned per local read. */
    public final TableHistogram liveScannedHistogram;
    /** Total number of live rows read from this CF (cumulative counter, suitable for rate/windowed calculations) */
    public final Counter totalRowsRead;
    /** Total number of rows mutated in writes to this CF (cumulative counter, suitable for rate/windowed calculations) */
    public final Counter totalRowsMutated;
    /** Affected rows per local partition update. */
    public final TableHistogram rowsMutatedPerWriteHistogram;
    /**
     * Minimum absolute timestamp difference between overwritten cells in a partition update, in microseconds.
     * Omits updates without a previous cell and caps samples at 18165375903306.
     */
    public final TableHistogram colUpdateTimeDeltaHistogram;
    /** Time to acquire the partition lock for a materialized-view update. Recorded on the base table. */
    public final TableTimer viewLockAcquireTime;
    /** Time for the local read needed to construct a materialized-view update. Recorded on the base table. */
    public final TableTimer viewReadTime;
    /** Additional disk bytes retained by snapshots, excluding files still present in the live table. */
    public final Gauge<Long> trueSnapshotsSize;
    /** Row-cache entries found but unable to cover the requested rows, requiring a regular read. */
    public final Counter rowCacheHitOutOfRange;
    /** Number of row cache hits */
    public final Counter rowCacheHit;
    /** Number of row cache misses */
    public final Counter rowCacheMiss;
    /** Local replica reads aborted because the tombstone failure threshold was exceeded. */
    public final Counter tombstoneFailures;
    /** Local replica reads exceeding the tombstone warning threshold but below the failure threshold. */
    public final Counter tombstoneWarnings;
    /** Time spent in the Paxos prepare phase of compare-and-set operations. */
    public final LatencyMetrics casPrepare;
    /** Time spent in the Paxos propose phase of compare-and-set operations. */
    public final LatencyMetrics casPropose;
    /** Time spent in the Paxos commit phase of compare-and-set operations. */
    public final LatencyMetrics casCommit;
    /** Time for locally run key migrations between consensus systems. */
    public final LatencyMetrics keyMigration;
    /** Time for range migrations performed by locally coordinated Accord repairs. */
    public final LatencyMetrics accordRepair;
    /** Time for Accord range migration after receiving streamed data. */
    public final LatencyMetrics accordPostStreamRepair;
    /** Unexpected failures of locally coordinated Accord repair range migrations. */
    public final TableMeter accordRepairUnexpectedFailures;
    /** Mutation rejections caused by routing a request to the wrong consensus system during migration. */
    public final TableMeter mutationsRejectedOnWrongSystem;
    /** Read rejections caused by routing a request to the wrong consensus system during migration. */
    public final TableMeter readsRejectedOnWrongSystem;
    /** Percentage of canonical SSTable uncompressed bytes marked repaired. Returns 100 for an empty table. */
    public final Gauge<Double> percentRepaired;
    /** Uncompressed bytes in canonical SSTables marked repaired. */
    public final Gauge<Long> bytesRepaired;
    /** Uncompressed bytes in canonical SSTables that are neither repaired nor pending repair. */
    public final Gauge<Long> bytesUnrepaired;
    /** Uncompressed bytes in canonical SSTables assigned to a pending repair. */
    public final Gauge<Long> bytesPendingRepair;
    /** Repair jobs started as coordinator. */
    public final Counter repairsStarted;
    /** Repair jobs that finished as coordinator, including failed jobs. */
    public final Counter repairsCompleted;
    /** Duration of anticompaction to separate repaired and unrepaired data. */
    public final TableTimer anticompactionTime;
    /** Time spent building Merkle trees for repair validation. */
    public final TableTimer validationTime;
    /** Time spent synchronizing data during repair. */
    public final TableTimer repairSyncTime;
    /** Approximate bytes read per repair validation. */
    public final TableHistogram bytesValidated;
    /** Partitions read per repair validation. */
    public final TableHistogram partitionsValidated;
    /** Bytes processed by anticompaction to split SSTables along repair ranges. */
    public final TableMeter bytesAnticompacted;
    /**
     * Bytes in SSTables wholly contained in repair ranges, whose repair status could change without rewriting
     * their data.
     */
    public final TableMeter bytesMutatedAnticompaction;
    /** Bytes examined during preview repair. */
    public final TableMeter bytesPreviewed;
    /** Token ranges with mismatching data detected by preview repair. */
    public final TableMeter tokenRangesPreviewedDesynchronized;
    /** Estimated bytes associated with mismatching data detected by preview repair. */
    public final TableMeter bytesPreviewedDesynchronized;
    /**
     * Fraction of processed bytes whose repair status changed without rewriting: mutated bytes divided by mutated
     * plus anticompacted bytes. Returns zero before activity.
     */
    public final Gauge<Double> mutatedAnticompactionGauge;

    /** Coordinator duration of single-partition reads, including communication with replicas. */
    public final SnapshottingTimer coordinatorReadLatency;
    /** Coordinator duration of partition-range reads, including communication with replicas. */
    public final Timer coordinatorScanLatency;
    /** Coordinator duration of writes, including waiting for the requested consistency level. */
    public final SnapshottingTimer coordinatorWriteLatency;

    private final TableMetricNameFactory factory;
    private final TableMetricNameFactory aliasFactory;

    /** Speculative read retries sent to additional replicas. */
    public final Counter speculativeRetries;
    /** Reads that timed out despite sending a speculative retry. */
    public final Counter speculativeFailedRetries;
    /** Reads that needed speculation but had no additional eligible replica. */
    public final Counter speculativeInsufficientReplicas;
    /**
     * Current speculative-read threshold derived from coordinator read latency and speculative_retry, in
     * nanoseconds.
     */
    public final Gauge<Long> speculativeSampleLatencyNanos;

    /**
     * Writes for which the coordinator contacted additional replicas after the additional-write latency threshold
     * elapsed.
     */
    public final Counter additionalWrites;
    /**
     * Current additional-write threshold derived from coordinator write latency and additional_write_policy, in
     * nanoseconds.
     */
    public final Gauge<Long> additionalWriteLatencyNanos;

    /** Number of level-zero SSTables under leveled compaction. Returns zero for other compaction strategies. */
    public final Gauge<Integer> unleveledSSTables;

    /**
     * Repaired-data mismatches detected by the coordinator with no pending repair sessions that could explain the
     * mismatch.
     */
    public final TableMeter confirmedRepairedInconsistencies;
    /**
     * Repaired-data mismatches detected by the coordinator while pending repair sessions could explain different
     * repaired sets.
     */
    public final TableMeter unconfirmedRepairedInconsistencies;

    /** Extra repaired rows read on a replica to compare repaired-data digests after a digest mismatch. */
    public final TableHistogram repairedDataTrackingOverreadRows;
    /** Time spent on replica overreads needed for repaired-data digest comparison. */
    public final TableTimer repairedDataTrackingOverreadTime;

    /** When sampler activated, will track the most frequently read partitions **/
    public final Sampler<ByteBuffer> topReadPartitionFrequency;
    /** When sampler activated, will track the most frequently written to partitions **/
    public final Sampler<ByteBuffer> topWritePartitionFrequency;
    /** When sampler activated, will track the largest mutations **/
    public final Sampler<ByteBuffer> topWritePartitionSize;
    /** When sampler activated, will track the most frequent partitions with cas contention **/
    public final Sampler<ByteBuffer> topCasPartitionContention;
    /** When sampler activated, will track the slowest local reads **/
    public final Sampler<String> topLocalReadQueryTime;
    /** When sampler activated, will track partitions read with the most rows **/
    public final Sampler<ByteBuffer> topReadPartitionRowCount;
    /** When sampler activated, will track partitions read with the most tombstones **/
    public final Sampler<ByteBuffer> topReadPartitionTombstoneCount;
    /** When sample activated, will track partitions read with the most merged sstables **/
    public final Sampler<ByteBuffer> topReadPartitionSSTableCount;

    /**
     * Read commands for which the coordinator reports a replica tombstone warning to the client; counts commands,
     * not replicas.
     */
    public final TableMeter clientTombstoneWarnings;
    /**
     * Read commands for which the coordinator reports a replica tombstone abort to the client; counts commands,
     * not replicas.
     */
    public final TableMeter clientTombstoneAborts;

    /** Read results that exceed the coordinator result-size warning threshold. */
    public final TableMeter coordinatorReadSizeWarnings;
    /** Read results rejected by the coordinator result-size abort threshold. */
    public final TableMeter coordinatorReadSizeAborts;
    /**
     * Result bytes accumulated at the coordinator when read-size checks run, including samples at size-triggered
     * aborts.
     */
    public final TableHistogram coordinatorReadSize;

    /** Read commands for which the coordinator reports a replica local-read-size warning. */
    public final TableMeter localReadSizeWarnings;
    /** Read commands for which the coordinator reports a replica local-read-size abort. */
    public final TableMeter localReadSizeAborts;
    /** Estimated data bytes accumulated by local replica reads while local read-size tracking is enabled. */
    public final TableHistogram localReadSize;

    /** Read commands for which the coordinator reports a replica row-index-size warning. */
    public final TableMeter rowIndexSizeWarnings;
    /** Read commands for which the coordinator reports a replica row-index-size abort. */
    public final TableMeter rowIndexSizeAborts;
    /**
     * Estimated in-memory bytes for materialized Big-format row-index entries when the row-index size check runs.
     */
    public final TableHistogram rowIndexSize;

    /**
     * Read commands for which the coordinator reports a replica warning about the number of SSTable indexes
     * accessed.
     */
    public final TableMeter tooManySSTableIndexesReadWarnings;
    /**
     * Read commands for which the coordinator reports a replica abort due to the number of SSTable indexes
     * accessed.
     */
    public final TableMeter tooManySSTableIndexesReadAborts;

    /** Writes for which the coordinator reports a mutation-size threshold warning. */
    public final TableMeter writeSizeWarnings;
    /** Writes for which the coordinator reports a mutation tombstone-count threshold warning. */
    public final TableMeter writeTombstoneWarnings;

    public final ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> formatSpecificGauges;

    /** Time to build the SSTable interval tree for a new tracker view while holding the tracker lock. */
    public final LatencyMetrics viewSSTableIntervalTree;

    private static Pair<Long, Long> totalNonSystemTablesSize(Predicate<SSTableReader> predicate)
    {
        long total = 0;
        long filtered = 0;
        for (String keyspace : Schema.instance.distributedKeyspaces().names())
        {

            Keyspace k = Schema.instance.getKeyspaceInstance(keyspace);
            if (SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(k.getName()))
                continue;
            if (k.getMetadata().params.replication.isMeta())
                continue;
            if (k.getReplicationStrategy().getReplicationFactor().allReplicas < 2)
                continue;

            for (ColumnFamilyStore cf : k.getColumnFamilyStores())
            {
                if (!SecondaryIndexManager.isIndexColumnFamily(cf.name))
                {
                    for (SSTableReader sstable : cf.getSSTables(SSTableSet.CANONICAL))
                    {
                        if (predicate.test(sstable))
                        {
                            filtered += sstable.uncompressedLength();
                        }
                        total += sstable.uncompressedLength();
                    }
                }
            }
        }
        return Pair.create(filtered, total);
    }

    public static final Gauge<Double> globalPercentRepaired = Metrics.register(GLOBAL_FACTORY.createMetricName("PercentRepaired"),
                                                                               new Gauge<Double>()
    {
        public Double getValue()
        {
            Pair<Long, Long> result = totalNonSystemTablesSize(SSTableReader::isRepaired);
            double repaired = result.left;
            double total = result.right;
            return total > 0 ? (repaired / total) * 100 : 100.0;
        }
    });

    public static final Gauge<Long> globalBytesRepaired = Metrics.register(GLOBAL_FACTORY.createMetricName("BytesRepaired"),
                                                                           () -> totalNonSystemTablesSize(SSTableReader::isRepaired).left);

    public static final Gauge<Long> globalBytesUnrepaired = 
        Metrics.register(GLOBAL_FACTORY.createMetricName("BytesUnrepaired"),
                         () -> totalNonSystemTablesSize(s -> !s.isRepaired() && !s.isPendingRepair()).left);

    public static final Gauge<Long> globalBytesPendingRepair = 
        Metrics.register(GLOBAL_FACTORY.createMetricName("BytesPendingRepair"),
                         () -> totalNonSystemTablesSize(SSTableReader::isPendingRepair).left);

    /** Replica requests sent to repair inconsistent data during reads. */
    public final Meter readRepairRequests;
    /** Additional replica read requests used to fetch missing rows or partitions after short reads. */
    public final Meter shortReadProtectionRequests;
    
    /** Additional replica requests used to complete results during replica filtering protection. */
    public final Meter replicaFilteringProtectionRequests;
    
    /**
     * Maximum rows cached at once by replica filtering protection per query. With no replica divergence, this
     * equals the largest cached partition during the query. Use this to tune replica_filtering_protection
     * thresholds in cassandra.yaml.
     */
    public final Histogram rfpRowsCachedPerQuery;

    public final EnumMap<SamplerType, Sampler<?>> samplers;

    private interface GetHistogram
    {
        EstimatedHistogram getHistogram(SSTableReader reader);
    }

    private static long[] combineHistograms(Iterable<SSTableReader> sstables, GetHistogram getHistogram)
    {
        Iterator<SSTableReader> iterator = sstables.iterator();
        if (!iterator.hasNext())
        {
            return ArrayUtils.EMPTY_LONG_ARRAY;
        }
        long[] firstBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
        long[] values = Arrays.copyOf(firstBucket, firstBucket.length);

        while (iterator.hasNext())
        {
            long[] nextBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
            values = addHistogram(values, nextBucket);
        }
        return values;
    }

    @VisibleForTesting
    public static long[] addHistogram(long[] sums, long[] buckets)
    {
        if (buckets.length > sums.length)
        {
            sums = Arrays.copyOf(sums, buckets.length);
        }

        for (int i = 0; i < buckets.length; i++)
        {
            sums[i] += buckets[i];
        }
        return sums;
    }

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs ColumnFamilyStore to measure metrics
     */
    public TableMetrics(final ColumnFamilyStore cfs)
    {
        factory = new TableMetricNameFactory(cfs, cfs.isIndex() ? INDEX_TYPE_NAME : TYPE_NAME);
        aliasFactory = new TableMetricNameFactory(cfs, cfs.isIndex() ? INDEX_ALIAS_TYPE_NAME : ALIAS_TYPE_NAME);

        samplers = new EnumMap<>(SamplerType.class);
        topReadPartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionSize = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topCasPartitionContention = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topLocalReadQueryTime = new MaxSampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };

        topReadPartitionRowCount = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        topReadPartitionTombstoneCount = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        topReadPartitionSSTableCount = new MaxSampler<ByteBuffer>()
        {
            @Override
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        samplers.put(SamplerType.READS, topReadPartitionFrequency);
        samplers.put(SamplerType.WRITES, topWritePartitionFrequency);
        samplers.put(SamplerType.WRITE_SIZE, topWritePartitionSize);
        samplers.put(SamplerType.CAS_CONTENTIONS, topCasPartitionContention);
        samplers.put(SamplerType.LOCAL_READ_TIME, topLocalReadQueryTime);
        samplers.put(SamplerType.READ_ROW_COUNT, topReadPartitionRowCount);
        samplers.put(SamplerType.READ_TOMBSTONE_COUNT, topReadPartitionTombstoneCount);
        samplers.put(SamplerType.READ_SSTABLE_COUNT, topReadPartitionSSTableCount);

        memtableColumnsCount = createTableGauge("MemtableColumnsCount", 
                                                () -> cfs.getTracker().getView().getCurrentMemtable().operationCount());

        // MemtableOnHeapSize naming deprecated in 4.0
        memtableOnHeapDataSize = createTableGaugeWithDeprecation("MemtableOnHeapDataSize", "MemtableOnHeapSize", 
                                                                 () -> Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOnHeap,
                                                                 new GlobalTableGauge("MemtableOnHeapDataSize"));

        // MemtableOffHeapSize naming deprecated in 4.0
        memtableOffHeapDataSize = createTableGaugeWithDeprecation("MemtableOffHeapDataSize", "MemtableOffHeapSize", 
                                                                  () -> Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOffHeap,
                                                                  new GlobalTableGauge("MemtableOnHeapDataSize"));
        
        memtableLiveDataSize = createTableGauge("MemtableLiveDataSize", 
                                                () -> cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());

        // AllMemtablesHeapSize naming deprecated in 4.0
        allMemtablesOnHeapDataSize = createTableGaugeWithDeprecation("AllMemtablesOnHeapDataSize", "AllMemtablesHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return getMemoryUsageWithIndexes(cfs).ownsOnHeap;
            }
        }, new GlobalTableGauge("AllMemtablesOnHeapDataSize"));

        // AllMemtablesOffHeapSize naming deprecated in 4.0
        allMemtablesOffHeapDataSize = createTableGaugeWithDeprecation("AllMemtablesOffHeapDataSize", "AllMemtablesOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return getMemoryUsageWithIndexes(cfs).ownsOffHeap;
            }
        }, new GlobalTableGauge("AllMemtablesOffHeapDataSize"));
        allMemtablesLiveDataSize = createTableGauge("AllMemtablesLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                return size;
            }
        });
        memtableSwitchCount = createTableCounter("MemtableSwitchCount");
        estimatedPartitionSizeHistogram = createTableGauge("EstimatedPartitionSizeHistogram", "EstimatedRowSizeHistogram",
                                                           () -> combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL),
                                                                                   SSTableReader::getEstimatedPartitionSize), null);
        
        estimatedPartitionCount = createTableGauge("EstimatedPartitionCount", "EstimatedRowCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                long memtablePartitions = 0;
                for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                   memtablePartitions += memtable.partitionCount();
                try(ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
                {
                    return SSTableReader.getApproximateKeyCount(refViewFragment.sstables) + memtablePartitions;
                }
            }
        }, null);
        estimatedColumnCountHistogram = createTableGauge("EstimatedColumnCountHistogram", "EstimatedColumnCountHistogram",
                                                         () -> combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), 
                                                                                 SSTableReader::getEstimatedCellPerPartitionCount), null);
        
        sstablesPerReadHistogram = createTableHistogram("SSTablesPerReadHistogram", cfs.keyspace.metric.sstablesPerReadHistogram, true);
        sstablesPerRangeReadHistogram = createTableHistogram("SSTablesPerRangeReadHistogram", cfs.keyspace.metric.sstablesPerRangeReadHistogram, true);
        compressionRatio = createTableGauge("CompressionRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                return computeCompressionRatio(cfs.getSSTables(SSTableSet.CANONICAL));
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                List<SSTableReader> sstables = new ArrayList<>();
                Keyspace.all().forEach(ks -> sstables.addAll(ks.getAllSSTables(SSTableSet.CANONICAL)));
                return computeCompressionRatio(sstables);
            }
        });
        percentRepaired = createTableGauge("PercentRepaired", new Gauge<Double>()
        {
            public Double getValue()
            {
                double repaired = 0;
                double total = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.isRepaired())
                    {
                        repaired += sstable.uncompressedLength();
                    }
                    total += sstable.uncompressedLength();
                }
                return total > 0 ? (repaired / total) * 100 : 100.0;
            }
        });

        bytesRepaired = createTableGauge("BytesRepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isRepaired))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesUnrepaired = createTableGauge("BytesUnrepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), s -> !s.isRepaired() && !s.isPendingRepair()))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesPendingRepair = createTableGauge("BytesPendingRepair", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isPendingRepair))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        readLatency = createLatencyMetrics("Read", cfs.keyspace.metric.readLatency, GLOBAL_READ_LATENCY);
        writeLatency = createLatencyMetrics("Write", cfs.keyspace.metric.writeLatency, GLOBAL_WRITE_LATENCY);
        rangeLatency = createLatencyMetrics("Range", cfs.keyspace.metric.rangeLatency, GLOBAL_RANGE_LATENCY);

        pendingFlushes = createTableCounter("PendingFlushes");
        bytesFlushed = createTableCounter("BytesFlushed");
        flushSizeOnDisk = ExpMovingAverage.decayBy1000();

        compactionBytesWritten = createTableCounter("CompactionBytesWritten");
        pendingCompactions = createTableGauge("PendingCompactions", () -> cfs.getCompactionStrategyManager().getEstimatedRemainingTasks());
        liveSSTableCount = createTableGauge("LiveSSTableCount", () -> cfs.getTracker().getView().liveSSTables().size());
        oldVersionSSTableCount = createTableGauge("OldVersionSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                int count = 0;
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    if (!sstable.descriptor.version.isLatestVersion())
                        count++;
                return count;
            }
        });
        maxSSTableDuration = createTableGauge("MaxSSTableDuration", new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                return cfs.getTracker()
                          .getView()
                          .liveSSTables()
                          .stream()
                          .filter(sstable -> sstable.getMinTimestamp() != Long.MAX_VALUE && sstable.getMaxTimestamp() != Long.MAX_VALUE)
                          .map(ssTableReader -> ssTableReader.getMaxTimestamp() - ssTableReader.getMinTimestamp())
                          .max(Long::compare)
                          .orElse(0L) / 1000;
            }
        });
        maxSSTableSize = createTableGauge("MaxSSTableSize", new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                return cfs.getTracker()
                          .getView()
                          .liveSSTables()
                          .stream()
                          .map(SSTableReader::bytesOnDisk)
                          .max(Long::compare)
                          .orElse(0L);
            }
        });
        liveDiskSpaceUsed = createTableCounter("LiveDiskSpaceUsed");
        uncompressedLiveDiskSpaceUsed = createTableCounter("UncompressedLiveDiskSpaceUsed");
        totalDiskSpaceUsed = createTableCounter("TotalDiskSpaceUsed");
        minPartitionSize = createTableGauge("MinPartitionSize", "MinRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (min == 0 || sstable.getEstimatedPartitionSize().min() < min)
                        min = sstable.getEstimatedPartitionSize().min();
                }
                return min;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long min = Long.MAX_VALUE;
                for (Metric cfGauge : ALL_TABLE_METRICS.get("MinPartitionSize"))
                {
                    min = Math.min(min, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return min;
            }
        });
        maxPartitionSize = createTableGauge("MaxPartitionSize", "MaxRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long max = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.getEstimatedPartitionSize().max() > max)
                        max = sstable.getEstimatedPartitionSize().max();
                }
                return max;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long max = 0;
                for (Metric cfGauge : ALL_TABLE_METRICS.get("MaxPartitionSize"))
                {
                    max = Math.max(max, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return max;
            }
        });
        meanPartitionSize = createTableGauge("MeanPartitionSize", "MeanRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    long n = sstable.getEstimatedPartitionSize().count();
                    sum += sstable.getEstimatedPartitionSize().mean() * n;
                    count += n;
                }
                return count > 0 ? sum / count : 0;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.CANONICAL))
                    {
                        long n = sstable.getEstimatedPartitionSize().count();
                        sum += sstable.getEstimatedPartitionSize().mean() * n;
                        count += n;
                    }
                }
                return count > 0 ? sum / count : 0;
            }
        });
        compressionDictionariesMemoryUsed = createTableGauge("CompressionDictionariesMemoryUsed", new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                CompressionDictionaryManager compressionDictionaryManager = cfs.compressionDictionaryManager();
                if (compressionDictionaryManager != null)
                    return compressionDictionaryManager.cachedDictionariesMemoryUsed();
                else
                    return 0L;
            }
        });

        compressionMetadataOffHeapMemoryUsed = createTableGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getCompressionMetadataOffHeapSize();
                return total;
            }
        });
        speculativeRetries = createTableCounter("SpeculativeRetries");
        speculativeFailedRetries = createTableCounter("SpeculativeFailedRetries");
        speculativeInsufficientReplicas = createTableCounter("SpeculativeInsufficientReplicas");
        speculativeSampleLatencyNanos = createTableGauge("SpeculativeSampleLatencyNanos", () -> MICROSECONDS.toNanos(cfs.sampleReadLatencyMicros));

        additionalWrites = createTableCounter("AdditionalWrites");
        additionalWriteLatencyNanos = createTableGauge("AdditionalWriteLatencyNanos", () -> MICROSECONDS.toNanos(cfs.additionalWriteLatencyMicros));

        tombstoneScannedHistogram = createTableHistogram("TombstoneScannedHistogram", cfs.keyspace.metric.tombstoneScannedHistogram, false);
        purgeableTombstoneScannedHistogram = createTableHistogram("PurgeableTombstoneScannedHistogram", cfs.keyspace.metric.purgeableTombstoneScannedHistogram, true);
        liveScannedHistogram = createTableHistogram("LiveScannedHistogram", cfs.keyspace.metric.liveScannedHistogram, false);
        totalRowsRead = createTableCounter("TotalRowsRead");
        totalRowsMutated = createTableCounter("TotalRowsMutated");
        rowsMutatedPerWriteHistogram = createTableHistogram("RowsMutatedPerWriteHistogram", cfs.keyspace.metric.rowsMutatedPerWriteHistogram, false);
        colUpdateTimeDeltaHistogram = createTableHistogram("ColUpdateTimeDeltaHistogram", cfs.keyspace.metric.colUpdateTimeDeltaHistogram, false);
        coordinatorReadLatency = createTableTimer("CoordinatorReadLatency");
        coordinatorScanLatency = createTableTimer("CoordinatorScanLatency");
        coordinatorWriteLatency = createTableTimer("CoordinatorWriteLatency");

        // We do not want to capture view mutation specific metrics for a view
        // They only makes sense to capture on the base table
        if (cfs.metadata().isView())
        {
            viewLockAcquireTime = null;
            viewReadTime = null;
        }
        else
        {
            viewLockAcquireTime = createTableTimer("ViewLockAcquireTime", cfs.keyspace.metric.viewLockAcquireTime);
            viewReadTime = createTableTimer("ViewReadTime", cfs.keyspace.metric.viewReadTime);
        }

        trueSnapshotsSize = createTableGauge("SnapshotsSize", cfs::trueSnapshotsSize);
        rowCacheHitOutOfRange = createTableCounter("RowCacheHitOutOfRange");
        rowCacheHit = createTableCounter("RowCacheHit");
        rowCacheMiss = createTableCounter("RowCacheMiss");

        tombstoneFailures = createTableCounter("TombstoneFailures");
        tombstoneWarnings = createTableCounter("TombstoneWarnings");

        casPrepare = createLatencyMetrics("CasPrepare", cfs.keyspace.metric.casPrepare);
        casPropose = createLatencyMetrics("CasPropose", cfs.keyspace.metric.casPropose);
        casCommit = createLatencyMetrics("CasCommit", cfs.keyspace.metric.casCommit);
        keyMigration = createLatencyMetrics("KeyMigration", cfs.keyspace.metric.keyMigration, GLOBAL_KEY_MIGRATION_LATENCY);
        accordRepair = createLatencyMetrics("AccordRepair", cfs.keyspace.metric.accordRepair, GLOBAL_RANGE_MIGRATION_LATENCY);
        accordPostStreamRepair = createLatencyMetrics("AccordPostStreamRepair", cfs.keyspace.metric.accordPostStreamRepair);
        accordRepairUnexpectedFailures = createTableMeter("AccordRepairUnexpectedFailures", cfs.keyspace.metric.rangeMigrationUnexpectedFailures);
        mutationsRejectedOnWrongSystem = createTableMeter("MutationsRejectedOnWrongSystem", cfs.keyspace.metric.mutationsRejectedOnWrongSystem);
        readsRejectedOnWrongSystem = createTableMeter("ReadsRejectedOnWrongSystem", cfs.keyspace.metric.readsRejectedOnWrongSystem);

        repairsStarted = createTableCounter("RepairJobsStarted");
        repairsCompleted = createTableCounter("RepairJobsCompleted");

        anticompactionTime = createTableTimer("AnticompactionTime", cfs.keyspace.metric.anticompactionTime);
        validationTime = createTableTimer("ValidationTime", cfs.keyspace.metric.validationTime);
        repairSyncTime = createTableTimer("RepairSyncTime", cfs.keyspace.metric.repairSyncTime);

        bytesValidated = createTableHistogram("BytesValidated", cfs.keyspace.metric.bytesValidated, false);
        partitionsValidated = createTableHistogram("PartitionsValidated", cfs.keyspace.metric.partitionsValidated, false);
        bytesAnticompacted = createTableMeter("BytesAnticompacted", cfs.keyspace.metric.bytesAnticompacted, true);
        bytesMutatedAnticompaction = createTableMeter("BytesMutatedAnticompaction", cfs.keyspace.metric.bytesMutatedAnticompaction, true);
        bytesPreviewed = createTableMeter("BytesPreviewed", cfs.keyspace.metric.bytesPreviewed);
        tokenRangesPreviewedDesynchronized = createTableMeter("TokenRangesPreviewedDesynchronized", cfs.keyspace.metric.tokenRangesPreviewedDesynchronized);
        bytesPreviewedDesynchronized = createTableMeter("BytesPreviewedDesynchronized", cfs.keyspace.metric.bytesPreviewedDesynchronized);
        mutatedAnticompactionGauge = createTableGauge("MutatedAnticompactionGauge", () ->
        {
            double bytesMutated = bytesMutatedAnticompaction.table.getCount();
            double bytesAnticomp = bytesAnticompacted.table.getCount();
            if (bytesAnticomp + bytesMutated > 0)
                return bytesMutated / (bytesAnticomp + bytesMutated);
            return 0.0;
        });

        readRepairRequests = createTableMeter("ReadRepairRequests");
        shortReadProtectionRequests = createTableMeter("ShortReadProtectionRequests");
        replicaFilteringProtectionRequests = createTableMeter("ReplicaFilteringProtectionRequests");
        rfpRowsCachedPerQuery = createHistogram("ReplicaFilteringProtectionRowsCachedPerQuery", true);

        confirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesConfirmed", cfs.keyspace.metric.confirmedRepairedInconsistencies);
        unconfirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesUnconfirmed", cfs.keyspace.metric.unconfirmedRepairedInconsistencies);

        repairedDataTrackingOverreadRows = createTableHistogram("RepairedDataTrackingOverreadRows", cfs.keyspace.metric.repairedDataTrackingOverreadRows, false);
        repairedDataTrackingOverreadTime = createTableTimer("RepairedDataTrackingOverreadTime", cfs.keyspace.metric.repairedDataTrackingOverreadTime);

        unleveledSSTables = createTableGauge("UnleveledSSTables", cfs::getUnleveledSSTables, () -> {
            // global gauge
            int cnt = 0;
            for (Metric cfGauge : ALL_TABLE_METRICS.get("UnleveledSSTables"))
            {
                cnt += ((Gauge<? extends Number>) cfGauge).getValue().intValue();
            }
            return cnt;
        });

        clientTombstoneWarnings = createTableMeter("ClientTombstoneWarnings", cfs.keyspace.metric.clientTombstoneWarnings);
        clientTombstoneAborts = createTableMeter("ClientTombstoneAborts", cfs.keyspace.metric.clientTombstoneAborts);

        coordinatorReadSizeWarnings = createTableMeter("CoordinatorReadSizeWarnings", cfs.keyspace.metric.coordinatorReadSizeWarnings);
        coordinatorReadSizeAborts = createTableMeter("CoordinatorReadSizeAborts", cfs.keyspace.metric.coordinatorReadSizeAborts);
        coordinatorReadSize = createTableHistogram("CoordinatorReadSize", cfs.keyspace.metric.coordinatorReadSize, false);

        localReadSizeWarnings = createTableMeter("LocalReadSizeWarnings", cfs.keyspace.metric.localReadSizeWarnings);
        localReadSizeAborts = createTableMeter("LocalReadSizeAborts", cfs.keyspace.metric.localReadSizeAborts);
        localReadSize = createTableHistogram("LocalReadSize", cfs.keyspace.metric.localReadSize, false);

        rowIndexSizeWarnings = createTableMeter("RowIndexSizeWarnings", cfs.keyspace.metric.rowIndexSizeWarnings);
        rowIndexSizeAborts = createTableMeter("RowIndexSizeAborts", cfs.keyspace.metric.rowIndexSizeAborts);
        rowIndexSize = createTableHistogram("RowIndexSize", cfs.keyspace.metric.rowIndexSize, false);

        tooManySSTableIndexesReadWarnings = createTableMeter("TooManySSTableIndexesReadWarnings", cfs.keyspace.metric.tooManySSTableIndexesReadWarnings);
        tooManySSTableIndexesReadAborts = createTableMeter("TooManySSTableIndexesReadAborts", cfs.keyspace.metric.tooManySSTableIndexesReadAborts);

        writeSizeWarnings = createTableMeter("WriteSizeWarnings", cfs.keyspace.metric.writeSizeWarnings);
        writeTombstoneWarnings = createTableMeter("WriteTombstoneWarnings", cfs.keyspace.metric.writeTombstoneWarnings);

        viewSSTableIntervalTree = createLatencyMetrics("ViewSSTableIntervalTree", cfs.keyspace.metric.viewSSTableIntervalTree);

        formatSpecificGauges = createFormatSpecificGauges(cfs);
    }

    private Memtable.MemoryUsage getMemoryUsageWithIndexes(ColumnFamilyStore cfs)
    {
        Memtable.MemoryUsage usage = Memtable.newMemoryUsage();
        cfs.getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);
        for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
            indexCfs.getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);
        return usage;
    }

    public void updateSSTableIterated(int count)
    {
        sstablesPerReadHistogram.update(count);
    }

    public void updateSSTableIteratedInRangeRead(int count)
    {
        sstablesPerRangeReadHistogram.update(count);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        Metrics.removeIfMatch(fullName -> resolveShortMetricName(fullName, TableMetricNameFactory.GROUP_NAME,
                                                                 factory.type(),
                                                                 factory.scope()),
                              factory::createMetricName, this::releaseMetric);
        Metrics.removeIfMatch(fullName -> resolveShortMetricName(fullName, TableMetricNameFactory.GROUP_NAME,
                                                                 aliasFactory.type(),
                                                                 aliasFactory.scope()),
                              aliasFactory::createMetricName, this::releaseMetric);
    }

    private ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> createFormatSpecificGauges(ColumnFamilyStore cfs)
    {
        ImmutableMap.Builder<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> builder = ImmutableMap.builder();
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            ImmutableMap.Builder<String, Gauge<? extends Number>> gauges = ImmutableMap.builder();
            for (GaugeProvider<?> gaugeProvider : format.getFormatSpecificMetricsProviders().getGaugeProviders())
            {
                Gauge<? extends Number> gauge = createTableGauge(gaugeProvider.name, gaugeProvider.getTableGauge(cfs), gaugeProvider.getGlobalGauge());
                gauges.put(gaugeProvider.name, gauge);
            }
            builder.put(format, gauges.build());
        }
        return builder.build();
    }

    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * will merge each CF gauge by adding their values
     */
    protected <T extends Number> Gauge<T> createTableGauge(final String name, Gauge<T> gauge)
    {
        return createTableGauge(name, gauge, new GlobalTableGauge(name));
    }

    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * is defined as the globalGauge parameter
     */
    protected <G,T> Gauge<T> createTableGauge(String name, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        return createTableGauge(name, name, gauge, globalGauge);
    }

    protected <G,T> Gauge<T> createTableGauge(String name, String alias, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), gauge, aliasFactory.createMetricName(alias));
        if (register(name, alias, cfGauge) && globalGauge != null)
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name), globalGauge, GLOBAL_ALIAS_FACTORY.createMetricName(alias));
        }
        return cfGauge;
    }

    /**
     * Same as {@link #createTableGauge(String, Gauge, Gauge)} but accepts a deprecated
     * name for a table {@code Gauge}. Prefer that method when deprecation is not necessary.
     *
     * @param name the name of the metric registered with the "Table" type
     * @param deprecated the deprecated name for the metric registered with the "Table" type
     */
    protected <G,T> Gauge<T> createTableGaugeWithDeprecation(String name, String deprecated, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        assert deprecated != null : "no deprecated metric name provided";
        assert globalGauge != null : "no global Gauge metric provided";
        
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), 
                                            gauge,
                                            aliasFactory.createMetricName(name),
                                            factory.createMetricName(deprecated),
                                            aliasFactory.createMetricName(deprecated));
        
        if (register(name, name, deprecated, cfGauge))
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name),
                             globalGauge,
                             GLOBAL_ALIAS_FACTORY.createMetricName(name),
                             GLOBAL_FACTORY.createMetricName(deprecated),
                             GLOBAL_ALIAS_FACTORY.createMetricName(deprecated));
        }
        return cfGauge;
    }

    /**
     * Creates a counter that will also have a global counter thats the sum of all counters across
     * different column families
     */
    protected Counter createTableCounter(final String name)
    {
        return createTableCounter(name, name);
    }

    protected Counter createTableCounter(final String name, final String alias)
    {
        Counter cfCounter = Metrics.counter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        if (register(name, alias, cfCounter))
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name),
                    (Gauge<Long>) () ->
                    {
                        long total = 0;
                        for (Metric cfGauge : ALL_TABLE_METRICS.get(name))
                            total += ((Counter) cfGauge).getCount();
                        return total;
                    },
                    GLOBAL_ALIAS_FACTORY.createMetricName(alias));
        }
        return cfCounter;
    }

    private Meter createTableMeter(final String name)
    {
        return createTableMeter(name, name);
    }

    private Meter createTableMeter(final String name, final String alias)
    {
        Meter tableMeter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, tableMeter);
        return tableMeter;
    }
    
    private Histogram createHistogram(String name, boolean considerZeroes)
    {
        Histogram histogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(name), considerZeroes);
        register(name, name, histogram);
        return histogram;
    }

    /**
     * Computes the compression ratio for the specified SSTables
     *
     * @param sstables the SSTables
     * @return the compression ratio for the specified SSTables
     */
    private static Double computeCompressionRatio(Iterable<SSTableReader> sstables)
    {
        double compressedLengthSum = 0;
        double dataLengthSum = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.compression)
            {
                // We should not have any sstable which are in an open early mode as the sstable were selected
                // using SSTableSet.CANONICAL.
                assert sstable.openReason != SSTableReader.OpenReason.EARLY;

                CompressionMetadata compressionMetadata = sstable.getCompressionMetadata();
                compressedLengthSum += compressionMetadata.compressedFileLength;
                dataLengthSum += compressionMetadata.dataLength;
            }
        }
        return dataLengthSum != 0 ? compressedLengthSum / dataLengthSum : MetadataCollector.NO_COMPRESSION_RATIO;
    }

    /**
     * Create a histogram-like interface that will register both a CF, keyspace and global level
     * histogram and forward any updates to both
     */
    protected TableHistogram createTableHistogram(String name, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        return createTableHistogram(name, name, keyspaceHistogram, considerZeroes);
    }

    protected TableHistogram createTableHistogram(String name, String alias, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        Histogram cfHistogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(alias), considerZeroes);
        register(name, alias, cfHistogram);
        return new TableHistogram(cfHistogram,
                                  keyspaceHistogram,
                                  Metrics.histogram(GLOBAL_FACTORY.createMetricName(name),
                                                    GLOBAL_ALIAS_FACTORY.createMetricName(alias),
                                                    considerZeroes));
    }

    protected TableTimer createTableTimer(String name, Timer keyspaceTimer)
    {
        Timer cfTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(name));
        register(name, name, keyspaceTimer);
        Timer global = Metrics.timer(GLOBAL_FACTORY.createMetricName(name), GLOBAL_ALIAS_FACTORY.createMetricName(name));

        return new TableTimer(cfTimer, keyspaceTimer, global);
    }

    protected SnapshottingTimer createTableTimer(String name)
    {
        SnapshottingTimer tableTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(name));
        register(name, name, tableTimer);
        return tableTimer;
    }

    protected TableMeter createTableMeter(String name, Meter keyspaceMeter)
    {
        return createTableMeter(name, keyspaceMeter, false);
    }

    protected TableMeter createTableMeter(String name, Meter keyspaceMeter, boolean globalMeterGaugeCompatible)
    {
        return createTableMeter(name, name, keyspaceMeter, globalMeterGaugeCompatible);
    }

    protected TableMeter createTableMeter(String name, String alias, Meter keyspaceMeter, boolean globalMeterGaugeCompatible)
    {
        Meter meter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, meter);
        return new TableMeter(meter,
                              keyspaceMeter,
                              Metrics.meter(globalMeterGaugeCompatible, GLOBAL_FACTORY.createMetricName(name),
                                            GLOBAL_ALIAS_FACTORY.createMetricName(alias)));
    }

    private LatencyMetrics createLatencyMetrics(String namePrefix, LatencyMetrics ... parents)
    {
        // All metrics which are registered with the same factory type will be removed when release() is called.
        return new LatencyMetrics(factory, namePrefix, parents);
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, Metric metric)
    {
        return register(name, alias, null, metric);
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * 
     * @param name the name of the metric registered with the "Table" type
     * @param alias the name of the metric registered with the legacy "ColumnFamily" type
     * @param deprecated an optionally null deprecated name for the metric registered with the "Table"
     * 
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, String deprecated, Metric metric)
    {
        boolean ret = ALL_TABLE_METRICS.putIfAbsent(name, ConcurrentHashMap.newKeySet()) == null;
        ALL_TABLE_METRICS.get(name).add(metric);
        return ret;
    }

    private void releaseMetric(CassandraMetricsRegistry.MetricName name)
    {
        Metric metric = Metrics.getMetrics().get(name.getMetricName());
        if (metric == null)
            return;

        Optional.ofNullable(ALL_TABLE_METRICS.get(name.getName())).ifPresent(set -> set.remove(metric));
    }

    public static class TableMeter
    {
        public final Meter[] all;
        public final Meter table;
        public final Meter global;

        private TableMeter(Meter table, Meter keyspace, Meter global)
        {
            this.table = table;
            this.global = global;
            this.all = new Meter[]{table, keyspace, global};
        }

        public void mark()
        {
            mark(1L);
        }

        public void mark(long val)
        {
            for (Meter meter : all)
            {
                meter.mark(val);
            }
        }
    }

    public static class TableHistogram
    {
        public final Histogram[] all;
        public final Histogram cf;
        public final Histogram global;

        private TableHistogram(Histogram cf, Histogram keyspace, Histogram global)
        {
            this.cf = cf;
            this.global = global;
            this.all = new Histogram[]{cf, keyspace, global};
        }

        public void update(long i)
        {
            for(Histogram histo : all)
            {
                histo.update(i);
            }
        }
    }

    public static class TableTimer
    {
        public final Timer[] all;
        public final Timer cf;
        public final Timer global;

        private TableTimer(Timer cf, Timer keyspace, Timer global)
        {
            this.cf = cf;
            this.global = global;
            this.all = new Timer[]{cf, keyspace, global};
        }

        public void update(long i, TimeUnit unit)
        {
            for(Timer timer : all)
            {
                timer.update(i, unit);
            }
        }

        public Context time()
        {
            return new Context(all);
        }

        public static class Context implements AutoCloseable
        {
            private final long start;
            private final Timer [] all;

            private Context(Timer [] all)
            {
                this.all = all;
                start = nanoTime();
            }

            public void close()
            {
                long duration = nanoTime() - start;
                for (Timer t : all)
                    t.update(duration, TimeUnit.NANOSECONDS);
            }
        }
    }

    static class TableMetricNameFactory implements MetricNameFactory
    {
        public static final String GROUP_NAME = TableMetrics.class.getPackage().getName();
        private final String keyspaceName;
        private final String tableName;
        private final String type;

        TableMetricNameFactory(ColumnFamilyStore cfs, String type)
        {
            this.keyspaceName = cfs.getKeyspaceName();
            this.tableName = cfs.name;
            this.type = type;
        }

        public String type()
        {
            return type;
        }

        public String scope()
        {
            return keyspaceName + '.' + tableName;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            assert metricName.indexOf('.') == -1 : String.format("Metric name must not contain '.' character (got '%s')", metricName);
            return new CassandraMetricsRegistry.MetricName(GROUP_NAME, type, metricName, scope(),
                                                           GROUP_NAME + ':' +
                                                           "type=" + type +
                                                           ",keyspace=" + keyspaceName +
                                                           ",scope=" + tableName +
                                                           ",name=" + metricName);
        }
    }

    static class AllTableMetricNameFactory implements MetricNameFactory
    {
        private static final String GROUP_NAME = TableMetrics.class.getPackage().getName();
        private final String type;
        public AllTableMetricNameFactory(String type)
        {
            this.type = type;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            assert metricName.indexOf('.') == -1 : String.format("Metric name must not contain '.' character (got '%s')", metricName);
            return new CassandraMetricsRegistry.MetricName(GROUP_NAME, type, metricName, "all",
                                                           GROUP_NAME + ':' +
                                                           "type=" + type +
                                                           ",name=" + metricName);
        }
    }

    private static class GlobalTableGauge implements Gauge<Long>
    {
        private final String name;

        public GlobalTableGauge(String name)
        {
            this.name = name;
        }

        public Long getValue()
        {
            long total = 0;
            for (Metric cfGauge : ALL_TABLE_METRICS.get(name))
            {
                total = total + ((Gauge<? extends Number>) cfGauge).getValue().longValue();
            }
            return total;
        }
    }
}
