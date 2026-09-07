<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Table and keyspace aggregates

Source inventory, 2026-09-06. Counts refer to canonical metric names defined by
this branch, including built-in SSTable-format gauges and conditional metrics.
They exclude aliases and individual timer/histogram attributes. This is not a
count of registrations observed in a particular running server.

## Scopes

| Scope | Canonical names | Instances | Covered by the current profile files? |
|---|---:|---|---|
| Table/IndexTable | 126 | Per table runtime | Yes, table section |
| Keyspace | 101 | Per keyspace | Yes, keyspace section |
| Global Table | 106 | One node-wide instance of each applicable metric | No |

These are local-node aggregates, not cross-node cluster totals. There is no
separate global Keyspace parent in KeyspaceMetrics. Global Table metrics can
aggregate table children directly, independently of the keyspace metrics.

The global [name factory](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1336)
uses registry scope `all`; its Java Management Extensions (JMX) ObjectName has
neither a keyspace nor a table scope property. Keyspace metrics have a keyspace
property; per-table metrics have both keyspace and table scope properties.
Legacy ColumnFamily aliases are additional names, not another aggregate.

## Where the values come from

| Source of values | Global Table names | Keyspace names | Can table recording be a no-op? |
|---|---:|---:|---|
| Read table counters/gauges | 41 | 22 | No, unless the aggregate gets real values another way |
| Read child latency distributions and totals | 10 | 20 | No, unless events reach an independent parent recorder |
| Receive updates through shared operation wrappers | 41 | 41 | Yes, if enabled parent destinations still receive every update |
| Read SSTable/storage state directly | 14 | 8 | Table exports are unnecessary; underlying storage statistics remain necessary |
| Record keyspace operations directly | 0 | 10 | No table recorder dependency for these entries |

The first two rows currently depend on real table metric inputs: 51 global names
and 42 keyspace names. These are output counts, not distinct backing-object counts;
the same child can feed more than one output.

## Global Table names

### Read table counters: 20

The [counter factory](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1046)
registers a global gauge that sums counts from ALL_TABLE_METRICS.

- `AdditionalWrites`
- `BytesFlushed`
- `CompactionBytesWritten`
- `LiveDiskSpaceUsed`
- `MemtableSwitchCount`
- `PendingFlushes`
- `RepairJobsCompleted`
- `RepairJobsStarted`
- `RowCacheHit`
- `RowCacheHitOutOfRange`
- `RowCacheMiss`
- `SpeculativeFailedRetries`
- `SpeculativeInsufficientReplicas`
- `SpeculativeRetries`
- `TombstoneFailures`
- `TombstoneWarnings`
- `TotalDiskSpaceUsed`
- `TotalRowsMutated`
- `TotalRowsRead`
- `UncompressedLiveDiskSpaceUsed`

### Read table gauges: 21

Most use [GlobalTableGauge](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1357)
to sum table values. MinPartitionSize and MaxPartitionSize use explicit minimum
and maximum reductions; UnleveledSSTables uses an explicit sum.

- `AdditionalWriteLatencyNanos`
- `AllMemtablesLiveDataSize`
- `AllMemtablesOffHeapDataSize`
- `AllMemtablesOnHeapDataSize`
- `CompressionDictionariesMemoryUsed`
- `CompressionMetadataOffHeapMemoryUsed`
- `LiveSSTableCount`
- `MaxPartitionSize`
- `MaxSSTableDuration`
- `MaxSSTableSize`
- `MemtableColumnsCount`
- `MemtableLiveDataSize`
- `MemtableOffHeapDataSize`
- `MemtableOnHeapDataSize`
- `MinPartitionSize`
- `MutatedAnticompactionGauge`
- `OldVersionSSTableCount`
- `PendingCompactions`
- `SnapshotsSize`
- `SpeculativeSampleLatencyNanos`
- `UnleveledSSTables`

The name alone does not specify the reduction: for example, MaxSSTableSize and
MaxSSTableDuration currently use the generic sum of their per-table gauges.
MutatedAnticompactionGauge also has an indirect metric dependency: its table
gauge reads BytesAnticompacted and BytesMutatedAnticompaction table meter counts.
Disabling either recorder changes the table gauge and its global output.

### Read table latency children: 10

The [five global latency families](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L89)
each expose a Latency timer and a TotalLatency counter. Parent implementations
read children in [LatencyMetrics](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L179).

- `KeyMigrationLatency`
- `KeyMigrationTotalLatency`
- `RangeLatency`
- `RangeMigrationLatency`
- `RangeMigrationTotalLatency`
- `RangeTotalLatency`
- `ReadLatency`
- `ReadTotalLatency`
- `WriteLatency`
- `WriteTotalLatency`

Global RangeMigrationLatency/RangeMigrationTotalLatency receive the table
AccordRepair child, despite the different name. Read, Write, Range, and
KeyMigration use the same prefixes at both scopes.

### Receive histogram updates: 13

[TableHistogram](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1118)
gives table, keyspace, and node-wide recorders their own copy of each observation.

- `BytesValidated`
- `ColUpdateTimeDeltaHistogram`
- `CoordinatorReadSize`
- `LiveScannedHistogram`
- `LocalReadSize`
- `PartitionsValidated`
- `PurgeableTombstoneScannedHistogram`
- `RepairedDataTrackingOverreadRows`
- `RowIndexSize`
- `RowsMutatedPerWriteHistogram`
- `SSTablesPerRangeReadHistogram`
- `SSTablesPerReadHistogram`
- `TombstoneScannedHistogram`

### Receive meter updates: 22

[TableMeter](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1155)
forwards updates to separate destinations.
Those destinations do not read the table meter. Other enabled gauges can still
need its counts, as with MutatedAnticompactionGauge above.

- `AccordRepairUnexpectedFailures`
- `BytesAnticompacted`
- `BytesMutatedAnticompaction`
- `BytesPreviewed`
- `BytesPreviewedDesynchronized`
- `ClientTombstoneAborts`
- `ClientTombstoneWarnings`
- `CoordinatorReadSizeAborts`
- `CoordinatorReadSizeWarnings`
- `LocalReadSizeAborts`
- `LocalReadSizeWarnings`
- `MutationsRejectedOnWrongSystem`
- `ReadsRejectedOnWrongSystem`
- `RepairedDataInconsistenciesConfirmed`
- `RepairedDataInconsistenciesUnconfirmed`
- `RowIndexSizeAborts`
- `RowIndexSizeWarnings`
- `TokenRangesPreviewedDesynchronized`
- `TooManySSTableIndexesReadAborts`
- `TooManySSTableIndexesReadWarnings`
- `WriteSizeWarnings`
- `WriteTombstoneWarnings`

### Receive timer updates: 6

[TableTimer](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1129)
forwards durations to separate destinations.

- `AnticompactionTime`
- `RepairSyncTime`
- `RepairedDataTrackingOverreadTime`
- `ValidationTime`
- `ViewLockAcquireTime`
- `ViewReadTime`

### Read storage state directly: 14

CompressionRatio and MeanPartitionSize calculate from SSTable metadata.
The four [repair-size gauges](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L323)
calculate over a filtered set of replicated, non-system tables; they do not
represent every table unconditionally. The eight SSTable-format gauges use
[SimpleGaugeProvider](../src/java/org/apache/cassandra/io/sstable/SimpleGaugeProvider.java#L44)
to inspect readers directly.

- `BloomFilterDiskSpaceUsed`
- `BloomFilterFalsePositives`
- `BloomFilterFalseRatio`
- `BloomFilterOffHeapMemoryUsed`
- `BytesPendingRepair`
- `BytesRepaired`
- `BytesUnrepaired`
- `CompressionRatio`
- `IndexSummaryOffHeapMemoryUsed`
- `KeyCacheHitRate`
- `MeanPartitionSize`
- `PercentRepaired`
- `RecentBloomFilterFalsePositives`
- `RecentBloomFilterFalseRatio`

## Keyspace names

### Read table counters/gauges: 22

[KeyspaceMetrics](../src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java#L222)
sums table values. The two Unreplicated names additionally divide by the
keyspace's full replication factor.

- `AdditionalWrites`
- `AllMemtablesLiveDataSize`
- `AllMemtablesOffHeapDataSize`
- `AllMemtablesOnHeapDataSize`
- `CompressionMetadataOffHeapMemoryUsed`
- `LiveDiskSpaceUsed`
- `MemtableColumnsCount`
- `MemtableLiveDataSize`
- `MemtableOffHeapDataSize`
- `MemtableOnHeapDataSize`
- `MemtableSwitchCount`
- `PendingCompactions`
- `PendingFlushes`
- `RepairJobsCompleted`
- `RepairJobsStarted`
- `SpeculativeFailedRetries`
- `SpeculativeInsufficientReplicas`
- `SpeculativeRetries`
- `TotalDiskSpaceUsed`
- `UncompressedLiveDiskSpaceUsed`
- `UnreplicatedLiveDiskSpaceUsed`
- `UnreplicatedUncompressedLiveDiskSpaceUsed`

### Read table latency children: 20

Ten families attach table children to keyspace parents. This includes more
families than the five node-wide latency parents.

- `AccordPostStreamRepairLatency`
- `AccordPostStreamRepairTotalLatency`
- `AccordRepairLatency`
- `AccordRepairTotalLatency`
- `CasCommitLatency`
- `CasCommitTotalLatency`
- `CasPrepareLatency`
- `CasPrepareTotalLatency`
- `CasProposeLatency`
- `CasProposeTotalLatency`
- `KeyMigrationLatency`
- `KeyMigrationTotalLatency`
- `RangeLatency`
- `RangeTotalLatency`
- `ReadLatency`
- `ReadTotalLatency`
- `ViewSSTableIntervalTreeLatency`
- `ViewSSTableIntervalTreeTotalLatency`
- `WriteLatency`
- `WriteTotalLatency`

### Receive shared operation updates: 41

These are the same 13 histogram, 22 meter, and six timer entries listed under
Global Table, with two naming differences:

- Global/table AnticompactionTime is keyspace AntiCompactionTime.
- Global/table AccordRepairUnexpectedFailures is keyspace RangeMigrationUnexpectedFailures.

Their recorders receive events through the table wrappers; their values do not
require querying the table recorder.

### Read storage state directly: 8

- `BloomFilterDiskSpaceUsed`
- `BloomFilterFalsePositives`
- `BloomFilterFalseRatio`
- `BloomFilterOffHeapMemoryUsed`
- `IndexSummaryOffHeapMemoryUsed`
- `KeyCacheHitRate`
- `RecentBloomFilterFalsePositives`
- `RecentBloomFilterFalseRatio`

### Record keyspace operations directly: 10

These are keyspace-scoped observations, rather than sums of per-table children.
Examples of producers are
[RepairCoordinator](../src/java/org/apache/cassandra/repair/RepairCoordinator.java#L273),
[AbstractWriteResponseHandler](../src/java/org/apache/cassandra/service/AbstractWriteResponseHandler.java#L355),
and [ConsensusKeyMigrationState](../src/java/org/apache/cassandra/service/consensus/migration/ConsensusKeyMigrationState.java#L331).

- `AccordGetMaxConflictsLatency`
- `AccordGetMaxConflictsTotalLatency`
- `IdealCLWriteLatency`
- `IdealCLWriteTotalLatency`
- `PaxosOutOfRangeToken`
- `ReadOutOfRangeToken`
- `RepairPrepareTime`
- `RepairTime`
- `WriteFailedIdealCL`
- `WriteOutOfRangeToken`

## Metrics without a Global Table counterpart

The following per-table names have no node-wide Table aggregate. Some have
keyspace counterparts, as listed above.

- `AccordPostStreamRepairLatency`
- `AccordPostStreamRepairTotalLatency`
- `CasCommitLatency`
- `CasCommitTotalLatency`
- `CasPrepareLatency`
- `CasPrepareTotalLatency`
- `CasProposeLatency`
- `CasProposeTotalLatency`
- `CoordinatorReadLatency`
- `CoordinatorScanLatency`
- `CoordinatorWriteLatency`
- `EstimatedColumnCountHistogram`
- `EstimatedPartitionCount`
- `EstimatedPartitionSizeHistogram`
- `ReadRepairRequests`
- `ReplicaFilteringProtectionRequests`
- `ReplicaFilteringProtectionRowsCachedPerQuery`
- `ShortReadProtectionRequests`
- `ViewSSTableIntervalTreeLatency`
- `ViewSSTableIntervalTreeTotalLatency`

AccordRepairLatency and AccordRepairTotalLatency are not omissions: their global
counterparts use the RangeMigration prefix.

## Profile implications and correction

The profile files currently select table and keyspace exports only. Global Table
aggregates remain enabled by existing code. Selecting fewer table exports must
not replace inputs to enabled aggregates with empty recorders. A future global
selection section can control those 106 names independently.

This audit found a missing table-only histogram in the initial profile catalog:
ReplicaFilteringProtectionRowsCachedPerQuery. The source uses createHistogram,
which the initial source check did not match. The histogram is now optional in
all_metrics.yml and disabled in simple_metrics.yml. The corrected table total is
126, with four required and 122 optional names in the all profile. The simple
profile still enables four required and 15 optional names; 107 names are disabled.

This inventory and catalog correction change no runtime recording or registration.
The corrected profile check passed; output is in
`logs/20260906-131754-check-metric-profiles.log`.
See [internal_metric_dependencies.md](internal_metric_dependencies.md) for
database control inputs and non-exported operational statistics.
