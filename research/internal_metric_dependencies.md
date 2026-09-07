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

# Internal metric dependencies

Source inventory, 2026-09-06. Branch `moar_tables`, production code at
`24714c59fd`. This document evaluates the proposed metric allowlist. No recording,
registration, or database behavior changed during this inventory.

## Result and scope

An export allowlist can suppress Java Management Extensions (JMX) registration
for a metric that Cassandra still needs internally. A recording allowlist must preserve the control inputs
listed below. A shared no-op for every unexported metric would change database
behavior.

The inventory separates:

- Registered metrics whose values drive decisions or scheduling.
- Operational statistics that are not ordinary metric registrations.
- Dependencies needed to compute other enabled metrics.
- Management/diagnostic reads with no confirmed database control dependency.

The distinction is the consumer's behavior. A read from a `metric` field is not
enough to classify it as a control dependency. This is a source audit of
`src/java` and the relevant Accord module integration, not a proof from disabling
metrics under every workload. Test sources help locate consumers but are not
evidence of production dependence. The reference OpenTelemetry checkout is
outside scope.

## Table metrics that supply control inputs

Names below are logical metric names under the Table/legacy ColumnFamily metric
types, including the index-table equivalents where that table runtime uses them.

| Metric | Value and consumer | Effect of a no-op | When needed |
|---|---|---|---|
| `CoordinatorReadLatency` | Percentile snapshot sets `sampleReadLatencyMicros`; the read executor and read-repair code use this threshold. | Speculative reads and read-repair speculation stop adapting to observed latency. An empty snapshot retains the previous threshold; it does not preserve the normal behavior. | Percentile and hybrid speculative retry policies. |
| `CoordinatorWriteLatency` | Percentile snapshot sets `additionalWriteLatencyMicros`; write response handlers and blocking read repair use it. | Changes when extra replicas receive writes or repair mutations. | Percentile and hybrid additional-write policies. |
| `CompressionRatio` — conditional helper dependency | `getExpectedCompactedFileSize(..., CLEANUP)` reads the gauge to adjust its expected output size. | Changes that helper's estimate if a caller uses its cleanup branch. Normal cleanup-path use of this branch was not established; see the qualification below. | The helper's cleanup branch for ordinary tables; other operations and index tables return earlier. |
| `TotalDiskSpaceUsed` | Automatic repair sorts tables by this counter before forming keyspace repair batches. | Changes table ordering and batch membership. This is scheduling behavior, not a proof of data corruption. | The repair-by-keyspace scheduling path. |

The latency paths start at
[ColumnFamilyStore.updateSpeculationThreshold](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L617).
[Percentile policy](../src/java/org/apache/cassandra/service/reads/PercentileSpeculativeRetryPolicy.java#L51)
and [hybrid policy](../src/java/org/apache/cassandra/service/reads/HybridSpeculativeRetryPolicy.java#L62)
read `getPercentileSnapshot()`. The read executor consumes the result in
[shouldSpeculateAndMaybeWait](../src/java/org/apache/cassandra/service/reads/AbstractReadExecutor.java#L243).
Additional writes consume it in
[maybeTryAdditionalReplicas](../src/java/org/apache/cassandra/service/AbstractWriteResponseHandler.java#L430).
Read repair also consumes the thresholds in
[AbstractReadRepair](../src/java/org/apache/cassandra/service/reads/repair/AbstractReadRepair.java#L185)
and [BlockingReadRepair](../src/java/org/apache/cassandra/service/reads/repair/BlockingReadRepair.java#L160).

Both table policies default to the 99th percentile in
[TableParams](../src/java/org/apache/cassandra/schema/TableParams.java#L452).
These are normal default dependencies. Fixed-time, always, and never policies do
not need a percentile distribution, but schema changes can later enable a
percentile policy. The first allowlist implementation should keep these internal
timers regardless of export selection unless it also handles that transition.
The confirmed control input is the latency distribution; this does not prove
that every rate/count component of the timer is needed by the decision.

The cleanup branch of the size-estimation helper reads compression ratio in
[getExpectedCompactedFileSize](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1751).
The gauge derives its value from SSTable metadata in
[TableMetrics](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L581).
However, the normal
[doCleanupOne](../src/java/org/apache/cassandra/db/compaction/CompactionManager.java#L1616)
path reads `sstable.getCompressionRatio()` directly and constructs its writer
without that helper. Compaction writers/space checks call the helper with their
operation type; this audit did not establish a normal caller passing CLEANUP
through those paths. The earlier conversational claim that cleanup generally
depends on the metric was too broad. Preserve this executable helper dependency
conservatively until it is moved to a direct calculation or its reachability is
resolved. It is not evidence that normal cleanup currently requires a registered
CompressionRatio metric.

Repair ordering reads the disk counter in
[RepairTokenRangeSplitter](../src/java/org/apache/cassandra/repair/autorepair/RepairTokenRangeSplitter.java#L275).
The counter also survives through asynchronous SSTable deletion via
[LogTransaction](../src/java/org/apache/cassandra/db/lifecycle/LogTransaction.java#L381).
An eventual replacement must preserve that accounting lifetime.

## Node-level metrics that supply control inputs

| Metric type / scope / name | Internal consumer | Effect of a no-op |
|---|---|---|
| `Storage` / no scope / `TotalHintsInProgress` | StorageProxy checks the count against the configured limit, together with destination-specific outstanding hints. | Disables this overload-rejection condition. This counter deliberately uses atomic updates. |
| `Compaction` / no scope / `PendingTasks` | ActiveRepairService checks the gauge through CompactionManager before accepting repair work. | Bypasses the pending-compaction repair admission limit, including incoming repair requests. |
| `ClientRequest` / `CASRead` / `Latency` | Paxos contention strategy uses latency percentiles to calculate waits and deadline checks. | Changes waits when a percentile-based contention strategy is selected. |
| `ClientRequest` / `CASWrite` / `Latency` | Same contention strategy can consult both read and write latency. | Same dependency; preserving only one histogram is insufficient for strategies that combine both. |
| `ClientRequest` / `AccordRead` / `Latency` | Accord wait strategies use read latency for slow-read and transaction timing. | Changes slow-read behavior under the default percentile-based configuration, and other waits under percentile-based configurations. |
| `ClientRequest` / `AccordWrite` / `Latency` | Accord transaction wait strategies can use combined read/write latency. | Changes transaction preaccept timing under the default configuration, and configured expiration/fetch/recovery waits when they use percentiles. |

Hint admission reads the atomic counter in
[StorageProxy](../src/java/org/apache/cassandra/service/StorageProxy.java#L1942);
its registration is in [StorageMetrics](../src/java/org/apache/cassandra/metrics/StorageMetrics.java#L55).
The pending-compaction gauge is defined in
[CompactionMetrics](../src/java/org/apache/cassandra/metrics/CompactionMetrics.java#L75),
forwarded by [CompactionManager](../src/java/org/apache/cassandra/db/compaction/CompactionManager.java#L2409),
and checked in [ActiveRepairService](../src/java/org/apache/cassandra/service/ActiveRepairService.java#L686).
The incoming path uses that check in
[RepairMessageVerbHandler](../src/java/org/apache/cassandra/repair/RepairMessageVerbHandler.java#L118).

The compare-and-set (CAS) request metrics enter Paxos timing through
[ContentionStrategy.LATENCIES](../src/java/org/apache/cassandra/service/paxos/ContentionStrategy.java#L97).
[TimeoutStrategy](../src/java/org/apache/cassandra/service/TimeoutStrategy.java#L204)
obtains the snapshots and [reads their percentiles](../src/java/org/apache/cassandra/service/TimeoutStrategy.java#L272).
[ContentionStrategy](../src/java/org/apache/cassandra/service/paxos/ContentionStrategy.java#L175)
uses the resulting delay for sleeping and deadline checks. This dependency is
conditional on the configured strategy. A no-op snapshot changes the latency
calculation and does not preserve normal adaptation.

Accord supplies its read/write metrics in
[AccordWaitStrategies](../src/java/org/apache/cassandra/service/accord/api/AccordWaitStrategies.java#L109).
The default slow-read and slow-preaccept strategies use
`30ms <= p50*2 <= 1000ms` in
[AccordConfig](../src/java/org/apache/cassandra/config/AccordConfig.java#L229).
Recovery, fetch and expiration currently use time/attempt formulas but support
percentile-based configuration. The resulting values feed
[AccordAgent](../src/java/org/apache/cassandra/service/accord/api/AccordAgent.java#L362),
then Accord execution/progress decisions such as
[ExecuteTxn](../modules/accord/accord-core/src/main/java/accord/coordinate/ExecuteTxn.java#L641).
These client metrics are node-level inputs; they do not require a separate
control histogram for every table.

## Operational statistics outside the registration allowlist

| State | Internal consumer | Treatment |
|---|---|---|
| `TableMetrics.flushSizeOnDisk` | Unified Compaction Strategy uses the moving average for flush shard density and the controller's base SSTable-size calculation. | Keep recording. It has no registered metric name in this branch, despite living in TableMetrics. |
| Per-SSTable `RestorableMeter` | Size-tiered compaction ranks/prunes candidates using the two-hour read rate. Index-summary redistribution allocates summary memory using the fifteen-minute rate. | Keep its existing operational configuration and lifecycle. Do not disable it by treating everything in the metrics package as optional telemetry. |
| Dynamic endpoint snitch latency reservoirs | Per-endpoint samples determine replica scores and ordering. | Keep the sampling state. The snitch constructs these reservoirs directly; they have no ordinary registry metric name. |

SSTable means sorted string table. These storage statistics exist independently
of whether an operator exposes a metric for them.

The flush-size average is constructed directly, without a registry call, in
[TableMetrics](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L659).
Flush completion updates it in
[ColumnFamilyStore](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1406).
Consumers are [UnifiedCompactionStrategy](../src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.java#L310)
and [Controller.getFlushSizeBytes](../src/java/org/apache/cassandra/db/compaction/unified/Controller.java#L397).
A configured flush-size override bypasses the latter read but not the direct
flush-density read in the strategy.

SSTable hotness consumers are
[SizeTieredCompactionStrategy](../src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java#L182)
and [IndexSummaryRedistribution](../src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummaryRedistribution.java#L120).
The meter is loaded and periodically persisted through
[SSTableReader.GlobalTidy](../src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java#L1716).
The index-summary dependency applies to formats that support index summaries;
it is not a BTI index-summary cost. The size-tiered compaction consumer is separate.

The dynamic snitch's direct reservoir sampling/scoring path starts in
[DynamicEndpointSnitch](../src/java/org/apache/cassandra/locator/DynamicEndpointSnitch.java#L253).

## Dependencies of enabled aggregate metrics

These are export-value dependencies. They do not by themselves drive database
decisions, but they prevent replacing every unexported child with zero while
promising that enabled parents still describe the whole node/keyspace.

| Enabled output | Backing dependency |
|---|---|
| Keyspace/global latency and total latency | Table `Read`, `Write`, `Range`, `CasPrepare`, `CasPropose`, `CasCommit`, `KeyMigration`, `AccordRepair`, and `AccordPostStreamRepair` latency children where attached. |
| Keyspace memory/storage/task gauges | Table `MemtableColumnsCount`, `MemtableLiveDataSize`, `MemtableOnHeapDataSize`, `MemtableOffHeapDataSize`, all three `AllMemtables*DataSize` gauges, `MemtableSwitchCount`, `PendingCompactions`, `PendingFlushes`, `LiveDiskSpaceUsed`, `UncompressedLiveDiskSpaceUsed`, `TotalDiskSpaceUsed`, and `CompressionMetadataOffHeapMemoryUsed`. Unreplicated disk-size gauges also depend on the corresponding disk counters. |
| Keyspace retry/repair counters | Table `SpeculativeRetries`, `SpeculativeFailedRetries`, `SpeculativeInsufficientReplicas`, `AdditionalWrites`, `RepairJobsStarted`, and `RepairJobsCompleted`. |
| Global table gauges/counters | Members of `ALL_TABLE_METRICS`; the global summation reads the table values. |
| Storage Attached Indexing (SAI) `DiskPercentageOfBaseTable` | The table's `LiveDiskSpaceUsed` counter supplies the denominator. |
| Table/global `MutatedAnticompactionGauge` | The table gauge reads `BytesAnticompacted` and `BytesMutatedAnticompaction` table meter counts; the global gauge reads table gauge values. |

Latency parents pull count, rate, snapshot, and total-latency values from children
in [LatencyMetrics](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L179).
On table release, they merge the child's history into the parent before removing
it, in [removeChildren](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L127).
These links attach at [TableMetrics](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L653)
and [its other latency families](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L850).

Keyspace summation is defined in
[KeyspaceMetrics](../src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java#L222)
and [its counter families](../src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java#L281).
Global counter summation is in
[TableMetrics](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1046).
The SAI denominator read is in
[TableStateMetrics](../src/java/org/apache/cassandra/index/sai/metrics/TableStateMetrics.java#L40).

`TableHistogram`, `TableTimer`, and `TableMeter` already fan updates out to
separate table/keyspace/global objects, starting at
[TableMetrics.TableMeter](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1205).
Those destinations can be selected independently. Latency children and pull-based
sums need a separate dependency decision or a change to how aggregation works.

The subsequent [aggregate inventory](metric_aggregates.md) lists all 106 global
Table names and 101 Keyspace names by backing dependency. It also identifies the
indirect table-meter dependency in
[MutatedAnticompactionGauge](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L874).

## Management and diagnostic consumers

These reads matter to an operator-facing compatibility policy, but this audit
does not classify them as database control inputs merely because server code
calls them.

| Metric or state | Observed consumer | Distinction |
|---|---|---|
| Table `MaxSSTableSize`, `MaxSSTableDuration` | ColumnFamilyStore MBean accessors return these gauges. | No internal scheduling consumer of those accessors was found. Size-related compaction variables with similar names are separate values. |
| Table memtable size/count gauges | StatusLogger, keyspace/global aggregation, metric exports. | Actual memory-pressure and flush decisions use allocator/memtable state. Suppressing a gauge must not suppress that underlying state. |
| Table tombstone failure/warning counters and scanned histograms | Report outcomes of per-request tombstone checks. | ReadCommand compares a local tombstone count to the configured limit, then increments the metric. A no-op counter does not disable the check. |
| Table speculative retry and additional-write counters | Count decisions made using the separate latency thresholds. | These counters do not determine when to speculate. |
| Table samplers and TopPartitionTracker | Operator sampling/top-partition output and its persistence. | Sampling flags control the cost of collecting diagnostics. They are separate from the query's data semantics and from ordinary metric registrations. |
| `Storage.Load` | LoadBroadcaster publishes the count through gossip; received load values feed the management load map. | Protocol-observable accounting. No in-tree placement/routing consumer was found on this branch. |
| Keyspace `ReadOutOfRangeToken`, `WriteOutOfRangeToken`, `PaxosOutOfRangeToken` | StorageService filters/populates an out-of-range diagnostic map. | No admission, recovery, or routing decision found in the traced consumers. |
| BufferPool private `memoryInUse` / `overflowMemoryUsage` counters | Exported size gauges, status output, and test accessors. | Allocation admission uses separate atomic `memoryAllocated` state. These private counters are also outside ordinary registration filtering. |
| `MemtablePool.PendingFlushTasks` | A gauge-reading accessor exists; no production caller of that accessor was found. | Memtable cleanup decisions use separate pool usage and cleaner state. |
| Accord cache/executor histograms and cache hit-rate shards | Record/report behavior in the inspected paths. | Cache eviction uses independent byte accounting. Concrete shard interfaces still need a compatible disabled implementation. |
| SAI query/builder metric families | Report query and build outcomes. | Timeout uses elapsed time and an execution quota; segment flushing uses independent byte accounting and a memory limiter. |

The maximum-SSTable accessors are in
[ColumnFamilyStore](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L3503).
The tombstone decision and its metric update are adjacent in
[ReadCommand](../src/java/org/apache/cassandra/db/ReadCommand.java#L688).
For samplers, see the management entry point
[beginLocalSampling](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L2137)
and the read-path collection flag in
[ReadExecutionController](../src/java/org/apache/cassandra/db/ReadExecutionController.java#L134).

Other boundaries: [LoadBroadcaster](../src/java/org/apache/cassandra/service/LoadBroadcaster.java#L93),
[StorageService diagnostics](../src/java/org/apache/cassandra/service/StorageService.java#L343),
[BufferPool allocation admission](../src/java/org/apache/cassandra/utils/memory/BufferPool.java#L446),
[SAI query quota](../src/java/org/apache/cassandra/index/sai/QueryContext.java#L84),
[SAI segment memory](../src/java/org/apache/cassandra/index/sai/disk/v1/segment/SegmentBuilder.java#L151),
and [Accord cache eviction](../src/java/org/apache/cassandra/service/accord/AccordCache.java#L227).

## Object contracts that a no-op must preserve

Even optional numeric state has callers that expect usable objects:

- Commit-log and journal waits pass timer contexts through wait completion.
  Elapsed timer values do not control durability, but `.time()` and `stop()` must
  remain valid. Timing a supplied operation must still execute that operation.
- Registry timer factories return `SnapshottingTimer`; percentile policies call
  its additional snapshot method. `LatencyMetrics` uses its concrete inner timer
  type. A generic Timer replacement does not cover those interfaces.
- Latency aggregation casts snapshots to `EstimatedHistogramReservoirSnapshot`
  and mutates/merges them. A generic empty snapshot can fail the cast; a shared
  mutable snapshot can contaminate another metric.
- Shared no-op identity must not enter per-table aggregation sets. Otherwise
  dropping one table can remove the singleton's membership for other tables.
- Omitting a registry entry must not prevent cleanup of a required internal
  metric attached to a parent. Current table cleanup discovers metrics through
  registered names.
- Accord histogram/hit-rate shards expose concrete update, replacement and
  decrement operations. A Dropwizard no-op alone does not cover those paths.

Relevant contracts: [SnapshottingTimer](../src/java/org/apache/cassandra/metrics/SnapshottingTimer.java#L39),
[mutable latency snapshots](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L137),
[table release](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L952),
[aggregation set membership](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1189),
[Accord cache shards](../src/java/org/apache/cassandra/metrics/AccordCacheMetrics.java#L64),
and [commit-log wait context](../src/java/org/apache/cassandra/db/commitlog/AbstractCommitLogSegmentManager.java#L343).

## Consequences for an allowlist

Treat `exported` and `recorded` as separate decisions. Required internal state
can remain unregistered. Optional metrics can use shared no-ops when nothing
else enabled depends on their values. Management endpoints must have an explicit
disabled-metric contract; returning a plausible zero silently is not equivalent
to reporting that collection is disabled.

Do not infer enablement from the package or Java class alone. A statistics
object can drive compaction or request timing even if it has no JMX registration.
Conversely, a metric with a name such as `TombstoneFailures` can be entirely
observational. Preserve the operation wrapped by a timer even when timing is
disabled.

The first implementation should keep the confirmed control inputs recorded
under all export configurations. Conditional removal can follow once feature
configuration and runtime transitions have focused tests. It should resolve
aggregate dependencies before creating optional child metrics, and evaluate
the allowlist before allocating names, metric IDs, reservoirs or registrations.

## Evidence and validation limits

The audit searched direct metric reads, snapshot/rate/count APIs, public table
metric field references, and indirect accessors. It then traced callers and
registration names. Candidate search output is preserved in
`logs/20260906-120343-internal-metric-read-candidates.log`; a candidate is not a
confirmed dependency. Map values, serialized database counters, checksum values,
and schema snapshots are examples of unrelated search matches.

This inventory changes documentation only. It does not run a disabled-metrics
server or establish that all other metrics can safely become no-ops. Before
shipping that behavior, targeted tests must cover speculation, Paxos/Accord wait
policies, hint admission, repair admission/order, cleanup sizing, compaction
statistics, aggregate values and table-drop/thread-exit lifecycles.
