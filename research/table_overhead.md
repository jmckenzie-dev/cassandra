<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Many-table overhead

## Executive summary

Cassandra's many-table scalability problem has two primary axes and one deeper lifecycle issue.

1. Table creation performs work that grows with the existing schema and metric population. The clearest defect is quadratic aggregate copying in `ThreadLocalMeter`.
2. Every live table keeps approximately linear standing heap state. The N=100 runs measured about 279–285 KB per empty table after garbage collection (GC).
3. Cassandra has no cold or dormant table state. A table is fully opened, with a `ColumnFamilyStore` (CFS), memtable, metrics, and management state, or it is absent.

The N=5,000 run completed every `CREATE TABLE` statement. Creation took 638.181 seconds, or about 128 ms per table. Early creates, after the first-table startup cost, took about 87–107 ms. The final creates mostly took about 165–198 ms. The run then completed the 300-second hold and entered teardown. The overall 30-minute task cap stopped teardown; table creation did not time out.

The first change to test is geometric or chunked growth of the static `ThreadLocalMeter.rates` array. It is small and directly removes accidental O(n²) copying. Heap-dump dominator analysis must precede claims about the main standing retained owner. Metrics are the largest identified resident family, but current evidence does not prove that metrics own most retained heap.

## Problem taxonomy

```text
many live tables
|
+-- creation-time work
|   +-- ThreadLocalMeter exact-size array growth    confirmed O(n²) aggregate
|   +-- schema BTree copy-on-write                 measured growing allocation
|   `-- periodic full metadata snapshots           plausible; contribution unmeasured
|
+-- standing heap residency
|   +-- eager CFS and active memtable              source-confirmed
|   +-- eager metrics and MBean registrations      source-confirmed and partly measured
|   `-- schema, registries, strings, containers    mostly unattributed retained state
|
`-- lifecycle design
    `-- no dormant table state: fully open or absent
```

Creation work and residency need separate fixes. Reducing allocation during `CREATE TABLE` does not remove steady heap state. Reducing one resident family does not remove schema-operation copying. The lifecycle issue spans both axes because eager opening creates much of the work and the retained state.

## Evidence labels

This note uses four labels:

- **Measured fact:** a value recorded by the profiling harness or a class census.
- **Source-confirmed mechanism:** behavior directly present in the current source.
- **Inference:** an explanation consistent with measurements and source, but not isolated by an experiment.
- **Unknown:** a question that needs a targeted profile or retained-heap analysis.

### Confirmed facts

- **Measured fact:** N=100 post-GC heap grew from about 82.4 MB to about 110.2 MB. This is about 285 KB per table in the first run and about 279 KB per table in the record-once run.
- **Measured fact:** the immediate N=100 class-histogram delta was about 432 KB per table. That snapshot used a different time and GC view from the steady post-GC measurement. The two values do not conflict.
- **Measured fact:** N=100 creation averaged about 89 ms per table and stayed flat from table 0 through table 99.
- **Measured fact:** all 5,000 creates completed, and per-table latency roughly doubled from early to late creation.
- **Measured fact:** `ThreadLocalMeter.allocateRateGroupOffset()` accounted for 9.9% of sampled creation allocation at N=100 and 60.8% at N=5,000.
- **Measured fact:** schema BTree copy paths accounted for about 12% of sampled N=5,000 creation allocation.
- **Source-confirmed mechanism:** CFS construction eagerly creates an active memtable and `TableMetrics`.
- **Source-confirmed mechanism:** metric registration constructs Java Management Extensions (JMX) wrappers and registers them through `MBeanWrapper`.
- **Measured fact:** the N=100 census found about 272 metric registry registrations and 33 `DecayingEstimatedHistogramReservoir` objects per table.

### Inferences

- The rising create latency is work amplification, not an explicit `CREATE TABLE` rate limiter. A review of the schema statement and TCM commit paths found no such limiter.
- Periodic full-schema snapshots probably add another growing cost. The exact share has not been isolated.
- The lack of a dormant state makes many cold tables pay active-table costs. It also makes isolated fixes less complete than a lifecycle design.

### Unknowns

- Which object graph dominates the roughly 86% of immediate histogram growth assigned to generic or diffuse classes?
- Which owners dominate retained heap at N=5,000 after GC?
- How much creation time and allocation come from each schema BTree operation versus metadata snapshots?
- How much heap would lazy memtables save after shared pools and indirect references are included?
- Which tools and users require per-table MBeans rather than the CQL virtual-table interfaces?

## Superlinear table-creation work

### Measured scaling

| Tables | Creation elapsed | Mean per table | Early latency | Late latency | Result |
|---:|---:|---:|---:|---:|---|
| 100 | about 8.9 s | about 89 ms | about 89 ms | about 89 ms | Flat at this scale |
| 5,000 | 638.181 s | 127.6 ms | about 87–107 ms after startup | mostly about 165–198 ms | All creates completed |

The N=5,000 phase ran from 12:35:34.409 to 12:46:12.590 on 2026-09-04. The final 16 CSV values span 152–198 ms, with most in the stated 165–198 ms range. The durable values above come from local run directory `logs/20260904-083519-many-tables-5000t`. The [N=100 run report](../.plans/many-tables-harness-run-1.md#first-findings-n100) records the smaller run and repeatability result.

### `ThreadLocalMeter` exact-size growth

**Source-confirmed mechanism:** each meter uses three adjacent doubles because `RATES_COUNT` is 3 ([`ThreadLocalMeter.java:64-68`](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L64-L68)). The static array starts with capacity for 16 rate groups ([`ThreadLocalMeter.java:116-126`](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L116-L126)). Each new group advances the identifier by exactly 3. When the identifier exceeds capacity, the code allocates an array of exactly `rateGroupId + 3` and copies the full old array ([`ThreadLocalMeter.java:128-154`](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L128-L154)). It leaves no spare capacity after an expansion.

Let `G` be the total number of allocated rate groups, including the groups created before the measured phase. After the initial 16 groups, expansion `g` copies `3g` doubles. With 8-byte doubles, cumulative copied bytes through `G` groups are approximately:

```text
8 * sum(g = 16 to G - 1, 3g)
= 12 * G * (G - 1) - 2,880 bytes
```

Thus one new meter can copy O(G) data, and creating G meters copies O(G²) data in aggregate. The N=100 profile assigned 9.9% of sampled creation allocation to this method. The N=5,000 profile assigned 60.8%. This sharp rise matches the source-level growth rule.

An earlier census estimate used `G ~= 125,000` and produced about 187.5 decimal GB, rounded to **about 190 GB copied**. This is a formula estimate, not a byte counter. The group-count assumption needs qualification. The N=5,000 class histograms contain 4,093 live `ThreadLocalMeter` objects before creation and 224,093 after it. That is 220,000 new live meter objects during the phase. If each unrecycled live meter received a distinct group, the same formula implies about 602 GB copied during the phase. The allocation profile estimated about 591 GB on stacks below `allocateRateGroupOffset`, which supports the larger order of magnitude. The prior 190 GB figure remains useful only as the result of its stated 125,000-group assumption.

### Schema BTree copy-on-write

**Measured fact:** stacks through `BTreeRemoval` and `BTreeMap.with/without` contributed about 12% of sampled N=5,000 allocation. `CREATE TABLE` builds updated immutable schema objects ([`CreateTableStatement.java:175-198`](../src/java/org/apache/cassandra/cql3/statements/schema/CreateTableStatement.java#L175-L198)). `BTreeMap.with` calls `BTree.update`, while `without` calls `BTreeRemoval.remove` ([`BTreeMap.java:54-80`](../src/java/org/apache/cassandra/utils/btree/BTreeMap.java#L54-L80)). Removal copies nodes along the affected path ([`BTreeRemoval.java:76-150`](../src/java/org/apache/cassandra/utils/btree/BTreeRemoval.java#L76-L150)).

The profile confirms a growing schema-copy family. It does not show that every BTree allocation is avoidable. Optimization needs isolation by call path and schema collection before changing immutable-schema semantics.

### TCM metadata snapshots

**Source-confirmed mechanism:** `metadata_snapshot_frequency` defaults to 100 ([`Config.java:244-247`](../src/java/org/apache/cassandra/config/Config.java#L244-L247)). The local metadata log schedules `TriggerSnapshot` when the epoch is divisible by that value ([`LocalLog.java:955-967`](../src/java/org/apache/cassandra/tcm/log/LocalLog.java#L955-L967)). The listener then stores a metadata snapshot ([`MetadataSnapshotListener.java:38-53`](../src/java/org/apache/cassandra/tcm/listeners/MetadataSnapshotListener.java#L38-L53)).

**Inference:** repeated serialization of a growing full schema can add superlinear aggregate work across thousands of mutations. **Unknown:** this run did not isolate snapshot allocation or elapsed time. The snapshot-frequency experiment below must measure the exact contribution.

## Standing heap residency

### Eager CFS and memtable state

Schema application calls `Keyspace.initCf`, which creates and retains a CFS for a new table ([`Keyspace.java:369-393`](../src/java/org/apache/cassandra/db/Keyspace.java#L369-L393)). When Cassandra has initialized the daemon, the CFS constructor immediately calls `createMemtable` and passes it to the tracker ([`ColumnFamilyStore.java:524-534`](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L524-L534)). The factory call creates the configured memtable implementation ([`ColumnFamilyStore.java:1462-1465`](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1462-L1465)). No read or write is needed.

**Measured fact:** direct shallow growth of named CFS, memtable, and compaction classes was only about 1.1 KB per table at N=100. This class-level number excludes objects retained through generic collections, strings, arrays, and shared structures. It does not prove that memtables dominate residency. A heap dominator report is required before assigning retained ownership.

### Eager metrics and MBeans

The CFS constructor creates `new TableMetrics(this)` after CFS initialization ([`ColumnFamilyStore.java:567-570`](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L567-L570)). The constructor creates gauges, counters, histograms, timers, meters, and samplers without a traffic gate ([`TableMetrics.java:433-579`](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L433-L579), [`TableMetrics.java:653-927`](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L653-L927)). Metric registration creates and registers an MBean wrapper before adding the metric to the registry ([`CassandraMetricsRegistry.java:425-445`](../src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java#L425-L445), [`CassandraMetricsRegistry.java:552-584`](../src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java#L552-L584)). Table unload removes registry entries and unregisters the MBeans ([`TableMetrics.java:949-962`](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L949-L962), [`ColumnFamilyStore.java:766-778`](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L766-L778)).

The N=100 census found these approximate standing amounts per table:

| Identified family | Approximate shallow growth per table |
|---|---:|
| `ObjectName`, `ObjectName.Property`, and property arrays | 46 KB |
| JMX gauge, meter, counter, timer, and histogram wrappers | 6.8 KB |
| Metric objects, including meters, reservoirs, buckets, counters, and timers | 7.7 KB |
| **Identified metrics/MBean total** | **about 62 KB** |

The census counted about 272 registry registrations and 33 `DecayingEstimatedHistogramReservoir` objects per table. The identified total is about 14% of the 432 KB immediate histogram delta. It is about 17% if compared with the smaller 279–285 KB post-GC standing estimate. Those denominators describe different snapshots, so the percentage is a range rather than exact attribution.

Metrics are the largest identified resident family. They are not proved to be the largest total retained owner. About 86% of immediate histogram growth remains in diffuse or generic classes. Only a dominator analysis can assign those objects to owning roots.

The in-JVM distributed test does not put node metric MBeans in the platform MBean server. It installs a dedicated `InstanceMBeanWrapper` server ([`IsolatedJmx.java:165-172`](../test/distributed/org/apache/cassandra/distributed/impl/IsolatedJmx.java#L165-L172), [`MBeanWrapper.java:225-240`](../src/java/org/apache/cassandra/utils/MBeanWrapper.java#L225-L240)). The platform server therefore stayed near 27 MBeans while the dedicated server retained the metric wrappers. This explains the inert harness counter; it does not show that MBean registration was absent.

### Other retained state

Schema metadata, metric registry maps, MBean repository structures, names, strings, arrays, and collection nodes all grow with table count. The N=100 class census assigned only about 0.4 KB per table directly to named schema classes. That shallow count cannot assign generic storage to schema or metric roots. The immediate histogram gap of about 370 KB per table is therefore an attribution gap, not an identified component.

### Separate flush and compaction finding

The N=100 allocation profile attributed 27.3% to `StreamingTombstoneHistogramBuilder$Spool`. This is not metrics allocation. `MetadataCollector` creates the tombstone histogram builder on the SSTable writer path ([`MetadataCollector.java:115-123`](../src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java#L115-L123)). The default spool capacity is 100,000 ([`SSTable.java:69-73`](../src/java/org/apache/cassandra/io/sstable/SSTable.java#L69-L73)). Its backing storage can approach about 3 MB per writer instance. Treat this as a separate flush and compaction tuning track.

## The cold-table lifecycle issue

Cassandra currently treats a schema table as active local runtime state. Schema application opens its CFS, initial memtable, compaction strategy, metrics, MBeans, managers, and trackers. The alternative is to drop or unload the table. There is no intermediate state for a valid but inactive table.

This design is broader than the exact-size meter array bug. Geometric growth can remove quadratic creation allocation while every cold table still retains active state. MBean or reservoir changes can reduce heap while schema updates still copy growing structures. A dormant state would change startup, first access, concurrent open, write admission, read behavior, repair, compaction, streaming, schema change, drop, and observability semantics. It therefore needs a design project rather than a local optimization patch.

## Existing metric-read alternatives

`system_views` registers table metric virtual tables ([`SystemViewsKeyspace.java:40-60`](../src/java/org/apache/cassandra/db/virtual/SystemViewsKeyspace.java#L40-L60)). Those tables iterate CFS instances and read `cfs.metric` directly ([`TableMetricTables.java:191-228`](../src/java/org/apache/cassandra/db/virtual/TableMetricTables.java#L191-L228)). The `system_metrics` virtual keyspace exposes registry groups and metric-type tables ([`CassandraMetricsRegistry.java:209-268`](../src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java#L209-L268)). The daemon registers that virtual keyspace at startup ([`CassandraDaemon.java:641-646`](../src/java/org/apache/cassandra/service/CassandraDaemon.java#L641-L646)).

These CQL interfaces provide metric reads without JMX. They reduce the need for per-table MBeans, but they do not by themselves preserve every JMX name or management-tool contract.

## Candidate interventions

| Candidate | Expected effect | Risk | Scope |
|---|---|---|---|
| **A. Geometric or chunked `rates` growth** | Remove O(n²) copies; reduce creation allocation and late latency | Low; check concurrency, recycling, and unused capacity | Small first A/B candidate in `ThreadLocalMeter` |
| **B. Reduce BTree/full-schema copying or snapshot work** | Reduce the next visible growing creation costs | Medium to high; immutable metadata and snapshot durability are sensitive | Needs profile isolation before design |
| **C. Default-off or configurable registry MBean registration** | Estimated 60–75 KB per table when retained names, wrappers, and related strings are avoided | Public monitoring contract risk; tools may depend on names | Registry chokepoint plus compatibility decision; use virtual tables for reads |
| **D. Lazy metrics or reservoirs** | Avoid idle-table metric construction; 33 reservoirs per table are a concrete target | Empty-value and first-update concurrency semantics | Table metrics and registry construction paths |
| **E. Lazy memtable creation or dormant tables** | Remove active runtime state from cold tables | Broad lifecycle, latency, concurrency, and operations risk | Architectural project |
| **F. Tune `MetadataCollector` spool separately** | Reduce transient per-writer allocation on flush and compaction paths | Histogram accuracy and writer-performance trade-offs | Separate from table residency and creation fixes |

The 60–75 KB estimate for candidate C is an isolation target, not a guaranteed saving. The direct measured MBean name and wrapper classes total about 52.8 KB per table. The larger range includes related retained registry names and structures that need an MBean-off A/B run to confirm.

## Experiment matrix

| Experiment | Change | Measurements | Pass signal | Fail or caution signal |
|---|---|---|---|---|
| Geometric-growth 5k A/B | Change only `ThreadLocalMeter.rates` expansion | Phase time, per-table latency curve, allocation stacks, GC, post-GC heap | Meter-copy share collapses and late creates approach early latency with no metric errors | No material time gain, changed rates, races, or high unused heap |
| Heap-dump dominators | No product change; dump baseline and 5k steady hold | Retained size by GC root and dominator; paths for generic arrays, strings, maps | Most of the 86% gap gains named owners | Dump distortion, incomplete roots, or no repeatable attribution |
| MBean-off isolate | Skip metric MBean creation and registration only | Post-GC heap, ObjectName/wrapper counts, registry reads, virtual-table output | About 60–75 KB per table disappears while CQL metrics remain correct | Smaller saving or broken monitoring contracts |
| Lazy-metrics isolate | Delay per-table metric/reservoir creation until use | Post-GC heap, creation allocation, first update/read latency, metric values | Idle residency and creation cost fall; first-use behavior stays correct | Large first-use stalls, races, or changed empty metrics |
| Lazy-memtable isolate | Delay initial memtable until first write | Dominator sizes, idle heap, first-write latency, flush and commit-log behavior | Meaningful idle heap saving with correct first-write concurrency | Small retained saving or lifecycle regressions |
| Snapshot-frequency isolate | Compare 100 with a much larger frequency for the same 5k run | Snapshot count, schema allocation stacks, create-latency sawtooth, recovery artifacts | Snapshot share and periodic latency are quantified | Result changes durability semantics or does not isolate snapshot work |

Each A/B run must keep the node count, Java Development Kit (JDK), heap, table schema, profiler settings, and write mode fixed. Run one intervention at a time.

## Prioritized recommendation

1. Fix and A/B test the accidental O(n²) meter-array growth first. It has direct source proof, dominant N=5,000 allocation, and a small implementation scope.
2. Complete N=5,000 heap-dump dominator analysis before naming the main residency owner. Class histograms show object families, not retained ownership.
3. Start the per-table MBean contract discussion early. The likely saving is material, and agreement is harder than the code change.
4. Isolate schema BTree and snapshot costs before changing metadata structures.
5. Treat lazy memtables and a dormant-table lifecycle as a later design project.
6. Keep `MetadataCollector` spool tuning on its own flush and compaction track.

## Limitations and cautions

- The harness used one node in an in-JVM distributed test, not a production multi-node deployment.
- The runs used JDK 21.
- User tables were empty and cold.
- The default run performed no user writes.
- Local run artifacts live under ignored `logs/` paths and will not travel with this document. This note therefore records the key values, timings, counts, and interpretations directly.
- N=100 repeat runs were stable, but the N=5,000 result is one scale run.
- Allocation profiles use sampled estimates.
- Class histograms report shallow class totals and do not provide dominator attribution.
- Extrapolating 279–285 KB per table to 100,000 tables gives roughly 28 GB of heap. That extrapolation assumes linear standing residency and remains provisional.
- The 190 GB meter-copy figure depends on the earlier 125,000-group estimate. Current live-meter counts indicate that this assumption understates the number of groups and must be reconciled.

## Evidence index

- [N=100 harness findings and environment](../.plans/many-tables-harness-run-1.md)
- [N=100 metrics census and attribution](../.plans/h1-metrics-scope.md)
- [Harness design, measurement methods, and record-once validation](../.plans/many-tables-profiling-harness-context.md)
- Local N=100 record-once artifacts: `logs/20260903-102943-many-tables-100t`
- Local N=5,000 artifacts: `logs/20260904-083519-many-tables-5000t`
