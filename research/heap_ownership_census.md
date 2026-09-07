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

# Resident heap ownership census

Measured 2026-09-06 on `moar_tables`, production code at `24714c59fd`.
This experiment adds measurement tools. It changes no production behavior.

## Finding

JMX registration and export state are the largest measured residency blocker.
Compact histogram storage reduced the recording state, but each table still
creates hundreds of registrations. Reading metric-name properties and recent
histogram values adds more persistent state outside the compact reservoirs.

Across 100 to 1,000 empty tables, whole-JVM heap grows by about **252 KiB/table**
after creation. It grows by about **493 KiB/table** after full attribute and name
property inspection, eight recording workers, and another scrape. These are
endpoint slopes in this experiment, not capacity guarantees or steady-load tests.

The attributes-only control reaches about **327 KiB/table** at the final
checkpoint. Name inspection accounts for most of the difference from the full
scrape. This distinction should stay in future performance comparisons.

The next substantial optimization should address metric registrations and their
JMX representation. Compact recent-value history is a smaller, separable change.
Further memtable initialization tuning cannot recover most of this measured cost:
every user TrieMemtable remained uninitialized throughout these runs.

## Method

The new [harness](../test/distributed/org/apache/cassandra/distributed/test/HeapOwnershipCensusHarness.java)
uses the existing in-JVM single-node cluster runner. Each table count gets a fresh
Java Virtual Machine (JVM): Java 21.0.12, eight reported processors, 8 GiB maximum
heap, 512 MiB initial heap, default G1 collection, compressed object/class
references, and eight-byte object alignment. The summary records effective flags.
Soft-reference retention uses the existing launcher's
`-XX:SoftRefLRUPolicyMSPerMB=0`. This affects cache retention and must remain fixed
when comparing results.

Configuration: compact metrics enabled; lazy TrieMemtables; BTI SSTable format;
SizeTieredCompactionStrategy; row and key caching disabled for user tables.
Cursor compaction is disabled. User tables contain no data or SSTables. System
tables still write and compact during schema creation.

Eight named worker threads start before the baseline checkpoint and remain alive
through all heap captures. Checkpoints follow these operations:

1. Node startup, before the user keyspace exists.
2. Create the user keyspace and N empty tables.
3. Enumerate all Cassandra metric managed beans (MBeans), then read every readable
   attribute, including recent histogram values and all aliases.
4. Activate worker zero. Record one observation per table in `TotalRowsRead`,
   `ReadRepairRequests`, `SSTablesPerReadHistogram`, and `ReadLatency`.
5. Activate the other seven workers sequentially, with the same observations.
6. Repeat the metric scrape.

These are synthetic updates to real table metrics. They isolate metric residency;
they do not measure request throughput, concurrent contention, or data residency.
All selected table counts must equal zero, one, or eight at the corresponding
checkpoint. Each active worker must have distinct live counter storage. Every
user memtable must remain clean and uninitialized, with no live user SSTables.

Each checkpoint performs collection, captures a live class histogram with
`jcmd GC.class_histogram`, records heap usage, and writes a live HPROF dump.
Histogram, heap-usage reading, and dump are separate instants. Background system
work can change between them.

The full scrape also inspects registered `ObjectName` properties. A separate
`--attributes-only` control reads identical MBean attributes while ordering and
matching names through their canonical strings. This avoids asking registered
names to materialize their property maps.

## Whole-JVM measurements

The full scrape includes name-property inspection. Values below are bytes, so the
original numbers can be reused without rounding.

| Checkpoint | 100 tables | 500 tables | 1,000 tables |
|---|---:|---:|---:|
| Baseline | 60,372,616 | 60,493,704 | 56,371,184 |
| Created | 76,820,336 | 178,802,088 | 309,313,352 |
| Full scrape | 116,405,888 | 312,387,712 | 559,962,544 |
| One recording worker | 116,811,504 | 313,731,760 | 562,437,584 |
| Eight recording workers | 118,877,216 | 319,668,472 | 572,953,120 |
| Repeat full scrape | 118,767,784 | 319,683,048 | 573,341,472 |

At 1,000 tables this is 295.0 MiB after creation and 546.8 MiB at the final
checkpoint. Baseline subtraction alone is misleading: startup system state can
disappear while schema work adds other system state. The creation slopes are
249.0 KiB/table between 100 and 500, and 254.9 KiB/table between 500 and 1,000.
The 100-to-1,000 slope is 252.3 KiB/table. There is no evidence of exponential
resident growth within this small range; these points do not establish behavior
at 100,000 or one million tables.

Both full scrapes passed with zero attribute failures at every size:

| Tables | Cassandra metric MBeans | Readable attributes per scrape | Recent attributes |
|---|---:|---:|---:|
| 100 | 41,831 | 226,473 | 9,916 |
| 500 | 141,431 | 746,473 | 32,716 |
| 1,000 | 265,931 | 1,396,473 | 61,216 |

The user-keyspace counts are `249 * tables + 101`. The fixed 101 belongs to
keyspace-level metrics. Aliases count as separate MBeans even when they share a
metric object.

## Retained owners

[Eclipse Memory Analyzer (MAT)](https://help.eclipse.org/latest/topic/org.eclipse.mat.ui.help/tasks/batch.html)
computed dominator trees. A dominator's retained size is the heap that loses its
root paths when that object disappears. The following rows group disjoint
top-level dominator subtrees by class. They are not totals for every instance of
that class, and a single metric can have several owners outside these subtrees.

| Root dominator group | 100 created, bytes | 1,000 created, bytes | Growth, KiB/table |
|---|---:|---:|---:|
| JMX servers | 25,361,336 | 161,123,264 | 147.3 |
| Four concurrent maps | 6,893,832 | 43,706,440 | 39.9 |
| Class objects and their dominated state | 8,135,856 | 25,686,176 | 19.0 |
| ColumnFamilyStore objects | 7,422,704 | 19,962,488 | 13.6 |
| TableMetrics objects | 650,608 | 4,380,208 | 4.0 |
| ThreadLocalMetrics contexts | 1,214,992 | 3,873,640 | 2.9 |

Field queries attribute 43,670,536 of the concurrent-map group's 43,706,440 bytes
at N1000 to `CassandraMetricsRegistry.metrics`. At N100 that registry map retains
6,857,928 bytes: a slope of 39.9 KiB/table. This is another substantial registration
cost alongside the JMX server.

Within the N1000 class-object group, the `ThreadLocalMetrics` class retains
11,449,240 bytes, `TableMetrics` retains 4,226,968 bytes, and `ThreadLocalMeter`
retains 2,971,256 bytes. These are static ownership totals, not class metadata
alone. They include metric-ID lifecycle and shared storage. They overlap the
class-object row and must not be added to it.

A field query further identifies 9,985,176 retained bytes in
`ThreadLocalMetrics.phantomReferences` and 1,463,424 bytes in `summaryValues`.
Metric-ID lifecycle tracking is therefore a residency target in its own right,
alongside worker arrays. These nested totals overlap the class-owned state.

At 1,000 tables, a full scrape increases JMX-server retained size from
161,123,264 to **411,405,432 bytes**. That accounts for almost all the whole-JVM
scrape increase. The metric recording objects remain shared outside much of this
JMX subtree; replacing histogram counters alone will not remove it.

The 1,000 individual user ColumnFamilyStore roots retain about 9.2 KiB each.
Nested examples are 1,592 bytes for the dictionary-compression manager, 3,096 bytes
for the compaction strategy manager, about 480–496 bytes for the index manager,
and 1,120 bytes for the data tracker. These sizes overlap the CFS total and must
not be added to it. Registered metrics, shared schema, scheduled tasks and other
external owners are outside this individual CFS retained size.

## Why property inspection matters

The installed JDK 21 implementation stores a lazy `_propertyList` in each
`ObjectName`. `getKeyProperty()` fills a hash map with every property, including
separate key/value strings. Natural `ObjectName` sorting also calls
`getKeyProperty("type")`. The initial harness did both natural sorting and an
explicit keyspace-property lookup.

The broad domain query used here returns the registered names themselves. Those
objects remain reachable through the JMX server. Their property caches survive
after the scrape returns. Canonical-name lookup and ordinary bean retrieval do
not require the same cache on the normal path used here.

This is a real cost for clients that inspect those names, but it is not an
unavoidable cost of every `getAttribute` call. The attributes-only controls
separate it from histogram history. Installed-JDK bytecode and source-line tables
are preserved in `logs/20260906-045256-jdk21-objectname-bytecode.txt`.

The controls read the same numbers of attributes, with zero failures:

| Checkpoint | 100 tables, bytes | 1,000 tables, bytes |
|---|---:|---:|
| Created | 76,994,424 | 306,973,368 |
| Attributes-only scrape | 88,796,144 | 379,924,184 |
| One recording worker | 89,194,960 | 382,402,232 |
| Eight recording workers | 91,259,560 | 392,971,528 |
| Repeat attributes-only scrape | 91,424,592 | 392,795,024 |

The attributes-only creation slope is 249.5 KiB/table. Reading all attributes
adds 66.4 KiB/table to that slope. At 1,000 tables, the class histogram shows
about 72.5 MB more `long[]` storage and no material increase in the property-map
classes. The full scrape adds those same arrays plus millions of strings and
map entries. The full-run creation heaps vary by about 2.3 MB at 1,000 tables,
so direct object attribution is more precise than subtracting cross-run totals.

## Direct metric ownership

At both 100 and 1,000 tables after a full scrape, canonical-name attribution finds
exactly 249 wrappers per table, plus 101 keyspace-only wrappers. This agrees with the
scrape count. Metric-identity-only traversal missed 25 wrappers per table;
final reports use each wrapper's registered name and check identity attribution
for conflicts.

| Per user table after full scrape | Objects | Shallow bytes |
|---|---:|---:|
| Metric JMX wrappers | 249 | 7,072 |
| Independent recent-value `last` arrays | 57 | 67,632 |
| Name-property cache graphs | 5,478, including 249 maps | 166,744 |

The history arrays contain 66,720 payload bytes per table; the remainder is array
headers/alignment. These fields are null before the first recent-value read.
The property-cache graphs contain maps, backing arrays, nodes, strings and string
storage. The table rows are identity-deduplicated; aliases have distinct cursor
arrays and property maps. These costs sit inside the JMX retained subtree and
must not be added to that subtree's total.

All 1,000 tables have the same counts and byte totals shown above. Name-based and
metric-identity attribution report no conflicts. Across the full JVM, including
system/keyspace/global metrics, the final N1000 dump contains 265,932 populated
ObjectName property maps whose deduplicated graphs total 177,765,424 bytes.

The N100 worker checkpoints isolate counter storage. After one worker records,
that worker owns a 36,725-element `long[]`. After eight workers record, all eight
own arrays of that size. Each array has 293,800 payload bytes. The seven added
arrays account for exactly 2,056,600 payload bytes, close to the whole-JVM
increase of 2,065,712 bytes. Only a few metric IDs per table receive observations;
the dense array still spans intervening IDs. This is an existing thread-local
counter cost, not eight copies of the compact histogram reservoir.

At N1000, every recording worker has 187,559 counter slots, or 1,500,472 payload
bytes. Eight workers retain 12,003,776 payload bytes in these arrays. The
100-to-1,000 increment is about 1.31 KiB/table per worker. This does not include
the other Cassandra threads' arrays or the shared metric-ID lifecycle state.

## Remaining work

1. Reduce metric registration and JMX name/wrapper residency while preserving
   names, aliases, types, discovery and query behavior. A replacement recorder
   behind the same eager registration graph will leave the largest cost in place.
2. Keep empty recent-value history implicit and investigate compact nonempty
   history. Preserve each alias's independent delta cursor and returned-array
   behavior. Percentile reads alone do not populate the JMX `last` field.
3. Replace holes in per-worker counter arrays with storage that follows used
   metric IDs, if the measured memory gain justifies the update/read cost.
4. Defer unused table services, then return to bounded automatic idle retirement
   and broader table-runtime unloading. Memtable retirement alone cannot reclaim
   names, registrations, counters, schema, or the remaining CFS graph.

Preserving all current names implies about 249 million table metric names at one
million tables for this schema/configuration. Even a virtual registration scheme
must account for the temporary output and runtime of clients that enumerate all
names. A compact recording backend alone does not solve export cardinality.

For one million tables, even 1 KiB/table costs about 0.95 GiB. Linear extrapolation
of the measured 252 KiB/table creation slope is about 241 GiB; the fully scraped
493 KiB/table slope is about 470 GiB. These figures describe the scale of the
gap, not a tested capacity. Larger heaps may also lose compressed references.
Data, SSTable readers, request bursts and collection headroom add further costs.

## Artifacts and reproduction

Full runs, each with six heap dumps and class histograms:

- `logs/heap-census-100/20260906-004611-heap-ownership-100t/`
- `logs/heap-census-500/20260906-004722-heap-ownership-500t/`
- `logs/heap-census-1000/20260906-004849-heap-ownership-1000t/`

Attributes-only controls:

- `logs/heap-census-attributes-100/20260906-005304-heap-ownership-100t/`
- `logs/heap-census-attributes-1000/20260906-005401-heap-ownership-1000t/`

Selected ownership evidence:

- `logs/20260906-005619-426722-rescraped-heap-ownership.json`: complete N100
  wrapper/history/name attribution.
- `logs/20260906-010055-127934-rescraped-heap-ownership.json`: complete N1000
  attribution, worker arrays, and size-method coverage (99.795% of indexed bytes
  use histogram-calibrated instances or explicit array layout; 0.205% estimated).
- `logs/20260906-005150-285885-workers-8-heap-ownership.json`: N100 worker arrays;
  this earlier report's table-wrapper attribution is incomplete and superseded
  by the final canonical-name attribution.
- `logs/20260906-005110-created-dominators.zip`: N1000 created dominators.
- `logs/20260906-005146-scraped-dominators.zip`: N1000 full-scrape dominators.
- `logs/20260906-005612-scraped-dominators.zip`: N1000 attributes-only dominators.
- `logs/20260906-005322-created-dominators.zip`: retained user CFS objects and
  selected nested services, queried by user keyspace.
- `logs/20260906-005649-created-dominators.zip` and
  `logs/20260906-005852-created-dominators.zip`: N1000 and N100 registry-map
  retained sizes.
- `logs/20260906-005853-created-dominators.zip`: N1000 class-owned static state.
- `logs/20260906-010218-created-dominators.zip`: retained metric-ID phantom
  references, shared summary values and free-ID tracker.
- `logs/20260906-005611-613958-census-summary.log`: checkpoint values, scrape counts,
  histogram deltas and endpoint slopes for all five runs.

Each directory contains `summary.json`, `checkpoint-*.json`, `scrape-*.json`,
`histogram-*.txt`, and `*.hprof`. The JSON files record effective configuration,
worker identities and assertion results. Generated artifacts stay out of Git.

```bash
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 100 --out logs/heap-census-1000
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --attributes-only --subnet 102 --out logs/heap-census-attributes-1000
distrobox enter dev -- bash .build/sh/ai-analyze-heap-dominators logs/heap-census-1000/20260906-004849-heap-ownership-1000t/created.hprof
venv/bin/python .build/sh/analyze-heap-ownership.py --expected-tables 1000 logs/heap-census-1000/20260906-004849-heap-ownership-1000t/created.hprof
```

The MAT wrapper uses the existing installation in `tmp/mat/mat`; it installs
nothing. CSV query reports avoid the chart renderer's headless graphics failure.
Timestamped copies preserve reports when subsequent queries reuse the same dump.
The wrapper returns failure if MAT embeds a query error without producing CSV.

The direct HPROF analyzer reports deduplicated field graphs and class counts.
These shallow-size groups can overlap. Its disjoint partition covers indexed
instance and array records, not every class mirror or native allocation. It uses
class-histogram sizes where available and explicit compressed-array layout.
Fallback instance sizes are estimates and have a reported coverage fraction.
Bounded traversal is not a substitute for a dominator tree; an object claimed by
one inspected table can still have other incoming references.

Validation includes full and attributes-only three-table smoke runs and three
focused configuration/name-boundary tests (`logs/20260906-005229-ai-ci-test.log`).
The full build and main/test Checkstyle passed
(`logs/20260906-005610-ai-build.log`). The larger runs validate the actual
metric and scrape paths. This experiment does not claim production-load or
long-duration stability coverage.
