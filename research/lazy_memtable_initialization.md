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

# Lazy TrieMemtable initialization

Phase 2 defers TrieMemtable's writable shard graph until its first local mutation.
Empty tables and replacements after a normal flush keep their lifecycle objects
but do not allocate shard storage. Correctness checks and all 12 comparison runs
pass. Matched live dumps confirm removal of 14,100 private state objects across
100 empty tables. The timing controls show lower standing heap and higher
first-write latency. Allocation recordings confirm that shard construction moves
from memtable creation and replacement onto writes. Phase 2 is implemented and
validated for this bounded TrieMemtable scope; changes remain uncommitted.

## Implementation and scope

Each TrieMemtable starts with a shared empty state. Its first mutation constructs
all shards and the merged trie under the memtable monitor. One volatile reference
publishes the complete state. Later writes read that reference and retain the
existing per-shard write locks. Reads, metrics, empty flushes, and diagnostics
do not initialize shard storage.

The existing allocator, commit-log bounds, metadata reference, shard boundaries,
metrics registration, and flush scheduling remain attached to the logical
memtable from construction. This preserves writes that entered operation ordering
before a memtable switch but reach the old memtable after that switch. Flush
still waits for those writers. Reclamation still waits for existing readers.

The TrieMemtable factory accepts `lazy_initialization`, default `true`; `false`
provides an eager control in the same build. The parser rejects values other
than case-insensitive `true` or `false`. Factory equality includes this option,
so schema changes can select the correct replacement behavior. The harness
exposes `--eager-memtable`, records configured parameters, and appends the
`initialized_trie_memtables` counter. A final `verified` checkpoint captures
state after exact-data reads.

This change applies to TrieMemtable. It does not unload ColumnFamilyStore,
schema, metrics, or SSTable readers. It does not change SkipList memtables,
flush policy, or slab sizes. The first write already allocates SlabAllocator's
one-MiB backing region on demand. Lazy shard construction cannot reclaim the
large slabs held by idle dirty tables. Safe idle flushing remains phase 3.

A read can perform a local mutation. The legacy table-backed secondary index
removes stale entries during reads; that cleanup correctly initializes its
index memtable. Pure reads preserve dormancy. An eventual idle policy must use
local mutation activity to account for this behavior.

Source and design:
[TrieMemtable](../src/java/org/apache/cassandra/db/memtable/TrieMemtable.java),
[phase plan](../.plans/lazy-memtable-initialization.md),
[continuation record](prosecute_memtable_tables.md),
[correctness review](../.reviews/lazy-memtable-correctness.md).

## Empty graph attribution

The original 100-table live heap dump contains eight shards per user table.
A bounded reference walk finds 140 objects per table in the complete eager
shard/merged-view graph. Their estimated aligned size is 8,592 bytes, about
8.4 KiB per table. Across 100 tables that is 14,000 objects and an estimated
859,200 bytes. The new eager control adds one state holder per initialized
memtable.

Object counts and array lengths come from the dump. Byte totals are layout
estimates assuming compressed references, compact field packing, and eight-byte
alignment. They are not a measured dominator retained size. The walk excludes
allocator, metrics, schema, and shared empty statistics. This sub-MiB target at
100 tables makes direct object disappearance more useful than a single
whole-JVM heap difference. See [ownership evidence](../.debug/lazy-memtable-ownership.md).

Matched new live dumps confirm the intended objects disappear:

| Mode | User TrieMemtables | Shards | Private state objects | Estimated private state bytes |
|---|---:|---:|---:|---:|
| Eager | 100 | 800 | 14,100 | 861,600 |
| Lazy | 100 | 0 | 0 | 0 |

Both created and settled dumps give the same counts. The eager total includes
one new holder per table, producing 141 private state objects and an estimated
8,616 bytes, about 8.41 KiB, per table. Every lazy user memtable refers to one
shared empty holder and zero-length shard array. Those shared objects remain
resident and are excluded from per-table ownership. The result proves removal
of the private shard graph; the byte total retains the layout-estimate limits
above.

Evidence: eager diagnostic `20260904-211139` and lazy diagnostic `20260904-211324`
under the comparison batch, with reference-walk output in
[the matched ownership log](../logs/20260904-211415-inspect-empty-trie-ownership.log).
Settled dumps precede final reads; the `verified` counters and tests establish
that those reads preserve dormancy.

## Validation

All executed workloads stay at 100 tables or fewer; the user's ceiling remains
1,000. The checks use Java 21.0.12 in the existing `dev` distrobox.
Build and Checkstyle passed in [the final build log](../logs/20260904-211425-ai-build.log),
including the final index-cleanup test correction.

| Check | Passed cases | Evidence |
|---|---:|---|
| New lifecycle and factory tests | 7 | [TrieMemtableLazyTest XML](../logs/20260904-210049-ai-test-memtable-lazy/TEST-org.apache.cassandra.db.memtable.TrieMemtableLazyTest.xml) |
| Generated operation model | 1 | [TrieMemtableLazyPropertyTest XML](../logs/20260904-210049-ai-test-memtable-lazy/TEST-org.apache.cassandra.db.memtable.TrieMemtableLazyPropertyTest.xml) |
| Commit-log recovery and index integration | 2 | [TrieMemtableLazyRecoveryTest XML](../logs/20260904-210049-ai-test-memtable-lazy/TEST-org.apache.cassandra.distributed.test.TrieMemtableLazyRecoveryTest.xml) |
| Harness configuration and seven three-table scenarios | 6 | [Harness log](../logs/20260904-205451-many-tables-launch.log) |
| Existing splittable and non-splittable flush ranges | 8 | [Existing test XML directory](../logs/20260904-210205-lazy-existing-tests/) |
| Existing heap/off-heap accounting | 6 | Same directory |
| Existing TrieMemtable metrics | 4 | Same directory |
| Existing memtable configuration | 15 | Same directory |

The XML reports zero failures, errors, and skips for the 43 production cases.
The harness reports `OK (6 tests)`; two integration test methods run seven
small real-cluster scenarios in total. These are targeted checks, not the full
Cassandra suite.

The lifecycle tests cover concurrent first writes, an accepted first write
arriving after switch-out, readers retained across flush, empty range and flush
metadata, schema addition, truncate, and a tombstone-only first mutation. Four
seeded sequences compare actual query results with an independent row model
after writes, deletes, flushes, and truncation.

The recovery test synchronizes the commit log, stops the node without draining
memtables, verifies that the user table has no data SSTable, and restarts the
node. It verifies every recovered row, then repeats writes and flushes. Index
integration covers Storage-Attached Indexing (SAI) and the legacy table-backed
index across insert, update, delete, flush, and disk reads. The legacy-index
test also checks the local stale-entry cleanup described above.

## Comparison method

Batch: `logs/20260904-210325-lazy-memtable-comparison/`.
The [batch script](../logs/20260904-210325-lazy-memtable-comparison/run-lazy-memtable-comparison.sh)
runs serially. It alternates modes in eager, lazy, lazy, eager order, each with
an empty workload and a written/flushed workload. Each mode also gets one
profiled written/flushed run and one separate empty-table heap diagnostic:
12 runs in total.

Each fresh Java Virtual Machine (JVM) uses an eight-GiB maximum heap and eight
reported processors. Both modes select TrieMemtable, BTI SSTables, heap buffers,
size-tiered compaction, and disabled key/row caches. Cursor compaction remains
disabled. The node's effective memtable heap budget is 10 MiB; its off-heap
budget is zero. Changing the default memtable mode also affects system tables.

The written/flushed workload writes four 128-byte seeded values per table,
across four cycles, at 100 scheduled writes/second: 1,600 writes and 400 first
writes into newly created or replacement memtables. Every cycle explicitly
flushes all user tables, waits one second, and checks exact rows and values.
Both workloads hold for one final second. Seed is 1; samples run every second.

`firstInCycleP99ms` selects the first write to each table within each cycle.
It isolates requests that can activate a replacement memtable, but includes
query preparation and other first-use costs. It is not an isolated timer for
shard construction. Overall service p99 covers all 1,600 writes. Arrival p99
includes delay from the scheduled arrival time. Completed rate uses total
completed writes divided by the sum of write-phase elapsed times.

The driver is serial and low load. These short runs do not establish saturation
throughput or production tail latency. No cold-page-cache read claim is valid.
Post-GC checkpoints request collection and wait; they are not independent proof
of collector completion. Whole-JVM heap includes client, system-table, metrics,
schema, and query state. Initial heap differs across processes. Do not turn a
single initial-to-created delta into an exact per-table ownership figure.

## Memory and timing results

All 12 runs completed with no failed writes, read mismatches, phase errors, or
profiler warnings. The [validated comparison](../logs/20260904-210325-lazy-memtable-comparison/comparison.json)
retains full metrics. Every written/flushed run ends with 400 flushes, 100
SSTables, and no dirty user memtables. Eager runs retain 100 initialized user
memtables; lazy runs retain zero, including after final data verification.

The eight unprofiled controls follow. Run identifiers are the time portion of each directory
under the batch root; all have date prefix `20260904`.

| Scenario | Mode/repeat | Run | Created heap MiB | Settled heap MiB |
|---|---|---|---:|---:|
| Never written | Eager 1 | 210334 | 106.243 | 106.270 |
| Never written | Lazy 1 | 210519 | 104.942 | 105.004 |
| Never written | Lazy 2 | 210702 | 105.131 | 105.159 |
| Never written | Eager 2 | 210846 | 106.258 | 106.276 |
| Written/flushed | Eager 1 | 210411 | 106.497 | 111.791 |
| Written/flushed | Lazy 1 | 210555 | 104.949 | 110.566 |
| Written/flushed | Lazy 2 | 210739 | 105.120 | 110.398 |
| Written/flushed | Eager 2 | 210923 | 106.307 | 112.095 |

The mean settled whole-JVM heap is 1.19 MiB lower for lazy empty controls and
1.46 MiB lower for lazy written/flushed controls. These are observations from two
repeats per mode, not exact ownership estimates or scaling predictions. The
matched object walk is needed to establish which allocations disappeared.

Each latency row contains 400 first-in-cycle writes and 1,200 later writes.
All values below are milliseconds. Quantiles use nearest-rank selection.

| Mode/repeat | First p50 | First p99 | Later p99 | Overall p50 | Overall p99 |
|---|---:|---:|---:|---:|---:|
| Eager 1 | 0.221 | 0.700 | 0.549 | 0.179 | 0.610 |
| Lazy 1 | 0.258 | 0.861 | 0.481 | 0.186 | 0.756 |
| Lazy 2 | 0.371 | 0.862 | 0.487 | 0.204 | 0.782 |
| Eager 2 | 0.232 | 0.848 | 0.452 | 0.181 | 0.765 |

| Mode/repeat | Arrival p99 ms | Completed writes/second |
|---|---:|---:|
| Eager 1 | 0.677 | 100.244 |
| Lazy 1 | 0.827 | 100.245 |
| Lazy 2 | 0.856 | 100.244 |
| Eager 2 | 0.826 | 100.243 |

Lazy first-write medians increased in both repeats. Its first-write p99 is about
0.86 ms, versus 0.70 and 0.85 ms for eager. Later-write p99 stays within the eager
range. This pattern is consistent with moving construction onto the first write,
but the small sample and variation do not isolate the constructor's cost. The
second eager repeat also shows why comparing only the first eager run would
overstate the p99 gap. The offered rate remains low enough for both modes to
complete about 100.24 writes/second. This result does not establish equal maximum
throughput or absence of a latency regression.

Latency derivation is retained in
[the breakdown log](../logs/20260904-211245-lazy-latency-breakdown.log), using each
cycle CSV's first row for each table.

The separate profile and diagnostic runs are retained below. Their heap and
latency values are excluded from the control comparisons above.

| Purpose | Mode | Run | Settled heap MiB | First p99 ms | Overall p99 ms |
|---|---|---|---:|---:|---:|
| Allocation profile | Eager | 211030 | 112.905 | 1.080 | 0.835 |
| Allocation profile | Lazy | 211215 | 110.707 | 1.096 | 0.831 |
| Empty heap diagnostic | Eager | 211139 | 105.971 | — | — |
| Empty heap diagnostic | Lazy | 211324 | 105.375 | — | — |

Each profiled run produced 19 async-profiler phase recordings. Each heap
diagnostic produced baseline, created, and settled live dumps. The diagnostic
whole-JVM heap difference differs from the control difference, reinforcing the
need for direct object attribution.

## Allocation evidence

Matched Java Flight Recorder (JFR) files contain the requested allocation events.
The event name `jdk.ObjectAllocationInNewTLAB` refers to allocation in a new
thread-local allocation buffer (TLAB):

| Phase | Eager allocation events | Lazy allocation events |
|---|---:|---:|
| Create tables | 5,095 | 5,008 |
| First write cycle | 109 | 101 |
| First flush cycle | 884 | 879 |

The allocation stacks provide more specific evidence:

| `generatePartitionShards` samples | Eager | Lazy |
|---|---|---|
| Write cycles 0, 1, 2, 3 | 0, 0, 0, 0 | 1, 2, 1, 4 |
| Flush cycles 0, 1, 2, 3 | 2, 0, 1, 2 | 0, 0, 0, 0 |

Eager flush samples include replacement construction through
`ColumnFamilyStore.Flush -> createMemtable -> TrieMemtable`.
Lazy write samples include `TrieMemtable.put -> initialize -> generatePartitionShards`.
This confirms the intended shift onto first writes. Creation still performs
system/schema mutations: all 14 lazy creation-phase shard samples come from
those writes. The eager creation phase has 17 shard samples, including two from
user table construction. A creation phase is not exclusively user-table
constructor work.

These sparse counts do not establish exact allocated-byte savings. The eight
lazy write samples carry about four MiB of statistical weight; the five eager
flush samples carry about 2.5 MiB. Those weights are sampling estimates, not
measured totals for the constructed shard graphs. Do not infer that lazy
initialization increases graph size from those weighted totals.

Evidence and stacks are retained in
[allocation-comparison.json](../logs/20260904-210325-lazy-memtable-comparison/allocation-comparison.json)
and [the allocation analysis](../.debug/lazy-memtable-allocation.md).
Profiled runs request async-profiler
`event=alloc,wall,cpu`, with `alloc,wall` fallback. This includes the allocation
event selected by command-line `-e alloc`. Allocation samples identify allocation
sites; heap dumps identify remaining objects. Thread-local allocation buffer
events carry sampling/reservation semantics and are not an exact count of every
allocated object. JDK recordings span the JVM recording lifetime; phase-specific
analysis must respect timestamps. Main-thread allocation counters omit node
and background work.

## Next step

Phase 2 establishes lightweight empty and post-flush memtables, with measured
object savings and a visible first-write cost. No numerical latency acceptance
threshold was agreed. Keep the eager control available for further workloads;
the current evidence supports this incremental prototype and does not establish
production readiness at 100,000 or one million tables.

Phase 3 can implement explicit idle flush/retirement using the lightweight
replacement behavior. The large dirty-slab cost remains the reason to pursue
that next step. Validate correct reclamation and local mutation tracking before
adding an idle timer or aggressive age/size policy.
