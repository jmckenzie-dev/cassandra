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

# Empty TrieMemtable ownership

The bounded phase 2 target is the shard graph. Keep the existing allocator,
commit-log bounds, shard boundaries, and memtable lifecycle. Defer construction
of the shard array and merged trie until the first mutation. Construct and
publish the complete state once. Empty reads and metrics must return empty
results without initialization. A normal flush then creates a lightweight
replacement through the same constructor.

This changes one implementation without making every Memtable caller handle a
missing memtable. Deferring all shards together also preserves the current
merged trie's stable inputs. Per-shard activation could save more for sparse
traffic, but would require extra concurrency and merged-view work.

## Heap evidence

Analyzed the existing live heap dump:
`logs/20260904-171404-residency-baselines/20260904-172228-residency-never-written-100t/created.hprof`.
No new benchmark ran. The dump contains exactly 100 user TrieMemtables, each
with eight shards. The walk selects tables by `metadata.keyspace`, then follows
the shard graph. It excludes allocator, metrics, schema, buffer-type enum, and
shared EncodingStats references. This is a bounded reference walk, not a full
dominator analysis.

Each table owns 137 objects in that graph: a shard reference array plus 17
objects per shard. The important eager arrays per shard are:

| Array | Length | Payload with four-byte references | Estimated aligned size |
|---|---:|---:|---:|
| UnsafeBuffer references | 23 | 92 bytes | 112 bytes |
| AtomicReferenceArray references | 25 | 100 bytes | 120 bytes |
| ApplyState integer stack | 80 | 320 bytes | 336 bytes |
| ColumnsCollector HashMap buckets | 16 | 64 bytes | 80 bytes |

The first three arrays alone total 568 aligned bytes per shard, or 4,544 bytes
per eight-shard table. They exist before any write. Array lengths and object
counts come directly from the dump. Size estimates assume four-byte compressed
references, 16-byte array headers, and eight-byte alignment, consistent with the
baseline Java 21, eight-GiB configuration.

An estimate for the entire traversed shard graph is 8,496 bytes per table,
849,600 bytes for these 100 tables. It assumes compact object field packing,
12-byte object headers, and eight-byte alignment. Inherited-field padding can
change actual shallow sizes; this value is not a measured retained-size total.
It excludes the merged trie wrapper. The estimate is large enough to measure
through exact object disappearance, but small relative to whole-JVM heap noise.
At more shards or more regular columns, construction allocates more objects.

The source explains the graph:

- [InMemoryTrie constructor](../src/java/org/apache/cassandra/db/tries/InMemoryTrie.java#L88)
  creates the two reference arrays. Buffer chunks and content chunks themselves
  already allocate on demand.
- [ApplyState](../src/java/org/apache/cassandra/db/tries/InMemoryTrie.java#L565)
  eagerly creates the 80-integer update stack, even before a nonrecursive write.
- [ColumnsCollector](../src/java/org/apache/cassandra/db/memtable/AbstractMemtable.java#L185)
  creates a HashMap, one flag and map node per predefined column, plus an empty
  ConcurrentSkipListSet and its map.
- [StatsCollector](../src/java/org/apache/cassandra/db/memtable/AbstractMemtable.java#L246)
  creates an AtomicReference to shared empty statistics.
- TrieMemtable's `MemtableShard` also owns the write lock, trie, and collectors.
  Its constructor is the useful deferred boundary. Source lines are moving
  while the production agent applies phase 2.

The output with corrected size estimates is
`logs/20260904-205250-inspect-empty-trie-ownership.log`.
The temporary parser is `tmp/inspect-empty-trie-ownership.py`. It validates
record boundaries and decoded instance-field lengths. The earlier
`20260904-205143` log incorrectly treated HPROF class field-storage sizes as
Java shallow sizes; do not use its byte totals. Counts agree. The corrected
parser labels every byte total as an estimate.

## Scope and risks

The [SlabAllocator](../src/java/org/apache/cassandra/utils/memory/SlabAllocator.java#L130)
already creates its one-MiB regions on demand. Lazy empty shard construction
does not reclaim dirty slabs. The allocator shell is much smaller than a slab
and participates in switch/discard accounting, so retain it for this increment.
Retain metric registration to keep metrics changes independent.

Publish the initialized state through a volatile reference and serialize first
construction. Keep the original commit-log lower bound and shard boundaries.
Do not move creation-time flush scheduling to first write. All first writes
must enter the existing operation ordering before initializing state. Test a
write racing with a memtable switch, replay into a fresh lazy table, and index
flush ordering. Empty read iterators can capture an empty view; writes completed
before a read starts must remain visible through normal ordering.

The strongest acceptance evidence is disappearance of the expected empty shard
objects, correct reads after activation and flush, and matched allocation and
first-write latency comparisons. Whole-JVM heap differences alone are too noisy
to attribute this sub-MiB change at 100 tables.

## Comparison parser

The temporary parser now accepts both the original direct `shards` field and
the new `state -> TrieState.shards` structure. It reports initialized user
memtables, shard counts, the original shard-only graph, and the complete state
graph including the merged trie wrapper. It excludes the shared empty holder
and zero-length shard array from per-table ownership and counts them separately.

The old diagnostic still yields 13,700 shard-graph objects. Including each
table's merged trie, ArrayList, and reference array gives 14,000 objects and an
estimated 859,200 bytes across 100 tables (140 objects and 8,592 bytes each).
New eager state adds one holder per initialized memtable. The parser will report
that holder explicitly when the matched new dumps are available. These remain
layout estimates; no new Java instrumentation or dependencies were introduced.

## Matched phase 2 result

Parsed created and settled live dumps from the same-build eager run
`logs/20260904-210325-lazy-memtable-comparison/20260904-211139-residency-never-written-100t/`
and lazy run
`logs/20260904-210325-lazy-memtable-comparison/20260904-211324-residency-never-written-100t/`.
All four dumps contain 100 user TrieMemtables. Created and settled results agree.

| Measurement | Eager | Lazy | Reduction |
|---|---:|---:|---:|
| Initialized user memtables | 100 | 0 | 100 |
| User shard objects | 800 | 0 | 800 |
| User shard-graph objects | 13,700 | 0 | 13,700 |
| Complete user state-graph objects | 14,100 | 0 | 14,100 |
| Estimated user state-graph bytes | 861,600 | 0 | 861,600 |

The eager graph contains 141 objects per table: the previous 140 plus one
TrieState holder. Estimated size is 8,616 bytes per table, about 8.41 KiB.
Every lazy user memtable references the same empty holder and zero-length shard
array. Those shared objects are excluded from per-table totals. The eager node
also defines this static empty state, so it is not 100 new objects in lazy mode.
The memtable shell, allocator, metrics, and other table state remain resident.

The exact object counts prove the deferred graph is absent. The byte figures
remain layout estimates, not measured dominator sizes. The lazy settled dump
confirms observation and settlement did not initialize the user shard graphs.
The harness captures this dump before its final verification reads, so read
nonactivation must also use the harness's later verified checkpoint and tests.

Raw output: `logs/20260904-211415-inspect-empty-trie-ownership.log`.
Both estimates and exact counts are preserved in that JSON output.
