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

# Review: lazy TrieMemtable correctness

## Gate

Status: COMPLETE. Verdict: APPROVE for the bounded source review. +1: YES.
Findings: 0 Blocker, 0 Major, 0 Minor, 0 Nit.

This source review does not replace performance acceptance. No production
defect was found in the inspected change. Build and targeted tests subsequently
passed; the evidence is recorded below.

## Scope

Reviewed the uncommitted production change in
`src/java/org/apache/cassandra/db/memtable/TrieMemtable.java` against
[the phase 2 plan](../.plans/lazy-memtable-initialization.md). Inspected the new
`TrieMemtableLazyTest`, `TrieMemtableLazyPropertyTest`, and residency harness tests.
The harness reviewer also authored the harness changes; the production review
is separate from that work. No source fingerprint was requested or computed.

## Findings

Blocking: None.

Non-blocking: None.

## Traced behavior

| Contract | Evidence and result |
|---|---|
| Publish one complete writable state | `TrieMemtable.initialize` uses the memtable monitor and publishes final shard/view fields through one volatile state reference. Concurrent first writers receive the same completed holder. The ordinary write path only reads that reference. |
| Reads and observation preserve dormancy | Point reads return null for the empty holder. Range reads use the shared empty trie. Counters and collectors traverse an empty shard array. `Trie.mergeDistinct` explicitly supports zero sources. A range iterator created before a concurrent first write can remain empty, consistent with the existing weak iterator contract. |
| Preserve flush metadata | Dormant `getFlushSet` calls the same `flushSet` builder with zero key counts. The builder retains table metadata, bounds, the memtable reference, and inherited column/statistics/commit-log accessors. It does not substitute a metadata-free empty collection. |
| Preserve empty metrics | Dormant discard supplies one zero size per configured shard to `lastFlushShardDataSizes`, matching eager empty shards. Dirty discard retains the existing path. |
| Preserve accepted writes during switch | Logical memtable construction, allocator creation, shard boundaries, and commit-log bounds remain eager. `AbstractMemtableWithCommitlog.accepts` is unchanged. `ColumnFamilyStore.Flush.run` waits on the write barrier before testing cleanliness; an accepted first write can initialize a switched-out memtable before that wait completes. |
| Preserve reads during reclamation | `ColumnFamilyStore.Flush.reclaim` still waits on the read barrier before discard. The change does not reset the holder or detach data during flush. |
| Preserve schema configuration behavior | `MemtableParams` copies and consumes factory options. Invalid non-boolean values are rejected. Factory equality and hash code include the new option; `AbstractAllocatorMemtable.shouldSwitch` compares factories for schema changes. Table serialization still stores the configuration key, not factory internals. |
| Observe current schema on delayed allocation | Shard construction uses the existing `TableMetadataRef`; column collectors can also retain columns introduced by updates through their existing extra-column set. The initial comparator remains captured by the base class and retains its existing schema-switch behavior. |

Source starting points:
[holder and initialization](../src/java/org/apache/cassandra/db/memtable/TrieMemtable.java:112),
[empty flush set](../src/java/org/apache/cassandra/db/memtable/TrieMemtable.java:395),
[factory](../src/java/org/apache/cassandra/db/memtable/TrieMemtable.java:803),
[flush barrier](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java:1286),
[read reclamation](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java:1440),
[schema switching](../src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:132),
[table serialization](../src/java/org/apache/cassandra/schema/TableParams.java:642).

## Validation and limitations

Only read-only source commands ran during this review. The root agent ran build
and tests separately and retains control of benchmarks.

The new unit test holds a real write context across a memtable switch, then
applies its first mutation to the old dormant memtable. It verifies the selected
memtable, commit-log bounds, resulting disk data, and dormant replacement. This
directly exercises the ordering case affected by moving initialization into put.
Other cases cover concurrent first writers, readers held across flush, schema
addition, truncate, and tombstone-only first writes. Generated operations compare
real queries against a row model after writes, deletes, flushes, and truncation.
Harness tests compare lazy/eager configuration and data across repeated flushes.

Updated validation snapshot: build and Checkstyle passed in
`logs/20260904-211425-ai-build.log`, including the final index-test correction. The XML under
`logs/20260904-210049-ai-test-memtable-lazy/` records all ten new production
cases passing: seven lifecycle/factory tests, one generated model, and two
real-node recovery/index tests. Factory coverage now checks default/explicit
mode equality and invalid values. Recovery verifies commit-log replay without
a user SSTable. Index coverage checks SAI and legacy table-backed index reads
through flush, including expected legacy stale-entry cleanup mutations.

All 33 existing cases in `logs/20260904-210205-lazy-existing-tests/` passed,
covering flush ranges, heap/off-heap accounting, metrics, and configuration.
Harness output `logs/20260904-205451-many-tables-launch.log` records six passing
tests, including seven small real-cluster scenarios. These results resolve the
earlier pending factory, recovery, and index evidence. There is still no direct
test of toggling the initialization mode through a schema change or asserting
the histogram sample count after discarding a never-initialized memtable; those
two paths were inspected in source.
