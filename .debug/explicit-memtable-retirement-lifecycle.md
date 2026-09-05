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

# Explicit retirement lifecycle review

Date: 2026-09-04. Scope: phase 3a in [.plans/explicit-memtable-retirement.md](../.plans/explicit-memtable-retirement.md), relative to the current uncommitted phase 2 implementation. This review covers `ColumnFamilyStore` flushing, tracker views, allocator reclamation, and the lazy `TrieMemtable` replacement. No production files changed during this review.

## Conclusion

`ColumnFamilyStore.forceFlush(USER_FORCED)` already supplies explicit retirement for a dirty trie memtable. It switches through the existing writer barrier, publishes SSTables, and schedules reclamation after readers finish. Phase 2 supplies the dormant replacement. No additional production operation is required by the reviewed lifecycle.

The returned future proves flush completion. It does not prove that readers released the old memtable or that garbage collection reclaimed its heap storage. A failed flush also prevents successful retirement. These distinctions belong in harness observations and any later scheduler policy.

## Dirty and clean requests

[ColumnFamilyStore.forceFlush](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1108) checks the base table and table-backed indexes under the tracker monitor. If any current memtable is dirty, it calls the existing switch path. The allocator memtable accepts `USER_FORCED` in [shouldSwitch](../src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java#L133). If all are clean, the call waits for preceding flush work without replacing the current memtable or creating an SSTable.

Clean retirement is therefore idempotent in a quiescent table with clean indexes. A mutation concurrent with the request can dirty the current or replacement memtable; the API does not promise a permanently dormant table.

## Writers and commit log

[Flush construction](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1233) switches the base and backing-index memtables together. It installs a shared write barrier and commit-log boundary before issuing the barrier. [Tracker.getMemtableFor](../src/java/org/apache/cassandra/db/lifecycle/Tracker.java#L390) selects the oldest memtable that accepts the operation. [AbstractMemtableWithCommitlog.accepts](../src/java/org/apache/cassandra/db/memtable/AbstractMemtableWithCommitlog.java#L71) uses both the operation group and commit-log position.

A writer accepted before the switch can still update the old memtable. Later writes can initialize and update the replacement. `Flush.run` waits for preceding writers before marking the old memtables as flushing and reading their contents. Allocator `DISCARDING` permits those accepted writers to finish; it is not a prohibition on further accepted allocations.

[PostFlush](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1171) waits for the flush task and releases commit-log segments only when `flushFailure` is null. This ordering must remain intact for explicit retirement.

## Readers and reclamation

[Tracker.replaceFlushed](../src/java/org/apache/cassandra/db/lifecycle/Tracker.java#L432) replaces the old memtable in the current view with its SSTables. Existing readers can still reference the earlier view. [Flush.reclaim](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1439) issues a read barrier, then registers a post-flush listener on the reclaim executor. The listener waits for preceding readers before calling `memtable.discard()`.

The future and reclamation have separate completion times. A pinned reader can keep the old allocator's accounting above zero after the flush future completes. After the reader finishes, a bounded wait on both `getAllocator().onHeap().owns()` and `offHeap().owns()` can verify accounting release without forcing garbage collection.

Accounting zero remains weaker than completed physical reclamation:

- [TrieMemtable.discard](../src/java/org/apache/cassandra/db/memtable/TrieMemtable.java#L188) clears allocator accounting in `super.discard()` before it calls `discardBuffers()` on each trie shard.
- [SlabAllocator](../src/java/org/apache/cassandra/utils/memory/SlabAllocator.java#L120) frees direct regions and clears accounting, but retains its `currentRegion` reference. For an on-heap slab, `currentRegion → Region.data → HeapByteBuffer.hb` still references the byte array while the allocator remains reachable.
- Holding the old memtable or allocator in test instrumentation itself keeps that slab reachable. Heap diagnostics must release those references and inspect a quiescent workload separately.
- The static `RACE_ALLOCATED` queue can retain spare regions from allocation races. Total process slab counts need not match user-table counts exactly.

The existing periodic-flush runnable [captures the owner](../src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java#L213), not the old memtable instance.

## Index coupling

The same `Flush` constructor switches all table-backed indexes with the base table, even if an individual member is clean. The shared barrier prevents an index flush from racing ahead of accepted base writes. During the base flush, [flushAllNonCFSBackedIndexesBlocking](../src/java/org/apache/cassandra/index/SecondaryIndexManager.java#L1037) invokes custom-index flush tasks through their intended API. Publication notifies indexes before reclaiming the memtable.

Pure base-table reads preserve dormancy. A legacy-index query can remove a stale index entry and therefore perform a local mutation. Such a read can legitimately activate an index memtable. The phase 2 recovery test already covers this distinction.

## Flush failure

Failures before SSTable publication abort the flush writers and lifecycle transaction. [Flush.run](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1309) stores the failure, and `PostFlush` propagates it without discarding commit-log segments. The failed memtable remains in the flushing view and readable; it is not reclaimed. Errors after publication may occur at different lifecycle points, so this is not a claim that every possible exception preserves identical tracker state.

A subsequent clean `forceFlush` call waits for prior work but does not retry the failed memtable. Do not interpret a clean current replacement as proof that every earlier retirement succeeded. The existing [CommitLogTest.testUnwriteableFlushRecovery](../test/unit/org/apache/cassandra/db/commitlog/CommitLogTest.java#L1092) documents the lack of retries and verifies recovery from retained commit-log data.

The supported failure setup is `Util.markDirectoriesUnwriteable(cfs)` with the disk failure policy set to `ignore` and restored in `finally`. The helper uses Cassandra's disallowed-directory mechanism; it does not patch source bytecode. The entire `CommitLogTest` class is too broad for the requested small default suite. A focused retirement test can use the same fixture.

## Focused existing test candidates

| Class | Cases | Relevant coverage |
| --- | ---: | --- |
| `SplittablePartitionerTrieMemtableFlushSetTest` | 4 | Flush partition counts and key sizes with 1, 2, 3, and 16 shards; independent skip-list comparison |
| `NonSplittablePartitionerTrieMemtableFlushSetTest` | 4 | Same flush-range checks with a non-splittable partitioner |
| `MemtableSizeHeapBuffersTest` | 3 | Heap-buffer accounting across supported memtable implementations |
| `org.apache.cassandra.db.lifecycle.ViewTest` | 3 | Optional tracker-view transitions; does not execute disk flushing |

The first three complement the new focused retirement lifecycle class. Storage-attached index failure tests use bytecode injection and were excluded. No existing small end-to-end flush-failure class without that injection was found.

## Property test change

`TrieMemtableLazyPropertyTest` retains its independent map of partition and clustering keys. Four fixed seeds generate 64 operations each over eight partition keys and three clustering keys. The domain includes inserts, row and partition tombstones, explicit switches, truncate, observation, explicit `USER_FORCED` flush, and repeated retirement.

After every generated operation, exact point and full-table reads must match the independent map. Retirement must leave a clean dormant replacement. If the prior memtable was clean, identity and SSTable count must remain unchanged. If dirty, the old and new identities must differ and old allocator accounting must reach zero. Each seed ends with two retirements, guaranteeing the clean idempotence assertion executes.

The existing test uses `java.util.Random`, not a property framework with automatic shrinking. Failures report the seed, step, and full executed operation prefix; this gives deterministic reproduction and a bounded prefix for manual minimization. No model logic copies the memtable implementation. The helper waits for allocator accounting only and does not assert garbage collection.

Root owns build and execution. This review records source evidence and test authorship; the phase 3a report records actual validation results.

## Independent integration review

Read-only review covered the new `TrieMemtableRetirementTest`, isolated `TrieMemtableRetirementFailureTest`, residency harness retirement phases and assertions, `run_tests.sh`, and the six-class `--retirement` runner. No confirmed correctness defect was found in the inspected implementation.

The default runner selects these exact classes:

- `org.apache.cassandra.db.memtable.TrieMemtableRetirementTest`
- `org.apache.cassandra.db.memtable.TrieMemtableRetirementFailureTest`
- `org.apache.cassandra.db.memtable.TrieMemtableLazyTest`
- `org.apache.cassandra.db.memtable.SplittablePartitionerTrieMemtableFlushSetTest`
- `org.apache.cassandra.db.memtable.NonSplittablePartitionerTrieMemtableFlushSetTest`
- `org.apache.cassandra.db.memtable.MemtableSizeHeapBuffersTest`

The writer case holds a real write context across the switch, checks the old memtable assignment, permits a later writer to activate the replacement, and verifies exact rows after both flushes. The reader case holds the production read ordering group and old iterator through flush completion. The index case exercises the real legacy-index cleanup mutation before retirement. The failure case checks a real dirty commit-log segment, rejected flush, retained readable memtable, and unreleased accounting. It runs in its own unit-test JVM, so its intentionally retained failed allocator does not affect later classes.

The names `assertReclaimed` and `-reclaimed` refer to accounting release in this validation. The harness observes three consecutive stable samples, including node-wide reclaiming counters, before its retirement checkpoint. These checks do not replace the separate heap diagnostics described above. Failure recovery is covered by the existing commit-log test described above, which was not run during this review. The earlier phase 2 recovery test exercises ordinary unflushed replay. This increment's focused failure test checks retention before restart.
