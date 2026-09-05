<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Lazy trie memtable lifecycle review

2026-09-04. Scope: defer TrieMemtable shard structures; retain the existing
logical memtable, allocator, boundaries, metrics, and flush policy.

## Recommendation

Use one volatile holder containing the shard array and its merged trie. A shared
empty holder can represent uninitialized storage. Construct all shards together
under a slow-path initialization lock. Publish the complete holder once. Do not
replace it again during the memtable lifetime. Capture the holder once per read,
statistics call, and flush-set operation. Reads must never call the initializer.

This preserves the current merged-trie implementation and avoids partially
published arrays. Per-shard initialization would save more on sparsely written
tables but would require a changing merged view and more concurrency cases.
Keep that as a separate measured change.

## Ordering and lifetime

- `CassandraKeyspaceWriteHandler.beginWrite` starts the operation group before
  appending the commit log (`db/CassandraKeyspaceWriteHandler.java:42`).
  `ColumnFamilyStore.apply` selects the memtable and constructs the index update
  transaction before calling put (`db/ColumnFamilyStore.java:1524`). Deferring
  the whole logical memtable to put would change commit-log bounds and identity.
- `AbstractMemtableWithCommitlog` captures its approximate lower bound at
  construction and retains the predecessor's shared exact bound
  (`db/memtable/AbstractMemtableWithCommitlog.java:35`). Keep both unchanged.
- `Tracker.getMemtableFor` chooses the oldest accepting memtable
  (`db/lifecycle/Tracker.java:390`). `accepts` accounts for writes racing with
  sealing the commit-log upper bound (`AbstractMemtableWithCommitlog.java:74`).
- Flush publishes a replacement, calls switchOut on the old memtable, then seals
  the commit-log upper bound and issues the write barrier
  (`ColumnFamilyStore.java:1253`). The worker waits for that barrier before
  deciding whether the old memtable is clean (`ColumnFamilyStore.java:1287`).
  **An old uninitialized memtable may receive its first write after switchOut.**
  Initialization must still work when its allocator is DISCARDING. The existing
  barrier permits these writes and waits for their operation groups.
- Discard runs after SSTable replacement, post-flush completion, and the read
  barrier (`ColumnFamilyStore.java:1432`, `ColumnFamilyStore.java:1440`). Never
  clear the holder at switchOut or at the start of flushing. Keep the existing
  allocator and trie-buffer release order.
- A read can capture an empty holder while the first write is in flight. This
  is consistent with current weakly consistent reads. After put returns, the
  holder and the trie update must both be visible through their existing
  synchronization. Publish the holder before applying the write; trie mutation
  continues to use its shard lock and reader-publication mechanism.

## Empty-state behavior and other callers

`TrieMemtable` read/statistics callers include isClean, rowIterator,
partitionIterator, getLiveDataSize, operationCount, partitionCount,
partitionKeysTotalSize, timestamp/deletion minima, columns, encodingStats,
getFlushSet, discard, and unusedReservedMemory. All need an empty-safe path.
`getFlushSet` needs explicit handling: its partial/full shard loops otherwise
index by the eager boundaries count (`TrieMemtable.java:360`). Return a real
empty flush set with metadata and commit-log accessors intact.

Empty timestamp minima must remain Long.MAX_VALUE; columns and encoding stats
must preserve their existing empty behavior. `discard` should preserve the
last-flush histogram behavior for zero-sized configured shards if that is part
of the current metric contract, even when no shard objects exist.

Keep initialComparator and initialFactory eager. Schema changes use these to
decide whether to switch (`AbstractAllocatorMemtable.java:135`). A delayed shard
collector can read current metadata because it starts empty and each put adds
the update's actual columns. Comparator changes still require the existing
logical-memtable switch. Boundaries must remain fixed at construction because
topology changes otherwise could send the same key to different shards
(`AbstractShardedMemtable.java:51`).

Index callbacks receive the logical memtable before its first put
(`SecondaryIndexManager.java:1509`). Storage-Attached Index allocation can charge
the allocator independently of trie initialization. Therefore an uninitialized
trie does not imply its allocator owns zero bytes. Keep allocator methods usable
before put. Flush switches base and index CFS memtables together, including clean
memtables (`ColumnFamilyStore.java:1254`). Truncate reuses this switch/barrier
sequence, then discards old contents without flushing them.

## Focused verification

New tests should cover empty point/range/flush reads and counters without
initialization; simultaneous first writes; a held old write group that first
initializes after switchOut; written/flush/replacement/read/write cycles;
tombstone-only first writes; schema changes while empty; and truncate followed
by first write. A small deterministic generated model should mix mutations,
partition/row deletions, reads, flush, and truncate and verify all visible rows.

Existing classes worth targeted runs:

- `SplittablePartitionerTrieMemtableFlushSetTest` and
  `NonSplittablePartitionerTrieMemtableFlushSetTest`: all/partial shard flush
  counts and key sizes, configured shard counts 1/2/3/16.
- `MemtableSizeHeapBuffersTest`, `MemtableSizeOffheapBuffersTest`,
  `MemtableSizeOffheapObjectsTest`, `MemtableSizeUnslabbedTest`: actual allocation
  versus accounting. New delayed constructor allocation can affect expectations.
- `TrieMemtableMetricsTest`: puts, flush persistence, cleanup on drop; its
  contention test uses existing Byteman, so do not copy that injection style.
- `MemtableQuickTest`: substantial read/delete/flush comparisons across types;
  expensive relative to the focused new tests (45,000 partitions, four rows).
- `CommitLogCQLTest`: switch and truncate segment discard. Confirm its table
  configuration actually selects Trie before using it as evidence for this patch.
- `RecoveryManagerFlushedTest` and `RecoveryManagerTruncateTest`: replay
  exclusion; likewise confirm the exercised memtable type. `CommitLogTest`
  explicitly builds SkipList tables and is not direct Trie coverage.
- Existing SAI `FlushingTest` and `FlushIndexWhileQueryingTest` can supplement a
  small direct indexed lazy table test. Confirm selected memtable type.

No production edits, tests, or benchmarks ran as part of this review.

## Integration-test diagnosis: legacy index read cleanup

The first phase-2 test run passed seven focused tests, the generated operation
test, and commit-log replay. `indexesRemainQueryableWithDormantReplacement`
failed for the legacy index's current memtable. Evidence is preserved under
`logs/phase2-initial-tests/`; the original runner output is
`logs/20260904-205737-ai-test-memtable-lazy.log`.

The archived `_jdk21/TEST-org.apache.cassandra.distributed.test.TrieMemtableLazyRecoveryTest.log`
records successful insert and update flushes at lines 4979 and 5005. Lines
5027–5041 record the final partition-delete flush: zero serialized data bytes,
one operation, and a 19-byte SSTable data component with zero live rows. The
index-dormancy assertion fails immediately afterward at line 5044. This locates
the failure at the final delete/flush/index-read sequence, rather than at initial
construction or an earlier flush.

This assertion conflicts with existing legacy-index maintenance:

- `CassandraIndex.indexerFor` ignores a partition deletion that supplies no
  indexed column value (`src/java/org/apache/cassandra/index/internal/CassandraIndex.java:363`).
  The index retains the old entry until read cleanup or compaction.
- `CompositesSearcher.filterStaleEntries` identifies entries shadowed by the
  base partition deletion (`src/java/org/apache/cassandra/index/internal/composites/CompositesSearcher.java:229`).
  Closing the partition invokes `deleteAllEntries` at line 317.
- `CassandraIndex.doDelete` constructs a deleted row and writes it through the
  index CFS write handler (`src/java/org/apache/cassandra/index/internal/CassandraIndex.java:585`).

The query therefore causes a local index mutation. Lazy initialization must
allow it. Keep the base memtable dormant, but expect the legacy index memtable
to initialize and become dirty. The precise test correction is to assert all
memtables dormant immediately after the delete flush, run the index query, then
assert the base remains dormant and the legacy index is initialized and dirty.
Flush again, verify all replacements are dormant, and repeat the query to check
the persisted cleanup suppresses the stale entry without another activation.
The SAI case can retain the all-dormant assertion. No production fix is indicated.

Applied that correction to `TrieMemtableLazyRecoveryTest` after the diagnosis.
The test now requires activation and dirty state for the legacy cleanup write;
it retains dormancy checks before the query, for the base table after the query,
and for all tables after cleanup flush and a repeated query. The earlier
read-versus-mutation coverage gap is addressed by explicit assertions. A targeted
rerun remains the validation step; this diagnosis agent has not run tests.

This also refines the requirement: ordinary reads do not initialize trie state;
reads that perform an existing local maintenance mutation can initialize the
affected index table. Future idle scheduling must count that local mutation.
