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

# Close paging coverage gaps

Scope: the feature diff from `0c202df78b^` through the working tree at
`168966f1c939d1e5368638636f73a898eed393a3`. Preserve the earlier uncommitted fixes.
Plan: [close-paging-coverage-gaps](../.plans/close-paging-coverage-gaps.md).

## Static cutoff defect and correction

Create a table with partition key `pk`, clustering column `ck`, three static
integer columns, and regular column `v`. Insert rows `(pk=0, ck=0, v=0)` and
`(pk=0, ck=1, v=1)` at timestamp 1. Delete all three static cells at timestamp 2.
Read the partition at ALL, with `LIMIT 3`, fetch size 1, and tombstone failure
and paging thresholds of 2.

The replica stops while processing the static row. It has no regular clustering
cursor. During full-data reconciliation, `ShortReadRowsProtection.moreContents`
rejects the incomplete response. Before the fix, its `TombstoneOverwhelmingException`
escaped to the transport as `ServerError`; the driver then defuncted the connection.
The disabled-paging control, with failure threshold 100, returned both live rows.

The coordinator now raises `ReadFailureException` with reason
`READ_TOO_MANY_TOMBSTONES` and the replica endpoint. It uses consistency ONE,
matching per-replica short-read retry plans in `ReplicaPlans.forSingleReplicaRead`.
A static-only cutoff still aborts because it has no resumable clustering.

The three-node regression checks identical and divergent replicas, coordinators
1 and 3, memory and flushed storage, typed failure reasons, and disabled controls.
It preserves the failing assertion rather than accepting `ServerError`.

Failure evidence: [initial distributed run](../logs/paging-coverage-20260907T010519Z-3215612.log).
Post-fix evidence: [distributed verification](../logs/paging-coverage-20260907T011405Z-3247456.log).
The exact Ant commands appear at the start of each dated log.

## Tests

Nine new test methods and one expanded property cover:

- `ReadCommandTest.staticTombstoneCheckpointHasNoRowContinuation`: real static
  tombstones, checkpoint metadata, failure flags, and memory/flushed reads.
- `ShortReadProtectionTombstonePagingTest.staticTombstoneCheckpointFailsBeforeReturningRows`:
  the native-protocol regression and controls described above.
- `QueryPagerTest.aggregationIsEmptyContinuesAcrossTombstoneSubPages`: forward
  and reverse reads, grouped and ungrouped aggregation, repeated `isEmpty()`
  calls, and a valid final empty fetch after a full group page.
- `QueryPagerTest.rangePagingResumesLegacyStaticPartitionBoundary`: a real static
  partition boundary restored from a V3 state without the legacy per-partition
  count. The next fetch returns the next partition without repeating the first.
- `TombstonePageContinuationTest.rejectsStaticAndEmptyClusterings`: reject
  invalid continuation positions through the public constructor.
- `DigestResolverTest.checkpointWithoutClusteringRejectsTransientReconciliationAtOne`:
  real collection tombstones and a newer transient response exercise the
  unsupported fallback without clustering columns.
- `DigestResolverTest.checkpointWithoutClusteringReconcilesAtQuorum`: the same
  fixture verifies the supported reconciliation path at QUORUM.
- `PagingStateTest.testEqualityIncludesCursorAndRemainingCounts`: distinguish
  cursor, protocol, and remaining-count changes.
- `StorageServiceServerTest.testTombstonePagingManagementAccessors`: management
  writes reach the runtime configuration and restore the original setting.
- `RandomizedPagingStateTest.testRoundTripThroughBufferViews`: its 1,024 fixed-seed
  cases also verify equal hash codes after state and row-mark round trips.

No new test uses sleeps, background test threads, or bytecode injection. The
distributed case reuses the class cluster and the bounded native-page reader.

## Validation and coverage

All 110 selected test executions passed with no failures, errors, or skips:
76 across five complete unit classes, five selected resolver methods, one
management method, and 28 methods across three distributed classes. The
distributed run included the fixed-seed fuzzer and all interaction tests.
The nine new methods took 9.128 seconds in total, excluding class/JVM setup;
8.784 seconds came from the native static-cutoff regression. The complete
distributed selection took 3 minutes 4 seconds.

Java 17 `ant dtest-jar`, targeted Checkstyle on 14 production and 17 test files,
and `git diff --check` passed. Ant runs were sequential. The sandbox launcher
became unavailable during verification; the existing test process completed,
and the remaining commands ran through the approved dev container.

| Scope | Before | After |
| --- | --- | --- |
| Pager lines | 554/579 (95.68%) | 558/579 (96.37%) |
| Pager branches | 340/378 (89.95%) | 352/378 (93.12%) |
| Changed executable lines | 275/306 (89.87%) | 286/306 (93.46%) |
| Branches on changed lines | 234/268 (87.31%) | 241/268 (89.93%) |

`SinglePartitionPager`, `PartitionRangeQueryPager`, and `AggregationQueryPager`
now have full line and branch coverage. `ShortReadRowsProtection` has 49/49
lines and 28/32 branches covered; its changed lines and branches are fully covered.

[HTML report](../build/paging-coverage/gaps-20260907/index.html) and
[line-by-line counts](../build/paging-coverage/gaps-20260907/coverage-summary.json).
Report reproduction uses `ant jacoco-report -Djacoco.export.dir=build/paging-coverage/gaps-20260907`.
The report and execution data are generated artifacts and are not committed.

## Remaining gaps

- The pager package has 21 unexecuted lines: 12 in `PagingState`, eight in
  `AbstractQueryPager`, and one exhausted-controller assertion in `MultiPartitionPager`.
- The multiple-checkpoint diagnostic is unreachable: its loop breaks on the
  first checkpoint. Do not construct impossible state solely to cover it.
- Paging-state misses include memory-stream I/O failures, oversized-buffer
  overflow checks, obsolete compact-storage encodings, and protocol/assertion guards.
- `DigestResolver.getData` checks for missing continuation metadata defensively.
  The normal executor first checks `responsesMatch`, which rejects that response
  and starts a full read. Existing tests exercise that routing.
- Some callback compatibility overloads, default query methods, top-K guards,
  and invalid read-repair states remain unexecuted in this selection.
- The disabled-paging warning-metadata branch has existing coverage scenarios in
  `TombstoneCountWarningTest`; that class was not part of this selection.

The final cumulative report combines the fresh baseline with successful new runs.
It excludes the original distributed failure and the first unit run, whose
aggregation exhaustion assertion was too strict. Historical-version mismatches
in unrelated classes do not affect the scoped metrics. Coverage of the changed
`ShortReadRowsProtection` class comes from runs using its current bytecode.
