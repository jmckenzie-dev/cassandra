<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Tests and defects since the original implementation

Baseline: `0c202df78b`, the original paging-across-tombstones implementation.
Current scope: commits `458b9b699c`, `168966f1c9`, and `2761428ed3`, plus the
correctness and performance follow-up on 2026-09-07 UTC. Counts include earlier
agent work and the current fixes.

## Test additions

There are **93 new `@Test` methods: 43 unit and 50 distributed**. This counts
distinct source methods, not parameter combinations, queries, assertions, seeds,
or generated examples. No baseline test method was removed. All 93 added methods
now have passing execution evidence across the recorded validation passes,
including the four deterministic [5.0.10-to-current upgrade tests and the mixed-version fuzzer](mixed-version-tombstone-paging.md).
The [coverage follow-up](close-paging-coverage-gaps.md) adds nine methods and
strengthens the paging-state round-trip property. The latest
[correctness and performance audit](paging-correctness-and-performance.md) adds
three regression methods and one bounded performance method, and makes the
current-version fuzzer use bounded native-page traversal throughout.

| Class | New methods |
| --- | ---: |
| Unit: ReadCommandTest | 6 |
| Unit: TombstonePageContinuationTest | 2 |
| Unit: QueryPagerTest | 14 |
| Unit: PagingStateTest | 8 |
| Unit: RandomizedPagingStateTest | 1 |
| Unit: DigestResolverTest | 8 |
| Unit: StorageServiceServerTest | 1 |
| Unit: BlockingReadRepairTest | 2 |
| Unit: FastByteOperationsTest | 1 |
| Distributed: PagingAcrossTombstonesDistributedTest | 13 |
| Distributed: PagingAcrossTombstonesFuzzTest | 3 |
| Distributed: PagingAcrossTombstonesInteractionTest | 16 |
| Distributed: PagingAcrossTombstonesTransientTest | 2 |
| Distributed: ShortReadProtectionTombstonePagingTest | 10 |
| Distributed: PagingAcrossTombstonesPerformanceTest | 1 |
| Upgrade: MixedVersionPagingAcrossTombstonesTest | 4 |
| Upgrade: MixedVersionPagingAcrossTombstonesFuzzTest | 1 |

Eight existing empty methods in `MixedConfigPagingAcrossTombstonesTest` now
perform real reads and assertions across four configurations: 32 executed cases.
These eight methods are additional implemented coverage, excluded from the 93
new-method count. The two existing distributed `PagingTest` methods now run
with the feature disabled and enabled, giving four cases. Existing synchronous
reverse-paging checks also now execute and assert their reads.

The source inventory uses `git diff --name-only 0c202df78b -- test`, untracked
test sources, and `git show 0c202df78b:<path>`. For each changed file, it compares
the sets of annotated method names and checks that every `@Test` annotation was
accounted for. Reproduction script: `tmp/paging-test-inventory.py`; detailed
method names: `tmp/paging-test-inventory.json`. These local artifacts are not
needed to run the tests.

## Implementation defects found and fixed

These are behavior failures and state errors, not counts of failing generated
queries. Several share checkpoint-state handling as a cause. The buffer crash
is in shared utility code. The static GROUP BY omission was a regression in the
earlier follow-up fix.

| Defect | Fix and regression evidence |
| --- | --- |
| Short-read protection treated a checkpointed response as exhausted, including empty and shortened refills. Reads could omit rows or expose stale data. | Carry checkpoint metadata and continue the affected source. `ShortReadProtectionTombstonePagingTest` covers zero/nonzero refills, repeated checkpoints, reverse reads, and deletion shadowing. |
| Re-executing a read command retained its previous checkpoint. | Clear execution state at the start of `ReadCommand.executeLocally`; `ReadCommandTest.testTombstoneLimitStateResetOnReuse`. |
| A late local read could restore an obsolete outer-pager cursor after full-data reconciliation took ownership. | Use command copies and transfer checkpoint ownership before repair. `BlockingReadRepairTest.fullDataRepairTransfersCheckpointBeforeLateLocalRead` and the distributed outer-pager regression. |
| Initial multi-replica checkpoint responses aborted instead of completing supported paged reads. | Compare digest, completion status, and clustering; use matching data or perform full reconciliation. Original ALL regressions pass in memory and after flush. |
| An empty checkpoint emitted a premature static-only row and consumed LIMIT. | Suppress static-only output at incomplete checkpoints. `PagingAcrossTombstonesDistributedTest.staticDataDoesNotCreateRowsAtEmptyCheckpoints`. |
| After empty checkpoints, final exhaustion suppressed a genuine static-only row. | Distinguish cursor movement from previously returned data. The same static-data regression checks the all-deleted case. |
| Reusing an IN child pager reapplied an old checkpoint, repeated rows, consumed LIMIT early, and omitted a tail row. | Keep only the current fetch's read commands. Direct cursor assertions, native IN regressions, and the IN property cover reuse. |
| An initial empty GROUP BY checkpoint created a noncanonical empty state and asserted on continuation. | Preserve `GroupingState.EMPTY_STATE` until a result supplies a key. `groupedReadContinuesAcrossEmptyCheckpoint`. |
| A checkpoint after a live row counted an unfinished group as complete. | Tell the group counter whether the sub-page ended at a checkpoint. Forward/reverse unit regressions now return every group member. |
| Completed result pages retained internal unfinished-group state, counting an old group again on later empty pages. | Reset internal limits/state when a result page completes. The forward/reverse regressions assert exhaustion and unchanged remaining group count after the final empty fetch. |
| Static-column GROUP BY skipped groups even with tombstone paging disabled. | Use the grouping state's partition key as evidence that earlier sub-pages returned rows. All 15 existing `SelectGroupByTest` methods pass. |
| An empty checkpoint in a later IN partition inherited the previous partition's grouping state and asserted. | Clear foreign grouping state when updating the current IN child. `groupedCheckpointsPreserveStaticRowsAndLimits`. |
| After a grouped checkpoint, short-read protection used the original row limit to judge a smaller refill. It stopped the deletion-bearing replica and exposed deleted `ck=1`, changing count 3 to 4. | Apply the original exhaustion check only before the first refill. `groupedCheckpointsReconcileReplicaDifferences`. |
| Deserializing paging state from a read-only heap buffer crashed the JVM. | Distinguish direct buffers from heap buffers without an exposed array. The enabled crash regression, buffer-copy test, and 1,024-example state property pass. |
| A static-row tombstone cutoff escaped coordinator reconciliation as `ServerError`, causing the driver to defunct the connection. | Report the failed per-replica retry as `ReadFailureException` with `READ_TOO_MANY_TOMBSTONES`. The three-node regression checks identical/divergent replicas, two coordinators, memory/flushed data, and disabled controls. |
| A local checkpoint without clustering columns entered unsupported ONE read repair and returned `ServerError`. | Reject unsupported initial checkpoints through `ReadCallback`; native ONE/LOCAL_ONE, NONE/BLOCKING repair, two coordinators, and memory/flushed cases pass. |
| A speculative transient response followed by a full-replica checkpoint at ONE required a nonexistent coordinator cursor, causing a null-pointer `ServerError`. | Build the typed tombstone failure from the response endpoint. A native RF 2/1 regression delivers real responses in a latched order. |
| Default unpaged local reads inherited the global paging setting and could silently omit a live tail after a tombstone cutoff. | Default local reads disable checkpoints; replica handlers and pagers opt in explicitly. The regression covers filtered and unfiltered APIs, including the cluster-metadata overload, in memory and after flush. |
| The numeric paging threshold did not control the cutoff; reads used the failure threshold. | Select the paging threshold when enabled and default it to 100000. `tombstonePagingThresholdIsIndependentOfFailureThreshold` checks unequal thresholds, disabled behavior, and the default; the strengthened fixed-seed property also reproduces the old cutoff error. |

Historical reproduction and validation records:
[initial read investigation](paging-initial-checkpoint.md),
[continuation comparison](compare-tombstone-page-continuations.md),
[IN cursor fix](fix-in-paging-checkpoint-reuse.md),
[coverage extension](extended-pager-coverage.md), and
[current fix](fix-grouped-checkpoint-paging.md).
