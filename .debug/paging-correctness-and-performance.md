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

# Paging correctness and performance

Plan: [paging-correctness-and-performance](../.plans/paging-correctness-and-performance.md).
Comparison: `e4d83b3bb74c4c9be3c8e5e1119709d1aa585eb7` to `2761428ed3`,
then the working-tree fixes from this execution. No wire-format change.

## Confirmed defects

1. A paged ONE read on a table without clustering columns retained a local
   checkpoint, failed `DigestResolver.responsesMatch`, then threw an untyped
   exception from `AbstractReadRepair.startRepair`. A native client received
   `ServerError`. The executor now uses REJECT for unsupported checkpoints, so
   `ReadCallback` reports the tombstone failure through its existing error path.
   The regression covers ONE and LOCAL_ONE, NONE and BLOCKING read repair,
   two coordinators, and memory/flushed data.
2. At ONE with speculative reads and transient replication, the transient
   response can arrive before a full-replica checkpoint. `DigestResolver.getData`
   tried to read a cursor from the coordinator command even though the response
   came from a remote replica. The native client received `ServerError` containing
   `NullPointerException: Local checkpoint response has no command cursor`.
   The unsupported reconciliation path now returns `ReadFailureException`, with
   the full replica's endpoint and `READ_TOO_MANY_TOMBSTONES` reason.
   The regression uses replication factor 2/1 in a three-node cluster, a remote
   coordinator, and ALWAYS speculation. It captures real response messages,
   waits on a bounded latch, and delivers transient then full responses through
   the coordinator's inbound message handler. It does not modify source internals.
3. `ReadCommand.executeLocally` implicitly enabled checkpoints for unpaged local
   callers. Three deleted rows before a live tail row caused `executeInternal`
   to return without an error and omit the tail. This violated the explicit
   default-local-read contract on `ReadQuery.executeLocally`. Both default
   local-read overloads now disable checkpoints. The local replica runnable,
   remote replica handler, and internal pager opt in explicitly. The regression
   covers filtered and unfiltered local APIs, with and without cluster metadata,
   and memory/flushed storage.

Pre-fix evidence:

- [No-clustering native failure](../logs/paging-coverage-20260907T143706Z-256325.log):
  10 tests, one failure; nine existing regressions passed.
- [Transient native failure](../logs/paging-coverage-20260907T144739Z-283486.log):
  two tests, one failure; the existing ALL reconciliation case passed.
- [Unpaged local truncation](../logs/paging-coverage-20260907T145114Z-299491.log):
  the regression failed because no exception was raised.

## Continuation and model audit

- Matching full replicas must agree on digest, checkpoint presence, and clustering.
  Static and empty clusterings cannot encode a regular-row continuation. The
  normal executor checks `responsesMatch` before calling `getData`.
- Copies of local commands isolate checkpoint writes. Full-data repair consumes
  an old outer checkpoint and dispatches local copies. Supported reconciliation
  obtains refill state from each response, including empty refills.
- The remaining unsupported-state exceptions in `AbstractReadRepair` are guarded
  by a command-local checkpoint. The production executor now rejects that state
  before repair. Direct component tests still exercise the defensive guard.
- IN child pagers retain only the current fetch's command. Group counters do not
  complete unfinished groups at checkpoints. Completed result pages reset group
  state; entering another partition drops the previous partition's group state.
- The current-version model uses increasing timestamps, row/range deletions,
  and inserts. Applying each generated mutation to a single replay partition and
  comparing against unpaged ALL reads supplies independent Cassandra controls.
  The mixed-version fuzzer keeps its replay partition on the old version.
- IN membership comes from the mutation model. The disabled unpaged/paged
  controls supply Cassandra's partition traversal order before LIMIT is applied.
  Empty pages with continuation remain valid. No assertion requires each page
  to contain its fetch size in live rows.
- The current-version fuzzer's `.all()` traversal lacked a page budget. It now
  uses the existing bounded reader for generated queries and unpaged controls.
  The helper enforces state progress, row/page budgets, and a traversal deadline.
- Old coordinators retain TOMBSTONE_FAIL and reject new checkpoints. Upgraded
  coordinators can reconcile old full responses. The wire IDs and serializer
  were unchanged by this pass.

## Configuration decision

The audit found that `ReadCommand.MetricRecording.countTombstone` used the failure
threshold even when a different paging threshold was configured. The user resolved
the contract on 2026-09-07: enabled paging uses its own numeric threshold and defaults
to 100000, the failure threshold's default numeric value. Changing the configured
failure threshold does not change the paging default. Explicit -1 still disables
paging. The implementation and YAML now follow this contract.

Before the fix, the strengthened property failed with paging threshold 3 and failure
threshold 11: the checkpoint count was 12 instead of 4. A new regression also caught
the old disabled default. After the fix, all 78 selected unit tests pass, including
the threshold matrix in memory and after flush, and the fixed-seed property.
The three-node fuzzer now uses unequal paging and failure thresholds. Its three
methods pass, as do all 32 mixed-configuration cases. Results are in
`build/paging-threshold-before`, `build/paging-threshold-unit`, and
`build/paging-threshold-distributed`; command output is saved in dated `logs/` files.

## Performance design

`PagingAcrossTombstonesPerformanceTest` measures native ALL queries on three
replicas with read repair NONE and speculation NONE. Each query returns 101 rows
with fetch size 100. The clean partition has 192 live rows. Checkpoint partitions
have a 64-row deleted prefix and a live tail; deletion is identical on all
replicas in the matching case and present only on node 3 in the divergent case.
Both tombstone settings are 50 when paging is enabled.

Each storage phase warms up 20 queries per mode, then measures four rounds of
25 queries per mode. The mode order rotates each round. Setup, flush, config
changes, counter reads, and result-value assertions are outside the timed read.
Timed work includes native requests, complete page traversal, and the bounded
reader's checks. Table read-latency counts include local and remote replica reads.
The coordinator's reconciliation meter verifies the intended fallback path.

This compares modes on the current implementation, not against a pre-feature
binary. In-JVM internode transport and single-client timing do not establish
production throughput or real-network latency. There is no absolute latency
assertion. Minimum, median, p95, maximum, and every round's read counts are retained.

## Validation

All final selections pass: **120 executions covering 117 distinct methods**, with
no failures, errors, or skips. The original failing runs are retained separately.

| Selection | Executions | Result |
| --- | ---: | --- |
| ReadCommandTest, TombstonePageContinuationTest, QueryPagerTest, PagingStateTest, RandomizedPagingStateTest | 77 | Pass, 12 seconds |
| Current fuzzer, interaction, performance, transient, and short-read distributed classes | 32 | Pass, 3 minutes 54 seconds |
| Strengthened local-read regression, five resolver methods, two repair methods | 8 | Pass, 7 seconds |
| Benchmark with saved output and mixed-version fuzzer | 2 | Pass, 1 minute 56 seconds |
| Benchmark repeat to assess timing variation | 1 | Pass, 26 seconds |

The mixed-version fuzzer ran both one- and two-upgraded-node stages with the
current dtest JAR and the existing 5.0 JAR. All three current-version fuzzer
methods passed with seed 18424 and bounded traversal. The local-read regression
was rerun after adding both unfiltered API variants.

The Java 17 `ant dtest-jar` build and targeted Checkstyle pass. Checkstyle inspected
15 production and 18 test files, including all Java files changed in this pass.
Ant builds and tests ran sequentially. No full-suite run or dependency change.
The older bundled JUnit lacked `Assert.assertThrows`; the new regression uses
the existing AssertJ API. Successful test stdout is discarded by the runner, so
the benchmark also saves a dated log; its measurement run was then repeated.

The exact commands, including test selections and output directories, appear in
dated `logs/paging-coverage-*.log` files. Final JUnit results are under
`build/paging-audit/{unit,distributed,unit-final,final-distributed,performance-repeat}/tests`.
The `*-before` directories contain the three retained failure reproductions.
Build output and measurement logs remain generated artifacts.

## Performance results

Each saved run contains 100 timed queries per mode per storage state.
[Run 1](../logs/paging-performance-20260907T150411Z.log) and
[run 2](../logs/paging-performance-20260907T150653Z.log) retain minimum, median,
p95, maximum, and all round counters. The complete test takes about 22–25 seconds,
including cluster setup, data loading, warm-up, measurements, and teardown.

| Mode | Replica reads/query | Memory median, runs 1 / 2 (ms) | Flushed median, runs 1 / 2 (ms) |
| --- | ---: | ---: | ---: |
| Clean, disabled | 6 | 1.348 / 1.526 | 1.840 / 1.825 |
| Clean, enabled | 6 | 1.742 / 1.593 | 1.797 / 1.901 |
| Matching checkpoint | 9 | 2.795 / 2.741 | 3.264 / 2.917 |
| Reconciliation | 12 | 2.994 / 3.060 | 3.951 / 3.942 |

Replica read counts were identical in all 64 measured batches across both runs.
Only the divergent case entered reconciliation, once per query. Clean reads have
two native pages; matching checkpoints add an empty page without invoking full-data
reconciliation. The divergent case performs more replica work to return the same
101-row live result as the matching case.

Enabled clean-read median differences varied from +29.3% to +4.3% in memory and
from -2.4% to +4.2% after flush. These short runs do not distinguish branch overhead
from JVM and scheduling effects. Read-count evidence is stable; a precise latency
overhead estimate would require a longer, warmed measurement. No production
latency or throughput claim follows from this test.
