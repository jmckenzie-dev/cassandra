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

# GROUP BY and buffer-copy fixes, 2026-09-06

The retained failures now pass. The two additional distributed grouping tests
also pass after exposing an IN state-transfer error and a short-read refill
error. The read-only heap crash regression is enabled, and its buffer shape is
restored to the paging-state property.

The [plan](../.plans/fix-grouped-checkpoint-paging.md) covers this pass.
The [inventory](tombstone-paging-test-inventory.md) counts all additions and
records defects since original implementation commit `0c202df78b`.
Changes remain uncommitted.

## Changes

- `AggregationQueryPager` preserves the canonical empty state before the first
  result and clears unfinished-group state when a result page completes.
- `AbstractQueryPager` checks checkpoint status before closing its counter.
  `DataLimits` accepts that status so a checkpoint cannot complete an unfinished
  group. The pager also uses grouping state to detect previously returned rows
  when deciding whether to suppress repeated static-only output.
- `MultiPartitionPager` removes another partition's grouping state when updating
  the current child after an empty checkpoint.
- `ShortReadRowsProtection` applies the original request's exhaustion check only
  to its initial response. Refills use their own requested row count.
- `FastByteOperations` uses ordinary buffer access for read-only heap sources
  instead of interpreting their heap offset as a native address.

The group state is derived from returned data. A tombstone cursor does not
become an unfinished live group. Raw pages can still be empty and resume.

## Failure evidence

The earlier [coverage report](extended-pager-coverage.md) retains the original
forward/reverse group truncation, static GROUP BY omission, and JVM crash evidence.
The initial distributed assertion is also preserved in the [IN fix report](fix-in-paging-checkpoint-reuse.md).

The first candidate returned complete forward/reverse groups but retained old
unfinished-group state across result pages. The exhaustion assertions still
failed in `logs/run_tests_20260906T190352Z-2505003.log`. Resetting that internal
state makes the final empty fetch exhaust without counting the old group again.
The disabled controls now assert the same contract.

The new grouped combination fixture has three partitions, three groups per
partition, eight clusterings per group, and static values. The first group and
last partition are deleted. Each other group has live clusterings 0, 6, and 7.
Matching-replica and divergent-replica variants run in both clustering orders,
before and after flush, with fetch sizes 1/2, LIMIT 3/6, and optional per-partition
LIMIT 1. Each disabled control must match independently specified group counts.
Enabled reads must match their ordered control and cross a tombstone threshold.

The first run of this fixture failed in
`logs/paging-coverage-20260906T190814Z-2519789.log`:

- With matching replicas and a per-partition limit, an empty checkpoint in `pk=1`
  received grouping state for `pk=0`, violating `CQLGroupByPagingLimits.newCounter`.
- With deletes only on replica 1, fetch size 2 and LIMIT 3 returned group count
  four instead of three. The disabled control passed.

Clearing the foreign partition state fixed the first case. The second still
failed in `logs/paging-coverage-20260906T191038Z-2530773.log`.
Temporary diagnostic logging in
`logs/paging-coverage-20260906T191239Z-2546935.log` showed short-read protection
stop replica 1 after one live row, then return its deleted `(g=1, ck=1)` from
another replica. The refill requested one row, but the early exhaustion check
still compared against the original two-row internal limit. The logs establish
stale-row visibility, not just an unexpected aggregate count. The diagnostic
logging was removed after the cause was confirmed.

## Validation

Java 17 and Ant. The wrapper `tmp/paging-coverage-run.sh` selects the JDK,
uses project-local temporary files, logs both streams, and preserves exit status.
Main builds completed before tests. A final rebuild followed removal of one
unused import; the later distributed regressions and fuzzer use that build.

| Selection | Result | Evidence |
| --- | --- | --- |
| QueryPagerTest, PagingStateTest, RandomizedPagingStateTest, FastByteOperationsTest, entire SelectGroupByTest | 56 pass | `logs/paging-coverage-20260906T191430Z-2556928.log` |
| PagingAcrossTombstonesTest | 98 pass, 6 existing skips | `logs/run_tests_20260906T190912Z-2524732.log` |
| Entire PagingAcrossTombstonesInteractionTest | 16 pass | `logs/run_tests_20260906T191430Z-2556675.log` |
| Entire ShortReadProtectionTest | 360 pass | `logs/run_tests_20260906T191523Z-2566099.log` |
| Earlier distributed continuation and tombstone short-read regressions | 21 pass | `logs/paging-coverage-20260906T192038Z-2579613.log` |
| Three-node fuzzer, seed 18424 | 3 methods pass | `logs/run_tests_20260906T192040Z-2580353.log` |
| Three-node fuzzer, seed 1 | 3 methods pass | `logs/paging-coverage-20260906T192246Z-2585499.log` |

These final selections total **557 passing executions and six existing skips**,
with no failures or errors. Seed 1 ran without JaCoCo; seed 18424 supplies fuzzer
execution data for the combined report. Repeated diagnostic and candidate runs
are excluded from this total.

The fuzzer checks divergent replicas against its independently verified mutation
model and exercises multi-partition IN paging. The paging-state property covers
1,024 generated examples, including read-only heap and direct views, plus its
existing format checks. No model expectation was relaxed to make a failure pass.

The two new distributed methods took about 5.2 seconds combined within the
existing shared cluster. The new copy test took 0.002 seconds. No sleep was added.
Native traversal has row/page budgets, repeated-state checks, request timeouts,
and a deadline. Internal aggregation regressions retain their 30-second timeouts.
The existing 360-case short-read suite was the longest validation run, about
4.3 minutes; it was run once after the refill fix.

Targeted Checkstyle passes for 14 production and 13 test files
(`logs/paging-coverage-20260906T191522Z-2565266.log`). `git diff --check` passes.
No full repository suite or dependency installation was performed.

## Coverage

The [current passing report](../build/paging-coverage/grouped-fix-combined/index.html)
measures **554/579 pager lines (95.7%) and 340/378 branches (89.9%)**. It uses
the current production class files with no class-mismatch warnings. The report
contains only passing profiles from this fix pass. It excludes diagnostic builds
and failed candidates. Inputs and merge command are in
`tmp/grouped-fix-coverage.xml`; counters and JUnit totals are printed by
`tmp/grouped-fix-summary.py`.

AggregationQueryPager and SinglePartitionPager have full line coverage.
The remaining 25 uncovered pager lines are the same kinds of guards, diagnostics,
and compatibility paths described in the earlier coverage report. These numbers
measure execution. The retained regressions check the ordering, completeness,
counting, and exhaustion properties that exposed the defects.

The subsequent [mixed-version run](mixed-version-tombstone-paging.md) passes all
three upgrade tests with 5.0.10 and the current branch. Together with the selections
above, this gives 560 passing executions and six existing skips. The upgrade run
did not use JaCoCo and does not change the coverage measurement above.
Partition-range checkpoint support remains outside the implemented contract;
its test continues to require a safe failure.
