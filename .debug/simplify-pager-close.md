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

# Simplify pager close — 2026-09-10

## Change

Replace the zero-or-one `pageQueries` list with `currentPageQuery`.
Clear the reference before creating each fetch and retain the new query after
assigning checkpoint ownership. Read its checkpoint once, in `Pager.onClose`,
through a read-only helper. Do not snapshot the checkpoint before iteration.

Remove the ineffective duplicate-checkpoint guard, its diagnostic catch, and
the redundant query-null check. The constructor already dereferences the final
query. Keep counter finalization, cursor recording, remaining-row accounting,
and exhaustion updates together and in their original order.

Existing tests exercise close-time checkpoint publication, replacement by later
live-row cursors, state round trips, reverse reads, grouped reads, unfiltered
reads, and early close. No new behavior or duplicate test was added for this
refactor. The pending response-order tests and warning-counter assertion fix
also remain in the validation scope.

## Complexity

PMD 7.27.0, Java 17, thresholds set to 1 to report all methods:

| Method | Cyclomatic before → after | Cognitive before → after | NPath before → after |
| --- | --- | --- | --- |
| `AbstractQueryPager.Pager.onClose` | 16 → 6 | 26 → 6 | 50 → 20 |
| New `currentTombstoneCheckpoint` helper | — → 3 | — → 2 | — → 3 |

PMD completed without analysis errors. Exit 4 means the measurement rules
reported methods at the deliberately low thresholds; it is not a test failure.

## Validation

Comparison scope: `c103032107` through the current worktree. All 25 changed
test classes are selected, plus four existing CQL paging classes. The four
changed test helpers run through their callers. Only the branch-added management
method is selected from `StorageServiceServerTest`.

The Java 17 build and focused `QueryPagerTest` run passed (26 tests).

| Selection | Passed | Skipped | Failures/errors |
| --- | ---: | ---: | ---: |
| Branch unit tests | 246 | 6 | 0 |
| Current-version distributed tests | 88 | 0 | 0 |
| Distributed fuzzer (126.734 seconds) | 4 | 0 | 0 |
| Legacy paging fuzzer (136.064 seconds) | 4 | 0 | 0 |
| Four existing CQL paging classes | 12 | 0 | 0 |
| 5.0.10 upgrade cases (316.880 seconds) | 5 | 0 | 0 |
| Generated mixed-version case, seed 18424 (105.970 seconds) | 1 | 0 | 0 |
| 4.1.13 fixture on Java 11 (37.107 seconds) | 1 | 0 | 0 |

The six skips are the existing collection and warning cases under the three
non-async-forward configurations in `PagingAcrossTombstonesTest`.
Total: 361 passed, six skipped, zero failures/errors across 29 unique classes.
The earlier focused run adds 26 passing executions but is not counted twice
in that total. Both Java 17 and isolated Java 11 builds passed.

The local runner is `tmp/pager-simplification-tests.sh`, invoked through
`distrobox enter dev -- bash tmp/paging-coverage-run.sh` so output is logged
under `logs/`. It runs the selected tests, both bounded current-version fuzzers,
existing CQL paging tests, fresh upgrade JARs and the 5.0.10 upgrade tests, then
the retained 4.1.13 fixture against an isolated Java 11 build.

Both current-version fuzzer classes have 180-second process limits. The legacy
fuzzer uses one minute per generated profile. Upgrade fixtures have separate
timeouts because cluster startup and upgrade are part of each test.

## Style checks

`ant checkstyle checkstyle-test` passed production checks and reported the same
22 existing test-source violations documented in `tombstone-test-followups.md`.
All 22 refer to files unchanged in this worktree: `MixedModePagingAcrossTombstonesTest`,
`PagingFuzzTest`, `AbstractPagingValidator`, `PagingAcrossTombstonesTest`, and
`RandomHelpers`. Neither changed test file nor either refactored production
file has a reported violation. No checks were disabled. The result was confirmed
in a second invocation using command output only; no Checkstyle configuration
or report files were opened. `git diff --check` passed.
