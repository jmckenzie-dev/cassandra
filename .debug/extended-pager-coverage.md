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

# Pager coverage extension, 2026-09-06

The failures recorded below are now fixed. The crash reproducer is enabled.
See [the fix report](fix-grouped-checkpoint-paging.md) for current validation;
the measurements and failing-run evidence below describe the earlier build.

Passing cumulative pager coverage increased from **393/576 lines (68.2%) and
229/368 branches (62.2%)** to **551/576 lines (95.7%) and 330/368 branches
(89.7%)**. These are measurements of the pager package, including nested classes.
They do not establish that all paging behavior is correct.

Scope: test additions over `168966f1c9` plus the existing, uncommitted IN cursor
fix. This pass changed tests, the property-test entrypoint, and documentation.
The production classes match the preceding IN-fix build.

## What the original report missed

The original report missed 183 lines. Aggregation accounted for 68, and paging
state parsing and compatibility accounted for 55. Limit changes, range bounds,
exhaustion, and guards accounted for the remaining 60.

Some tests already existed but were absent from that measurement. Adding the
eight original `PagingStateTest` methods and two passing `SelectGroupByTest`
methods raised coverage to **478/576 lines (83.0%) and 268/368 branches (72.8%)**.
The added tests then covered another 73 lines and 62 branches.

| Source | Previous lines | Current lines | Current branches |
| --- | ---: | ---: | ---: |
| AbstractQueryPager | 96/113 | 105/113 | 58/64 |
| AggregationQueryPager | 35/103 | 103/103 | 37/40 |
| MultiPartitionPager | 59/75 | 74/75 | 49/52 |
| PagingState | 142/197 | 181/197 | 141/166 |
| PartitionRangeQueryPager | 33/46 | 46/46 | 29/30 |
| QueryPager, including EMPTY | 3/10 | 10/10 | — |
| SinglePartitionPager | 25/32 | 32/32 | 16/16 |

## Added tests and runtime

`QueryPagerTest` adds seven passing methods covering:

- Aggregation sub-pages over single-partition, IN, and range commands, in both
  clustering orders. GROUP BY uses an internal row limit of two to force limit
  replacement and sub-page stitching. External paging state is serialized and restored.
- Non-grouped aggregation across two empty tombstone checkpoints. The test
  checks the returned live rows, remaining LIMIT, exhaustion, and exactly two
  threshold crossings in each direction.
- Inclusive and exclusive range endpoints, per-partition limits, fetch sizes
  one and two, and serialized continuation restoration.
- Final static-only partition exhaustion through internal, coordinator, and
  unfiltered fetch methods. The coordinator case stops before network dispatch.
- Iterator metadata and early close. An operation-order barrier proves that the
  caller retains the read controller and releases it on close, without waiting.
- Tables without clustering columns, exhausted fetches, EMPTY, and an IN state
  whose partition key is absent from the query.

`PagingStateTest` adds six passing methods for absent state, truncated or trailing
input, oversized lengths/counts, negative legacy lengths/counts, static and
collection row marks, and tables without clustering columns.

`RandomizedPagingStateTest` adds 1,024 round-trip examples with seed 18424 across
protocols V3–V6. It varies optional keys/marks, count boundaries, clustering bytes,
buffer offsets, heap slices, and direct/read-only-direct buffers. It checks both
the serialized bytes and the decoded continuation. The existing 100,000 format
checks also pass. The root property-test entrypoint now includes this class.

The 14 new passing methods took about **0.61 seconds combined** in the final
JUnit results. The three containing suites took **8 seconds wall time**, including
startup: 18 pager tests, 14 state tests, and three property tests passed. One
crash reproducer was explicitly skipped. The two existing native CQL GROUP BY
methods passed in a separate 9-second invocation. Total: **37 passing tests**.

The test code adds no sleeps or explicit worker threads. Page traversal has finite
row/page budgets. Aggregation tests that can refetch internally have 30-second
JUnit timeouts. All configuration changes restore previous values in `finally`.
No new multi-node startup was needed; the cumulative baseline retains the prior
passing distributed tests and fuzzer runs.

## Failures exposed or confirmed

### GROUP BY completes an unfinished group at a checkpoint

The new forward and reverse regressions remain enabled in `QueryPagerTest`.
The forward fixture has live `c0`, deleted `c1`–`c5`, and live `c6`, `c7`, all in
one partition and one group. The paging threshold is two, the internal row limit
is two, and the external page size is one group.

The disabled control returns `[c0, c6, c7]` as one group. With tombstone paging
enabled, the first group contains only `[c0]`. The threshold metric confirms a
checkpoint. This violates group completeness, rather than an expectation that
every raw page contain the requested number of rows.

The reverse control returns `[c7, c1, c0]`; the enabled case returns only `[c7]`.
Both controls pass and both enabled cases fail independently.

The relevant counter is `DataLimits.CQLGroupByLimits.GroupByAwareCounter.onClose`:
it counts an unfinished group when the row count is below the internal row limit.
A tombstone checkpoint can satisfy that condition before the group ends.
`AggregationQueryPager` then treats the group page as complete. The fix must
distinguish checkpoint termination from group/data exhaustion. This extends the
previously retained initial-empty-checkpoint GROUP BY assertion failure.

Reproduction:

```bash
ant testsome -Dtest.name=org.apache.cassandra.service.QueryPagerTest -Dtest.methods=groupedAggregationContinuesAfterLiveRowCheckpoint -Dno-build-test=true
ant testsome -Dtest.name=org.apache.cassandra.service.QueryPagerTest -Dtest.methods=reversedGroupedAggregationContinuesAfterLiveRowCheckpoint -Dno-build-test=true
```

### Existing GROUP BY test with static columns fails

`SelectGroupByTest.testGroupByWithStaticColumnsWithPaging` fails with tombstone
paging disabled (`tombstone_paging_threshold=-1`). At the `GROUP BY a, b` query
near line 1776, the second result has partition key `a=2` where the test expects
`a=1`. This occurred before the new tests were compiled and reproduced when run
alone in a fresh test JVM. The exact cause has not
been reduced or attributed to a particular branch change. Keep this regression
separate from the passing coverage profile.

### Read-only heap buffers crash shared byte-copy code

The initial state property crashed the JVM while decoding a read-only heap
buffer. The stack goes through `PagingState.legacyDeserialize`,
`RebufferingInputStream.read`, and `FastByteOperations.UnsafeOperations.copy`.
That copy method treats `hasArray()==false` as a native buffer address; a
read-only heap buffer also has no exposed array. The crash accessed address
`0x15`.

`PagingStateTest.testReadOnlyHeapInput` retains a small reproducer with an explicit
`@Ignore`, so routine runs cannot crash their test JVM. The passing property uses
writable heap views and direct buffers, including read-only direct views. The
crash report is `logs/paging-readonly-heap-20260906T1726-hs_err_pid2332453.log`.
This is a shared byte-copy issue; no claim is made here about remote reachability.

Two initial state-test assumptions were corrected after checking the code:
deserialization consumes its buffer, and a truncated V3 prefix can also be a
valid modern state because format fallback is intentional. Neither was counted
as a production failure.

## Remaining uncovered code

There are **25 uncovered lines and 38 uncovered branches**.

| Source | Missing lines | Reason / remaining work |
| --- | ---: | --- |
| AbstractQueryPager | 8 | Multiple-checkpoint exception and diagnostic logging. The loop breaks after its first checkpoint, so the second-hit guard cannot fire through that loop. Null-check assertions also remain unexercised. |
| MultiPartitionPager | 1 | Assertion when requesting a controller after exhausting every child. Remaining branches include null child slots and a null pager array, neither produced as a live pager by the constructor. |
| PagingState | 16 | Four lines of in-memory I/O exception handling; two overflow guards requiring near-maximum buffer positions; two private protocol-dispatch guards; two hash methods; two wrong-type equality returns; one unreachable null row-mark return; three obsolete compact-storage encoding/decoding lines. |

Other uncovered branches include empty aggregation row iterators, range state
with a key but no row mark despite a positive partition budget, and negative
equality/protocol assertions in paging state. These remain visible in the report.
The test suite does not fabricate private state, allocate multi-gigabyte buffers,
or construct unsupported compact tables to cover them. `TableMetadata.Flag`
documents that compact tables are unsupported in current Cassandra.

Behavior gaps remain despite high line coverage: the GROUP BY failures above,
the ignored shared-buffer crash, and real mixed-version execution still waiting
for historical/current dtest JARs. Coverage of an already executed line cannot
distinguish all these conditions.

## Evidence and reproduction

- Plan: `.plans/extend-pager-coverage.md`.
- Original cumulative report: `build/paging-coverage/in-cumulative/report.xml`.
- Existing tests added to measurement: `build/paging-coverage/extended-existing-only/report.xml`.
- Final cumulative report: `build/paging-coverage/extended-cumulative/index.html`
  and `report.xml`. The merge includes only passing execution profiles and has
  no class-mismatch warnings.
- Final passing unit selection: `build/paging-coverage/extended-unit-final-passing/tests/`;
  `logs/paging-coverage-20260906T173522Z-2357312.log`, exit 0.
- Existing passing GROUP BY methods: `build/paging-coverage/extended-existing-group-by-passing/tests/`;
  `logs/paging-coverage-20260906T173002Z-2344599.log`, exit 0.
- Failing checkpoint regressions: `build/paging-coverage/group-by-live-checkpoints-failing/tests/`;
  `logs/paging-coverage-20260906T173946Z-2366908.log`, exit 1, two assertion failures.
- Original static GROUP BY failure: `build/paging-coverage/pager-existing-group-by/tests/`.
- Independent static GROUP BY reproduction: `build/paging-coverage/group-by-static-alone-failing/tests/`;
  `logs/paging-coverage-20260906T174202Z-2371211.log`, exit 1, one assertion failure.
- Initial property crash and passing state cases: `build/paging-coverage/extended-unit/tests/`.

All commands ran on Java 17 through `tmp/paging-coverage-run.sh`, which writes
stdout/stderr to dated files in `logs/` and preserves exit status. Tests compiled
with `ant _build-test` after the existing production build. Targeted Checkstyle
passed for nine production files and twelve test files
(`logs/paging-coverage-20260906T173947Z-2367391.log`, exit 0).
`git diff --check` and `bash -n run_property_tests.sh` passed.

The selected test harness is `tmp/paging-selected-unit.xml`; the `passing` target
selects the three unit suites while leaving the known GROUP BY failures out of
that coverage profile. The normal class still runs those failures. The `selected`
target takes `selected.class` and `selected.methods` and uses a fixed output
filename. It imports the main Ant project with the same project name to use the
current Cassandra JAR.

```bash
ant -f tmp/paging-selected-unit.xml passing -Dno-build-test=true -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/extended-unit-final-passing -Dbuild.test.output.dir=build/paging-coverage/extended-unit-final-passing/tests
ant -f tmp/paging-extended-cumulative.xml -Djacoco.export.dir=build/paging-coverage/extended-cumulative
```

The merge inputs are the preceding cumulative profile, the final passing unit
profile, and the two existing passing GROUP BY methods. Failed/crashed profiles,
including the initial malformed test assumptions, are excluded. Historical
multi-node runs were reused; they were not rerun in this test-only pass.
