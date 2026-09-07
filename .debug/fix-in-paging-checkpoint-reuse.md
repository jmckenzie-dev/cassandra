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

# IN paging checkpoint fix

Implemented and validated on 2026-09-06 against production baseline
`168966f1c9`. Changes remain uncommitted. The
[reviewed plan](../.plans/fix-in-paging-checkpoint-reuse.md) defines the scope.

## Change and regression evidence

`SinglePartitionPager.nextPageReadQuery` now clears the tracked commands
before registering the current fetch. `AbstractQueryPager` comments describe
that lifetime. The production behavior change is two lines; LIMIT accounting,
replica digest comparison, and wire continuations retain their implementation.

Before the fix, the direct real-storage regression returned live clustering
`c3` but saved continuation `c2`. The assertion reports
`expected:<c[3]> but was:<c[2]>` in
`logs/paging-coverage-20260906T162121Z-2164190.log`.
An earlier run failed at the same assertion with ByteBuffer diagnostics in
`logs/paging-coverage-20260906T162038Z-2161253.log`.

After the fix, the retained three-node native-protocol query
`WHERE pk IN (0,1,2) LIMIT 7`, with fetch size 1 and both thresholds 2,
returns `(0,3),(0,4),(1,3),(1,4),(2,3),(2,4)` once each.
The direct cursor regression and the full `QueryPagerTest` class pass.

## Added coverage

- Three direct test methods cover reuse, serialized-state reconstruction,
  consecutive checkpoints, both clustering directions, remaining LIMIT,
  storage exhaustion, and unfiltered rows. Filtered cases run across supported
  protocol versions. The unfiltered case checks actual deletion state and
  allows its counter to include tombstoned rows.
- Three additional distributed IN methods cover 112 enabled query variants,
  with corresponding disabled controls. Cases include fetch sizes 1 and 2;
  LIMIT below, equal to, and above the live count; no LIMIT; both clustering
  orders; memory and flushed data; all-deleted partitions; repeated checkpoints;
  per-partition limits; static-only output; and divergent replica reconciliation.
  Reconciliation counters bracket only enabled reads.
- A three-node IN property generates 12 histories per seed with 2–4 partitions
  and up to 12 rows per partition. Identical replicas receive explicit
  timestamped inserts, deletes, and updates. An independent live-row model
  checks the unlimited disabled control. Disabled controls also establish
  traversal order and LIMIT truncation. Seeds 18424 and 1 pass in memory and
  after flush, including all-deleted histories and forced checkpoint prefixes.
- A shared native-page reader accepts empty pages and enforces row, page, and
  time bounds. It detects repeated nonterminal paging states. Fuzzer failures
  identify the seed, mutation history, query, and fetch size; traversal failures
  include paging states. Comparisons preserve duplicate rows.

The existing divergent-replica fuzzer still runs through all three coordinators
and its independent model controls. The new IN property uses coordinator 1.
All generated paged reads use fetch size below LIMIT, or omit LIMIT.

## Commands and results

Commands ran through this wrapper, which selects Java 17, uses project-local
temporary files, writes dated logs, and preserves exit status:

```sh
distrobox enter dev -- bash tmp/paging-coverage-run.sh
```

The red regression command was:

```sh
ant _build-test testsome -Dno-build-test=true -Dtest.name=org.apache.cassandra.service.QueryPagerTest -Dtest.methods=tombstoneCheckpointDoesNotReplaceLaterCursor -Dbuild.test.output.dir=build/paging-coverage/in-before/tests -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
```

The original run built production and test classes through `testsome`.
After the fix, `ant build-test` passed, then test-only additions compiled with
`ant _build-test`. Builds finished before test execution started.

| Selection | Result | Log |
|---|---|---|
| QueryPagerTest | 11 pass | `logs/run_tests_20260906T162316Z-2169959.log` |
| Original distributed IN regression alone | Pass | `logs/paging-coverage-20260906T162315Z-2169562.log` |
| PagingAcrossTombstonesTest | 104 cases: 98 pass, 6 existing skips | `logs/run_tests_20260906T162725Z-2183936.log` |
| PagingAcrossTombstonesDistributedTest | 13 pass | `logs/run_tests_20260906T162837Z-2187686.log` |
| Interaction methods excluding GROUP BY | 13 pass | `logs/paging-coverage-20260906T163050Z-2195612.log` |
| Full distributed fuzzer, seed 18424 | 3 methods pass | `logs/run_tests_20260906T162725Z-2183838.log` |
| Full distributed fuzzer, seed 1 | 3 methods pass | `logs/run_tests_20260906T163050Z-2196284.log` |
| Final IN property, seed 18424, after removing duplicate query variants | Pass | `logs/paging-coverage-20260906T163322Z-2206753.log` |
| GROUP BY alone, current build | Known assertion still fails | `logs/paging-coverage-20260906T163322Z-2206876.log` |
| Targeted Checkstyle | Pass: 9 production and 10 test files | `logs/paging-coverage-20260906T163050Z-2195707.log` |

Each `run_tests.sh` log starts with the exact Ant invocation. Class runs use
`bash run_tests.sh coverage <fully-qualified-class> -Dno-build-test=true`.
Fuzzer runs add `-DmaxMemory=2G` and
`'-Dtest.jvm.args=-Dcassandra.test.seed=<seed>'`.

The final interaction command was:

```sh
ant -f tmp/paging-interactions-passing.xml -Dno-build-test=true -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/in-interactions-current -Dbuild.test.output.dir=build/paging-coverage/in-interactions-current/tests -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
```

The local harness lists the 13 methods explicitly and uses a fixed output
filename. GROUP BY ran separately with
`-Dinteraction.methods=groupedReadContinuesAcrossEmptyCheckpoint`, without
coverage. Its second reported error is the same uncaught assertion observed
again at cluster teardown.

`git diff --check` passes. No full-suite run or commit was performed.

## Harness correction

The first expanded interaction run loaded the stale
`build/paging-interactions-passing-7.0-SNAPSHOT.jar`. Ant derives `final.name`
from the importing project's name; `-Dno-build-test=true` then reused that
older JAR. The JUnit classpath and `javap` confirmed that the old JAR lacked
`ArrayList.clear`, while `apache-cassandra-7.0-SNAPSHOT.jar` contained it.
The local harness now uses the main project's name and loads the current JAR.
The corrected run passes all 13 selected methods.

Exclude `build/paging-coverage/in-interactions` from current-build coverage.
Its failures reproduce the old implementation, including duplicated rows and
a read timeout while repeatedly visiting an all-deleted partition. The log is
`logs/paging-coverage-20260906T162724Z-2182876.log`.

## Coverage

The [current-run report](../build/paging-coverage/in-combined/index.html)
combines only passing profiles from this fix's runs. The
[cumulative report](../build/paging-coverage/in-cumulative/index.html) also
includes the earlier passing audit profiles. Reports use current class files;
JaCoCo emitted no class-mismatch warnings. Neither report includes the stale
harness run or the failing GROUP BY run.

| Scope | Earlier passing audit | Cumulative after fix |
|---|---|---|
| Whole pager package, lines | 379/575 (65.9%) | 393/576 (68.2%) |
| Whole pager package, branches | 226/368 (61.4%) | 229/368 (62.2%) |
| AbstractQueryPager, lines | 83/113 (73.5%) | 96/113 (85.0%) |
| AbstractQueryPager, branches | 48/64 (75.0%) | 51/64 (79.7%) |
| SinglePartitionPager, lines | 24/31 (77.4%) | 25/32 (78.1%) |
| SinglePartitionPager, branches | 16/16 (100%) | 16/16 (100%) |

The changed executable line is covered (1/1). `nextPageReadQuery` has
11/11 covered lines and 6/6 covered branches. The current-run-only package
coverage is 354/576 lines and 211/368 branches; it uses fewer test classes than
the earlier audit. The cumulative comparison includes both selections.

The original bug existed despite complete branch coverage in
SinglePartitionPager. The added assertions check cursor and LIMIT behavior
across successive fetches. The generated CQL parser remains too large for
the bundled JaCoCo instrumenter; pager classes are instrumented.

Reports were generated with `ant -f tmp/paging-in-coverage.xml
-Djacoco.export.dir=build/paging-coverage/in-combined` and
`ant -f tmp/paging-in-cumulative.xml
-Djacoco.export.dir=build/paging-coverage/in-cumulative`.
`python3 tmp/paging-in-summary.py` reports the counters above.

## Remaining scope

GROUP BY still fails in `DataLimits.CQLGroupByPagingLimits.newCounter` after
an empty grouping checkpoint. Its regression remains active and its fix is
the next TODO item. Historical/current upgrade JAR availability also remains
separate. This patch does not establish range-query checkpoint support or
overlapping fetches through one pager.
