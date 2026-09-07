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

## IN fix follow-up

The IN and GROUP BY regressions are now fixed and validated. See the
[IN implementation report](../.debug/fix-in-paging-checkpoint-reuse.md) and
[grouped paging fix](../.debug/fix-grouped-checkpoint-paging.md).
All three [mixed-version tests](../.debug/mixed-version-tombstone-paging.md)
also pass with 5.0.10 and the current branch.
The sections below record the earlier test-only batch and its original results.

## Test Author Result

- Status: COMPLETE (test authoring; later fixes and upgrade execution validated in the linked follow-ups)
- Domain: integration
- Applicability: APPLICABLE
- Plan file: [.plans/remaining-tombstone-paging-coverage.md](../.plans/remaining-tombstone-paging-coverage.md)
- Requested scope: remaining paging interaction and upgrade coverage
- Resolved comparison basis: production changes `4e454fb560..168966f1c9`
- Production files reviewed: ReadCommand, ReadCommandVerbHandler,
  TombstonePageContinuation, ParamType, ReadCallback, DigestResolver,
  AbstractReadExecutor, AbstractQueryPager; related MultiPartitionPager,
  SinglePartitionPager, AggregationQueryPager, GroupingState, and DataLimits
- Test files changed: PagingAcrossTombstonesInteractionTest,
  PagingAcrossTombstonesTransientTest, MixedConfigPagingAcrossTombstonesTest,
  MixedVersionPagingAcrossTombstonesTest
- Production code changed: no

The earlier implementation and coverage work was committed first as
`168966f1c9`, “Compare replica digests and continuations for tombstone paging.”
This test batch remains uncommitted.

## Tests Added or Updated

| Selection | Coverage and result |
|---|---|
| Interaction test: nine passing methods | Real writes while a checkpoint response is held; writes behind a consumed cursor; server speculation; response timeout and retry of saved paging state; replica shutdown/restart with QUORUM success and ALL unavailability; TTL expiry before and between pages; matching and divergent collection-element deletions; safe range-read rejection |
| Interaction test: two failing methods | Partition-key IN duplicates rows and loses a tail row through LIMIT; GROUP BY aborts after an empty checkpoint. Both disabled-paging controls pass. Neither regression is ignored or weakened. |
| Transient replica test: one passing method | Actual replication factor 3/1, all three coordinators, memtables and SSTables, stale rows shadowed by full-replica tombstones, newer transient data, and observed transient responses |
| Mixed configuration: 32 passing cases | Eight populated scenarios across matching thresholds, differing thresholds, one disabled node, and two disabled nodes. Each checks both directions, memory/disk, and all three coordinators: 384 native-protocol queries. |
| Mixed version: three compiled tests | Real UpgradeTestBase upgrade from 5.0 to current; upgraded coordinator reconciliation, old coordinator rejection of unknown checkpoints, and genuine old-replica failure propagation. Execution requires missing dtest JARs. |

The mixed-configuration fixture now applies and checks its requested settings.
The mixed-version class no longer inherits a factory that ignores versions.
The new setting is applied only to the upgraded node before it restarts.

The concurrent-write oracle changes rows beyond the checkpoint while a real
remote response is held by the harness network filter. Those writes complete at
ALL before the response is released. The next page must reflect those writes.
A separate test inserts behind a consumed cursor and requires that the pager
does not revisit that position. Neither test assumes snapshot isolation.

An initial TTL fixture created independent writes on each replica. Different
timestamps and expiry metadata legitimately caused digest mismatch and full
reconciliation, so its empty-page assertion was invalid. The corrected fixture
writes once at ALL, giving replicas the same mutation. It then passes the empty
page, continuation, and complete-result assertions.

The full interaction run reports a GROUP BY error and repeats the uncaught
assertion at cluster teardown. That teardown error is the same defect. A separate
selection confirms that the other nine methods pass without that teardown error.

## Commands and Outcomes

Commands below ran with this prefix, which selects Java 17, uses project-local
temporary files, logs stdout/stderr, and preserves the command status:

```sh
distrobox enter dev -- bash tmp/paging-coverage-run.sh
```

| Command suffix | Exit | Evidence |
|---|---:|---|
| `bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesInteractionTest -DmaxMemory=4G` | 1 | Initial run: `logs/run_tests_20260906T044326Z-1056597.log`; corrected TTL fixture and disabled controls: `logs/run_tests_20260906T045123Z-1083278.log` |
| `bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesTransientTest -DmaxMemory=4G` | 0 | `logs/run_tests_20260906T045251Z-1090489.log` |
| `bash run_tests.sh coverage org.apache.cassandra.distributed.test.storage.paging.MixedConfigPagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=4G` | 0 | `logs/run_tests_20260906T045407Z-1097985.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=4G` | 1 | `logs/run_tests_20260906T045253Z-1091757.log`: no 5.0 versions found |
| `ant -f tmp/paging-debug-checkstyle.xml` | 0 | Checks the changed test files with repository rules |
| `ant -f tmp/paging-gap-coverage.xml -Djacoco.export.dir=build/paging-coverage/gaps-passing` | 0 | Combined report from passing tests only |
| `python3 tmp/paging-gap-summary.py` | 0 | Reads the old and new JaCoCo counters |

The passing interaction selection uses a fixed JUnit output filename, avoiding
Ant's filename-length failure with a long method list:

```sh
ant -f tmp/paging-interactions-passing.xml -Dno-build-test=true -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/interactions-passing -Dbuild.test.output.dir=build/paging-coverage/interactions-passing/tests -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
```

Exit 0; nine methods pass. Log:
`logs/paging-coverage-20260906T045639Z-1104326.log`.

The delayed-response scenario now has a ten-second read timeout so acknowledged
writes can finish without a two-second scheduling race. The timeout test sets
its coordinator timeout to one second and restores ten seconds in finally.
The changed methods were rebuilt and checked with:

```sh
ant -f tmp/paging-interactions-passing.xml -Dinteraction.methods=acknowledgedWritesDuringCheckpointReadAppearOnLaterPages,timedOutContinuationCanBeRetried -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/interactions-timeouts -Dbuild.test.output.dir=build/paging-coverage/interactions-timeouts/tests -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
```

Exit 0; two methods pass. Log:
`logs/paging-coverage-20260906T045908Z-1110726.log`.

An earlier mixed-configuration attempt overlapped an Ant rebuild of Accord and
failed before any test ran with `NoClassDefFoundError: accord/utils/async/AsyncResult`.
The same command passed all 32 cases after the build finished. The failed attempt
is `logs/run_tests_20260906T045252Z-1091057.log`; its coverage is excluded.

`git diff --check` passes. `git diff --name-only -- src/java` is empty.

The [passing coverage report](../build/paging-coverage/gaps-passing/index.html)
adds the mixed-configuration, transient, and passing interaction runs to the
previous implementation report. It excludes both failing interaction runs and
the unavailable upgrade tests. Report generation emitted no class mismatch
warnings.

| Source scope | Before: lines / branches | After: lines / branches |
|---|---|---|
| ReadCallback | 96/124 / 62/106 | 105/124 / 69/106 |
| AbstractReadExecutor, including nested classes | 120/174 / 59/102 | 138/174 / 72/102 |
| DataResolver, including nested classes | 109/157 / 52/82 | 109/157 / 53/82 |
| Whole pager package | 379/575 / 226/368 | 379/575 / 226/368 |

The passing pager counters stay unchanged. The new sequence tests still expose
two failures in interactions among already-executed statements. The earlier
JaCoCo limitation on the generated CQL parser remains.

## Findings for the Implementer

### integration-001

- Status: resolved by the current-fetch reset; see the follow-up report above.
- Severity: Blocker
- Production location: [SinglePartitionPager.java](../src/java/org/apache/cassandra/service/pager/SinglePartitionPager.java:91),
  [AbstractQueryPager.java](../src/java/org/apache/cassandra/service/pager/AbstractQueryPager.java:233),
  [MultiPartitionPager.java](../src/java/org/apache/cassandra/service/pager/MultiPartitionPager.java:193)
- Behavior: With three identical replicas, each of pk 0, 1, and 2 has deleted
  ck 0–2 and live ck 3–4. Threshold 2, fetch size 1, and
  `SELECT pk, ck ... WHERE pk IN (0,1,2) LIMIT 7` should return six rows.
  It returns `(0,3),(0,3),(0,4),(1,3),(1,3),(1,4),(2,3)`.
  Duplicate rows consume LIMIT and omit `(2,4)`.
- Evidence or failing test: `partitionKeyInContinuesEachPartition` fails in
  both full runs; its disabled-paging control passes in the corrected run.
- Required implementation action: Restrict checkpoint consumption to the
  current fetch. Source inspection indicates that SinglePartitionPager retains
  earlier commands in pageQueries while MultiPartitionPager repeatedly fetches
  through the same pager; onClose selects the first old checkpoint and can
  rewind after returning a live row. This mechanism is an evidence-backed
  diagnosis, not a validated patch.

### integration-002

- Severity: Major
- Production location: [AggregationQueryPager.java](../src/java/org/apache/cassandra/service/pager/AggregationQueryPager.java:276),
  [DataLimits.java](../src/java/org/apache/cassandra/db/filter/DataLimits.java:1121)
- Behavior: Group 0 contains three deleted rows; groups 1 and 2 each contain
  three live rows. At ALL, threshold 2, fetch size 1, and LIMIT 3,
  `GROUP BY pk,g` should return `(1,3),(2,3)`. Instead it aborts with
  ReadFailureException and a local CQLGroupByPagingLimits assertion.
- Evidence or failing test: `groupedReadContinuesAcrossEmptyCheckpoint`
  reproduces twice. Its disabled-paging control passes.
- Required implementation action: Preserve empty grouping state across an empty
  tombstone page. Source inspection shows updatePagerLimit constructing
  `new GroupingState(null, null)` before any row has been returned. The later
  counter requires the canonical EMPTY_STATE or a matching partition key.
  Verify both initial empty pages and empty pages between groups.

## Unresolved Test Gaps

- The GROUP BY regression remains failing. The IN regression is resolved in the follow-up above.
- Real upgrade execution needs historical and current dtest JARs. The three
  upgrade expectations are compiled, but are not validated against old nodes.
- Range checkpoint coverage establishes safe rejection, not successful range
  continuation.
- Concurrent writes use a controlled schedule. Abrupt process death, broader
  generated concurrent histories, and more GROUP BY/IN combinations remain.

## Blockers

The local artifact search found the dtest API dependency but no Cassandra
`dtest-*.jar` distributions. UpgradeTestBase fails with
`No 5.0-alpha1 versions found` before starting a cluster. The v50 constant selects
the latest available 5.0 artifact; it does not require that exact alpha release.
Build the current and a supported 5.0 branch with `ant dtest-jar`, then place both
artifacts in this checkout's build directory as documented in build.xml.
No dependency was added or installed.
