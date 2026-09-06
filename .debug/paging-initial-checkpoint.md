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

# Initial distributed tombstone checkpoint investigation, 2026-09-05

This is the pre-fix investigation. Digest and continuation comparison now
resolves these failures. See [the implementation results](compare-tombstone-page-continuations.md)
for the change and validation.

HEAD: `4e454fb560`. This follow-up changes tests and documentation.
The production code matches the earlier coverage investigation.

## Confirmed result

Initial remote checkpoint responses fail at the coordinator. Five rows with
identical mutations on three replicas reproduce the failure without a model,
randomness, range deletions, or replica divergence. The branch explicitly rejects
these responses; this is an unsupported distributed continuation path. The
failure is a query abort, not an assertion that a page must be full or evidence
of silent row loss.

## Correction to the original fuzzer

The original enabled matrix required every query to succeed, including fetch
sizes greater than or equal to LIMIT. `SelectStatement.execute` bypasses the
pager for those queries. Existing direct-read tests deliberately expect safe
failure when no component can resume a checkpoint. The original 1,465 failures
therefore must not be presented as 1,465 independently established defects.

Enabled cases now require fetch size strictly below LIMIT. The disabled control
retains the full size/limit matrix. Result collection still uses the native
driver's `ResultSet.all()`, which consumes all pages. The test makes no demand
that individual pages contain the fetch size.

## Model verification

The generated schema contains an integer partition key, integer clustering key,
and one integer value. Every insert writes the whole row. The generator uses
row deletes and inclusive range deletes. Initial writes use timestamp 1; the
deleted prefix uses timestamp 2; random operations use unique timestamps 3–66.
Later operations therefore supersede earlier row values or deletions. The
TreeMap's `put`, `remove`, and inclusive `subMap(...).clear()` match this contract.
There are no TTLs, null cells, static columns, timestamp ties on conflicting
operations, or concurrent mutations. Deletion times remain within GC grace.

The inherited `init(cluster)` creates a replication factor equal to cluster
size, verified from `dtest-api-0.0.18.jar`'s `DistributedTestBase` bytecode. Thus
ALL reconciles all three full replicas. Hinted handoff, read repair writes, and
speculative retries are disabled. Replica disagreement is intentional.

The strengthened test checks every history before enabling tombstone paging:

1. Replay all mutations on replica 1 under a separate negative partition key.
   Compare its local, unlimited Cassandra read against the model in both orders.
2. Read the divergent partition through the native protocol at ALL, without
   LIMIT or a pager, using each coordinator. Compare the entire result.
3. Run the original disabled paging control across both orders, all three
   coordinators, LIMIT 1/5/49, and fetch size 1/4/16.

Each check runs before and after flush. The separate
`mutationModelMatchesCassandra` test passed seeds 18424, 1, and 8675309: 18
histories and 2,232 control reads, including 72 local replays, 216 unlimited ALL
reads, and 1,944 size/limit combinations. The enabled fuzzer repeats its seed's
controls before testing paging, so model failure stops that history first.

These controls would detect incorrect range endpoints, omitted deletes,
incorrect resurrection after deletion, wrong ordering, and extra or missing
live rows for the generated histories. This is reasoned defect detection plus
executed differential checks; no mutation-testing tool was run. Equal-timestamp
conflicts, TTLs, and partial-row updates remain outside the generator's scope.

## Five-row reproduction

`PagingAcrossTombstonesDistributedTest` creates three identical replicas:

```text
ck              0       1       2       3       4
timestamp 1     insert  insert  insert  insert  insert
timestamp 2     delete  delete  delete  -       -
```

Paging and failure thresholds are 2. A native client pinned to node 1 executes:

```sql
SELECT ck, v FROM <table> WHERE pk = 0 ORDER BY ck ASC LIMIT 2;
```

Fetch size is 1. The final result should contain `(3, 3)` and `(4, 4)`.

| Case | Observed result |
| --- | --- |
| Three tombstones, ALL, memtable | ReadFailureException |
| Three tombstones, ALL, flushed | ReadFailureException |
| Two tombstones, ALL | Correct two rows |
| Three tombstones, ONE | Empty initial page with paging state, then both live rows |
| Three tombstones, ALL, paging disabled and failure threshold 10000 | Correct two rows |

The test checks the empty initial page and non-null paging state explicitly at
ONE. Its passive message observer records remote responses without dropping or
changing them. Both failing cases recorded:

```text
replica=2, digest=true, params={TOMBSTONE_FAIL=3, TOMBSTONE_PAGE_CHECKPOINT=3}
replica=3, digest=true, params={TOMBSTONE_FAIL=3, TOMBSTONE_PAGE_CHECKPOINT=3}
```

The native error reports three required responses, one successful response,
and two failed replicas. The two ALL success assertions remain failing to mark
the missing continuation support.

## Failure path

1. `ReadCommand.MetricRecording.countTombstone` stops iteration after the third
   row tombstone because the failure threshold is 2 and paging is enabled.
2. `ReadCommandVerbHandler.doVerb` attaches both the checkpoint count and a
   tombstone failure marker to the shortened remote response.
3. The initial `AbstractReadExecutor` callback selects `USE_LOCAL_COMMAND` for
   a pager-owned command, or `REJECT` otherwise.
4. `ReadCallback.onResponse` removes the failure marker only for `ACCEPT` on a
   full-data response. It does not remove it for these digest responses.
5. `WarningContext.updateCounters` converts the marker to
   `READ_TOO_MANY_TOMBSTONES`. `ReadCallback` records failure before calling the
   resolver; ALL then fails with `ReadFailureException`.

The local ONE control can use the checkpoint cursor on the original command.
The initial remote path does not transmit or adopt that cursor. Existing
`DigestResolverTest.tombstonePageCheckpointFailsDigestResolverRead` explicitly
requires this rejection. Later full-data reconciliation in `AbstractReadRepair`
can instead select ACCEPT when `DataResolver.supportsTombstonePageCheckpoints`
allows short-read continuation. This explains why the existing short-read
regressions pass while this initial-response case fails.

Simply accepting shortened digests is not a demonstrated fix: matching partial
digests do not establish agreement beyond the checkpoint, and unequal replica
checkpoint positions need reconciliation. A fix needs a supported continuation
path for initial responses, such as an explicit transition to full-data
reconciliation, with tests for divergent replicas and checkpoint ownership.
No production change was attempted during this diagnosis.

## Commands and results

Commands below ran with Java 17 selected using `set_jdk 17` in the dev distrobox.
`tmp/paging-coverage-run.sh` recorded stdout, stderr, command, and exit status
through its return code. Initial compilation needed an IOException catch in
the message observer; the corrected test built successfully.

```sh
bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesDistributedTest -DmaxMemory=4G
bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=4G
ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dtest.methods=mutationModelMatchesCassandra -Dno-build-test=true -DmaxMemory=4G -Dtest.jvm.args=-Dcassandra.test.seed=1 -Dbuild.test.output.dir=build/paging-model-seed1 -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dtest.methods=mutationModelMatchesCassandra -Dno-build-test=true -DmaxMemory=4G -Dtest.jvm.args=-Dcassandra.test.seed=8675309 -Dbuild.test.output.dir=build/paging-model-seed8675309 -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp
```

| Run | Result | Log |
| --- | --- | --- |
| Deterministic reproduction | 5 tests, 2 failures, 3 passes; exit 1 | `logs/run_tests_20260905T172204Z-3964850.log` |
| Corrected fuzzer, seed 18424 | Model test passes; paging test fails; exit 1 | `logs/run_tests_20260905T172245Z-3967150.log` |
| Model only, seed 1 | Pass; exit 0 | `logs/paging-coverage-20260905T172440Z.log` |
| Model only, seed 8675309 | Pass; exit 0 | `logs/paging-coverage-20260905T172617Z.log` |

The corrected enabled fuzzer executed 1,080 reads: 868 ReadFailureExceptions,
212 matching results, and no row mismatches. Failure counts by configuration
were 314 (equal thresholds), 291 (different thresholds), and 263 (one disabled
node with a high failure threshold). All 744 control reads in that test passed.
Scheduling can change failure counts. The seed fixes the data and query sequence,
not replica response order. The deterministic reproduction confirms one cause;
not every fuzzer failure was traced individually.

An initial seed-1 run used a relative Ant temporary directory, which produced
logback file-URL errors. It was repeated with the absolute directory above.
The additional-seed runs logged the harness's shutdown thread-count invariant
without failing JUnit; query/model assertions completed successfully. The error
is in `Instance.shutdown`'s thread-group check, outside the read assertions.

The repository's test Checkstyle rules pass for both changed test classes via
`ant -f tmp/paging-debug-checkstyle.xml`. An import-spacing violation was corrected.
No JaCoCo rerun was needed for this diagnosis; the earlier coverage reports
describe the earlier test selection and must not be labeled as this run's coverage.
