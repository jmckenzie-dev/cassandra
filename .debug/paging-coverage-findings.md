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

# Paging coverage findings, 2026-09-05

This is the pre-fix audit. The corrected fuzzer and deterministic regressions
now pass. See [the implementation results](compare-tombstone-page-continuations.md)
for the fix, added tests, and current coverage. The failure counts and coverage
below remain historical evidence.

The branch starts at `e4d83b3bb7`, with feature commits `0c202df78b` and
`458b9b699c`, and inspected HEAD `4e454fb560`. Production code was not changed
during this investigation. Tests were rebuilt with Java 17 and JaCoCo 0.8.8.

## Distributed failure reproduction

The counts below describe the original audit run. Follow-up debugging removed
unpaged queries from the enabled success expectation and independently checked
the mutation model. See [the checkpoint investigation](paging-initial-checkpoint.md)
for the corrected test scope, five-row reproduction, and current results.

Run from the repository with Java 17 selected and tests built:

```sh
bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=4G '-Dtest.jvm.args=-Dcassandra.test.seed=18424'
```

The six generated histories cover 2,592 reads: four configurations, memtables
and flushed data, both scan directions, limits 1/5/49, page sizes 1/4/16, and
each of three coordinators. Clients connect through the native protocol. Every
query uses consistency ALL. Read repair writes and speculative retries are off.

All 2,592 reads ran. 1,127 matched the independent mutation model; 1,465 returned
`ReadFailureException`. All 648 reads in the all-disabled control matched the
model. There were no observed row mismatches among successful reads. 877 failures
had a fetch size smaller than the LIMIT, so the failure is
not confined to queries where Cassandra can omit the pager.

| Configuration | Failures / reads |
|---|---:|
| All enabled, failure thresholds 2/2/2 | 545 / 648 |
| All enabled, failure thresholds 2/3/4 | 484 / 648 |
| Node 2 disabled with failure threshold 10000; others enabled | 436 / 648 |
| All disabled, failure thresholds 10000/10000/10000 | 0 / 648 |

One failing case is seed 18424, example 0, memtables, matching enabled
configuration, coordinator 1, ascending LIMIT 5, fetch size 1. The model has a
deleted prefix on replica 1 and stale rows on the other replicas. The full query
matrix and exceptions are in:

- `logs/run_tests_20260905T153052Z-3762279.log`
- `build/paging-coverage/20260905T153052Z-3762279/tests/TEST-org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest.xml`
- `build/paging-coverage/20260905T153052Z-3762279/index.html`

The test remains failing. The failure counts describe this run, not a claimed
stable distribution across replica scheduling decisions. The seed reproduces
the mutation history and query sequence.

Source inspection explains an initial-response limitation: `ReadCommandVerbHandler`
adds both `TOMBSTONE_PAGE_CHECKPOINT` and `TOMBSTONE_FAIL` to shortened responses.
`ReadCallback` removes the failure marker only for the ACCEPT policy on a data
response. Initial reads in `AbstractReadExecutor` use USE_LOCAL_COMMAND or REJECT.
The existing eight distributed regressions exercise later short-read refills,
checkpoint ownership, stale-row reconciliation, descending reads, and safe failure
of direct reads. All eight passed, but did not catch these initial-response failures.

The first fuzzer attempt used `ICoordinator.executeWithPaging`. That path also
exposed missing coordinator-warning initialization when a threshold-crossing
query omitted the pager. Native-protocol clients avoid that harness limitation.
An intermediate run failed to bind port 9042. The final fuzzer uses loopback
subnet 10 and completed its matrix.

## Repaired test defects

- A clean build failed in `ReadCommandTest`: QuickTheories has `withFixedSeed`,
  not `withSeed`, and does not provide the imported chain's `withPure` method.
  Corrected those calls. All 32 tests, including the 24-example property, passed.
- The synchronous validator ignored reverse scans and checked results through
  a map that hid order. Added an order assertion. It exposed 19 failures, then
  all 104 cases passed after synchronous reads used `generateQueryStatement`.
  Six cases intentionally skip duplicate collection/warning checks.
  Red run: `logs/paging-coverage-20260905T145906Z.log`.
  Green run: `logs/paging-coverage-20260905T150024Z.log`.
- `run_tests.sh` called missing `ai-ci-test` and `ai-build` helpers. It now uses
  the repository's Ant build and targeted test targets, logs commands, and can
  generate isolated coverage reports even when a test fails.

## Inventory limitations

- `PagingAcrossTombstonesTest`: 26 methods across four client/direction profiles.
  Row, cell, range, static, collection, exhaustion, and warning cases; one node.
- `AbstractPagingValidatorTest`: 22 tests of the model and supporting operations.
- `PagingFuzzTest`: synchronous/asynchronous single-node histories, with row,
  cell and range deletes, static data, and direction changes. No replica divergence.
- `ReadCommandTest`, `DigestResolverTest`, and `BlockingReadRepairTest` provide
  checkpoint state, property, transient-response, and ownership unit coverage.
  Transient-response unit coverage does not establish transient-replica cluster coverage.
- `MixedConfigPagingAcrossTombstonesTest` has two populated tests, six empty
  tests, and two reference methods without `@Test`. Its factory ignores all
  supplied versions, enablement flags, and thresholds.
- `MixedVersionPagingAcrossTombstonesTest` inherits that same current-version
  factory. Its four named configurations do not create mixed-version clusters.
- `MixedModePagingAcrossTombstonesTest` includes a real two-node upgrade test
  of old-node tombstone failure propagation, one empty test, and three tests
  that do not enable tombstone paging. Upgrade tests were not run in this audit.
- Existing distributed `PagingTest` now runs with paging enabled and disabled;
  all four cases passed. The 360 existing `ShortReadProtectionTest` cases passed
  with the feature at its default disabled setting.
- Five existing unit paging classes (20 cases) passed with paging enabled and
  both thresholds set to 2. JaCoCo shows that this selection never entered the
  threshold-exceeded branch in `ReadCommand`; these runs do not establish
  continuation across tombstones.

Missing distributed scenarios include aggregation/GROUP BY, multi-partition and
range scans crossing thresholds, transient replicas, read-repair writes, static
and collection tombstones, TTL expiry, node failures, and actual mixed versions.
The bounded fuzzer covers a subset of these dimensions and is not a soak test.

## Coverage from passing runs

`build/paging-coverage/passing/index.html` merges these fresh execution files:

| Input directory under `build/paging-coverage/` | Cases | Result |
|---|---:|---|
| `unit` | 206 | Pass; six intentional skips |
| `sync-fixed` | 104 | Pass after order fix; six intentional skips |
| `distributed` | 372 | Pass |
| `enabled` | 20 | Pass with thresholds 2/2 |
| `single-fuzz-isolated` | 2 | Pass, one minute per client profile |

The final single-node fuzz command exited zero. It used a copy of the test YAML
with internode ports 17022/27022 because another process occupied port 7012.
See `logs/run_tests_20260905T152741Z-3752047.log`. Earlier retries encountered a
port conflict and an active-wrapper edit; those runs are excluded from this merge.

The distributed failing fuzzer's data is deliberately separate from this report.
All reports analyzed the freshly compiled Java 17 production classes and emitted
no class-mismatch warnings.

| Source scope, including nested classes | Lines | Branches |
|---|---:|---:|
| Entire `service.pager` package | 391/573 (68.2%) | 245/366 (66.9%) |
| `AbstractQueryPager.java` | 84/111 (75.7%) | 48/62 (77.4%) |
| `SinglePartitionPager.java` | 24/31 (77.4%) | 16/16 (100%) |
| `MultiPartitionPager.java` | 59/75 (78.7%) | 41/52 (78.8%) |
| `PartitionRangeQueryPager.java` | 33/46 (71.7%) | 22/30 (73.3%) |
| `AggregationQueryPager.java` | 35/103 (34.0%) | 12/40 (30.0%) |
| `ShortReadRowsProtection.java` | 47/49 (95.9%) | 27/30 (90.0%) |
| `ShortReadPartitionsProtection.java` | 60/61 (98.4%) | 24/32 (75.0%) |
| `ReadCommandVerbHandler.java` | 65/116 (56.0%) | 26/58 (44.8%) |

Intersecting added/modified lines from `git diff --unified=0 e4d83b3bb7 HEAD --
src/java` with JaCoCo source-line records gives 183/220 executable lines covered
(83.2%) and 125/154 branches on those lines (81.2%). These are execution measures,
not proof of correct checkpoint handling. Uncovered changed code includes
unfiltered paging, checkpoint failure/rejection handling, transient-response
consumption, and management accessors.

Some missed lines are unreachable diagnostics rather than missing test cases:
the `hitTombstoneLimit` guard inside `AbstractQueryPager.Pager.onClose` cannot
be true on the next loop iteration because the first hit immediately breaks.
Do not add an artificial test to chase coverage of that guard.

The full code check stopped on 18 pre-existing license violations. Test Checkstyle
reported 27 existing violations after the new code's violations were corrected.
The new distributed fuzzer has no Checkstyle violations. See `build/rat.txt` and
`build/checkstyle/checkstyle_report_test.xml`.
