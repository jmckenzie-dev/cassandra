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

# Mixed-version tombstone paging, 2026-09-06

## One and two upgraded nodes

The deterministic suite now has four methods, all passing in 209.825 seconds.
Each test upgrades node 1, checks that stage, then upgrades node 2 and checks
again. Node 3 remains on 5.0.10. Every upgraded coordinator must reconcile
checkpoints with legacy full responses. Old coordinators must report a read
failure when a replica checkpoints. Genuine failures from old replicas must
still reach upgraded coordinators.

The new shared-cluster matrix exercises six query shapes:

- Single-partition GROUP BY.
- Regular rows with a static column.
- Partition-key IN with static columns and a static-only partition.
- GROUP BY with IN and static columns, including the static-only result.
- The same grouped IN query with PER PARTITION LIMIT 1.
- Expired time-to-live (TTL) rows followed by live rows.

Each shape runs through all three coordinators at both upgrade stages, before
and after an explicit flush, with fetch sizes 1 and 2 and several LIMIT values.
This gives 432 enabled reads: 216 successful upgraded-coordinator reads and
216 expected old-coordinator failures. Another 504 reads validate full results
and limited results with tombstone paging disabled. Some LIMIT values repeat
for the two-row fixtures. Static values and group counts have explicit expected
results; the disabled read supplies only IN's partition traversal order.

The test observes TTL expiration on every replica, with a ten-second deadline.
Native reads have request and traversal deadlines, a row budget, a page budget,
and a repeated-state check. Empty checkpoint pages remain valid. Metrics must
confirm actual threshold crossings for each enabled read. The second upgrade
stage starts with the data flushed during the first stage; it does not claim
fresh memtable coverage on every node.

The three existing methods take 41–44 seconds each. The combined feature
method takes 81.494 seconds, including cluster startup and two upgrades.

Evidence:

- `logs/run_tests_20260906T211236Z-2799312.log`, exit 0.
- `build/paging-upgrade-combinations/tests/TEST-org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.xml`.
- Focused Checkstyle passes for the modified tests and related production code.

## Mixed-version fuzzer

`MixedVersionPagingAcrossTombstonesFuzzTest` now generates six histories of
24 starting rows. Seeds 18424 and 1 pass in 93.125 and 94.110 seconds. Node 3
stays on 5.0.10. Nodes 1 and 2 upgrade in order. Native clients pin reads to all three coordinators at
each stage. The generator includes inserts, row deletes, and inclusive range
deletes on independently selected replicas, plus an all-deleted partition.
Deleted ends force checkpoints for forward and reverse reads.

The model applies mutations in strictly increasing timestamp order. A separate
partition on the old node replays every mutation through Cassandra. Unpaged
and paged reads with the feature disabled must also match the model before an
enabled query runs. Single-partition queries use both clustering directions;
IN queries cross groups of three generated partitions. All use consistency
ALL, fetch sizes 1 and 2, and LIMIT 3, 11, or 73. Failures include the seed,
upgrade stage, coordinator, query, fetch size, and mutation history.

Each seed checks 1,008 enabled query cases: 504 successful reads from upgraded
coordinators and 504 expected old-coordinator failures. It also checks 1,008
paged controls, 192 unpaged controls, and 48 local mutation-replay reads. The
two seeds found no new implementation defect. Neither run added production
changes. Focused Checkstyle and `git diff --check` also pass.

Evidence for seed 18424:

- `logs/run_tests_20260906T211934Z-2816555.log`, exit 0.
- `build/paging-upgrade-fuzz-18424/tests/TEST-org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesFuzzTest.xml`.

Evidence for seed 1:

- `logs/run_tests_20260906T212142Z-2821319.log`, exit 0.
- `build/paging-upgrade-fuzz-1/tests/TEST-org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesFuzzTest.xml`.

Run it explicitly after building both version JARs:

```sh
bash run_property_tests.sh --mixed-version -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=480000
```

The default seed is 18424. Set `-Dtest.jvm.args=-Dcassandra.test.seed=1` to
change it. The default property-test entrypoint remains usable without
historical JARs. This fuzzer does not generate grouped, static-column, or TTL
mutations; the deterministic upgrade matrix covers those combinations.

## Initial three-test validation

All three `MixedVersionPagingAcrossTombstonesTest` methods pass: no failures,
errors, or skips. The suite took 99.921 seconds on Java 17. The historical
version is 5.0.10, and the upgraded node uses the current branch's 7.0 dtest JAR.
This completes the previously blocked upgrade-test TODO. No test or production
source change was needed in this validation pass.

Each test creates three old-version nodes with identical rows 0–4 and deletes
0–2. It upgrades node 1 and enables tombstone paging there with threshold 2.
Nodes 2 and 3 remain on 5.0.10. A native client selects a specific coordinator,
uses consistency ALL, fetch size 1, and LIMIT 3.

| Method | Contract verified | Seconds |
| --- | --- | ---: |
| `upgradedCoordinatorReconcilesOldFullResponses` | The upgraded coordinator reconciles its checkpoint with old replicas' complete responses and returns rows 3 and 4. | 28.478 |
| `oldCoordinatorRejectsNewCheckpoint` | The old coordinator reports `ReadFailureException` when the upgraded replica returns a checkpoint. | 29.575 |
| `oldReplicaFailureReachesUpgradedCoordinator` | With old replicas' failure threshold also set to 2, the upgraded coordinator propagates their genuine tombstone failure. | 41.695 |

The supplied historical JARs resolved the missing-version blocker. The supplied
`dtest-7.0.jar` lacked this branch's `ReadCommand` checkpoint methods, as shown by
`javap`. A copy is retained at `tmp/dtest-7.0-supplied.jar`. `ant dtest-jar` then
rebuilt `build/dtest-7.0.jar` from this working tree before any upgrade test ran.

Commands ran through `distrobox enter dev -- bash tmp/paging-coverage-run.sh`,
which selects Java 17, uses project-local temporary files, logs both streams,
and preserves exit status:

```sh
ant dtest-jar
bash run_tests.sh test org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=300000 -Dbuild.test.output.dir=build/paging-upgrade-first/tests
```

The focused Ant entrypoint uses the supplied version JARs. The general upgrade
helper also prepares a checkout and historical builds, which are unnecessary
for this selection. The suite has a five-minute outer timeout. No sleeps or
dependencies were added, and no artifact was committed.

Evidence:

- Build: `logs/paging-coverage-20260906T204418Z-2739179.log`, exit 0.
- Test run: `logs/run_tests_20260906T204511Z-2742258.log`, exit 0.
- JUnit: `build/paging-upgrade-first/tests/TEST-org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest.xml`.

The initial run supplied passing execution evidence for all 77 methods then
present in the [test inventory](tombstone-paging-test-inventory.md). Those three executions brought the preceding
fix-validation selections to 560 passes and six existing skips. This run did
not use JaCoCo; the earlier coverage figures remain unchanged.
