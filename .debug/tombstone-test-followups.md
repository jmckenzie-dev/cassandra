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

# Tombstone paging test follow-ups

Date: 2026-09-09. Repository: `wt_cassandra-18424`.
Plan: `.plans/tombstone-paging-overarching-plan.md`.
Review: `.reviews/review-full-tombstone-paging-branch-2026-09-08.md`.

The previously staged fixes are committed as `ea5a0ad8ff`, without an
Assisted-by trailer. This pass addresses FUL-006, FUL-023, FUL-024, and
FUL-036. Its comparison is `ea5a0ad8ff` against the working tree. Production
behavior is unchanged. The production context includes `ReadCommand`,
`AbstractQueryPager`, `PagingState`, and `ReadCallback`; the preceding
production diff is `ea5a0ad8ff^..ea5a0ad8ff`.

## Changes

- FUL-006: Replace the ignored empty method with
  `MixedVersionPagingAcrossTombstonesTest.asymmetricTombstonesWithOldReplicaMissingRows`.
  Upgraded replicas have twelve row tombstones and three live rows. Old
  replicas have only the live rows. Each query has a fresh partition, so
  blocking read repair cannot remove the next case's divergence. The matrix
  covers one and two upgraded nodes, all three coordinators, NONE/BLOCKING
  read repair, and memory/flushed data. Upgraded coordinators must return all
  three rows; old coordinators must return a native `ReadFailureException`.
  Every case requires a positive checkpoint-counter delta. Traversal has a
  deadline and page/row budgets.
- FUL-023: Remove `mixedModeReadColumnSubsetDigestCheck`,
  `testPagingWithCompactStorage`, and `mixedModeReadRepairDeleteRow` from the
  tombstone-specific copy. The original digest, compact-storage, and
  mixed-version read-repair tests remain. No new paging coverage is attributed
  to these removed copies.
- FUL-024: Both legacy validators now check a finite page budget, repeated
  state bytes, strictly advancing clustering marks, row count, and returned
  row order. The synchronous validator requests individual pages instead of
  calling unbounded `all()`. The asynchronous validator consumes only rows
  already available before fetching the next page. The budget includes model
  tombstones, since empty checkpoint pages are valid. Requests have a
  five-second timeout and traversal has a thirty-second deadline.
- FUL-036: Remove the logger sleep. Emit the failure seed, fixture state,
  replay command, generated test, and exception in one log event. Support the
  existing `cassandra.test.seed` JVM property in the test-only random helper.
  Keep the legacy environment-seed fallback. Replay mode asserts exactly one
  iteration per validator profile.

## Findings reproduced while strengthening tests

1. Driver `PagingState` objects do not compare by content. The first new
   repeated-empty-checkpoint regression failed because the object-based set
   accepted a second wrapper for the same checkpoint. Comparing serialized
   bytes fixes it. The distributed `readPages` helper used the same object
   comparison; it now records bytes too. Its new regression obtains equivalent
   checkpoints from separate real native reads.
2. The legacy replay environment variable does not survive the Ant launcher's
   `sh`. A diagnostic confirmed that `bash` retains `cassandra.test.seed` but
   `sh` drops it. The first long-suite invocation therefore ran in timed mode,
   not with the requested seed. The JVM-property regression then failed before
   the helper fix and passed after it. Both seeded reruns log `replay: true`
   and pass the one-iteration assertion.

These are test-harness defects, not new Cassandra production findings.

## Validation

Java 17 commands run from the repository through
`distrobox enter dev -- bash tmp/paging-coverage-run.sh`. The wrapper preserves
stdout, stderr, and exit status in a dated `logs/` file. Java 11 commands use
`distrobox enter dev -- env TOMBSTONE_TEST_JDK=11 bash tmp/paging-coverage-run.sh`.

| Selection | Result | Report directory |
| --- | --- | --- |
| Initial validator guard tests | 28 cases, one guard failure | `build/review-followup-validator` |
| Corrected validator guard tests | 28 passed | `build/review-followup-validator-fixed` |
| JVM seed before helper fix | One expected failure | `build/review-followup-replay-before` |
| Final validator tests | 29 passed | `build/review-followup-validator-final` |
| Existing paging suite | 98 passed, six existing assumption skips | `build/review-followup-paging` |
| Timed long suite plus generated property | Four passed, 376.072 seconds | `build/review-followup-property-18424` |
| Seed 18424 replay plus generated property | Four passed, 20.146 seconds | `build/review-followup-property-replay-18424` |
| Seed 1 replay plus generated property | Four passed, 20.634 seconds | `build/review-followup-property-replay-1` |
| Real 5.0.10-to-current upgrade suite | Five passed, 310.928 seconds, no skips | `build/review-followup-upgrade` |
| Distributed regressions and state comparison | Fifteen passed, 50.275 seconds | `build/review-followup-distributed` |
| Retained 4.1.13 upgrade fixture | Startup error before the query | `build/review-followup-legacy-upgrade` |
| Retained 4.1.13 upgrade fixture on Java 11 | One passed, 37.128 seconds | `build/review-followup-legacy-java11` |
| Distributed fuzzer with the corrected state guard | Three passed, 93.648 seconds | `build/review-followup-distributed-fuzz` |

The generated property uses QuickTheories seed 18424 and twenty tuples per
validator profile. It varies deleted rows (0–12), live rows (0–4), page size
(1–3), and threshold (1–4). Every tuple runs in both directions, with and
without static data, against the existing independent live-cell model. Cases
above the threshold must increment the checkpoint counter. The seeded command
enables twenty QuickTheories shrink attempts; failing tuples and the fixed
seed provide replay input. The older command-sequence fuzzer retains its seed
and generated-operation diagnostics.

`bash run_tests.sh build -s` passed. Early test-author compile attempts found
an unavailable JUnit `assertThrows`, the two-argument protocol decoder, and
the driver's byte-array paging-state API. Corrected these to the branch's
existing APIs. `ant dtest-jar` passed and rebuilt the current upgrade JAR.

Commands, after the common wrapper prefix:

```text
bash run_tests.sh build -s
bash run_tests.sh test org.apache.cassandra.service.pager.AbstractPagingValidatorTest -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-validator
bash run_tests.sh test org.apache.cassandra.service.pager.AbstractPagingValidatorTest -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-validator-fixed
ant testsome -Dtest.name=org.apache.cassandra.service.pager.AbstractPagingValidatorTest -Dtest.methods=testFuzzReplayUsesJvmSeed -Dno-build-test=true -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp -Dbuild.test.output.dir=build/review-followup-replay-before
bash run_tests.sh test org.apache.cassandra.service.pager.AbstractPagingValidatorTest -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-validator-final
bash run_tests.sh test org.apache.cassandra.service.pager.PagingAcrossTombstonesTest -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-paging
env cassandra.test.seed=18424 bash run_property_tests.sh --long -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-property-18424
bash run_property_tests.sh --long '-Dtest.jvm.args=-Dcassandra.test.seed=18424 -DQT_SHRINKS=20' -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-property-replay-18424
bash run_property_tests.sh --long '-Dtest.jvm.args=-Dcassandra.test.seed=1 -DQT_SHRINKS=20' -Dno-build-test=true -Dbuild.test.output.dir=build/review-followup-property-replay-1
ant dtest-jar
bash run_tests.sh test org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=480000 -Dbuild.test.output.dir=build/review-followup-upgrade
bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesDistributedTest -Dno-build-test=true -DmaxMemory=8G -Dbuild.test.output.dir=build/review-followup-distributed
bash run_tests.sh test org.apache.cassandra.distributed.upgrade.MixedModePagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=180000 -Dbuild.test.output.dir=build/review-followup-legacy-upgrade
bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=240000 -Dbuild.test.output.dir=build/review-followup-distributed-fuzz
```

Java 11 commands, after the Java 11 wrapper prefix:

```text
ant jar -Dbuild.dir=build/review-java11
ant dtest-jar -Dbuild.dir=build/review-java11
bash run_tests.sh test org.apache.cassandra.distributed.upgrade.MixedModePagingAcrossTombstonesTest -Dbuild.dir=build/review-java11 -Dbuild.test.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/build/test -Dno-build-test=true -Dtest.jvm.args=-Dcassandra.test.dtest_jar_path=build/review-java11 -DmaxMemory=8G -Dtest.timeout=180000 -Dbuild.test.output.dir=build/review-followup-legacy-java11
```

All three Java 11 commands exited zero. Before the test, copied the old JAR
without overwriting any existing artifact:
`cp -n build/dtest-4.1.13.jar build/review-java11/dtest-4.1.13.jar` (exit zero).
The Java 17 build and original archived JARs remain intact.

The final selected classes contain 161 JUnit cases: 155 passed and six skipped
through the existing parameter assumptions. Seed reruns are additional
executions of the same tests, not additional test cases.

## Style checks

`ant checkstyle checkstyle-test` passed production Checkstyle and reported
23 test violations. One was an import made unused by the validator refactor;
removed it. `ant build-test checkstyle checkstyle-test` then compiled the test
changes and passed production Checkstyle, but exited one on 22 existing test
violations. The existing baseline script confirmed that zero violations point
at changed lines compared with `4decacfc0d` (a superset of this pass's diff):
`venv/bin/python tmp/review-style-baseline.py` exited zero. These commands used
the common Java 17 logging wrapper. No checks or suppressions were disabled.
`git diff --check` passed. The full license/documentation check was not rerun;
its earlier failures remain recorded in `tombstone-review-reproductions.md`.

## Runtime limit resolved

The first retained `mixedModeTOEException` run could not execute its query:
the archived 4.1.13 harness calls `Verb.unsafeSetSerializer`, which attempts
`Field.class.getDeclaredField("modifiers")` and fails with
`NoSuchFieldException` on Java 17. A separate build under `build/review-java11`
resolved the mismatch. The unchanged retained test passed on Java 11 with
4.1.13 and the current branch. No archived binary, dependency, or bytecode was
changed, and the test was not skipped or weakened.

## Remaining work

FUL-011 and FUL-019 still need concrete user-visible reproductions. The
allocation measurement and API/state-design suggestions remain separate
follow-ups. This test pass does not establish those findings as defects.

# Unit author result

## Test Author Result

- Status: COMPLETE
- Domain: unit
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-024 and FUL-036 test-helper behavior
- Resolved comparison basis: `ea5a0ad8ff..working-tree`
- Production files reviewed: `PagingState.java`, `ReadCommand.java`
- Test files changed: `AbstractPagingValidator.java`,
  `AbstractPagingValidatorTest.java`, `RandomHelpers.java`
- Production code changed: no

## Tests Added or Updated

Six guard/replay regressions. Five exercise empty-state repetition, cursor
rewind, duplicate/backward rows, and page/row budgets. The sixth verifies JVM
seed reset and repeated random output. Row and state inputs come from real
native reads; no production state was fabricated or patched.

## Commands and Outcomes

See the validation table and commands above. The final class passed 29 cases.
The state-comparison and seed regressions each failed before their helper fix.

## Findings for the Implementer

None. The two test-helper defects are fixed; no production repair is required.

## Unresolved Test Gaps

None for the requested guard and replay scenarios.

## Blockers

None.

# Integration author result

## Test Author Result

- Status: COMPLETE
- Domain: integration
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-006, FUL-023, and real-read validation of FUL-024
- Resolved comparison basis: `ea5a0ad8ff..working-tree`
- Production files reviewed: `ReadCommand.java`, `ReadCallback.java`,
  `AbstractQueryPager.java`, `PagingState.java`
- Test files changed: `MixedModePagingAcrossTombstonesTest.java`,
  `MixedVersionPagingAcrossTombstonesTest.java`,
  `PagingAcrossTombstonesDistributedTest.java`, `TombstonePagingTestUtil.java`,
  and the validator files listed in the unit result
- Production code changed: no

## Tests Added or Updated

The asymmetric upgrade matrix and the distributed state-content regression
exercise real native reads. Removed the ignored placeholder and three
redundant tests. Both legacy validators now enforce bounded traversal.

## Commands and Outcomes

See the validation table and commands above. Five 5.0.10 upgrade tests and
fifteen current-version distributed tests passed. The retained 4.1.13 test
passed with the separate Java 11 build after its Java 17 startup failure.

## Findings for the Implementer

None. The asymmetric scenario completed without production changes.

## Unresolved Test Gaps

The archived 4.1 harness remains incompatible with Java 17; its retained test
is verified on Java 11. This does not block the requested upgrade scenario.

## Blockers

None.

# Property author result

## Test Author Result

- Status: COMPLETE
- Domain: property
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: generated live-row equivalence under bounded traversal
- Resolved comparison basis: `ea5a0ad8ff..working-tree`
- Production files reviewed: `ReadCommand.java`, `AbstractQueryPager.java`,
  `PagingState.java`
- Test files changed: `PagingFuzzTest.java`
- Production code changed: no

## Tests Added or Updated

Added the generated model property described above. Retained the existing
command-sequence fuzzer and made its seed replay usable through Ant. Failure
diagnostics use one log event; there is no logger sleep.

## Commands and Outcomes

See the validation table and commands above. Both profiles passed the timed
run and seed replays 18424 and 1. The three-method distributed fuzzer also
passed with the corrected byte-content guard.

## Findings for the Implementer

None.

## Unresolved Test Gaps

The wider mixed-version fuzzer was not rerun in this pass. The deterministic
upgrade matrix and current-version distributed fuzzer were run.

## Blockers

None.
