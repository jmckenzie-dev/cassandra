<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Bounded idle flush admission

Date: 2026-09-10. Branch: moar_tables. Base commit: fc4aa2ab43174ced611718a7a79ac6bfb6198401.
The checkout already contained the UCS hierarchy experiment and census work.
This change adds idle admission budgets and manual-DDL lifecycle validation.
Changes remain uncommitted.

## Result

Idle retirement remains optional, disabled by default, and restricted to eligible
lazy Trie memtables using Unified Compaction Strategy (UCS). Operators retain
control of the existing table compaction options. There is no automatic tuner or
new opportunistic compaction path.

The existing node-wide candidate set and concurrency limit now share an operation
budget and an estimated-byte budget. This adds constant node-wide state, without
another table-name queue or per-table budget objects. It bounds optional admission;
it does not reduce the reader/metrics heap retained by each completed SSTable.

## Configuration and behavior

```yaml
memtable_idle_timeout: 0s
memtable_idle_flush_max_concurrent: 2
memtable_idle_flush_max_per_second: 100
memtable_idle_flush_throughput: 16MiB/s
```

Set the timeout to a positive duration such as 30s to enable the existing policy.
Node settings take effect at restart. Both new budget values must be positive;
zero does not mean unlimited. Defaults are initial safety settings, not a measured
optimum for every workload.

Each budget accumulates at most one second of credit, including initial credit.
Consequently, max_per_second is a sustained rate with a one-second burst allowance,
not a strict count in every sliding one-second window. Existing concurrency limits
still bound outstanding flushes, including those waiting for reader reclamation.

Admission requires one operation token and positive byte credit. An accepted flush
charges its estimated live memtable data size. One flush can overdraw byte credit;
no further idle flush is admitted until credit becomes positive again. Large
memtables therefore make progress even when they exceed one second's byte budget.
The scan resumes on the existing 100ms scheduler and completion/reclamation wakeups.
Empty or rejected/stale submissions do not consume admission credit.

The byte estimate is not actual disk output. Compression, indexes, metadata, and
writes racing with the estimate can change the amount written. There is no bound
on the downstream compaction bytes implied by a single flush. Ordinary pressure,
commit-log, and forced flushes retain their existing paths and limits. Retirement
does not wait for background compaction to finish.

An idle timeout is eligibility, not a residency deadline. A queued table receiving
another write loses idle eligibility. A table near the timeout boundary can remain
resident for multiple cycles. Normal memory-pressure flushing remains available.

Operators can use CREATE TABLE or ALTER TABLE compaction maps to set
`min_hierarchy_size` and `scaling_parameters`. The hierarchy setting accepts 1B
through 1MiB, defaults to 1MiB, and sets a floor/rounding quantum rather than a hard
L0 size limit. `L4,T4` is an optional operator choice; this change does not select
it automatically. Manual changes can regroup existing files and cause additional
work. Include other compaction options that should remain in effect when replacing
the map. README.asc contains an ALTER example.

## Measurements

These are small real-scheduler admission checks, not throughput or heap-capacity
benchmarks. Each run starts a fresh one-node distributed-test cluster, uses lazy
Trie memtables and UCS, and checks every stored row after retirement. Timeout is
two seconds. Elapsed time starts before the writes and ends after all tables have
retired; it includes idle waiting, flushes, and scheduler/reclamation observation.

| Workload | Admission controls | Elapsed |
|---|---|---:|
| Six tables, one small integer row each | 100/s, 1MiB/s | 2.223s |
| Same six-table workload | 1/s, 1MiB/s | 7.149s |
| Four tables, one 64KiB text value each | 100/s, 32KiB/s | 7.151s |

The matched count runs show approximately five seconds of added drainage from the
1/s budget. The byte run admits individual flushes larger than the byte credit,
then delays subsequent admissions. Its repeated text compresses well; the delay
still follows live input data, confirming this is not a compressed-output limiter.
The high-rate run uses the new implementation with nonbinding budgets; it is not
a separate stock-code benchmark. No new heap saving or production latency claim
follows from these runs. No run exceeded six user tables in this task.

Measurements are in the distributed test log under:
`logs/20260910-004027-ai-test-memtable-lazy/org.apache.cassandra.distributed.test.IdleMemtableFlushTest/_jdk21/TEST-org.apache.cassandra.distributed.test.IdleMemtableFlushTest.log`.

## Validation and behavioral coverage

Final focused suite: 73 tests across nine classes, no failures or errors.
Property suite: one test with 100 seeds and 1,000 steps each, no failures or errors.
The focused suite also includes eight generated real-memtable lifecycle traces
with 40 operations each and the existing generated UCS hierarchy checks.

| Requirement or risk | Evidence |
|---|---|
| Disabled defaults, positive validation, configuration units | IdleMemtableConfigTest: defaultsAndYamlUnits, invalidSettingsFailEvenWhenIdleFlushingIsDisabled; distributed configurationDefaultsToDisabled |
| Production getters and scheduler use configured rates | schedulerHonorsConfiguredRateAndDrainsAllCandidates, schedulerHonorsByteDebtForOversizedFlushes |
| Count refill, capped credit, zero-size charge | IdleFlushAdmissionBudgetTest: countCreditRefillsAndDoesNotAccumulateAcrossLongSilence |
| Oversized debt, positive byte threshold, capped byte credit | oversizedFlushRepaysDebtAndRequiresPositiveByteCredit; byteDebtDelaysAnotherTableUntilRepaid |
| Clock wrap, backward observations, extreme sizes/rates | monotonicClockWrapAndBackwardObservations, extremeSizeRemainsDebtWithoutOverflow, invalidRatesAreRejected |
| Count/byte envelopes and eventual recovery | IdleFlushAdmissionBudgetPropertyTest.generatedTracesRespectEnvelopesAndRecover, 100,000 steps |
| Rejected submission does not spend budget | staleSubmissionDoesNotSpendBudget |
| Completion cannot reset admission credit; forced flush remains possible | rateBudgetSurvivesCompletionAndDoesNotBlockForcedFlush |
| New writes and old generations cannot force stale retirement | queuedWriteIsRecheckedBeforeAdmission, newWriteAndOldGenerationPreventStaleFlush |
| Reclamation keeps a concurrency slot occupied | admissionWaitsForPinnedReaderReclamation, readerPinsStorageAfterRetirementCompletes |
| Accepted/concurrent replacement writes survive retirement | acceptedWriterAndReplacementWriterSurviveRetirement |
| Failed flush retains memory/data/commit-log state | failedSubmissionStopsAdmissionWithoutDiscardingData, failedRetirementRetainsDataMemoryAndCommitLog |
| Drop and ineligible strategy changes | droppedCandidatesAreRemoved, exclusionsAndStrategyChanges, localStrategyOverridesRefreshTracking |
| Index data and queries survive retirement | scheduledFlushPreservesIndexQueries, retirementFlushesDirtyIndexWhenBaseIsClean |
| Operator changes hierarchy down and up with L4,T4, then restarts | operatorCanAlterHierarchyAndScalingBeforeRestart |
| Existing hierarchy math, actual compaction, TTL/deletions and reads | ControllerTest, ControllerHierarchyTest |
| Settings exposure | SettingsTableTest |

The review found no uncovered material admission behavior in this scope. This is
behavioral traceability, not a numeric coverage result or a production readiness
claim for all workloads. The new per-table adaptation and opportunistic compaction
ideas are outside this implementation.

### Initial quantitative coverage limitation (resolved below)

The installed JaCoCo agent is 0.8.8. An actual instrumented probe failed with
`Unsupported class file major version 65` when loading AdmissionBudget compiled
for Java 21. The JVM continued and the five budget tests passed uninstrumented;
that successful process exit is not proof of coverage. Neither line nor branch
percentages are available from that probe. No dependencies were installed or changed.
Evidence: `logs/20260910-004048-many-tables-launch.log`; the resulting exec file is
not a valid coverage report for these classes.

### Java 21 coverage after the dependency upgrade

On explicit user request, `build.xml` now selects JaCoCo 0.8.11. This is the first
release with official Java 21 support according to the
[release notes](https://github.com/jacoco/jacoco/releases/tag/v0.8.11).
The shared version property supplies both the agent and Ant reporting dependency.
The installed agent manifest confirms 0.8.11. The earlier 0.8.8 limitation above
is historical; the new run instruments the Java 21 classes successfully.

All 74 focused/property tests passed with the new agent attached. No instrumentation
errors or class-data mismatch warnings appeared. Ant generated HTML, XML, and CSV
reports in `logs/20260910-jacoco-idle-admission/`.

| Measured scope | Lines | Branches |
|---|---:|---:|
| AdmissionBudget | 19/19 (100%) | 12/12 (100%) |
| validateIdleMemtableOptions | 7/7 (100%) | 10/10 (100%) |
| Two new configuration getters | 2/2 (100%) | No branches |
| IdleMemtableFlusher outer class, including existing lifecycle code | 84/92 (91.3%) | 50/58 (86.2%) |
| Existing scheduler Holder | 11/12 (91.7%) | 1/2 (50%) |

The uncovered scheduler paths include shutdown races, rejected executor submission,
the synchronous runtime-exception handler, invalid scheduler constructor arguments,
and a scan-loop boundary. The budget arithmetic itself has complete measured line
and branch coverage; that does not prove every concurrent interleaving or workload.
Instrumentation was restricted to IdleMemtableFlusher and its nested classes plus
DatabaseDescriptor. Other project classes appear as uncovered in the whole-project
report and should not be included in a percentage for this test scope.

Evidence:

- Build and Checkstyle: `logs/20260910-010118-rebase-command.log`, exit 0.
- Instrumented focused suite: `logs/20260910-010215-ai-test-memtable-lazy/`, 73 tests, exit 0.
- Instrumented property suite: `logs/20260910-010502-ai-test-memtable-lazy/`, one test / 100,000 steps, exit 0.
- Ant report task: `logs/20260910-010523-rebase-command.log`, exit 0.
- Report: `logs/20260910-jacoco-idle-admission/report.xml` and `index.html`.

The test wrappers used ANT_OPTS with `-Dno-build-accord=true` and
`-Dadditionalagent=-javaagent:<checkout>/build/lib/jars/jacocoagent.jar=destfile=<checkout>/logs/20260910-jacoco-idle-admission/coverage.exec,includes=org.apache.cassandra.db.memtable.IdleMemtableFlusher*:org.apache.cassandra.config.DatabaseDescriptor`.
The existing Ant `jacoco-report` task ran through `.build/sh/_run-ant.sh`, with
`jacoco.export.dir` pointing at that report directory and `jacoco.finalexecfile`
pointing at its `merged.exec`. Instrumented timings are not performance measurements.

## Commands and artifacts

All commands used the existing dev distrobox and repository wrappers. Test wrappers
ran sequentially; no full test suite was run.

```sh
distrobox enter dev -- env 'ANT_OPTS=-Dno-build-accord=true -Dant.gen-doc.skip=true -Drat.skip=true' bash tmp/run-logged.sh .build/check-code.sh --summary
distrobox enter dev -- env ANT_OPTS=-Dno-build-accord=true ./run_tests.sh --idle-admission
distrobox enter dev -- env ANT_OPTS=-Dno-build-accord=true ./run_property_tests.sh --idle-admission
```

- Build and production/test Checkstyle: exit 0, `logs/20260910-004004-rebase-command.log`.
  RAT and documentation generation were skipped through existing properties.
- Final focused suite: exit 0, `logs/20260910-004027-ai-test-memtable-lazy/`.
- Property suite: exit 0, `logs/20260910-004305-ai-test-memtable-lazy/`.
- Shell syntax and git diff whitespace checks passed.
- During development, new tests used a JUnit assertion absent from this repository's
  JUnit 4.12; switched to its existing AssertJ assertion API and fixed import order.
- An earlier wrapper run returned 127 after its script was edited while executing.
  All its test classes passed, but that wrapper run was not accepted as final
  validation. The final unchanged-script run exited 0.

## Next steps

The next heap task remains trimming unused SSTable tombstone histogram storage.
These admission budgets control the rate of optional work; they do not solve
resident reader metadata overhead. Larger mixed-load tests can later select
operational budget values and compare fixed T4 against L4,T4. An external tuner
can remain separate, using read participation, live file count, latency, and
compaction bytes across replicas.
