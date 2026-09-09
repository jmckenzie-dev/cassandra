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

# Rebase onto origin/trunk, September 9, 2026

## Scope and history

Replayed the seven `moar_tables` commits onto `origin/trunk` at `88fd0f6a0e`.
The replay completed at `feef1e8f8b`. The pre-rebase tip `b6cb723cd1` remains at
`backup/moar_tables-before-origin-trunk-rebase-20260909`.

Only the build README and two build entrypoints required manual conflict
resolution. Upstream replaced `ai-build`, `ai-ci-test`, and the old `ci-test`
implementation with shared build/test wrappers. The branch still uses the
`ai-*` entrypoints in its runners and documentation.

The resolution keeps thin compatibility entrypoints that delegate to upstream:

- `ai-build` runs `build-jars.sh --clean` and `check-code.sh`, retaining the
  branch's timestamped full logs and concise console summaries.
- `ai-ci-test` selects the unit or distributed target, converts the fully
  qualified class name to the exact source-path expression expected by
  `run-tests.sh`, and accepts the existing `--reuse` argument.
- The memtable runner collects JUnit XML from the new per-target output paths.
- The README retains both upstream build instructions and branch profiling
  instructions, and explains the compatibility entrypoints.

A range comparison against the backup showed that the first commit and the
last five replayed without patch changes. The second contained the conflict
resolutions above. Follow-up validation fixes are recorded separately.

## Failures found during validation

The initial 52-job matrix completed with four failed classes:

| Class | Evidence | Correction |
| --- | --- | --- |
| `MemtableResidencyHarnessTest` | An existing Java process owned `127.0.0.1:7012`. Later scenarios also reported an RMI `ObjID already in use` after failed startup. | Give this fixture subnet 143 through the existing harness option and assert the recorded subnet. |
| `TrieMemtableLazyRecoveryTest` | The same occupied storage address prevented both tests from starting. | Use the cluster builder's existing subnet option with subnet 144. |
| `MetricProfileTest` | Standard Ant execution could not find `all_metrics.yml` on the classpath. The profiling launcher had supplied `conf/`. | Test the shipped files by absolute path and test classpath loading independently with a small test resource. |
| `CompactMetricsIntegrationTest` | Its existing bootstrap test and cleanup called `setConfig(null)`, which dereferenced `config.metrics_config_file`. | Restore null-reset support in the test configuration setter and verify that reset also removes the previous metric allowlist. |

The null-reset failure came from two branch changes interacting: compact-metric
bootstrap testing and later profile loading. It does not require an upstream
runtime change to reproduce. The existing failing test remains in place.

Trunk also added `TrieMemtable.limitsConcurrentWritesTo`. The lazy state retains
its shard boundaries independently of the allocated trie. The dormant-state
test now checks both true and false answers from this new API and verifies that
these queries leave the memtable uninitialized.

## Build and test evidence

Validation uses JDK 21 in the existing `dev` container and cached dependencies.
The initial clean JAR build and main/test Checkstyle passed:
`logs/20260909-115510-ai-build.log`.
After the Java and fixture fixes, an incremental build and main/test Checkstyle
passed through `check-code.sh`: `logs/20260909-120403-rebase-command.log`.

The initial matrix inventories every added or modified `*Test.java` under
`test/` relative to trunk: 36 classes. Each runs in its own Ant test invocation;
the runner requires a nonempty JUnit XML report, checks failures/errors, and
copies the report into the run's artifact directory. Another 16 jobs exercise
nested property classes and the branch's Python, Java Management Extensions
(JMX), OpenTelemetry storage, and histogram-width validation tools.

Initial results and exact commands:
`logs/20260909-115737-rebase-suite/{manifest,results}.json`.

The follow-up matrix reruns the four failures and the changed dormant-state
test. It also runs nine existing flush, heap-accounting, histogram, meter,
registry, and virtual-table regression classes selected by the branch runners.
A final profile integration run enables lazy metric IDs, compact table
bookkeeping, and transient JMX together.

Follow-up results and exact commands:
`logs/20260909-120444-rebase-suite/{manifest,results}.json`.

The follow-up completed with every job passing. Taking the final result for
each class, the 45-class matrix reports 224 cases: 223 passed, zero failures,
zero errors, and one existing upstream `@Ignore`
(`DecayingEstimatedHistogramReservoirTest.NonParameterizedTests.showHistogramOffsetOverflow`).
All 36 branch classes pass, with 153 cases and no skips.

The supporting checks also pass:

- Nested no-op, JMX-registration, and bookkeeping property classes.
- All 16 Python reference-generator and history-array accounting tests.
- Local/remote JMX boundaries, protected access, late builder installation,
  name retention, 16,000 generated name cases, and 16,000 query cases.
- OpenTelemetry storage boundaries and 32,000 generated operations.
- Histogram width lifecycle checks and 32,000 generated operations.
- The combined optimized-profile integration run: 41 passing cases. This
  repeats relevant profile cases with lazy metric IDs, compact bookkeeping,
  and transient JMX enabled together; these are not extra unique test cases.

Finally, `run_property_tests.sh --lazy` passed through the updated memtable
runner and retained the expected nonempty JUnit XML at
`logs/20260909-121402-ai-test-memtable-lazy/TEST-org.apache.cassandra.db.memtable.TrieMemtableLazyPropertyTest.xml`.
This validates the report-collection path as well as the individual-class
entrypoint. Shell syntax checks and `git diff --check` also passed.

The metric reference freshness check passes: the generated reference and
runtime catalog still describe 126 table and 101 keyspace metrics.

## Submodule and validation limits

The Accord checkout was already at trunk's required `047da324` revision and
remained clean. The clean build used an archive exported to `tmp/rebase-accord`
via `-Daccord.dir=tmp/rebase-accord`; it did not build into or change the actual
submodule checkout. Subsequent tests reused those compiled classes.

This is branch regression validation, not the full Cassandra test suite or a
repeat of the 5000-table performance comparison. No new throughput or retained
heap claim follows from these tests.
