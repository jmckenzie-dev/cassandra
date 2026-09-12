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

# Memtable residency

Branch: many-tables/memtable-residency. Worktree: wt_moar_memtables.
Owner code: MEM. Parent: frozen many-tables/base.

You own only this branch and worktree. Other workers may edit other worktrees.
Do not revert their work or modify the original source worktree.

## Extract

- Final TrieMemtable lazy state publication, reads, initialization, retirement
  accounting, idle eligibility/tracking, shard timestamps, and configuration.
- IdleMemtableFlusher including final scan refactor, admission budgets, completion
  and reader-reclamation accounting, failure shutdown, and scheduler lifecycle.
- ColumnFamilyStore idle reason, locked recheck/flush, shutdown, invalidation,
  and local compaction-override handling.
- Memtable_API.md and the idle configuration/documentation hunks.
- Source defaults: lazy initialization true; idle timeout zero; concurrency 2;
  admissions 100/second; estimated throughput 16 MiB/second.
- Relevant lazy, retirement, recovery, idle, and admission tests.

Keep lazy initialization and bounded automatic idle flushing in separate
reviewable commits. Explicit retirement tests exercise the existing flush API.
Do not invent a new retirement API.

## Shared files and tests

Use 08-shared-files.md for Config, DatabaseDescriptor, YAML, and wrappers.
Do not import any metric setting, metric implementation, or UCS hierarchy option.

Split IdleMemtableFlushTest by behavior. Keep default hierarchy scheduler,
rate/byte admission, disabled defaults, index, and reactivation cases here.
Move smallHierarchyRetiresReactivatesAndRecovers and
operatorCanAlterHierarchyAndScalingBeforeRestart to integration coverage.
Remove optional hierarchy arguments from standalone fixtures if needed.
Record moved methods and preserved assertions in the result.

A feature-local subset of the residency harness may be extracted for measurement.
Retain the final combined harness in integration. Do not make compilation depend
on SELECT, STORE, EXPORT, or EXTRA.

## Verification

Build with .build/sh/ai-build. Run relevant classes through ai-ci-test:
TrieMemtableLazyTest, TrieMemtableLazyPropertyTest, TrieMemtableRetirementTest,
TrieMemtableRetirementFailureTest, IdleMemtableFlusherTest,
IdleFlushAdmissionBudgetTest, IdleFlushAdmissionBudgetPropertyTest,
IdleMemtableConfigTest, TrieMemtableLazyRecoveryTest, and IdleMemtableFlushTest.
The inventory gives package paths.

Include existing SplittablePartitionerTrieMemtableFlushSetTest,
NonSplittablePartitionerTrieMemtableFlushSetTest, MemtableSizeHeapBuffersTest,
and SettingsTableTest. Run wrappers serially in this worktree.

Verify first writes, held readers/writers, failed flushes, replay, truncate/drop,
index reads, CQL/JMX strategy changes, and ordinary flushes with idle policy off.
Retain generated tests; do not count only top-level test methods.

After correctness, run small never-written and idle/reactivation measurements.
Use legacy metrics and original UCS hierarchy. Record data correctness, initialized
memtables, allocator reclamation, file count, compaction bytes, and drain time.
Large benchmark runs require scheduling by the coordinator.

## Return

Commit identifiers if commits are authorized, source/hunk manifest, adaptations,
moved integration tests, test counts/failures/skips, logs, and unresolved issues.
Do not change defaults or repair unrelated defects during extraction.
