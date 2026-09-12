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

# Split execution record

## Source protection

- Source commit: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
- Committed snapshot: refs/backup/many-tables-source-20260911.
- Tracked dirty snapshot: refs/backup/many-tables-working-20260911 at
  cfcb259d6403bfad12a10005ba7550dd6dd8c130.
- Untracked source plan copy:
  tmp/many-tables-split-backup-20260911/source-proposal.md.
- Original source worktree remains unchanged.

## Shared base

Created many-tables/base in wt_moar_base from
50a10d42a5d633150fbf4ae518b4da85466472b2.
Initial frozen base: c6631dcef555d278cbb0f6734a887cc94ee90e1b.
Current base with verified test correction: f7d5a7e2c0fcf769293ee27073630a3574584b06.
Tools: ef355235e0. JaCoCo: 12cd818fd4. Contracts: c6631dcef5.
The JDK 21 build and main/test Checkstyle passed. Targeted tests passed (48 passed,
one existing ignored diagnostic). Three-table smoke passed. Independent review
found no issues. The wrapper skips generated docs and license auditing.

## Scheduling

Host: 64 GiB memory, 16 logical processors.
Use three extraction workers at most. Serialize heavy build wrappers to protect
shared Accord build/cache output. Targeted tests set no-build-test=true in
run-tests.sh and can use their frozen outputs during another worktree's build.
Serialize Cassandra fixture tests unless ports are explicitly separated.
Run performance measurements without competing workloads.

## Blocked checkpoint

Stopped after UnifiedCompactionStrategyTest.testDropExpiredSSTables failed on
EXTRA and the frozen base. See ../debug/ucs-expiration-validation-blocker.md.
All feature branches derive from c6631dcef555d278cbb0f6734a887cc94ee90e1b.

| Branch | State |
| --- | --- |
| many-tables/memtable-residency | Extracted in a17a32333a and 2e2c8b0d3d; not built or tested |
| many-tables/metric-selection | Extraction preserved uncommitted; not built or tested |
| many-tables/metric-storage | Partial extraction preserved uncommitted; lazy IDs and related hunks pending |
| many-tables/metric-exports | Worktree/branch created; no feature extraction |
| many-tables/remaining | Extraction preserved uncommitted; build/Checkstyle passed; 55 cases passed, 1 failed |

Integration worktree, combined source reconstruction, and final branch reviews
have not started. Original source commit and dirty state remain unchanged.
No branch was deleted or pushed. No test was skipped to hide this failure.

## Test correction committed

Base: f7d5a7e2c0fcf769293ee27073630a3574584b06.
Remaining: 1cfcbfab95b74c4d1ee1d25e2f8821a0df95426b.
Both commits contain identical test-only changes and records. Their trees match.
The remaining hierarchy test and all other feature edits stay uncommitted.
The other four feature branches still need the correction. No code changed
during committing; the prior passing build and repeated-test evidence applies.

## Resume

The baseline failure is resolved by the committed test-only correction in
BASE and EXTRA. Three full-class runs in each passed after build/Checkstyle.
Propagate that correction; see ../reviews/ucs-expiration-test-fix.md.
Use each worktree's checkpoint and source/adaptation manifest. Complete STORE
and EXPORT extraction, validate each branch, split remaining commit units,
review, and reconstruct the combined source. Six EXTRA test classes remain
unrun after its fail-fast selector stopped. Do not repeat completed base checks
unless the base code or build environment changes.

## Resumed branch validation

The corrected base is now an ancestor of all five branches. SELECT, STORE,
and EXPORT fast-forwarded without losing extraction edits. MEM merged it as
3ff4202ddc. EXTRA merged its equivalent fix ancestry as a98cab98b7.
No history was rewritten.

- MEM: build/Checkstyle passed; 66 cases across 14 classes passed. Two
  single-node fixtures use subnet 0 on macOS. The first bind failure remains
  in the report history. Quantitative compaction/drain measurements remain
  assigned to the integration harness with legacy controls.
- SELECT: build/Checkstyle passed; 40 unique Java test methods passed, including
  nested no-op and bookkeeping properties. Generator checks passed 11 unit
  tests and one generated-input test; catalog and packaging checks passed.
- STORE: build/Checkstyle and all 16 classes passed, with one pre-existing
  skipped diagnostic. Isolated reservoir probes/JMH and three-fork worker
  measurements completed successfully. See the branch report for exact counts
  and the memory/update-cost tradeoffs.
- EXPORT: committed as cac63b319e (history), 60bb106292 (registration), and
  b8efde8316 (transient names). Build/Checkstyle passed. Seven top-level
  classes and nested registration properties passed 28 unique cases.
  Local/remote, notification, operation, late-install, and generated-query
  boundary programs passed.
- EXTRA: committed as 47d36ac487 (UCS hierarchy) and 92689444b6 (lazy tombstones).
  All nine classes passed 82 cases. Independent verification review approved
  the branch with no findings. No numeric line/branch coverage is claimed.

Independent extraction checks found no missing production hunks in any feature
branch. MEM reviewed STORE and EXPORT. The coordinator reviewed MEM and SELECT.
SELECT reviewed EXTRA against its plan, source hashes, and preserved XML.
Branch measurements and final record commits remain in progress.

## Reconstruction decisions

Created many-tables/integration at the corrected base in wt_moar_integration.
Merged MEM first by fast-forward. The remaining order is SELECT, STORE,
EXPORT, EXTRA. No integration build has run yet.

Shared Config.java and README.asc final-newline normalization appeared during
feature extraction. It is harmless formatting and matches the final source.
The coordinator accepts it as the reconstruction normalization assigned in
08-shared-files.md.

Branch-local root selectors are intentional standalone subsets. Integration
will restore the combined source selectors, retain portable interpreter/build
entrypoints, and explicitly run nested property suites. It will not choose a
feature branch's whole selector during an add/add merge conflict.

## Combined verification resumed, September 12

All five final feature heads are merged. Integration commit eeb3e6957e restores
combined fixtures, imports 28 combined tools/harness paths, and saves the full
split plan package in the integration branch. Its production tree matches
319995ce5c exactly, including configuration, catalog, packaging, and operator
documentation. The documentation/tool/test differences have named adaptations.

The independent audit accounts for all 246 paths. All 89 archive paths remain
in the original checkout; its existing TODO entry remains untouched. Two
reservoir analyzers had lost executable modes. Storage commit 3e8478f751 fixes
those modes, and integration merge 52aa4cc814 includes the correction.

Java 21 build and both Checkstyle passes succeeded. Five new live-node matrix
cases passed. The full targeted combined run is still active. The generator's
11 unit tests, one property test, current-reference check, and heap analyzer's
four tests passed. No matched measurement has started yet.

No branch was deleted, rebased, pushed, or overwritten. The original source
checkout retains its initial planning-only changes.

## Correctness gate passed

Integration dea76d81d0 records the passing combined gate. The 57 top-level
classes contain 333 cases: 332 passed and the existing histogram diagnostic
was skipped. Five nested property cases also passed, for 337 unique passes.
Ten profile cases passed again with the transient MBean server builder.
All boundary and generator/analyzer checks passed. Independent coverage review
approved extraction/correctness. Its minor raw-command provenance note is
addressed with the exact invocation in integration-validation.md; the original
raw log remains unchanged.

All eight three-table measurement smoke cases passed with audited effective
controls. Both idle-enabled cases retired all three memtables and returned
allocator-owned bytes to zero. Both 100-table profile cases passed 1,600 writes
and 500 queries. The 1,000-table all-profile case passed 16,000 writes and
5,000 queries. Its matched simple-profile case is still running. No other
build or test runs alongside measurements.

## Final measurement and handoff checkpoint

All 18 matched cells passed: eight smoke, four profile, four idle-policy,
and two management-history census cases. Each used the frozen combined
binaries. Six selected live heaps passed the existing ownership analyzer.
Independent audits approved the completed measurement controls and raw data.

The first 1,000-table idle run exceeded a 121-second harness deadline during
its fourth drain. It continued flushing while compaction ran. The unchanged
case passed with --settle-seconds 300; its final drain took 148.104 seconds.
No production fix was needed. Preserve the failed attempt and the successful
retry. The BTI format forced normal compaction fallback despite the cursor
setting; no cursor-compaction performance claim follows from these runs.

At 1,000 tables, the profile pair settled at 547.09 MiB for all metrics and
202.06 MiB for simple metrics. The idle pair settled at 1,311.27 MiB with
idle flushing off and 333.04 MiB with it on. Enabled flushing returned
initialized table memtables and allocator-accounted heap bytes to zero.
It also performed 4,000 flushes and 1,000 compactions. These observations
do not establish stable effect sizes or production capacity.

The history census read 41,875 attributes per scrape without failures.
After eight workers and rescrape, user-table history arrays occupied 892,000
shallow bytes with standard history and 32,800 with adaptive history.
This bounded array result is not a full dominator or retained-size analysis.

The handoff and four raw-linked measurement records live in integration's
.agentlogs/reviews. Each split branch has clean tracked files and its own
validation record. Local tmp directories and the original planning changes
remain untouched. No branch was deleted, rebased, pushed, or overwritten.

Lessons: derive performance claims from effective settings and concrete object
state; a configured flag does not prove a runtime path executed. Keep harness
deadlines separate from product behavior. Preserve aborted runs when a larger
workload needs a longer observation window. Do not add isolated benchmark
savings to matched whole-process heap differences.
