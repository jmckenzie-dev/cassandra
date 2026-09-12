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

## Test Author Result

- Status: COMPLETE
- Domain: unit
- Applicability: APPLICABLE
- Plan file: .agentlogs/plans/fix-ucs-expiration-test.md
- Requested scope: fix the reproduced expiration-selection test failure.
- Resolved comparison basis: c6631dcef555d278cbb0f6734a887cc94ee90e1b;
  preserve existing remaining-worktree changes.
- Production files reviewed: src/java/org/apache/cassandra/db/compaction/
  UnifiedCompactionStrategy.java, CompactionController.java, unified/Controller.java.
- Test files changed: test/unit/org/apache/cassandra/db/compaction/
  UnifiedCompactionStrategyTest.java in wt_moar_base and wt_moar_remaining.
- Production code changed: no

## Tests Added or Updated

The existing testDropExpiredSSTables test previously failed in both worktrees.
Its two helper fixtures inject a mock Controller. The mock returns zero for
getExpiredSSTableCheckFrequency unless configured. The real strategy scans only
when elapsed wall-clock milliseconds exceed that interval. Consecutive calls in
one millisecond can skip the expiration scan and return null.

Set the interval to -1 only on the injected mock in testDropExpiredFromBucket and
testDropExpiredAndCompactNonExpired. This permits scans at equal timestamps.
No configuration default or production scheduling behavior changes. No sleep,
global-clock mutation, private-state change, or weaker assertion was introduced.
All empty-selection, expired-only, mixed-file selection, size, and level checks
remain. The source's additional hierarchy test remains in the remaining branch.

The test-unit skill kept the correction within unit-test fixtures and required
real compaction-selection assertions and repeated execution evidence.

## Commands and Outcomes

Environment for every command:

```sh
env JAVA_HOME=/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home PATH=/opt/homebrew/opt/openjdk@21/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin .build/sh/ai-build
env JAVA_HOME=/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home PATH=/opt/homebrew/opt/openjdk@21/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin .build/sh/ai-ci-test org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTest
```

Both worktrees passed the build and main/test Checkstyle (exit 0).
The wrappers skip generated documentation and license auditing.
Both worktrees passed git diff --check.

| Worktree | Test runs | Cases per run | Failures/errors/skips |
| --- | ---: | ---: | ---: |
| wt_moar_base | 3 | 27 | 0 |
| wt_moar_remaining | 3 | 28 | 0 |

All six test commands exited 0. These are 165 executions, not 165 unique tests.
Preserved reports: logs/ucs-expiration-fix/base-run-{1,2,3}.xml in wt_moar_base;
logs/ucs-expiration-fix/remaining-run-{1,2,3}.xml in wt_moar_remaining.

Base logs:
- logs/20260911-213122-ai-build.log
- logs/20260911-213253-ai-ci-test.log
- logs/20260911-213355-ai-ci-test.log
- logs/20260911-213434-ai-ci-test.log

Remaining logs:
- logs/20260911-213256-ai-build.log
- logs/20260911-213530-ai-ci-test.log
- logs/20260911-213621-ai-ci-test.log
- logs/20260911-213700-ai-ci-test.log

The earlier failures remain in the blocker record and its original logs.
The correction and its validation records are committed separately from features:
base: f7d5a7e2c0fcf769293ee27073630a3574584b06.
remaining: 1cfcbfab95b74c4d1ee1d25e2f8821a0df95426b.
The commits have identical trees. Remaining feature changes stay uncommitted.
The original source worktree and the other four feature worktrees were not edited.

## Findings for the Implementer

None. This was a test-fixture timing defect, not an expired-file selection defect.

## Unresolved Test Gaps

This selection test does not test the scheduler's interval boundaries.
Production scheduling was not changed. Other branch-split tests remain pending;
this result does not claim full branch verification.

## Blockers

None for this test correction. Propagate it to the other four feature branches
before resuming their validation. No build or test was rerun during committing;
the staged code matches the verified correction.

## Historical checkpoint

This record preserves the earlier task state. See
.agentlogs/reviews/many-tables-handoff.md for final branch and validation status.
