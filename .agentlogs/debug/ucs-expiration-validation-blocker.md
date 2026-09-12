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

# Compaction validation blocker

## Resolution

Resolved by a test-only correction in wt_moar_base and wt_moar_remaining.
The mock interval is now -1 in both expiration-selection fixtures, which permits
scans in the same millisecond. Production code and assertions are unchanged.
Build/Checkstyle passed in both worktrees. Three full-class runs passed in each:
27 cases per base run and 28 per remaining run, with no failures/errors/skips.
The fix is committed as base f7d5a7e2c0 and remaining 1cfcbfab95.
See the coordinator's
.agentlogs/reviews/ucs-expiration-test-fix.md for commands and evidence.
The original failure record below is retained for comparison.

## Result

Stop requested by the coordinator after a domain smoke test failed on both the
remaining-work branch and the frozen shared base. No fix, skip, or assertion
change was made. This blocks a claim that all split branches are verified.

## Reproduction

JDK 21.0.12.1. Run .build/sh/ai-ci-test
org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTest with the same
explicit JAVA_HOME/PATH used for the shared-base gate.

Remaining worktree: build and main/test Checkstyle passed.
ControllerTest: 26 passed. ControllerHierarchyTest: 2 passed.
UnifiedCompactionStrategyTest: 28 cases, 27 passed, 1 failure, no skips/errors.
The failure is testDropExpiredSSTables -> testDropExpiredFromBucket, line 613:
assertNotNull(pick). The compaction selection returned null after expiration.

Shared base c6631dcef555d278cbb0f6734a887cc94ee90e1b:
27 cases, 26 passed, same failure at line 582. Its compaction strategy and test
have no working-tree changes. The 31-line offset is the source's added hierarchy
test. This reproduces the failure without either EXTRA feature.

This evidence rules out the remaining extraction as a necessary cause. It does
not establish whether the cause is an upstream defect, clock/timing behavior,
or another environment condition. No production diagnosis or fix is claimed.

## Evidence

- wt_moar_remaining/logs/20260911-174517-ai-build.log
- wt_moar_remaining/logs/20260911-174750-run-tests.log
- wt_moar_remaining/logs/20260911-174750-run-tests-reports/
- wt_moar_base/logs/20260911-174906-ai-ci-test.log
- wt_moar_base/logs/base-validation/TEST-org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTest.xml

The targeted selector stopped at the failure. ShardedMultiWriterTest and all
five tombstone classes were not run. Their extraction remains uncommitted.

## Next decision

Diagnose this reproducible baseline failure separately, then decide whether to
fix the test/environment or carry an explicit baseline exception. Do not mark
the split verified or silently weaken the required test gate.
