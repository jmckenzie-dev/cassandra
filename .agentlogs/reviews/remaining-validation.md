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

# Remaining-work validation

Build and main/test Checkstyle passed on JDK 21.0.12.1.
Log: logs/20260911-174517-ai-build.log.

The initial expiration-selection failure also reproduced on the shared base.
The separate shared test-fixture correction resolved it without production edits.
The corrected class passed three full runs; original failure logs are retained.

All nine planned classes now pass, with no errors, failures, or skips:

| Class | Cases |
| --- | ---: |
| ControllerTest | 26 |
| ControllerHierarchyTest | 2 |
| UnifiedCompactionStrategyTest | 28 |
| ShardedMultiWriterTest | 3 |
| StreamingTombstoneHistogramBuilderTest | 11 |
| LazyTombstoneHistogramBuilderTest | 7 |
| LazyTombstoneHistogramBuilderPropertyTest | 1 |
| MetadataCollectorHistogramTest | 3 |
| TombstoneHistogramAllocationTest | 1 |

Total: 82 cases across nine classes. The generated test includes 200 traces of
500 operations each (100,000 operations), plus initial/final snapshot checks.
The allocation test ran all three samples: empty lazy allocation below 1% of
eager allocation; populated allocation within 1% of eager. This is thread-local
allocation evidence, not a timing benchmark or whole-node retained-heap result.

Sharded-writer command: .build/sh/ai-ci-test
org.apache.cassandra.db.compaction.unified.ShardedMultiWriterTest, exit 0.
Log: logs/20260911-221539-ai-ci-test.log; preserved XML in logs/remaining-validation/.
Tombstone command: ./run_tests.sh --tombstone, exit 0.
Log: logs/20260911-221630-run-tests.log; XMLs in
logs/20260911-221630-run-tests-reports/.
Controller/hierarchy XMLs: logs/20260911-174750-run-tests-reports/.
Corrected strategy XMLs: logs/ucs-expiration-fix/remaining-run-{1,2,3}.xml.
JDK 21 environment matches the shared-base validation records.

Independent review and combined-branch tests remain pending.
Exact extracted production files match final source; shared property allocation
and the two test adapters are recorded in the worklog. bash -n passed for both
branch-local test selectors. git diff --check passed.
