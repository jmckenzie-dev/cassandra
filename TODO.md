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

# TODO

- [ ] Restore combined fixtures after merging all five feature branches.
- [ ] Account for all source paths and verify production reconstruction.
- [ ] Build and run targeted combined tests and property entrypoints.
- [ ] Run matched measurements with verified effective controls.

# DONE

- [x] Record never-written, retirement, idle, reactivation, and admission assertions.
- [x] Defer quantitative memory, compaction, and drain measurements to integration.
- [x] Record validation, review, and source-independence evidence.
- [x] Build and run all planned targeted tests with JDK 21.
- [x] Extract final idle flushing and bounded admission behavior.
- [x] Split the mixed distributed idle-flush fixture for standalone use.
- [x] Extract lazy Trie memtable initialization and retirement coverage.
- [x] Extract metric recording and storage changes from source commit 319995ce5c.
- [x] Adapt combined test and tool assumptions to the standalone STORE branch.
- [x] Build and run focused production, property, compatibility, and probe tests.
- [x] Review the branch, record validation evidence, and create local commits.
- [x] Complete the standalone build/Checkstyle and all nine planned test classes: 82 cases, no errors/failures/skips. Preserve generated traces and allocation checks.
- [x] Record the two source/test adaptations for combined verification and prepare separate UCS and lazy tombstone commits.
- [x] Fix the expiration-test clock-tick dependency without production changes. Build/Checkstyle and three 28-case runs passed. Commit the isolated correction as 1cfcbfab95; see the coordinator's .agentlogs/reviews/ucs-expiration-test-fix.md.
- [x] Pass build/Checkstyle, 26 ControllerTest cases, and 2 ControllerHierarchyTest cases. Reproduce the compaction expiration failure on the frozen shared base.
- [x] Extract UCS and lazy tombstone source/tests; record combined-test adapters.
- [x] Extract generic tools, JaCoCo upgrade, and shared metric contracts.
- [x] Build with JDK 21 and pass main/test Checkstyle.
- [x] Pass both ThreadLocalHistogramTest cases.
- [x] Complete all five targeted test classes: 48 passed and one existing skip.
- [x] Complete the three-table generic harness smoke test.
- [x] Complete independent review with no findings and freeze the shared base.
- [x] Run isolated history, registration, and retained-name measurements.
- [x] Complete independent extraction review with no missing production hunks.
- [x] Extract export code onto the corrected shared base.
- [x] Keep all switches opt-in and exclude storage and profile dependencies.
- [x] Adapt mixed tests to legacy storage and record integration assertions.
- [x] Build and pass main/test Checkstyle.
- [x] Pass 28 unique test cases, including the nested registration properties.
- [x] Run name and server boundary diagnostics.
- [x] Split production changes into three review units.
