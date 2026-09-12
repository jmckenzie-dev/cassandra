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

- [ ] Complete independent review and resolve any findings.

# DONE

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
