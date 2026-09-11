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

- [ ] Extract final idle flushing and bounded admission behavior.
- [ ] Split the mixed distributed idle-flush fixture for standalone use.
- [ ] Build and run all planned targeted tests with JDK 21.
- [ ] Run small never-written and idle/reactivation measurements.
- [ ] Record validation, review, and source-independence evidence.

# DONE

- [x] Extract lazy Trie memtable initialization and retirement coverage.
- [x] Extract generic tools, JaCoCo upgrade, and shared metric contracts.
- [x] Build with JDK 21 and pass main/test Checkstyle.
- [x] Pass both ThreadLocalHistogramTest cases.
- [x] Complete all five targeted test classes: 48 passed and one existing skip.
- [x] Complete the three-table generic harness smoke test.
- [x] Complete independent review with no findings and freeze the shared base.
