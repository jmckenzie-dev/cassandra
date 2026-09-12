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

# Memtable residency validation

State: build and focused tests passed; measurements pending.

The first validation attempt stopped when an existing Unified Compaction Strategy
expiration test failed on both a sibling branch and the frozen base. The corrected
shared base fixed that test timing defect. The coordinator merged the fix here as
3ff4202ddc. The final JDK 21 build and main/test
Checkstyle run passed. Log: logs/20260911-222728-ai-build.log. The earlier build
also passed before the portable fixture adapter. Log:
logs/20260911-221507-ai-build.log.

Static checks confirmed the recorded source blobs before validation. A final
source-independence and parity audit remains pending.

## Focused tests

| Test class | Passed | Skipped | Failures/errors | Log |
| --- | ---: | ---: | ---: | --- |
| TrieMemtableLazyTest | 7 | 0 | 0 | 20260911-221842 |
| TrieMemtableLazyPropertyTest | 1 | 0 | 0 | 20260911-221911 |
| TrieMemtableRetirementTest | 5 | 0 | 0 | 20260911-221956 |
| TrieMemtableRetirementFailureTest | 1 | 0 | 0 | 20260911-222008 |
| IdleMemtableFlusherTest | 13 | 0 | 0 | 20260911-222018 |
| IdleFlushAdmissionBudgetTest | 5 | 0 | 0 | 20260911-222049 |
| IdleFlushAdmissionBudgetPropertyTest | 1 | 0 | 0 | 20260911-222057 |
| IdleMemtableConfigTest | 2 | 0 | 0 | 20260911-222104 |
| SplittablePartitionerTrieMemtableFlushSetTest | 4 | 0 | 0 | 20260911-222122 |
| NonSplittablePartitionerTrieMemtableFlushSetTest | 4 | 0 | 0 | 20260911-222134 |
| MemtableSizeHeapBuffersTest | 3 | 0 | 0 | 20260911-222146 |
| SettingsTableTest | 13 | 0 | 0 | 20260911-222206 |
| TrieMemtableLazyRecoveryTest | 2 | 0 | 0 | 20260911-222833 |
| IdleMemtableFlushTest | 5 | 0 | 0 | 20260911-222921 |

All logs use the `-ai-ci-test.log` suffix under logs/. The final total is 66
passed, zero skipped, and zero failures or errors. XML reports are preserved by
class under logs/validation/.

The generated lazy trace covers four seeds with 64 operations each. The idle
lifecycle trace covers eight seeds with 40 operations each. Admission property
coverage uses 100 seeds with 1,000 steps each. These generated cases execute
inside three reported JUnit test cases.

The first recovery attempt used source subnet 144. Both test cases errored before
logic ran because macOS could not bind 127.0.144.1. Log:
logs/20260911-222226-ai-ci-test.log. Its XML is preserved under attempt-1. Both
single-node fixtures now use subnet 0 and run serially. The retry passed.

Small correctness measurements and the final parity audit remain pending.
