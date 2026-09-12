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

# Memtable residency extraction

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Base: c6631dcef555d278cbb0f6734a887cc94ee90e1b.

Create two reviewable commits. The first commit contains lazy Trie memtable
state publication and explicit retirement coverage. The second commit contains
idle flush policy, bounded admission, configuration, operator behavior, and the
standalone subset of the mixed distributed fixture.

Preserve source defaults. Keep metric features and the Unified Compaction
Strategy hierarchy option out of this branch. Validate through the project
wrappers only. Preserve each XML report before the next test wrapper runs.

## Lazy source manifest

| Destination | Source revision | Source blob | Treatment |
| --- | --- | --- | --- |
| src/java/org/apache/cassandra/db/memtable/TrieMemtable.java | f5f830e5d8 | 5fb45416e31d | Exact lazy-era blob; idle hunks follow separately |
| src/java/org/apache/cassandra/db/memtable/Memtable_API.md | f5f830e5d8 | 7cf67b9f86f1 | Exact lazy-era blob; idle text follows separately |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableLazyTest.java | 319995ce5c | 7555c27bbd32 | Final source blob |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableLazyPropertyTest.java | 319995ce5c | 25b859804664 | Final source blob |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableRetirementTest.java | 319995ce5c | 9039971940b8 | Final source blob |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableRetirementFailureTest.java | 319995ce5c | 1f3bc33101bd | Final source blob |
| test/distributed/org/apache/cassandra/distributed/test/TrieMemtableLazyRecoveryTest.java | 319995ce5c | d04dd3877202 | Final source blob |

The final test blobs retain the source rebase adaptations from 1096f47ed4. The
production blob still defaults `lazy_initialization` to true. Retirement tests
call the existing `forceFlush` API and add no retirement API.

## Idle source manifest

| Destination | Source revision | Source blob | Treatment |
| --- | --- | --- | --- |
| src/java/org/apache/cassandra/db/memtable/TrieMemtable.java | 319995ce5c | 55534c45a2aa | Exact final source blob |
| src/java/org/apache/cassandra/db/memtable/Memtable_API.md | 319995ce5c | 479425586053 | Exact final source blob |
| src/java/org/apache/cassandra/db/ColumnFamilyStore.java | 319995ce5c | bee07f11b6ff | Exact final source blob |
| src/java/org/apache/cassandra/db/memtable/IdleMemtableFlusher.java | 319995ce5c | d35b0a9f9e97 | Exact final source blob |
| test/unit/org/apache/cassandra/config/IdleMemtableConfigTest.java | 319995ce5c | 0059ee8224f8 | Exact final source blob |
| test/unit/org/apache/cassandra/db/memtable/IdleFlushAdmissionBudgetPropertyTest.java | 319995ce5c | 7950fcc9f974 | Exact final source blob |
| test/unit/org/apache/cassandra/db/memtable/IdleFlushAdmissionBudgetTest.java | 319995ce5c | 66c9cf3a8781 | Exact final source blob |
| test/unit/org/apache/cassandra/db/memtable/IdleMemtableFlusherTest.java | 319995ce5c | 67991470fbfc | Exact final source blob |

Config.java contains only the four source `memtable_idle_*` fields.
DatabaseDescriptor.java contains only idle validation and getters. The YAML file
keeps source defaults: timeout 0s, concurrency 2, admissions 100/s, and estimated
throughput 16MiB/s. README.asc keeps idle policy and operator behavior. It omits
cross-branch benchmark links, metric settings, and UCS hierarchy documentation.

## Distributed test adaptation

Source `IdleMemtableFlushTest` blob cad371f51a80 becomes b77ab9e6f3f3. The
standalone file keeps five test methods. It removes these integration methods:

- `smallHierarchyRetiresReactivatesAndRecovers`: move the 1KiB hierarchy case.
  Integration must preserve idle retirement, ineligible-table state, dormant
  reads, write reactivation, restart recovery, hierarchy persistence, and the
  post-restart write and second retirement assertions.
- `operatorCanAlterHierarchyAndScalingBeforeRestart`: move all three alter,
  write, idle, and read cycles. Integration must preserve restart persistence
  assertions for `1MiB` and `L4,T4`, plus all three recovered rows.

The standalone helper removes its optional hierarchy argument and the hierarchy
persistence assertion. It preserves every default-hierarchy lifecycle assertion.
The standalone test fixtures also use subnet 0 instead of source subnets 144 and
146. macOS exposes 127.0.0.1 but did not accept those high loopback aliases. Both
fixtures create one node and tests run serially, so this changes only address
provisioning. Production code has no extraction adapter.

CommitLogTest uses SkipListMemtable explicitly. It does not exercise the changed
Trie paths. Dedicated retirement-failure and distributed recovery tests cover the
relevant commit-log behavior, so this branch does not add CommitLogTest to its
targeted list.
