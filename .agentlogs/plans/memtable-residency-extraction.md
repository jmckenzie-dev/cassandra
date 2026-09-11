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
