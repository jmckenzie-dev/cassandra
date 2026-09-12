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

# Memtable residency source review

Review status: source extraction complete; runtime validation blocked.

The lazy extraction keeps the volatile `TrieState` publication boundary. Reads
use the empty state without allocation. The first write initializes once before
it applies the mutation. Flush and discard paths accept an uninitialized table.
The factory validates the boolean option and defaults it to true.

Final idle review and validation evidence will follow after extraction.

Idle tracking applies only to lazy, user, non-index Trie memtables under Unified
Compaction Strategy (UCS). The first write registers a candidate. Each shard
records its last local write. Reads do not change that time. ColumnFamilyStore
rechecks identity, eligibility, and idle time under the memtable replacement
lock before it flushes with the new idle reason.

The flusher bounds concurrent work until reader reclamation. Separate operation
and byte budgets cap admission. A large flush can create byte debt. Failed flush
futures close the scheduler until restart. Shutdown and invalidation stop idle
tracking. Local compaction changes switch an initialized memtable when its UCS
eligibility changes.

Exact final source blobs cover the fully owned production files and unit tests.
Shared-file review found only the assigned MEM settings and methods. The mixed
distributed fixture has the documented hierarchy-only removals.

The corrected shared base resolved the unrelated UCS test timing failure. Build,
Checkstyle, and all 14 planned test classes now pass. The test-only loopback
adapter and pending measurements are recorded in the validation report.
