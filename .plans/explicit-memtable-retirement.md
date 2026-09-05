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

# Explicit memtable retirement

Implement phase 3a: explicitly flush dirty tables through ColumnFamilyStore.forceFlush(USER_FORCED), leave the phase 2 lazy replacement dormant, and prove safe reclamation and reactivation. Existing production flushing already supplies this operation; add production code only if a demonstrated lifecycle defect requires it. No scheduler, idle timestamps, new flush policy, or full table eviction in this increment.

## Work

1. Add an opt-in --explicit-retirement harness control for idle-reactivate and rotating-bursts. Retire after the observation pause through existing forceFlush, record before/after checkpoints, wait for reclamation separately from flush completion, and verify reads preserve dormancy. Record the mode and completed retirement requests. Keep untouched tables lightweight.
2. Add focused unit coverage for repeated clean retirement, dirty retirement and allocator reclamation, outstanding reader and writer barriers, concurrent replacement writes, and coupled index flushing where practical through supported interfaces. Use actual flush futures, ordering groups, and allocator observations; no byte patching.
3. Extend generated CRUD/flush/truncate coverage with explicit forceFlush retirement and repeated retirement. Preserve independent exact-row model assertions.
4. Make root run_tests.sh default to a small set of lifecycle, flush-range, and memory-accounting unit classes. Preserve --lazy and --long; retain property tests through run_property_tests.sh. Use an ai-* wrapper for Ant, log stdout/stderr to timestamped logs, and preserve exit codes.
5. Build with Checkstyle; run isolated unit/property/harness tests. Compare lazy-only and explicit retirement at N=100 for idle-reactivate and rotating-bursts, with matched separate heap diagnostics for dirty versus retired storage. Every workload stays at or below 1,000 tables.
6. Record results and remaining risks in research/explicit_memtable_retirement.md and the durable continuation record. Move this task to DONE only after validation.

## Acceptance

Dirty retired data remains readable from SSTables. Pure reads do not activate replacement trie storage. A subsequent mutation activates it and preserves prior rows. Empty retirement does not switch or create SSTables. Flush completion must not prematurely discard memory pinned by a reader. Accepted writes reach their original logical memtable, while later writes can reach the replacement. After readers finish, old allocator accounting reaches zero and matching heap diagnostics show slab removal. A flush failure must preserve existing durability behavior; do not add new exception handling that hides it.

## Ownership and comparison basis

All existing uncommitted phase 1/2 changes are the starting baseline. Root owns runner scripts, integration, diagnostics, and reports. Unit-test agent owns a new TrieMemtableRetirementTest; harness agent owns the residency harness/configuration/scenario tests and its guide; lifecycle agent owns the generated property test and independent lifecycle/failure review. Agents preserve other edits. Production scope is existing ColumnFamilyStore flushing and TrieMemtable lazy replacement, relative to the current uncommitted phase 2 implementation.

## Completed

The existing forceFlush path supplies explicit retirement with the phase 2 lazy replacement. Added the opt-in harness mode, per-cycle state checkpoints, six focused retirement/failure cases, generated retirement operations, and the reusable six-class default run_tests.sh. No production lifecycle code was added.

Final build and Checkstyle passed, as did 24 isolated unit cases, one generated model case, eight harness cases, and all 12 serial N100 comparisons. Matched heap dumps show 40 MiB of user slab payload disappears after retirement. Mean whole-JVM settled heap falls 8.032 MiB for 10 idle tables and 38.717 MiB for 40 rotating tables. Reads preserve dormancy; writes reactivate it.

The failure fixture proves asynchronous flush errors retain readable dirty memory and commit-log coverage. Separate source and test evidence records synchronous switch errors and the lack of automatic retries for failed memtables. Profiling found a substantial existing flush allocation cost in eager tombstone-histogram spools; this is recorded for follow-up without expanding implementation scope.

See research/explicit_memtable_retirement.md and research/prosecute_memtable_tables.md for results and continuation details. All changes remain uncommitted.
