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

# Explicit retirement failure fixture

The first failure test did not prevent flushing. This was a test fixture defect.

`TrieMemtableRetirementFailureTest` initially used `Util.markDirectoriesUnwriteable(cfs)`. That helper marks table subdirectories. With token boundaries, `Flushing.flushRunnables` passes an explicit data directory to `flushRunnable`; descriptor creation then bypasses `Directories.getWriteableLocationAsFile`, which checks the table subdirectory. `Directories.getWriteableLocations`, used to calculate disk boundaries, checks data directories instead.

The initial execution is preserved under `logs/20260904-225540-ai-test-memtable-lazy/`. Its failure-test log is `org.apache.cassandra.db.memtable.TrieMemtableRetirementFailureTest/_jdk21/TEST-org.apache.cassandra.db.memtable.TrieMemtableRetirementFailureTest.log`. Line 396 records the table subdirectory being disallowed. Lines 397–406 record the requested flush and successful SSTable publication. The test then correctly failed with `Retirement must report the flush failure`. The five success cases in `TrieMemtableRetirementTest` passed in this execution.

A second execution under `logs/20260904-225945-ai-test-memtable-lazy/` marked data directories before requesting the flush. This failed during synchronous memtable switching: a compaction strategy notification reloaded disk boundaries inside the flush constructor. No flush future had been returned, so this did not exercise asynchronous flush failure retention.

The corrected fixture snapshots the data-directory locations before failure injection. Inside the existing helper's cleanup scope, it holds a real `Keyspace.writeOrder` group, calls `forceFlush`, and verifies that the returned future remains incomplete. The flush writer waits for that group. After synchronous switching finishes, the test marks each data location with the public `DisallowedDirectories.maybeMarkUnwritable` API, then closes the group. `Flushing.flushRunnables` now rejects all locations with `FSNoDiskAvailableForWriteError`, a subclass of `FSWriteError`, on the flush writer. The helper clears the markers on exit, and a `finally` block restores the disk failure policy. No production code or internal fields are patched.

The test requires the flush future to fail. It checks that the old memtable remains in the flushing view, retains allocator accounting, and serves its original rows; no SSTable is published and the replacement stays dormant. It also checks that the actual commit-log segment containing the write remains active and dirty for this table. Run this class in an isolated Java virtual machine: Cassandra intentionally retains failed flushing memtables, and this test does not manually discard their memory.

The final fixture passed its isolated test in logs/20260904-231021-ai-test-memtable-lazy/. Final build and Checkstyle passed in logs/20260904-230931-ai-build.log. The six-class default runner passed all 24 cases. `git diff --check` passed after the fixture corrections.
