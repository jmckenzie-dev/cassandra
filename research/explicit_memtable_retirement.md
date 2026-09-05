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

Phase 3a is implemented and validated. Explicit retirement uses existing production flushing and the phase 2 lazy replacement. Matched heap dumps show 40 MiB of user-table slab storage disappears after retiring 40 lightly written tables. All 33 targeted test cases and 12 N100 comparison runs passed. Changes remain uncommitted.

## Behavior

The residency harness accepts --explicit-retirement for idle-reactivate and rotating-bursts. After each observation pause, it calls ColumnFamilyStore.forceBlockingFlush(USER_FORCED) for each table in that cycle’s active subset. The operation uses existing forceFlush behavior and the phase 2 lazy TrieMemtable replacement. A completed request means the flush future completed; it does not mean the old storage was already collected.

No additional production lifecycle API is needed for this increment. Existing flushing coordinates base/index memtables, write barriers, SSTable publication and commit-log completion. Reclamation separately waits for the read barrier. Unit tests hold a real read operation open across flush completion and then check allocator release after closing it.

Clean tables without dirty indexes do not switch or create SSTables. A dirty index can require a base-table switch even when the base memtable is clean. Legacy index cleanup triggered by a read is an actual mutation and can activate the index.

The harness records per-cycle written, pre-retire, reclaimed and read checkpoints. The reclaimed checkpoint waits for existing work and allocator accounting to settle, without requesting GC. Final settled heap diagnostics run separately from timing controls. Ordinary reads preserve dormant trie storage; the next mutation initializes it.

## Focused tests

Run ./run_tests.sh with JDK 21, or distrobox enter dev -- ./run_tests.sh in this workspace. The existing root entrypoint now defaults to these isolated classes:

- TrieMemtableRetirementTest: clean idempotence, dirty retire/read/reactivate, reader retention, accepted and replacement writers, and dirty-index coupling.
- TrieMemtableRetirementFailureTest: failed flush propagates the error and retains dirty data.
- TrieMemtableLazyTest: complementary first-write ordering, reads, schema/truncate and tombstones.
- SplittablePartitionerTrieMemtableFlushSetTest and NonSplittablePartitionerTrieMemtableFlushSetTest: actual range flush contents and boundaries.
- MemtableSizeHeapBuffersTest: actual memtable heap allocation accounting.

The wrapper compiles tests and executes each class in its own test JVM through ai-ci-test, preserving exit codes, JUnit XML and timestamped console logs. Generated mutation/retirement tests use ./run_property_tests.sh --lazy. The harness cluster scenarios use ./run_tests.sh --long. Existing --lazy and configuration-only --harness modes remain available.

Validation evidence:

| Check | Result | Artifact |
| --- | --- | --- |
| Build and Checkstyle | Passed, including final failure fixture | logs/20260904-230931-ai-build.log |
| ./run_tests.sh | 24 cases in six isolated classes; no failures, errors or skips | logs/20260904-231021-ai-test-memtable-lazy/ |
| ./run_property_tests.sh --lazy | One case, four seeds × 64 generated operations; passed | logs/20260904-225630-ai-test-memtable-lazy/ |
| ./run_tests.sh --long | Eight cases, including nine three-table cluster scenarios; passed | logs/20260904-225654-many-tables-launch.log |
| Serial comparison recipe | 12 fresh N100 runs; no failed writes, mismatched reads, phase errors or profiler warnings | logs/20260904-230056-explicit-retirement-comparison/comparison.json |
| Shell syntax and git diff --check | Passed | Final local checks |

The initial build caught a JUnit-version mismatch and import spacing; both were corrected. Two failed attempts established the correct directory-failure fixture. The first did not block token-boundary flushing. The second failed synchronously during switching. The final test holds a real writer group, requests the flush, marks the data directories unavailable, releases the group, and checks the asynchronous failure. The failed memtable remains readable and accounted; its actual commit-log segment stays dirty. See [.debug/explicit-retirement-failure-fixture.md](../.debug/explicit-retirement-failure-fixture.md). No production defect was repaired in this increment.

## Experiment setup

The serial comparison recipe is tmp/run-explicit-retirement-comparison.sh. Its artifact root is logs/20260904-230056-explicit-retirement-comparison/. Each run starts a fresh JVM/node with Java 21.0.12, a maximum heap of 8 GiB, eight reported processors, and 100 tables. Ten tables receive four rows per cycle, with 128-byte values at an aggregate scheduled rate of 100 writes/second. Idle-reactivate uses two cycles; rotating-bursts uses four and touches 40 distinct tables. Observation and final hold periods are one second. These pauses position an explicit request; no idle threshold or timer is being tested.

Both modes use lazy TrieMemtable, heap buffers, BTI SSTables, size-tiered compaction and disabled user key/row caches. Cursor compaction remains disabled. The node reports a 10 MiB accounted memtable heap budget, which excludes unused slab capacity. The control retains existing flush triggers. Explicit retirement flushes the active subset serially after each observation pause.

Two unprofiled repeats per workload/mode alternate control, retirement, retirement, control. Separate matched rotating-burst heap dumps establish slab ownership. Separate idle/reactivation profiles record allocation events. Extra retirement checkpoints and flush/reclamation work extend the gap between bursts; the serial paced driver does not measure saturation throughput.

## Resident memory results

These are whole-JVM settled heap measurements in MiB, after requesting garbage collection. They include system tables, the driver, metrics and SSTable readers. Use the ownership walk below for direct slab attribution.

| Workload | Control repeats | Retirement repeats | Mean reduction |
| --- | --- | --- | --- |
| Idle/reactivate, 10 written tables | 115.453 / 115.805 | 107.718 / 107.475 | 8.032 MiB |
| Rotating bursts, 40 written tables | 146.047 / 146.134 | 107.456 / 107.291 | 38.717 MiB |

Control runs ended with 10 or 40 dirty memtables and no user SSTables. Retirement runs ended with zero dirty or initialized user memtables and zero user allocator accounting. Idle/reactivate completed 20 requests and produced 20 SSTables; rotating bursts completed 40 requests and produced 40 SSTables. Data components occupied 11,745 and 23,472 bytes respectively. These sizes exclude other components and filesystem overhead. No user compaction bytes were recorded, so these experiments do not measure compaction cost.

Every retirement cycle records initialized dirty memtables after writes, zero after reclamation, and zero after exact-data reads. Idle/reactivate then initializes the same ten replacements in the next cycle. It completed 80 writes and 300 verified partition reads; rotating bursts completed 160 writes and 500 verified partition reads.

Matched live heap dumps:

| Checkpoint | User TrieMemtables | Initialized | User current slab arrays | Slab payload bytes |
| --- | ---: | ---: | ---: | ---: |
| Control created | 100 | 0 | 0 | 0 |
| Control settled | 100 | 40 | 40 | 41,943,040 |
| Retirement created | 100 | 0 | 0 | 0 |
| Retirement settled | 100 | 0 | 0 | 0 |

The parser follows user memtable → allocator → currentRegion → Region.data → HeapByteBuffer.hb. It counts exact arrays and payload lengths; the 40 MiB excludes array headers. The independent whole-JVM slab-region count falls from 45 to 5. Both results support disappearance of the 40 user slabs, without inferring ownership from a noisy heap delta. All 100 memtable lifecycle objects remain; table metadata and metrics remain resident.

Diagnostic runs are `20260904-230639-residency-rotating-bursts-100t` (control) and `20260904-230802-residency-rotating-bursts-100t` (retirement) under the batch root. Analysis is in logs/20260904-230953-inspect-empty-trie-ownership.log and logs/20260904-230959-count-hprof-arrays.log. The recipe and parsers are copied into the batch root.

## Allocation and latency costs

Lower standing memory does not imply lower heap occupancy throughout a run. Sampled heap peaks were about 121 MiB in idle controls versus 171–327 MiB with retirement, and about 165 MiB in rotating controls versus 238–247 MiB with retirement. Samples include uncollected temporary objects and can miss the true peak. They do not establish peak live retained memory, but show that flush/reactivation allocation and garbage collection still matter.

Allocation recordings confirm the tradeoff. During the second idle write cycle, the control has zero sampled SlabAllocator.getRegion allocations; retirement has eight. The two retirement phases have 101 and 87 allocation samples and no sampled trie shard construction. Their total allocation weights are approximately 108 and 100 MiB. Sampling weights are not exact allocated-byte totals. The recordings include flush background work; future cursor compaction alone cannot remove allocations in a workload with no user compaction.

The dominant flush allocation is concrete: the current default SSTable metadata collector eagerly creates 3 MiB of tombstone histogram spool arrays, including for writers that later abort because their disk range is empty. Spool construction accounts for 78.7% and 83.0% of the retirement phases’ sampled allocation weights. Lazy histogram storage and avoiding empty-range writer construction are worthwhile follow-ups before aggressive retirement. See [allocation evidence and source locations](../.debug/explicit-retirement-allocation.md). These profiles explain where allocations occur; they do not quantitatively attribute the separate runs’ heap maxima.

Profile runs are `20260904-230722-residency-idle-reactivate-100t` (control) and `20260904-230847-residency-idle-reactivate-100t` (retirement). Eight create/write/retire recordings were checked for actual allocation events and converted to collapsed stacks; allocation HTML views are under each run’s allocation-analysis directory. The batch allocation-comparison.json and logs/20260904-231002-analyze-lazy-memtable-allocation.log retain counts and conversion commands.

Unprofiled retirement took 276–549 ms per serial group of ten tables, excluding the later reclamation-settlement checkpoint. Idle write p99 was 2.121–2.155 ms in controls and 2.130–2.476 ms with retirement. Rotating write p99 was 0.865–0.890 ms in controls and 0.904–0.922 ms with retirement. First-in-cycle medians were 0.406–0.429 ms versus 0.488–0.574 ms for rotating bursts; both modes initialize previously untouched tables there, so this difference cannot be assigned solely to reactivation. The samples are short and do not establish a numerical latency acceptance threshold.

All controls completed roughly 102.4 scheduled writes/second within write windows. The slight excess over the offered 100 comes from scheduling the first request at time zero in short windows. Flushes, observation, reads and settlement occur outside those windows. This is not equal end-to-end throughput or a saturation result.

## Limits and next work

A successful flush releases accounted memory after readers finish. Trie buffer disposal follows the allocator transition, and the heap slab remains reachable while any old memtable or allocator reference survives. Thus a zero counter alone cannot establish actual heap reclamation; matching heap dumps provide that evidence.

A failed flush stays in the flushing view and retains its commit-log coverage. Existing Cassandra behavior does not retry that failed memtable on a later clean forceFlush. Any future scheduler must treat errors and retained flushing memory explicitly. This increment preserves that behavior.

Failure injection also confirmed that directory selection can throw synchronously during the memtable switch, before forceFlush returns a future. A future caller must handle both request-time exceptions and asynchronous flush failures. The focused failure test injects after switching to exercise the latter path.

Automatic idle selection, timestamps, scheduling, queue limits, age/size policy, smaller slabs, metrics overhead and full table eviction remain future work. All executed workloads must remain at or below 1,000 tables. The tests and N100 experiments do not establish production behavior at 100,000 or one million tables.
