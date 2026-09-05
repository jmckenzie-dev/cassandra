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

# Lazy memtable initialization

## Scope and rationale

Implement phase 2 for TrieMemtable. Its constructor creates a writable graph for
every shard before data arrives. Defer that graph, while keeping the existing
allocator, commit-log bounds, metadata, metrics, and shard boundaries. SkipList
implementations and flush policies retain their behavior. This isolates one
measurable candidate before a broader dormant-table lifecycle.

The first-write 1 MiB slab is already lazy and remains a separate cost. This
change cannot reclaim a dirty idle table; later idle flushing addresses that.

## Implementation

1. Store shards and their merged trie in one safely published holder. Share an
   empty holder across dormant memtables. First put initializes once under the
   memtable monitor; subsequent puts use a volatile read. Create all shards
   together so existing merged-trie behavior remains intact.
2. Read and observation paths take the current holder without activating it.
   Preserve empty statistics, empty range and flush iterators, and discard
   behavior. Preserve writes already accepted before a flush barrier.
3. Add a strict boolean `lazy_initialization` TrieMemtable factory option,
   default true. False supports same-build eager controls. Include it in factory
   equality so schema changes get the correct replacement behavior.
4. Extend the harness with an eager control and explicit mode/state reporting.
5. Add focused concurrency/lifecycle and generated operation tests. Run relevant
   existing flush-range, accounting, metrics, and configuration tests.
6. Alternate eager/lazy N=100 never-written and written/flushed runs. Capture
   matching allocation profiles and separate heap diagnostics when needed.
   Measure first-write service/arrival latency and actual completed rate.

## Acceptance and evidence

Build and Checkstyle must pass, targeted tests must verify data, and dormant read
and replacement behavior must be exercised. Report measured savings and costs
without extrapolating whole-JVM differences into exact ownership. Use the empty
graph analysis for direct attribution. Keep benchmarks serial and all executed
runs at or below 1,000 tables. No numeric savings target was agreed.

Root owns production edits, integration, scripts, and the final report. Separate
agents own constructor attribution, lifecycle/tests, and harness controls. Keep
their findings in `.debug/` and return concise summaries to the main context.

## Completed

Implemented the bounded TrieMemtable change and eager comparison mode. Final
build and Checkstyle passed (`logs/20260904-211425-ai-build.log`). The new
production tests passed 10 cases, existing targeted classes passed 33 cases,
and harness/configuration tests passed six cases covering seven real-cluster
scenarios. The corrected index test verifies that legacy read cleanup is a
local mutation that must activate the index memtable.

All 12 N=100 comparison runs passed. Matched heap dumps show 14,100 fewer private
state objects, including 800 shards, with an estimated 8.41 KiB saved per table.
Whole-JVM mean settled heap fell 1.19 MiB for empty tables and 1.46 MiB for flushed
tables. These whole-JVM differences include system state and measurement noise.
Allocation stacks confirm that shard construction moves to first writes.

First-write medians increased from 0.221–0.232 ms in eager controls to
0.258–0.371 ms in lazy controls. First-write p99 was 0.700–0.848 ms eager and
0.861–0.862 ms lazy. Later-write p99 remained within eager variation. This is
a measured tradeoff in a short low-load experiment, not a saturation result or
a guarantee that latency is unchanged. The eager option permits further
comparisons. No numeric performance threshold had been agreed.

The [results report](../research/lazy_memtable_initialization.md) records full
configuration, individual runs, test evidence, limitations, and next work.
Idle flushing, smaller initial slabs, and metrics remain separate changes.
