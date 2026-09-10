<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Smaller UCS hierarchy experiment

Status: complete under the residency stop condition; 85 focused tests and 22
benchmark runs passed. No candidate advanced to the conditional promotion and
1,000-table stages.
The optional setting preserves the legacy default. Results are recorded in
[the experiment report](../research/ucs_small_hierarchy.md).

Execution decision after the 48-cycle sweep: 4KiB and 1KiB doubled final reader
counts. A bounded 1B follow-up removed coarse rounding but doubled average
reader counts and raised the peak from three to eight files per table. Apply
the residency gate to those measured costs, not only the final checkpoint.
The longer append comparison, T8 control, automatic flushing, and allocation
sampling confirmed the result. No candidate advanced to the three-repeat
promotion or 1,000-table stage. This follows the stop condition below; the
existing 1,000-table census remains the evidence for the per-file cost.

## Hypothesis and source constraints

Repeated tiny flushes keep merging their growing result in the lowest Unified
Compaction Strategy (UCS) level. At 20 tables and 48 append cycles, T4 wrote
7.79 compaction bytes per flushed byte; T8 reduced that to 3.18 but retained twice
as many files. T4,L10 behaved like T4 while the files stayed in the lowest level.
Cursor compaction lowers allocation cost; it does not remove those disk writes
or the memory occupied by readers.

The matched [post-retirement census](../research/post_retirement_heap_census.md)
measured 129.00 / 162.98 / 213.25 MiB at 1,000 tables with one / three / six files
each. Budget roughly 17.3 KiB per extra file for this workload, while measuring
each candidate directly. Statistics metadata is a separate opportunity to lower
that cost; do not silently change it between compaction controls.

`Controller.getBaseSstableSize()` floors the estimated flush size at 1 MiB.
There is another constraint: `Controller.getFlushSizeBytes()` rounds observed
flush sizes **up to a whole MiB**. Changing only the floor cannot help tiny
observed flushes. `flush_size_override` also rejects values below 1 MiB.
`min_sstable_size` controls shard splitting, not the compaction hierarchy base.
Keep that option at 100 MiB throughout this experiment.

`UnifiedCompactionStrategy.formLevels()` applies these boundaries to file density,
including local token coverage. Tests must use density and coverage, not assume
that an SSTable's byte length alone determines its level.

## Small implementation, unchanged control

Add one UCS table option, provisionally `min_hierarchy_size`, default `1MiB`.
Use the existing byte-size parser and Controller option validation. Accept a
positive bounded byte value; reject zero, negative, overflow, and values above
1 MiB for this experiment. Store one scalar in Controller. Do not introduce a
new strategy, selector, global cache, timer, or special L0 compactor.

Use that value as both the minimum hierarchy base and the rounding quantum for
observed flush size. Preserve the existing 50% estimate-refresh rule initially.
With the option absent or set to 1 MiB, preserve the exact current computation.
Keep `flush_size_override` and its existing validation unchanged initially;
leave it unset in measured cases. A smaller quantum must never produce a zero
base or non-increasing density boundary. Validate finite estimates and integer
arithmetic bounds in targeted tests.

Scope actual experiments and idle flushing to lazy Trie memtables with UCS.
The UCS option belongs in each table's compaction map, where Controller already
loads its options. Do not add duplicate cassandra.yaml compaction configuration.
Document that tuning this option is experimental; the idle timeout remains
independently opt-in and defaults to zero.

## Pre / implementation / post sequence

1. Preserve the current 1 MiB result and repeat a matched baseline with the
   measurement additions below before editing Controller behavior.
2. Add Controller unit tests for default equivalence, validation, rounding,
   zero initial estimate, exact boundaries, 50% refresh, and large estimates.
   Extend UCS level tests to cover promotion, gaps, and local token coverage.
3. Implement the scalar option and harness pass-through. Run the tests and the
   isolated Trie retirement/integration tests. Build and Checkstyle must pass.
4. Compare T4 with bases 1 MiB, 64 KiB, 4 KiB, and 1 KiB. Start with 20 tables,
   48 then 192 cycles, four 256-byte rows per table per cycle. Run append and
   overwrite separately. Keep T8/1 MiB as a separate tradeoff control.
5. Repeat promising configurations three times in alternating order. Use
   explicit flush boundaries for causality, then automatic idle flushing to
   validate the actual production path. Do not compare different flush counts.
6. Validate the selected candidate at 1,000 tables, with matched writes, pool
   size, heap flags, metrics, data format, logging, and compaction concurrency.
   Do not exceed 1,000 tables. Take live dumps after final settlement.

## Workloads and correctness

First use the existing one-partition append/overwrite workload. Then vary
partition distribution and flush size enough to cross hierarchy levels and
the original 1 MiB boundary. Exercise a tiny-to-large-to-tiny burst to detect
estimate churn. Include partition/row/range deletions, TTL expiry with a
controlled clock, reads during compaction, reactivation, and restart recovery.
Keep repaired/pending-repair groups and expiration safety under the existing
UCS rules; use existing targeted compaction tests for these invariants.

Relevant classes: `compaction.unified.ControllerTest`,
`compaction.UnifiedCompactionStrategyTest`,
`compaction.unified.ShardedMultiWriterTest`, `memtable.IdleMemtableFlusherTest`,
and distributed `IdleMemtableFlushTest`. Add any boundary regression in the
closest existing class. Run through the repository's ai-* wrappers, explicitly
compile changed tests, and never rebuild the shared JAR during a benchmark.

## Measurements and decision

Record completed writes and verified rows; flush count/size distribution;
compaction input/output bytes and jobs; live and peak file counts; backlog and
drain time; per-level density membership; estimated flush size and effective
hierarchy base over time. UCS levels are computed dynamically; on-disk
`sstableLevel` is not a substitute for measuring `formLevels()` membership.
Capture process CPU, allocation with async-profiler `-e alloc`, compaction time,
read/first-write/other-write latency distributions, and post-GC heap. Profile
allocation separately from unprofiled timing runs. Add a bounded harness trace
for actual level membership rather than deriving it from unrelated metadata.

Predeclare a useful result: at least 30% less append compaction output at 192
cycles than matched T4/1 MiB, no correctness failure, and no persistent backlog.
Also report the cost: each additional resident file consumes measured reader
heap and operating-system resources. Reject a candidate for the default path
if it doubles file count or increases settled heap by more than 10% without a
separate decision to trade memory for I/O. Treat a repeatable >10% p99 regression
as requiring investigation; short sequential harness tails alone cannot prove
production throughput. Report overwrite results independently, since immediate
merging can be beneficial there.

If smaller boundaries only trade rewrites for too many retained files, preserve
the legacy default and stop. Use the heap census to prioritize reader metadata
compaction or batch more ingest before changing compaction selection. Do not
introduce periodic compaction: normal flush completion already submits work.
