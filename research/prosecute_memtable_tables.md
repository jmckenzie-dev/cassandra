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

# Memtable residency: continuation record

Updated 2026-09-05. Read this first after context compaction. This file preserves
decisions and continuation details; the linked baseline report retains the full
measurement evidence. Update this file at implementation milestones.

## Current state and authorization

Worktree: `/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables`.
Branch: `moar_tables`. Steps 1 and 2 are complete. All changes remain uncommitted.
The user authorized phase 2 while this continuation record was being written.
Phase 2 implements lazy shard initialization for TrieMemtable. See the completion
section below and [results](lazy_memtable_initialization.md) before resuming work.
The user subsequently authorized phase 3a: implement and validate explicit retirement,
with a reusable root run_tests.sh selecting isolated flush tests. Phase 3a is complete;
see [results](explicit_memtable_retirement.md) and
[plan](../.plans/explicit-memtable-retirement.md). The harness uses the existing
forceFlush path; no new production lifecycle operation was needed. All 33 targeted
cases and 12 N100 comparison runs passed. Automatic idle scheduling and aggressive
age/size flushing remain future work. No build, test, or benchmark process remains active.
The current request also authorizes using subagents in subsequent implementation
to keep exploration, test output, and review details out of the main context.

Keep every executed workload at **1,000 tables or fewer**. The user imposed this
limit because creation allocation and timing become costly above that scale.
Current baselines use 100 tables. Historical 5,000-table results are existing
evidence, not permission to repeat those runs.

## Current metrics compatibility contract (2026-09-05)

The user relaxed numerical identity for a separate optimized metrics
implementation. Keep the legacy implementation intact and selectable. In the
subsequent implementation request, the user explicitly selected the optimized
path as the cassandra.yaml default for this branch. The optimized path must
expose all existing metrics so existing tooling continues to work. Preserve names,
aliases, types, units, access paths, and meaningful cumulative/recent behavior.
Changing internal buckets, storage, and aggregation timing is allowed if the
distribution shape and median, p99, p99.99, and max remain accurately representative.
An accuracy tolerance and permitted time-window differences have not been agreed.
Implementation is now authorized as sequential optimizations, each with pre-change,
iteration, and final measurements in a single commit. See
[plan](../.plans/compact-runtime-metrics.md) and
[measurements](compact_runtime_metrics.md) for current execution state.

This supersedes the numerical-identity requirement below for the proposed optimized
runtime metrics path. It does not authorize dropping per-table metrics, disabling
their exports, discarding cumulative history on idleness, or changing stored
SSTable tombstone statistics. Existing exact-equivalence candidates retain their
tested contracts. The next step is design and compatibility analysis, not a selected
replacement algorithm or dependency.

Candidate direction: allocate no bucket arrays for empty reservoirs; use compact
storage for sparse observations; grow to dense storage only when useful. Share
immutable bucket definitions and evaluate fewer permanent contention stripes.
Record every observation to protect rare tails. Define quantile error in terms
of value as well as rank; preserve exact observation counts and investigate explicit
extrema tracking for the required time scope. A lifetime maximum cannot substitute
for a recent maximum. Deferred aggregation must preserve event values and account
for event time. Changing the decay/window needs burst and idle-transition tests.

Different internal buckets may need conversion to the legacy external histogram
layout. CassandraMetricsRegistry already translates log_linear buckets for legacy
values()/getRecentValues() exports while exposing native rawValues(), rawBuckets(),
and bucketsId(). Extend that approach only after auditing consumers. Conversion
itself contributes approximation error, and adaptive changes must not move old
counts between exported buckets and corrupt cumulative deltas. Current reservoir
max is the highest nonempty decayed bucket's upper bound, with special empty and
overflow behavior; exact lifetime max would change its meaning. LatencyMetrics
child removal and aggregation also cast concrete original snapshots and require
compatible offsets for merge/rebase. Validate optimized results
against recorded events with controlled time and the chosen weighting rules;
legacy comparisons become diagnostics where numerical differences are intentional.
Keep exact comparisons for preserved interface and counter contracts. Test rare
outliers, multiple modes, changing traffic, concurrency, merge/rebase, and actual
tool consumers. Continue matched fresh-JVM performance runs at <=1,000 tables.

Preserve the other exported statistics too, including mean and standard deviation.
The existing named JMX percentile attributes end at p99.9; test p99.99 through
Snapshot.getValue(0.9999). An exposed p99.99 attribute would be an additive change.
PercentileSpeculativeRetryPolicy consumes percentile snapshots inside Cassandra,
so accuracy and freshness changes can affect read behavior, not just dashboards.
Include that consumer in validation. Stable cumulative export storage may remain
necessary alongside a compact recent-distribution representation; budget both.

## Current compact runtime metrics implementation (2026-09-05)

The first optimization is implemented and validated. `optimized_metrics_enabled`
defaults to true in Config and cassandra.yaml; false selects the legacy reservoir.
Bootstrap metrics created before configuration loads remain legacy. The compact
reservoir uses empty/sparse pages and a dense fallback, private shared bucket
definitions, and additive snapshot construction hooks. Existing histogram
arithmetic and exported bucket geometry remain unchanged. Compact parents also
fix pre-existing child-release bucket inflation; legacy parents retain it.

Pre-change, three implementation iterations, and final measurements are recorded
in [compact runtime metrics](compact_runtime_metrics.md). N100 settled heap fell
from 105.091 to 74.155 MiB for never-written tables and 108.848 to 77.853 MiB after
writes/flushes (medians of two matched runs per mode/scenario). User-owned counter
payload fell from 15,596,800 bytes to zero untouched, or 349,184 bytes after writes.
Final one-thread JMH median throughput is about 5% lower; four-thread throughput
is unchanged. Empty reservoir graphs fall from 5437.44 to 141.44 amortized bytes.

Validation: clean build/Checkstyle; 58 focused passes with one existing ignored
legacy diagnostic; 11 harness cases; eight N100 comparisons; direct heap ownership;
focused post-extraction reruns. Next: commit this optimization, then capture a
fresh baseline before adaptive stripe storage. No stripe or counter-width changes
have been implemented yet. These must each receive their own measured commit.

## Historical exact-value constraint (2026-09-05)

The following records the requirement used for the completed allocation candidates.
For future optimized runtime metrics work, the current contract above takes precedence.

The user requires: "whatever values we store for those metrics need to remain
identical to what is recorded today." Treat this as a hard constraint on future
histogram and metrics work. Optimize allocation, representation and registration
without dropping observations, resetting history on retirement, changing histogram
precision or merge behavior, or changing rate/decay semantics. Apply the same
preservation requirement to the tombstone histogram discussed alongside metrics.

Earlier suggestions to expire detailed instrumentation or change its availability
are not the accepted direction. Lazy allocation is a candidate only where the
unallocated state reproduces existing behavior exactly, including initialization
time, empty reads, first update and release. A smaller tombstone spool that drains
earlier may change approximation output and is not acceptable on memory savings
alone. Geometric backing-array growth remains a candidate if logical offsets,
values, concurrency and tick behavior remain unchanged.

Validate candidates against the current implementation using identical ordered
input events and controlled clock/tick schedules. Compare counts, rates, buckets,
snapshots and serialized histogram output where applicable; do not accept merely
similar distributions. This is a semantic compatibility requirement, not a claim
that separate benchmark runs have identical measured operation durations.

The user additionally requires preserving the old implementation intact and
developing a separate candidate so both can be compared side by side. Keep the
existing implementation as the reference and default during evaluation. Select
old or new through a narrow configuration/factory boundary and expose that choice
in the harness and effective run configuration. Avoid rewriting the old algorithm
into shared helpers that would let both implementations acquire the same defect.

Use two comparison modes. Differential tests instantiate both implementations,
feed the same events and controlled time, and compare observable values exactly
at intermediate checkpoints and finalization. Keep registries, counters and tick
state isolated so the comparison itself cannot double-record observations. Cover
empty input, first update, deletes/expiration, overflow/draining boundaries,
release and lifecycle transitions where applicable. Record reproducible failing
event sequences. Concurrent behavior also needs focused ordering/race tests;
matching sequential traces alone does not establish concurrency correctness.

Performance A/B runs select one implementation per fresh JVM/node with matched
workloads and settings, alternating run order. Running both simultaneously would
distort allocation and resident-memory measurements. Keep all runs <=1,000 tables.
Start with the tombstone-histogram candidate and evaluate metrics changes as
separate increments, retaining the reference path throughout evaluation. The
Java candidates are now implemented; see the current experiment section below.

The user is also considering a separate native Rust implementation through JNI
or the Foreign Function and Memory (FFM) API. The user permits adding JDK 23+
support to this branch if FFM proves worthwhile. This removes the existing JDK
ceiling as a design constraint; it does not establish a native performance benefit
or select a rewrite. Preserve the Java reference and exact-value requirement for
any native candidate. Compare total process resident memory and native allocation
as well as Java heap, and separate runtime-version gains from implementation gains.

## Current Java allocation experiment (2026-09-05)

The user chose Java and authorized implementation and testing. See
[plan](../.plans/java-histogram-metrics-equivalence.md) and
[results](java_histogram_metrics.md). Native code and JDK upgrades are not selected.

Implemented two independent candidates, both disabled by default:

- `cassandra.lazy_tombstone_histograms`: a separate builder starts with an empty
  zero-spool reference delegate and creates the original full-spool builder on
  first observation. Original algorithm unchanged; only its interface declaration
  changes. MetadataCollector selects the candidate and supports constructor DI.
- `cassandra.geometric_meter_arrays`: separate GeometricThreadLocalMeter doubles
  the rate array; the original ThreadLocalMeter remains unchanged. Meter.create
  selects registry and timer meters. Counts, rates, ticking and cleanup remain
  identical; spare array capacity can increase retained memory. Copy-on-write
  registration remains quadratic and is not changed in this experiment.

The residency harness exposes matching `--lazy-tombstone-histograms` and
`--geometric-meter-arrays` flags before cluster startup, including effective
configuration reporting. Both are false unless explicitly selected.
`run_tests.sh --histograms` and `--meters` run the isolated suites; matching
property-runner options select generated cases. `--long` also tests individual
and combined candidates with three-table retirement scenarios.

All 23 histogram cases and 13 meter cases passed on the final clean Java 21 build.
Build/Checkstyle: `logs/20260905-004254-ai-build.log`; tests:
`logs/20260905-004401-ai-test-memtable-lazy/` and
`logs/20260905-004515-ai-test-memtable-lazy/`. Adversarial source review found no
blocking defects. The first compile failed at the registry's Dropwizard import;
the factory call now preserves that return contract. A later source edit during
compilation left stale bytecode, so the final clean build occurred after source
edits stopped. `ai-ci-test` now retains raw output; the allocation probe uses
SLF4J because successful JUnit XML omits stdout.

Histogram comparison completed six N100 fresh-JVM runs in
`logs/20260905-003831-java-allocation-histogram/`: reference/candidate/candidate/
reference unprofiled, then a profiled pair. Each completed 80 writes, 300 verified
reads and 20 retirement requests, leaving zero initialized/dirty/flushing user
memtables. Weighted retirement allocation fell from 208,143,905 to 27,787,211
bytes across two cycles (86.7%); spool samples fell from 103 to zero. Settled
heap remained about 108 MiB. Exact calling-thread allocation per empty builder
lifecycle fell from 3,148,512 to 2,816 bytes; one-observation lifecycle adds 1,416
bytes versus reference. This is a transient allocation improvement.

The meter comparison also completed six N100 runs in
`logs/20260905-004614-java-allocation-meter/`. CREATE TABLE weighted allocation
fell from 4,228,405,835 to 3,664,273,513 bytes (13.3%). Rate-array allocation
samples fell from 1,135 to zero; this means below sample resolution, not zero
actual allocation. Unchanged meter registration still accounted for substantial
copying. Both profiled settled heaps contain 8,650 live meters, but geometric
storage retains 185,352 more payload bytes: 180,840 spare capacity and 4,512 from
different high-water/cleanup timing. See
[ownership report](../.debug/meter-rate-array-ownership.md).

The final `run_tests.sh --long` passed all ten cases, including twelve small
clusters, in `logs/20260905-005021-many-tables-launch.log`. Total validation:
46 targeted cases, twelve N100 comparisons, clean build/Checkstyle, and direct
heap-array ownership checks. All work is complete and uncommitted; both candidates
remain disabled by default. No test/build/benchmark process remains active.

The main analyzer is `.build/sh/analyze-java-allocation.py <batch>`; use local
venv. Heap analysis is `tmp/inspect-meter-rate-arrays.py <batch>` with shared
`tmp/hprof_reader.py`; results are in `logs/20260905-005018-inspect-meter-rate-arrays.json`.
The shared reader also reproduced prior trie ownership evidence after extraction.
Next resident-memory target: compact empty/sparse metric reservoirs under the
current compatibility contract above. Next creation-allocation
target: copy-on-write meter registration. Neither follow-up is implemented.

## Intent and design direction

Priority update after commit `c274d4232b`: the user explicitly wants the resident
memory blockers to 100k+ tables addressed before further allocation optimization.
Defer copy-on-write registration optimization and other allocation-only work.
Rank candidates by retained bytes per table and distinguish fixed node costs from
per-table growth. Continue the <=1,000-table workload limit and the current metrics
compatibility contract above. The ownership investigation measures reservoir-owned primitive arrays
directly in the existing live heaps; the old shallow-class census did not assign
those arrays. Disk-backed metrics are a design option under discussion, not a
selected implementation. No metric history may be reset or discarded on idleness.

Direct ownership analysis of the committed reference N100 heap found 33
TableMetrics reservoirs plus one Trie contention reservoir per user table.
All 3,400 user reservoirs are zero-filled at both created and settled checkpoints.
Their 6,800 unique mutable backing arrays retain 155,968 payload bytes per table
(152.3125 KiB), excluding headers and wrappers. At an unchanged per-table cost,
100k tables would require about 14.5 GiB for these arrays alone; this is a linear
projection, not a 100k-table measurement. There are also 20 private 127-long
offset arrays per table (20,320 bytes/table), alongside two shared default offset
arrays. Shared LOW_BUCKET_COUNT offsets are another candidate, subject to an
alias/mutation audit because arrays escape through public accessors.
Evidence: `logs/20260905-090932-inspect-resident-reservoirs.json` and
`tmp/inspect-resident-reservoirs.py`. Changing physical stripes can change numeric
results because rescaling rounds per stripe; evaluate those differences under the
optimized path's accuracy contract. Preserve LatencyMetrics child
snapshot merge/rebase behavior, which currently depends on a concrete original
snapshot type. These were pre-change findings; the implementation above now
addresses empty/sparse storage and shares the snapshot contract.

The eventual target is 100,000 or 1,000,000 tables without overflowing heap or
superlinear growth. The user identified two independent costs: allocation during
table creation, and standing on-heap memory per table. This work focuses on the
second cost, specifically memtables. Metrics remain a separate workstream.

The user's proposed behavior is to flush and unload idle writable table state
after roughly 30 seconds, then recreate it on a subsequent write. Reads should
continue from immutable Sorted String Tables (SSTables) without recreating the
writable structures. The first scope keeps ColumnFamilyStore (CFS), schema,
readers, and maintenance state resident. Full table eviction would be a much
larger lifecycle change and is not the first implementation.

The user expects future cursor-based compaction with no allocation. Under that
assumption, frequent compaction and small SSTables may be less costly. The user
also reports that disk reads through the kernel page cache with BTI format can
match BTree or trie memtable performance. Treat these as design context, not
claims established by the current baseline. Memtables should absorb ingest
bursts and separate writes into a Log-Structured Merge (LSM) storage pipeline.
The user prefers an external Redis/Memcached-style cache for application hot data.

This suggests eventually bounding dirty bytes and residence time, rather than
keeping memtables as a hot read cache. Even with cheaper compaction, measure
physical write amplification, file/reader overhead, disk bandwidth, flush queue
growth, and burst latency. Flushing memory stays resident until writers and
readers release it. Count that memory and apply backpressure when needed.

## Agreed experiment sequence

The conversation discussed three production changes, plus their prerequisite
harness work. Use these explicit names to avoid numbering ambiguity:

1. **Harness and eager baseline — complete.** Add residency workloads and capture
   current behavior before changing production allocation or flush policies.
2. **Lazy initialization — complete for TrieMemtable.** Keep existing flush policy. Retain
   minimal lifecycle/ordering state and defer useful writable structures until
   first local mutation. After a normal flush, the replacement should also be
   lightweight. Reads and observation must not activate it.
3. **Idle retirement — explicit operation complete, scheduling later.** Phase 3a
   validates forceFlush with a lazy replacement, safe reclamation and reactivation.
   Add idle scheduling separately. Use last local mutation, not last read, for
   idleness. A proposed 30-second default remains a design parameter to validate.
4. **Aggressive age/size flushing — later.** Bound residence age even for trickle
   traffic that never becomes idle. Add resource-aware policy after correctness
   and costs of the preceding mechanisms are established.

Implement, correct, and benchmark each increment before proceeding. Once all
mechanisms exist, use a same-build comparison matrix: baseline, lazy only,
lazy+idle, lazy+age/size, and all enabled. Configuration controls are useful for
isolating effects, but do not implement every policy before understanding the
first change. Keep compaction implementation constant within each comparison;
repeat separately when cursor compaction supports the target format.

## Phase 2 approach and acceptance

Start by inspecting empty-table heap ownership and constructors. Find objects
whose deferred construction can produce a worthwhile retained-memory reduction.
Choose the smallest change that preserves the current lifecycle. A lightweight
existing memtable with lazy internals may be safer than replacing the entire
Memtable abstraction with nullable state or a proxy. This is a design option,
not an implementation decision made without evidence.

Measure never-written and written/flushed tables first, using matched controls.
Capture post-collection heap, creation/flush allocation, first-write latency,
and repeated activation behavior. Keep the same Java version, heap, reported
processors, schema, payload, compaction, and profiling settings. Initial
acceptance requires correct lifecycle behavior, measured savings, and understood
first-write cost. No numeric savings target has been agreed. If empty-state
savings are small, say so and revisit the scope rather than adding complexity
to claim completion.

Correctness work must cover concurrent first writes, first-write/flush races,
read visibility during flush, commit-log coverage/replay, truncate/drop/schema
changes, and secondary-index interactions where the changed code applies.
Use targeted existing tests plus direct regression tests for new branches.
Test production behavior, not only counters or a mock of the implementation.

## Lifecycle constraints carried from the investigation

- Current flush constructs a replacement memtable before switching out the old
  one. See `ColumnFamilyStore.Flush` and `createMemtable`.
- `Tracker.getMemtableFor` selects by operation group and commit-log position.
  Preserve `OpOrder` barriers and acceptance of writes already in progress.
- A mutation can enter the commit log before memtable selection. Creating a
  lower bound from the current commit-log position during lazy activation could
  exclude that mutation. Preserve the logical memtable's original lower bound.
- Flush completion, durable SSTable publication, commit-log discard eligibility,
  and allocator reclamation are separate events. Do not retire dirty data early.
- Old memtables remain readable while flushing. Readers need safe references;
  retiring writable storage must not break scans or reads already in progress.
- Index memtables and index callbacks participate in flush ordering. Inspect
  coupled behavior before treating each table's lifecycle as independent.
- Reads, metrics, diagnostics, and empty flushes should not instantiate expensive
  writable structures as a side effect.
- Avoid a permanent timer or large scheduler object per table. Future idle
  scheduling needs to scale with active tables and bound work per tick.

Source starting points: `src/java/org/apache/cassandra/db/ColumnFamilyStore.java`
(construction, Flush, apply, truncate); `db/lifecycle/Tracker.java` and `View.java`;
`db/Keyspace.java`; `db/memtable/AbstractMemtable.java`,
`AbstractAllocatorMemtable.java`, `TrieMemtable.java`, `SkipListMemtable.java`,
and `ShardedSkipListMemtable.java`; `utils/memory/SlabAllocator.java`.
Paths in this paragraph after the first are relative to
`src/java/org/apache/cassandra/`. Re-read changed methods before editing.

## Confirmed findings and important corrections

Full evidence: [baseline report](memtable_residency_baseline.md).

The new rotating workload touches 40 of 100 tables with four small rows each.
User memtable counters report 27,680 data bytes and 75,680 accounted heap bytes.
Whole-JVM heap rises by about 40 MiB after creation. Live heap dumps show exactly
40 additional byte arrays of size 1,048,576, all reached by
`SlabAllocator$Region.data -> HeapByteBuffer.hb`.

`SlabAllocator.REGION_SIZE` is 1 MiB. Allocation charges requested slices while
the allocator reserves a full region. The before/after diagnostic contained
5 and 45 such region-backed arrays: 5,242,880 and 47,185,920 payload bytes.
This is direct reference evidence, not a full dominator analysis.

**The slab is already allocated on first write.** Lazy construction of empty
memtables alone cannot recover slabs retained by cold dirty tables. Idle flushing
can recover them after safe reclamation. A smaller initial slab is a separate
candidate; do not quietly fold it into the lazy-init experiment. The 1 MiB is
SlabAllocator capacity, not the initial trie buffer (which starts at 256 bytes).

Other corrections that must survive compaction:

- The historical N=100 creation run used SkipListMemtable, not TrieMemtable.
  The old context assumption was wrong. The new harness records actual type.
- Selecting the node's default memtable also changes system memtables. Trie
  versus SkipList whole-heap controls do not isolate user-table ownership.
- Initial heap varies several MiB due to shared initialization. Do not divide
  one initial-to-created delta by 100 and call it exact bytes per table.
- Allocator counters omit reserved slack and empty object graphs. They cannot
  alone define the future physical-memory budget.
- Allocation profiles describe allocated bytes; heap dumps describe live objects.
  Class histograms alone do not establish retained ownership.
- The historical metrics investigation found exact-size copied array growth in
  ThreadLocalMeter. Aggregate copying is quadratic, not exponential. About
  591 decimal GB of allocation was sampled at N=5,000. Fixed-size increments to
  a single copied array still have quadratic aggregate copying; geometric growth
  or independently allocated segments are different solutions. Older docs use
  the term "chunked" loosely; do not repeat it as proof of linear total work.
- Historical empty-table estimates of 279–285 KB/table were whole-heap deltas.
  Metrics are a large identified family, but most retained ownership was not
  assigned. Do not claim metrics own most heap without attribution.

## Step 1 implementation inventory

All files below are uncommitted. Preserve unrelated existing user files.

| File | Change |
|---|---|
| `test/distributed/org/apache/cassandra/distributed/test/ProfiledClusterHarness.java` | Adds protected no-op `configureNode(IInstanceConfig)` before node startup. |
| Same directory: `MemtableResidencyProfileHarness.java` | Five deterministic real-cluster workloads, timings, samples, checkpoints, validation, optional heap dumps. |
| Same directory: `MemtableResidencyConfigTest.java` | 500 seeded generated cases plus invalid-input and fixed-subset checks. |
| Same directory: `MemtableResidencyHarnessTest.java` | One test executes all five scenarios with three real tables and exact row verification. |
| `.build/sh/ai-profile-memtable-residency` | Reuses existing many-table launcher with new main class. |
| `run_tests.sh`, `run_property_tests.sh` | Targeted JUnit entrypoints through existing launcher. |
| `.build/sh/ai-build` | Fixes string-false shell condition that silently skipped Checkstyle; adds raw timestamped build log. |
| `.build/README.md`, `.build/memtable-residency.md` | Harness discovery and usage documentation. |
| `.plans/memtable-residency-baseline.md` | Completed step 1 plan. |
| `research/memtable_residency_baseline.md` | Baseline report with durable values and evidence links. |
| `TODO.md` | Step 1 moved to DONE; phase 2 tracked under TODO. |

Pre-existing untracked files include `.opencode/`, `opencode.json`,
`.plans/h1-metrics-scope.md`, and the two original research documents. Do not
delete, overwrite, or commit them incidentally. No production memtable, metrics,
or flush changes were made in step 1.

## Harness behavior and measurement limits

Read [the harness guide](../.build/memtable-residency.md) for all options.
The default is 100 tables, all active, four rows/cycle, two cycles, 128-byte
printable ASCII payload, seed 1, 100 aggregate writes/second, TrieMemtable, BTI,
one-second idle and final hold, and one-second samples. Main written/flushed
baselines override cycles to four. Heap maximum is 8 GiB; reported CPUs are eight.

Each run uses a fresh JVM and node directory. User tables disable key and row
caches, and use size-tiered compaction with thresholds 4 and 32. Cursor compaction
is disabled. Other caches retain defaults. No cold-cache read claim is valid.

Only `written-flushed` forces a flush, using USER_FORCED. `idle-reactivate`
reuses a fixed seeded subset; `rotating-bursts` advances through a permutation;
`trickle` is one continuous paced phase with no intermediate reads or garbage
collection (GC). `never-written` checks empty tables.

The serial paced driver records scheduled/start/end times, service latency,
arrival latency, success, and lateness. It does not drop overdue writes. It is
not a concurrent saturation load generator. Exact expected rows and payloads
are verified on reads. Independent sampling reports failures.

Samples distinguish live/dirty/flushing memtables; data/accounted heap/off-heap;
pending flushes; SSTables and data-component disk bytes; flush and compaction
bytes; compacting/pending compaction; and node pool used/reclaiming memory.
Node pool values include system tables. The client, sampler, and node share a
JVM. Main-thread allocation deltas omit background work; use flight recordings.

Settlement requires three consecutive 100 ms quiet observations for submitted
flush/compaction work and allocator reclamation, with a default 60-second timeout.
It does not flush dirty memtables or prevent future periodic activity. Post-GC
checkpoints call System.gc and wait 500 ms; this is a request, not a verified
collector completion guarantee. Diagnostic heap dumps run separately from timing
controls. Sampled maxima are not exact peak heap.

## Baseline evidence index

Artifact root: `logs/20260904-171404-residency-baselines/`. These ignored files
are local only. Full configuration is in each run's `summary.json`.

| Run suffix under that root | Scenario | Settled heap MiB | User state |
|---|---|---:|---|
| `20260904-171414-residency-never-written-100t` | Empty repeat 1 | 106.054 | 100 live, 0 dirty |
| `20260904-171451-residency-written-flushed-100t` | Flushed repeat 1 | 111.498 | 100 live, 0 dirty, 100 SSTables, 400 flushes |
| `20260904-171600-residency-never-written-100t` | Empty repeat 2 | 106.321 | 100 live, 0 dirty |
| `20260904-171640-residency-written-flushed-100t` | Flushed repeat 2 | 111.611 | Same counts as flushed repeat 1 |
| `20260904-171751-residency-idle-reactivate-100t` | 10 active, two 31-second pauses | 116.864 | 10 dirty, no flushes |
| `20260904-171933-residency-rotating-bursts-100t` | 10 active/cycle, 40 touched | 146.887 | 40 dirty, no flushes |
| `20260904-172018-residency-trickle-100t` | 10 active, 160 writes, ~16 seconds | 116.022 | 10 dirty, no flushes |
| `20260904-172113-residency-written-flushed-100t` | Profiled, 1,600 writes | 112.194 | 100 live, 0 dirty, 100 SSTables |
| `20260904-172228-residency-never-written-100t` | Empty diagnostic heap dumps | 106.273 | Baseline/created/settled dumps |
| `20260904-172320-residency-never-written-100t` | SkipList control 1 | 105.208 | 100 live, 0 dirty |
| `20260904-172400-residency-never-written-100t` | SkipList control 2 | 105.207 | 100 live, 0 dirty |
| `20260904-172440-residency-trickle-100t` | 10 active, 400 writes, ~40 seconds | 116.654 | 10 dirty, no flushes |
| `20260904-172558-residency-rotating-bursts-100t` | Dirty diagnostic heap dumps | 146.440 | 40 extra 1 MiB slabs |

The two unprofiled flushed runs have service p99 of 0.714 and 0.869 ms, arrival
p99 of 0.783 and 2.606 ms, and completed rates of 100.24 and 99.84 writes/second.
One run has a 91.35 ms maximum start delay. These short low-load baselines do
not establish production latency or throughput limits.

The 40-second trickle gives each of ten tables about one write per second.
It distinguishes maximum-age flushing from an idle policy with a 30-second
threshold. Read phases in other scenarios also extend gaps between bursts.

## Allocation profiling is already enabled

The user asked whether runs need async-profiler `-e alloc`. `ResourceProfiler`
already starts `event=alloc,wall,cpu`, with `alloc,wall` fallback. Explicit
`--no-profile` runs are controls. The profiled flushed baseline produced 19
async-profiler phase recordings plus JDK recordings without profiler warnings.

The first flush recording has 897 ObjectAllocationInNewTLAB events, 219 execution
samples, and 1,924 wall-clock samples. The first write has 100 allocation events.
Both converted to allocation views with the existing async-profiler 4.2
`jfrconv --alloc`; output files are `03-cycle-000-alloc.html` and
`03-cycle-000-flush-alloc.html` in the profiled run. `recording-validation.log`
retains verification. Default rendering can show a different event type.

## Completed checks and local tooling

- Build and Checkstyle: `logs/20260904-171126-ai-build.log`.
- Final targeted tests: `logs/20260904-171224-many-tables-launch.log`,
  `OK (4 tests)`, including all five real-cluster scenarios.
- All 13 N=100 runs passed with no failed writes, read mismatches, phase errors,
  or profiler warnings.
- Original creation harness smoke, N=1:
  `logs/20260904-172641-many-tables-1t/summary.json`.
- SkipList/BIG smoke, N=3:
  `logs/20260904-172717-residency-written-flushed-3t/summary.json`.
- ShardedSkipList/BTI smoke, N=3:
  `logs/20260904-172744-residency-written-flushed-3t/summary.json`.
- Shell syntax and `git diff --check` passed at completion of step 1.

The baseline root contains copies of `run-residency-baselines.sh`,
`run-residency-controls.sh`, `summarize-residency.py`,
`count-hprof-arrays.py`, and recording verification tooling. Working copies are
under project `tmp/`. `comparison.json` retains derived run comparisons.
`slab-array-counts.json` and `logs/20260904-172651-count-hprof-arrays.log`
retain the reference-walk results. The heap parser resolves class/superclass
fields and validates record/instance lengths; it is not a dominator engine.

## Environment and workflow constraints

- Host Java 26 is unsuitable. Existing `dev` distrobox has OpenJDK 21.0.12 and
  dependencies. Use `distrobox enter dev -- <command>`; this prefix is approved.
- Build only through `.build/sh/ai-*` wrappers. Never invoke Ant directly or
  install dependencies. `.build/sh/ai-build` includes Checkstyle.
- `distrobox enter dev -- ./run_tests.sh --long` runs the targeted harness checks.
  Use `.build/sh/ai-ci-test <class>` for relevant existing unit tests. No full suite.
- Use project `tmp/` for temporary files, never system `/tmp`. Scripts write
  stdout and stderr to timestamped `logs/` files and console, preserving status.
- Use `uv` for Python environment operations and local `venv`; activate with
  `source venv/bin/activate`. Read `~/.config/opencode/TESTING.md` before test work.
- Do not commit unless asked. Do not alter generated sources or dependencies.
- Maintain `TODO.md`; move completed implementation entries from TODO to DONE.
- Keep benchmark runs serial to avoid resource interference, even with agents.

## Context and delegation practice for continuation

The user asked to preserve detail in this file before compaction and to use
subagents for subsequent work. Keep the main agent responsible for constraints,
design decisions, integration, and acceptance. Delegate bounded constructor/heap
analysis, lifecycle review, tests, or log analysis. Give agents exact questions,
owned files for edits, and this document plus relevant evidence paths. Ask for
concise findings with source locations and durable report paths. Avoid returning
raw file dumps or full test logs to the main chat.

Keep one owner for interdependent production lifecycle edits. Other agents can
inspect correctness and own separate tests. Tell workers they share a workspace
and must preserve other edits. Current session supports the main agent plus
three concurrent subagents. Use inherited model settings unless instructed.

Compaction is useful after milestones but is not a substitute for this record.
The assistant has no callable compaction tool in this session. The Codex client
offers `/compact`; automatic context compaction can also occur. Do not claim
manual compaction happened unless the client performs it. After compaction,
read this file, TODO, and the current phase plan; check Git status and active
work before resuming. Do not repeat completed baselines without a reason.

## Phase 2 completion after the handoff was created

Plan: [lazy initialization](../.plans/lazy-memtable-initialization.md).
Production scope is TrieMemtable's shard graph. One volatile TrieState holder
publishes all shards and the merged read view once under the memtable monitor.
A shared empty state serves dormant reads and observation. The existing logical
memtable, allocator, commit-log bounds, metrics, and shard boundaries remain.
The `lazy_initialization` factory option defaults true; false is an eager control.
SkipList implementations and flush scheduling remain unchanged.

The constructor analysis found 800 user shards across 100 empty tables. The
old complete shard/merged graph contains 140 objects per eight-shard table,
estimated 8,592 bytes with compressed references. The new eager layout adds a
holder. These are reference-walk counts and layout estimates, not dominator sizes.
See [.debug/lazy-memtable-ownership.md](../.debug/lazy-memtable-ownership.md).

The new harness option is `--eager-memtable`. It appends
`initialized_trie_memtables` to sampled/checkpoint counters and records a
post-verification checkpoint. Seven three-table scenarios (five lazy, two eager
controls) passed with the configuration tests: `OK (6 tests)` in
`logs/20260904-205451-many-tables-launch.log`.

New production tests passed: seven focused lifecycle/factory tests, one generated
test (four seeds, 64 operations each), and two recovery/index tests. Evidence is
`logs/20260904-210049-ai-test-memtable-lazy/` and its sibling `.log`. The replay
test shuts down without flushing, verifies no data SSTables exist for the table,
then verifies recovered rows and subsequent writes/flushes. `./run_tests.sh --lazy`
runs these classes through the standard Ant runner. `./run_property_tests.sh
--lazy` selects the generated test.

An initial index assertion was incorrect and was corrected from source and test
evidence. Legacy-index queries after partition deletion write cleanup tombstones
for stale entries. Such a query must activate the index memtable, even while the
base table remains dormant. The corrected test asserts the actual cleanup write,
flushes it, then verifies another query does not reactivate it. This strengthens
the local-mutation definition of idleness. Details and original failure are in
`.debug/lazy-memtable-lifecycle.md` and `logs/phase2-initial-tests/`.

Final build and Checkstyle passed in `logs/20260904-211425-ai-build.log`, including
the final index-test correction. Existing range-flush, memory-accounting, metrics,
and config tests passed: 33 tests, zero failures/errors/skips. Their XML and logs
are under `logs/20260904-210205-lazy-existing-tests/`.
`tmp/run-lazy-memtable-comparison.sh` completed successfully in the dev distrobox,
writing to
`logs/20260904-210325-lazy-memtable-comparison/`.
It executed 12 serial N=100 runs: two repeats per mode for empty and flushed
cases, plus profiled flushed and diagnostic empty-heap runs for each mode.
All runs passed without failed writes, verification mismatches, phase errors,
or profiler warnings. `comparison.json` and the recipe are in the batch root.

Matched eager and lazy empty-table dumps confirm 14,100 private state objects
and 800 user shards disappear; the estimated size is 861,600 bytes total,
8.41 KiB per table. Created and settled dumps agree. The lazy tables share one
empty holder and array; these shared objects are excluded from per-table counts.
Heap diagnostics are eager `20260904-211139-residency-never-written-100t` and lazy
`20260904-211324-residency-never-written-100t` under the batch root. Exact counts
and estimated sizes are recorded in `logs/20260904-211415-inspect-empty-trie-ownership.log`.

Unprofiled eager empty settled heap is 106.270/106.276 MiB; lazy is
105.004/105.159 MiB. Eager flushed settled heap is 111.791/112.095 MiB; lazy is
110.566/110.398 MiB. Mean differences are 1.19 MiB empty and 1.46 MiB flushed.
These whole-JVM deltas include system memtables and noise. Use the object walk
for direct user-state attribution.

There is a first-write tradeoff. Eager first-in-cycle medians are 0.221/0.232 ms;
lazy medians are 0.258/0.371 ms. Eager first p99 is 0.700/0.848 ms; lazy is
0.861/0.862 ms. Later-write p99 falls within eager variation. All controls deliver
about 100.24 writes/second within write windows. Do not call this equal maximum
throughput or claim no latency regression. The two repeats are low-load evidence;
no numeric acceptance threshold was agreed. Keep the eager control available.

Actual allocation recordings show shard-construction samples in lazy writes and
eager flushes, with none in the corresponding opposite paths. Lazy creation-phase
samples are schema/system-table mutation paths, not user-table constructors.
These are sparse samples, not exact allocated-byte counters. Profiled runs are
eager `20260904-211030-residency-written-flushed-100t` and lazy
`20260904-211215-residency-written-flushed-100t`. Analysis and rendered artifacts
are indexed by `.debug/lazy-memtable-allocation.md` and the final results report.

Phase 2 has been moved to DONE in TODO.md. Source and test changes are ready for
review, with no commits. Next design work is idle retirement to reclaim the
already-confirmed 1 MiB slabs held by lightly written cold tables. This lazy
change saves the empty shard graph but does not reclaim dirty data. Keep the
user's <=1,000-table execution constraint and local-mutation definition of idle.

Agents used: `lazy_ownership` (heap analysis and recovery/index tests),
`lazy_lifecycle` (lifecycle trace and unit/property tests), `lazy_harness`
(controls and production correctness review). Their reports live in `.debug/`
and `.reviews/lazy-memtable-correctness.md`. No production defect was found in
that source review. All work remains uncommitted.

## Phase 3a completion and continuation

Explicit retirement is complete. The user authorized this increment and requested
a reusable root run_tests.sh selecting a small set of flush unit tests. No new
production operation was needed: forceFlush(USER_FORCED), through forceBlockingFlush
in the harness, already switches dirty base/index memtables and uses phase 2’s
lazy replacement. CFS, schema and metrics remain resident. Automatic scheduling,
idle timestamps and age/size policy were not implemented.

The harness now accepts --explicit-retirement for idle-reactivate and rotating-bursts.
It flushes the current active subset after the observation pause. Per-cycle written,
pre-retire, reclaimed and read checkpoints establish activation, release of
accounted memory and pure-read dormancy. Intermediate checkpoints request no GC;
final matched live dumps establish actual slab disappearance. Completed retirement
requests count successful flush futures, not GC completion.

Root run_tests.sh defaults to six isolated classes: TrieMemtableRetirementTest (5),
TrieMemtableRetirementFailureTest (1), TrieMemtableLazyTest (7), the two trie
flush-range classes (4 each), and MemtableSizeHeapBuffersTest (3). All 24 passed
with zero failures/errors/skips in logs/20260904-231021-ai-test-memtable-lazy/.
The generated model passed four seeds × 64 operations through
run_property_tests.sh --lazy (logs/20260904-225630-ai-test-memtable-lazy/).
run_tests.sh --long passed eight cases covering nine three-table cluster scenarios
(logs/20260904-225654-many-tables-launch.log). Final build and Checkstyle passed:
logs/20260904-230931-ai-build.log. Total: 33 unique targeted cases. --harness retains
configuration-only tests; --lazy retains the earlier lifecycle/recovery selection.
Run through the existing Java 21 dev container: distrobox enter dev -- ./run_tests.sh.

The successful failure fixture holds a real write-order group, requests the flush,
then marks data directories unwritable before releasing the group. This targets
the async failure after switching. The test verifies readable retained data,
unchanged allocator ownership and dirty commit-log segment coverage. Earlier
fixtures either failed to block the token-boundary path or threw during synchronous
switching. Evidence: .debug/explicit-retirement-failure-fixture.md. Future scheduling
must handle both request-time exceptions and failed futures; a later clean flush
does not retry a previously failed memtable.

All 12 serial N100 comparisons passed with no failed writes, read mismatches, phase
errors or profiler warnings. Root: logs/20260904-230056-explicit-retirement-comparison/.
Recipe: tmp/run-explicit-retirement-comparison.sh, copied into the batch root.
Two repeats give mean settled heap reductions of 8.032 MiB for 10 repeatedly active
tables and 38.717 MiB for 40 tables touched by rotating bursts. Retired tables remain
readable, then initialize again on writes. All final user memtables are dormant.

Matched rotating-burst dumps (control 230639, retired 230802) show 40 user slab arrays
and 41,943,040 payload bytes versus zero. All 100 logical memtables remain present.
An independent whole-JVM slab-region count falls from 45 to 5. Logs:
20260904-230953-inspect-empty-trie-ownership.log and
20260904-230959-count-hprof-arrays.log. This establishes actual slab disappearance;
allocator accounting alone can reach zero before all trie buffers are discarded.

There is an allocation cost. Sampled heap peaks increase despite lower settled
heap. Profiled retirement allocates again on reactivation. The main flush allocation
is the eager tombstone-histogram spool: current defaults allocate 3 MiB array payload
per SSTable metadata collector. These stacks account for 78.7%/83.0% of sampled
retirement allocation weights. Writers for empty disk ranges incur this cost too.
No user compaction occurred in these workloads; zero-allocation cursor compaction
alone cannot remove these flush allocations. Consider lazy histogram storage and
avoiding empty-range writer creation before aggressive retirement. This is a
follow-up finding, not authorization to implement another phase.

Full measurements, limits and artifact paths: research/explicit_memtable_retirement.md.
Source/ordering review: .debug/explicit-memtable-retirement-lifecycle.md.
Allocation source evidence: .debug/explicit-retirement-allocation.md.
Agents: retirement_unit, retirement_lifecycle, retirement_harness. All work remains
uncommitted. Keep workloads <=1,000 tables and preserve the existing user files.
