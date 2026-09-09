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

# Optional idle flushing with TrieMemtable and UCS

Date: 2026-09-09. Starting production code: `216cb4ba58`.
Plan: [ucs-idle-flush.md](../.plans/ucs-idle-flush.md).

## Question and implementation

Can previously written tables return close to their empty-table heap footprint
after local writes stop? What compaction cost does that create with Unified
Compaction Strategy (UCS)?

The new policy is disabled by default. To opt in at node startup:

```yaml
memtable_idle_timeout: 30s
memtable_idle_flush_max_concurrent: 2
```

The shipped timeout is `0s`. The policy applies only to user tables with lazy
TrieMemtable and UCS. Other memtables, eager TrieMemtable, other compaction
strategies, system keyspaces, and index backing tables do not independently
enter the policy. The normal base-table flush still coordinates its indexes.

Each initialized trie shard records its last completed write under the existing
write lock. Reads do not update that time. An initialized eligible memtable
registers once with a node-wide controller. Empty replacements have no entries.
The disabled path performs no new clock reads or scheduling. Enabled writes add
a clock read and a volatile timestamp store; they do not update a shared queue
or acquire a new lock.

The controller scans at most 16,384 candidates per pass. A periodic pass runs
every 100 ms. Flush completion and reader reclamation also request a coalesced
pass, so fast flushes need not wait for another timer tick. Before submitting a
flush, ColumnFamilyStore rechecks the current memtable generation, table validity,
strategy, and idle time under its existing lifecycle lock. A concurrent write can
still join an admitted flush through the existing write-order barrier. The
timeout is an admission condition, not a strict cutoff for each individual write.

The concurrency limit includes flushed memtables waiting for readers to release
their storage. A stalled reader can therefore slow further idle admission. This
keeps idle retirement from accumulating an unbounded set of unreclaimed old
memtables. Other flush reasons retain their existing limits. A failed idle flush
stops automatic idle admission until restart and logs an error. Existing flush
failure handling retains responsibility for the data.

Switching or dropping a memtable unregisters it. A strategy change into or out of
UCS, through schema or a local JMX override, switches an initialized tracked state
as needed. Node shutdown stops idle
admission before closing flush executors. The tests cover a node restart too.

The operation releases trie buffers and memtable allocator storage. Schema,
ColumnFamilyStore, metrics, JMX registrations, and SSTable readers remain resident.
Metric history is not reset. Legacy secondary-index reads can write cleanup
tombstones into index memtables. Those backing tables do not independently enter
this policy, so base-table dormancy does not guarantee index-memtable dormancy.
This is a policy for a bounded recently written set;
it cannot fit a million simultaneously hot memtables into a small heap.

## Measurement method

The existing residency harness now supports UCS options, overwrite traces,
automatic idle flushing, per-cycle settled checkpoints, and compaction history.
Automatic runs wait for dirty memtables and reclamation to drain before each
settled checkpoint. This avoids mistaking an empty executor queue before the
idle deadline for completed retirement. Every cycle reads and verifies the data.

Common settings: Java 21, BTI SSTables, cursor compaction enabled, lazy
TrieMemtable, compact JMX registration/history, transient JMX access, lazy metric
IDs, and compact metric bookkeeping. A copy of `simple_metrics.yml` enables
`BytesFlushed`, `CompactionBytesWritten`, and `MemtableSwitchCount` in every mode.
Those additional counters make the comparison observable without changing one
side's metric footprint. Raw arguments and the derived profile accompany each run.

The main comparison uses 100 tables, all written, four rows per table per cycle,
12 cycles, 256-byte values, one partition per table, and a 2 GiB heap ceiling.
Appends add new clustering rows. Overwrites repeatedly update the same four rows.
The automatic timeout is shortened to one second for this experiment. The driver
offers 10,000 writes/second through a sequential coordinator; this is a residency
experiment, not a sustained-throughput capacity benchmark. The test node uses
three data directories on the same host.

Heap numbers are whole test-JVM used heap after reclamation, compaction settling,
and an explicit garbage collection. They include schema, metrics, driver state,
system compaction history, and SSTable metadata. They are not exact per-table
dominator sizes or operating-system resident-set measurements. Allocation profiles
were disabled; `-e alloc` would answer a different question.

## Before production changes

Only harness changes were built for these runs. The production path still used
the starting branch code. All four runs completed 4,800 writes and passed reads.

| Mode | Settled heap, MiB | Above created heap, MiB | Initialized memtables | SSTables | Compaction output bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle off, append | 254.03 | 203.21 | 100 | 0 | 0 |
| Explicit retirement, append | 59.56 | 8.65 | 0 | 300 | 2,285,872 |
| Idle off, overwrite | 251.88 | 201.09 | 100 | 0 | 0 |
| Explicit retirement, overwrite | 58.52 | 7.69 | 0 | 300 | 331,109 |

The explicit controls flush each active table after the idle observation period.
They establish what successful automatic retirement should reclaim.

Raw evidence: `logs/20260909-154504-ucs-idle-baseline/`.

## Iteration and final 100-table comparison

The initial controller test run passed five of six cases. The remaining case
found that schema reload can construct a replacement memtable before publishing
the updated metadata reference. Eligibility now reads the active compaction
manager when initializing shard storage. The test then passed.

Code inspection also found that timer-only admission would cap fast completions
at roughly 20 tables/second with two permits and a 100 ms interval. Completion
and reclamation wakeups remove that artificial cap. The measured drain time still
includes the actual table-switch, flush, and storage-reclamation work.

A later review found that local JMX compaction overrides bypass schema reload.
A new regression test failed before the fix: the initialized memtable did not
switch when the local strategy changed into UCS. The override path now refreshes
tracking when its strategy changes. Schema checks use the active strategy too,
even when schema metadata still names the strategy from before a JMX override.
The pre-fix failure is in `logs/20260909-162030-idle-jmx-regression.xml`.
The 100-table and long measurements below precede this lifecycle-only correction;
they use fixed UCS throughout and never call the override path.

| Mode | Settled heap, MiB | Above created heap, MiB | Initialized memtables | SSTables | Compaction output bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle off, append | 254.12 | 202.20 | 100 | 0 | 0 |
| Automatic, append | 59.26 | 8.47 | 0 | 300 | 2,285,674 |
| Idle off, overwrite | 252.75 | 201.95 | 100 | 0 | 0 |
| Automatic, overwrite | 59.42 | 9.60 | 0 | 300 | 331,061 |

Automatic retirement saves 194.86 MiB for appends and 193.33 MiB for overwrites,
about 77% of whole-JVM settled heap. The result closely matches explicit
retirement. All four runs passed 4,800 writes and every read verification.
All final compaction queues were empty.

Automatic append flushes wrote 1,311,407 bytes; compaction wrote another
2,285,674 bytes. That is 1.74 compaction-output bytes per flush-output byte,
excluding commit log writes and other SSTable components. Overwrite compaction
wrote only 0.25 bytes per flush byte because it removed superseded versions.

Observed request service time after excluding cycle zero:

| Workload / operation | Idle off median / p99, ms | Automatic median / p99, ms |
| --- | ---: | ---: |
| Append, first write to each table in a cycle | 0.120 / 0.790 | 0.184 / 1.817 |
| Append, remaining writes | 0.116 / 0.220 | 0.108 / 0.250 |
| Append, reads | 0.247 / 1.372 | 0.313 / 0.900 |
| Overwrite, first write to each table in a cycle | 0.124 / 0.695 | 0.175 / 1.778 |
| Overwrite, remaining writes | 0.119 / 0.204 | 0.103 / 0.265 |
| Overwrite, reads | 0.224 / 0.720 | 0.277 / 1.311 |

Reactivation has a visible cost. These short, single-host samples do not establish
a durable throughput regression or improvement for already-active writes. The
off-append run also overlapped the end of targeted testing. Treat latency tails
as diagnostic observations, not production performance guarantees.

Raw evidence: `logs/20260909-160611-ucs-idle-post/`.

## UCS behavior over 48 cycles

Normal flush completion already submits background compaction. An extra periodic
compaction trigger would not solve the problem observed here. With `T4`, settled
file counts repeat 1, 2, 3 per table, then return to 1 after compaction. Append
compaction output grows as the previously merged data participates again.

UCS uses a minimum 1 MiB base for its compaction hierarchy in
`Controller.getBaseSstableSize`. These approximately 1 KiB flushes and their
aggregates remain in the same lowest level for many cycles. A `T4,L10` hybrid
therefore behaves like `T4` until data reaches higher levels. Higher tiering
fanout can reduce merge frequency at the cost of more live files. Keeping every
small-file compaction in that same level can preserve the repeated-rewrite cost.

The branch already defaults `min_sstable_size` to 100 MiB. UCS can choose one
shard below `base_shard_count`, and the measured runs do so. This setting controls
sharding; it does not prevent a small flush or lower the hierarchy's 1 MiB base.
The one-partition trace cannot establish sharding behavior for broad partition
distributions. No new compaction strategy is introduced in this change.

The longer comparison used 20 tables, 48 cycles, 3,840 writes per run, and a
100 ms experimental timeout. Each run completed with zero initialized memtables,
zero pending compactions, and correct data.

| Workload / UCS setting | Settled heap, MiB | SSTables | Compaction jobs | Compaction output bytes | Output / flush bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Append / T4 | 47.98 | 60 | 300 | 8,150,070 | 7.79 |
| Append / T8 | 49.61 | 120 | 120 | 3,325,329 | 3.18 |
| Append / T4,L10 | 48.84 | 60 | 300 | 8,148,252 | 7.79 |
| Overwrite / T4 | 48.78 | 60 | 300 | 331,250 | 0.32 |

`T8` reduced append compaction output by 59.2%, but doubled the final file count
and increased measured heap. `T4,L10` did not change the small-file behavior.
The append runs ended with about 1 MiB of live Data.db content across all 20
tables; the overwrite run ended with about 64 KiB. Equal logical writes do not
mean equal live data for these two workloads.

The next compaction experiment should test a smaller optional minimum hierarchy
base, so a merged small file can advance to another size level. That is a narrower
change than introducing a second compactor or a strategy that always merges back
into the lowest level. It needs separate validation across size transitions,
partition distributions, repaired/unrepaired groups, and sustained ingestion.
This implementation keeps UCS selection unchanged. For the residency objective,
retain `T4` as the comparison setting; `T8` is an operator tradeoff when extra
reader residency is acceptable.

Raw evidence: `logs/20260909-161115-ucs-idle-long/`.

## Real 30-second timeout

The final-code smoke test used three tables and two cycles at `30000ms`.
All 24 writes and every read verification passed. At the last observation
samples near 29.8 seconds, all three tables were still dirty. The automatic drain
then completed about 0.20 seconds after the 30-second observation ended in each
cycle. Both policy checkpoints had zero initialized memtables. Reads kept them
dormant, and the next cycle recreated their storage. The final six SSTables were
below UCS's four-file compaction threshold per table, as expected.

Raw evidence: `logs/20260909-162651-ucs-idle-smoke/`.

An earlier smoke run in `logs/20260909-162349-ucs-idle-smoke/` is invalid. A test
wrapper rebuilt the shared JAR during the run, causing ClassNotFoundException.
That process was terminated after its failed drain check. The successful repeat
ran after all builds and tests finished. It supplies the timeout evidence above.

## 1,000-table comparison

The larger pair uses four cycles, 16,000 writes, an 8 GiB JVM ceiling, and an
explicit 256 MiB memtable pool on both sides. The 100-table runs used the harness's
10 MiB pool and a 2 GiB JVM ceiling. Compare within each matched pair; these are
not interchangeable heap configurations for a per-table extrapolation.

The first larger attempt in `logs/20260909-162824-ucs-idle-scale/` exposed the
independent 10 MiB pool limit. Its disabled control retained only 894 initialized
memtables and already had 517 SSTables from ordinary pressure flushing. Increasing
only `-Xmx` had not changed that limit. The companion automatic run was stopped,
and both modes were restarted with the larger pool. The benchmark now rejects a
disabled control that flushed user tables. That check was also tested against
the contaminated control and correctly rejected it.

| Mode | Settled heap, MiB | Above created heap, MiB | Initialized memtables | SSTables |
| --- | ---: | ---: | ---: | ---: |
| Idle off | 1,124.14 | 1,021.45 | 1,000 | 0 |
| Automatic | 142.23 | 39.57 | 0 | 1,000 |

The saving is **981.91 MiB, or 87.35% of settled heap**. Both runs passed all
16,000 writes and 5,000 read queries. The disabled control had no user-table
flushes. The automatic run logged exactly 4,000 user-table flushes, all with
`MEMTABLE_IDLE`, and completed 1,000 UCS compactions. Final compaction queues
were empty. Flushes wrote 4,372,000 Data.db bytes; compaction wrote 4,358,818 bytes.

Admission and drain remain a constraint. After the one-second observation,
retiring all 1,000 tables required another **31.88–42.23 seconds per cycle**
with the default two permits. The 100-table runs required another 3.13–4.34
seconds for appends. These are measurements of this verbose, single-host harness
with three data directories, not a calibrated production flush-throughput limit.
Profile this work with normal production logging before changing admission limits
or scheduling. Increasing the timeout does not increase the service rate.

Observed 1,000-table request service times, excluding cycle zero:

| Operation | Idle off median / p99, ms | Automatic median / p99, ms |
| --- | ---: | ---: |
| First write to each table in a cycle | 0.039 / 0.102 | 0.558 / 0.968 |
| Remaining writes | 0.038 / 0.086 | 0.048 / 0.101 |
| Reads | 0.093 / 0.161 | 0.147 / 0.307 |

Reactivation is substantially more expensive in this run. Remaining writes and
reads were also slower. The policy adds timestamp work on every enabled write,
and repeated reactivation creates allocation and garbage-collection work. These
measurements do not isolate those causes. A steady-state throughput test is
needed before assigning a durable performance cost to the timestamp operation.

The automatic run still adds 39.57 MiB above its created-table checkpoint, about
40.5 KiB per written table in this workload. SSTable readers, populated metrics,
and system activity contribute to that total. An ownership census is needed to
separate them; the total is not an attribution to SSTable metadata alone.

Raw evidence: `logs/20260909-163641-ucs-idle-scale/`. Both command exit codes and
the benchmark's comparison checks passed. No experiment exceeded 1,000 tables.

## Validation and reproduction

`./run_tests.sh --idle-flush` passed 58 tests across eleven classes after the final changes:

- `IdleMemtableFlusherTest`: deadlines, read activity, renewed writes, stale
  generations, pinned readers, bounded admission, schema/JMX changes, disabled mode,
  failure, drop, and eight generated traces of 40 write/delete/flush steps.
- `IdleMemtableFlushTest`: actual scheduled flushing, exclusions, reactivation,
  restart, the default disabled configuration, and index queries through
  updates/deletes with both Storage-Attached Indexing and legacy local indexes.
- Existing retirement, flush-failure, lazy-initialization, and recovery/index tests.
- Trie flush-set tests for both partitioner types and heap-buffer accounting tests
  for TrieMemtable, SkipListMemtable, and ShardedSkipListMemtable.
- Residency harness configuration tests and `SettingsTableTest`.

The generated traces also run through `./run_property_tests.sh --idle-flush`;
the wrapper runs the containing test class. Test evidence is preserved in
`logs/20260909-163335-ai-test-memtable-lazy/`. Build and Checkstyle passed.

Use the project's JDK/build environment. Build before running comparisons;
the benchmark script deliberately reuses those classes. Each script writes
timestamped evidence under `logs/` and keeps command exit status.

```bash
./run_tests.sh --idle-flush
uv run --no-project --offline --python venv/bin/python python .build/sh/benchmark_ucs_idle.py baseline
uv run --no-project --offline --python venv/bin/python python .build/sh/benchmark_ucs_idle.py post
uv run --no-project --offline --python venv/bin/python python .build/sh/benchmark_ucs_idle.py long
uv run --no-project --offline --python venv/bin/python python .build/sh/benchmark_ucs_idle.py smoke
uv run --no-project --offline --python venv/bin/python python .build/sh/benchmark_ucs_idle.py scale
uv run --no-project --offline --python venv/bin/python python .build/sh/analyze_ucs_idle.py logs/<run-directory>
```

The `baseline` suite selects manual retirement on the current compiled code.
To reproduce the actual pre-change production control, use the starting code
with only the harness additions. Do not run performance comparisons concurrently
with tests or other benchmarks.

## Limits relevant to one million tables

The timeout bounds age, not the count or bytes of active memtables. A large burst
can initialize many tables before any becomes idle. Flush admission and disk
throughput can delay retirement after the deadline. A very slow reader can hold
the bounded reclamation permits. The candidate map also retains its peak backing
array capacity after entries leave, though dormant table objects are not entries.

Even after retirement, every written table still needs SSTable files and reader
metadata. More aggressive tiering trades rewrite work for more of that residency.
Schema and monitoring residency remain. A million-table claim needs both a bounded
write working set and measurements of these remaining per-table/per-file costs.

Next work:

1. Census the heap after retirement, with matched one-, three-, and six-SSTable
   cases. Separate populated metrics, SSTable readers, schema, and allocator
   reservations. Profile admission/flush work under normal logging and determine
   whether byte-based admission is needed to protect against wide write bursts.
2. Benchmark an optional smaller UCS hierarchy base. Preserve the existing base
   by default, and validate transitions to larger files before adopting it.
   Cursor compaction can reduce computation and allocation; it does not remove
   disk rewrites or retained SSTable metadata.
