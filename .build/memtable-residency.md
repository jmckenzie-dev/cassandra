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

# Memtable residency experiments

Run from the repository root with JDK 21 and an existing Cassandra build:

```bash
.build/sh/ai-build
./run_tests.sh
./run_tests.sh --harness
./run_tests.sh --long
./run_tests.sh --lazy
./run_property_tests.sh --lazy
./run_tests.sh --histograms
./run_tests.sh --meters
./run_tests.sh --reservoirs
./run_property_tests.sh --histograms
./run_property_tests.sh --meters
./run_property_tests.sh --reservoirs
.build/sh/ai-profile-memtable-residency --scenario never-written --tables 100 --no-profile
```

`run_tests.sh` defaults to isolated memtable retirement, flush-range, and memory
accounting unit tests through the standard Ant runner. `--harness` selects the
harness configuration tests through the profiling launcher. `--long` adds five
small real-cluster scenarios, eager empty/flushed controls, and explicit
retirement with reactivation and rotating bursts. `run_property_tests.sh`
exercises generated configurations and schedules. Each launcher writes console
output to a timestamped file in `logs/` and preserves failures.

`--lazy` selects the production lazy-initialization lifecycle and recovery tests
through the standard Ant test runner. `run_property_tests.sh --lazy` selects the
generated mutation/flush/truncate model. Both compile tests first and retain
timestamped console logs.

## Workloads

| Scenario | Behavior |
|---|---|
| `never-written` | Create tables, hold, then verify they are empty. |
| `written-flushed` | Write each cycle, explicitly flush user tables, observe, verify. |
| `idle-reactivate` | Write a fixed subset, wait, verify, and write the same subset again. |
| `rotating-bursts` | Move the active subset through a seeded table permutation each cycle. |
| `trickle` | Pace all cycles continuously with no intermediate reads or idle gaps. |

For example:

```bash
.build/sh/ai-profile-memtable-residency --scenario idle-reactivate --tables 100 --active-tables 10 --cycles 2 --rows-per-table 4 --rate 100 --idle-ms 31000 --hold-ms 1000 --seed 1 --payload-bytes 128 --no-profile
```

| Option | Default | Meaning |
|---|---:|---|
| `--tables` | 100 | Total schema tables, at least one. |
| `--active-tables` | all | Tables written in each cycle. |
| `--rows-per-table` | 4 | New clustering rows per active table per cycle. |
| `--cycles` | 2 | Write cycles; ignored for never-written. |
| `--rate` | 100 | Aggregate scheduled writes/second, within a write phase. |
| `--payload-bytes` | 128 | Seeded printable ASCII value length; excludes keys/protocol overhead. |
| `--seed` | 1 | Table permutation and per-row payload seed. |
| `--idle-ms` | 1000 | Observation pause per cycle; ignored by trickle. |
| `--hold-ms` | 1000 | Final observation period. |
| `--sample-ms` | 1000 | Delay between resource samples. |
| `--settle-seconds` | 60 | Timeout waiting for current background work. |
| `--memtable` | TrieMemtable | Also accepts SkipListMemtable or ShardedSkipListMemtable. |
| `--eager-memtable` | off | Initialize TrieMemtable storage eagerly for a same-build control; requires TrieMemtable. |
| `--explicit-retirement` | off | Flush the active subset after each observation pause; requires idle-reactivate or rotating-bursts. |
| `--lazy-tombstone-histograms` | off | Allocate the original tombstone spool on the first deletion or expiration observation. |
| `--geometric-meter-arrays` | off | Use separate meters whose shared rate array doubles when full. |
| `--legacy-metrics` | off | Set `optimized_metrics_enabled: false`; the default uses optimized metrics. |
| `--subnet` | 0 | Use node address `127.0.N.1`, where N is 0–255, to isolate local test listeners. |
| `--format` | bti | Also accepts big. |
| `--out` | logs | Parent for timestamped run directories. |
| `--no-profile` | off | Disable async-profiler and Java flight recordings. |
| `--heap-dumps` | off | Dump live heap at baseline, created, and settled checkpoints. |

The launcher accepts `MANY_TABLES_XMX` (default 8g) and `MANY_TABLES_CPUS`
(default 8). Every invocation starts a fresh JVM and node directory. User tables
use size-tiered compaction and disable Cassandra key/row caching. Cursor compaction
is disabled. Compression, chunk caching, and operating-system page caching retain
the node defaults. Effective table parameters and selected node/JVM settings are
in `summary.json`. TrieMemtable defers its shard storage until the first write.
`--eager-memtable` sets its `lazy_initialization` factory parameter to `false`.
Both initialization modes retain existing flush triggers. Explicit retirement
uses normal user-forced flushing. Automatic idle selection and age/size policies
remain future work. Selecting the node's default memtable also changes system
memtables, so whole-JVM comparisons include that effect.

## Resident metrics comparisons

The harness sets the node YAML option `optimized_metrics_enabled` to true unless
`--legacy-metrics` is present. The option selects the metrics implementation before
node startup. Requested and effective `optimizedMetricsEnabled` values appear in
`summary.json`. Keep the earlier tombstone and geometric-meter experiment flags
fixed across each comparison.

The comparison wrapper assigns a distinct subnet to each run, beginning at 71.
Use `--subnet-start N` to select another unused range. The run summary records
the requested subnet and effective listen/RPC addresses and ports. The standalone
harness keeps subnet 0 by default. Separate addresses avoid conflicts with tests
that use `127.0.0.1`; the workload and metrics selection stay the same.

Run a fresh comparison before, during, and after each optimization:

```bash
.build/sh/ai-compare-resident-metrics --step 1 --checkpoint pre
.build/sh/ai-compare-resident-metrics --step 1 --checkpoint peri
.build/sh/ai-compare-resident-metrics --step 1 --checkpoint post
```

In the development container, prefix each command with `distrobox enter dev --`.
The script defaults to 100 tables and two repetitions. It alternates legacy and
optimized order across repetitions and runs both `never-written` and
`written-flushed` in separate JVMs. Written runs use one cycle with four rows per
table at 100 writes/second. All runs disable profiling and include the existing
settled post-GC heap measurement. `--tables` accepts 1 through 1,000;
`--repeats` controls repetitions. `MANY_TABLES_XMX` and `MANY_TABLES_CPUS` retain
the launcher defaults of 8g and eight processors.

Use `--heap-dumps --repeats 1` for separate retained-object diagnostics. Heap dumps
can affect timing and produce large artifacts; compare timing from runs without
that option. The script creates a timestamped directory under `logs/` with its
recipe and `runs.tsv`. Each row identifies the step, checkpoint, repetition,
implementation, scenario, exit status, and absolute `summary.json` path. The
corresponding run directory contains checkpoints and any requested heap dumps.
The script stops on a failed run and preserves its exit status.

`run_tests.sh --reservoirs` selects reservoir compatibility and export tests.
`run_property_tests.sh --reservoirs` selects generated reservoir equivalence tests.

## Java allocation candidates

Both candidate switches default to off. The harness sets the corresponding
`cassandra.lazy_tombstone_histograms` and `cassandra.geometric_meter_arrays`
properties before node startup and restores them after shutdown. This affects
system and user tables. Requested and effective values appear in `summary.json`.
Use the startup properties directly outside the harness; do not switch a running
node between implementations during a performance comparison.

The histogram candidate delegates observations to the unchanged original
algorithm with the same spool capacity. It saves the large allocation only for
writers that receive no deletion or expiration observations. The first such
observation pays the full spool cost plus a small initial empty delegate.
It does not change histogram precision or serialized values.

The meter candidate keeps independent rate storage and a background ticker.
It doubles the rate-array capacity to avoid copying the whole array for each
new meter. Spare capacity can increase retained rate-array memory. Registration
still uses the original copy-on-write list and can still grow quadratically.

`run_tests.sh --histograms` selects the reference builder tests, differential
tests, generated traces, collector serialization tests, and an allocation probe.
`--meters` selects the reference meter tests and candidate differential,
concurrency, cleanup, and generated tests. The matching property-runner options
run only the generated suites. `--long` also tests both candidate selections and
their combination in three-table retirement workloads.

Run each comparison after its correctness tests pass:

```bash
.build/sh/ai-compare-java-allocation histogram
.build/sh/ai-compare-java-allocation meter
source venv/bin/activate
python .build/sh/analyze-java-allocation.py logs/<comparison-directory>
```

Each comparison starts six fresh JVMs at 100 tables. Four unprofiled runs use
reference/candidate/candidate/reference order. A profiled reference/candidate
pair follows. Histogram runs retire ten active tables for two write cycles;
meter runs create never-written tables. The other candidate stays disabled.
The profiled meter pair also captures live heap dumps to measure spare rate-array
capacity; keep these diagnostic runs separate from unprofiled timing comparisons.
The analysis validates completed operations and retirement, then converts
allocation recordings with `--alloc`, separately reporting sample counts and
weighted bytes. Weighted bytes are estimates, not exact allocated-byte counts.

## Evidence

- `writes-*.csv`: scheduled, start, and completion times relative to the write
  phase; service time and latency from scheduled arrival; success status.
- `reads-*.csv`: verified partition reads and their service times. Each table has
  one partition. Reads verify all expected clustering rows and exact values.
- `*-samples.csv`: periodic whole-JVM heap and user-table counters during writes
  and observation periods, including pending flushes and SSTable counts. Explicit
  retirement adds sampled and profiled `03-cycle-NNN-retire` phases.
- `checkpoint-*.json`: named point measurements. `policy` precedes forced GC.
  `settled` follows observed completion of flushes, compactions, and allocator
  reclamation, then a requested GC. It does **not** force dirty memtables to flush.
  `verified` records state after the final data reads, without another GC.
  Explicit retirement also records per-cycle `-written`, `-pre-retire`,
  `-reclaimed`, and `-read` checkpoints. `-reclaimed` waits for the same background
  settlement condition, without requesting GC or taking a heap dump. It measures
  allocator reclamation completion; unreachable heap objects can await GC.
- `summary.json`: arguments, effective configuration, completed operations,
  failures, lateness, all checkpoints, and phase recording status.
- `*.ap.jfr` / `*.jdk.jfr`: allocation/execution recordings when profiling is on.
- Optional `*.hprof`: whole-JVM heap dumps for dominator analysis. Capture these
  outside throughput comparisons because pauses can affect later activity.

Profiled runs already request async-profiler's allocation event, equivalent to
the command-line `-e alloc` selection. The Java API receives
`event=alloc,wall,cpu` (or `event=alloc,wall` without CPU permission). To inspect
allocation, select `--alloc` when converting a phase recording with `jfrconv`.
The default view of a recording can show a different event. `--no-profile` runs
are explicit controls with no allocation recording. Allocation samples explain
where bytes were allocated; heap dumps explain which objects remain live.

`written-flushed` explicitly flushes all user tables. `--explicit-retirement`
flushes only the current cycle's active subset through
`ColumnFamilyStore.forceBlockingFlush(USER_FORCED)`, which waits on `forceFlush`.
Each request waits for flush completion; reclamation settlement follows in a
separate phase. Errors propagate and fail the run. The summary records the flag
and `completedRetirementRequests`. Reads then verify all data, including older
cycles and untouched tables. Later cycles activate new writable storage.
Read phases, flushes, and reclamation waits extend the time between bursts.
The continuous-trickle phase has no such interruptions.
SSTable byte counts cover data components, not all on-disk components.

The paced driver is serial. It does not drop overdue operations, so overload
extends the phase. Arrival latency and start lateness expose this; report completed
rate alongside the requested rate. Lateness above 1 ms is counted in the summary.
This harness is not an open-loop concurrent saturation driver.

Sampler scans and the client driver share the node JVM and add allocation. The
sampler runs independently of requests but sampling cannot establish the exact
heap peak. `allocatedBytesDelta` in the shared phase summary counts the main thread
only. Use JFR allocation stacks for background work. JDK recordings are cumulative
from JVM recording start; restrict analysis to phase timestamps.

Memtable memory counters report allocator-accounted bytes, not the full retained
object graph. Empty structures can consume heap while reporting zero allocated
data bytes. Node pool counters include system tables. Whole-JVM heap includes
driver objects, metadata, metrics, system tables, and compiled query state.
The existing platform MBean count does not describe isolated-node MBeans.
`initialized_trie_memtables` counts user TrieMemtables with initialized shard
storage, including flushing instances. It excludes other memtable types. Reads
and sampling should leave dormant TrieMemtables uninitialized. A zero count
does not mean that table metadata or the memtable lifecycle object was unloaded.

Settlement requires three consecutive quiet samples for current user-table work
and node allocator reclamation. It does not stop future periodic work. GC is a
request; use GC events/heap dumps to verify collection when attribution matters.
Read measurements retain the current cache state; no cold-cache claim is valid.

## Comparison sequence

The original eager baseline is in `research/memtable_residency_baseline.md`.
Compare lazy and eager TrieMemtable initialization using the same build:

```bash
.build/sh/ai-profile-memtable-residency --scenario never-written --tables 100 --no-profile --eager-memtable
.build/sh/ai-profile-memtable-residency --scenario never-written --tables 100 --no-profile
.build/sh/ai-profile-memtable-residency --scenario written-flushed --tables 100 --cycles 4 --no-profile --eager-memtable
.build/sh/ai-profile-memtable-residency --scenario written-flushed --tables 100 --cycles 4 --no-profile
```

The summary records the selected mode in `memtableInitialization` and the node's
configured parameters in `effectiveConfiguration.memtableParameters`. Lazy
initialization does not release dirty slabs; those remain allocated until a
flush and safe reclamation.

Compare explicit retirement with the existing lazy-only control:

```bash
.build/sh/ai-profile-memtable-residency --scenario rotating-bursts --tables 100 --active-tables 10 --cycles 4 --no-profile
.build/sh/ai-profile-memtable-residency --scenario rotating-bursts --tables 100 --active-tables 10 --cycles 4 --no-profile --explicit-retirement
```

Repeat the pair with `--scenario idle-reactivate` to measure repeated activation
of the same subset. Keep all other arguments fixed. Add `--heap-dumps` to separate
diagnostic runs and compare their `settled.hprof` files: the control retains dirty
storage, while the retired run has completed flushes and reclamation. Baseline
and created dumps identify shared initialization costs. No intermediate cycle
takes a heap dump or requests GC. Dirty allocator accounting can understate slab
capacity; use ownership analysis of the diagnostic heaps for physical savings.

Implement and validate automatic idle retirement and age/size flushing separately. Once available,
compare baseline, lazy, lazy+idle, lazy+age/size, and all policies using fresh JVMs
with the same inputs. Alternate run order and repeat measurements. Keep the
compaction implementation fixed; repeat when cursor compaction supports the target
format. Current baseline runs use N=100. Keep executed runs at or below 1,000
tables until the separate creation-allocation and timing work is complete.
