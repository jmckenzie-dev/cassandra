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

# Memtable residency baseline, 2026-09-04

## Scope

Step 1 adds the residency harness and captures current eager-allocation behavior.
It does not change production memtables, metrics, or flush policies. Executed
experiments stay below the requested limit of 1,000 tables; baselines use 100.

The [plan](../.plans/memtable-residency-baseline.md) and
[harness guide](../.build/memtable-residency.md) describe implementation and use.
The raw batch is in
[`logs/20260904-171404-residency-baselines`](../logs/20260904-171404-residency-baselines/).
These logs are ignored and do not travel with a normal checkout. This report
therefore retains the important values and configuration. Each run has its exact
arguments and selected effective configuration in `summary.json`.

## Configuration

- One node in a fresh Java Virtual Machine (JVM) and fresh node directories per run.
- OpenJDK 21.0.12, maximum heap 8 GiB, initial heap 512 MiB, eight reported processors.
- Explicit `TrieMemtable` default, BTI SSTable format, heap-buffer allocation.
- User tables use size-tiered compaction with thresholds 4 and 32.
- Cursor compaction is disabled. The experiments do not assume its future cost.
- Cassandra key and row caches are disabled for user tables. Other caches retain
  their existing settings. Read measurements do not represent a cold disk cache.
- One partition per table, four new clustering rows per active table per cycle,
  seeded 128-byte printable ASCII values, seed 1.
- Default write schedule: 100 aggregate writes/second. Samples: one second apart.
- Default final hold: one second. Explicit-flush runs use four write cycles.
- Empty and written/flushed controls repeat in alternating order without profiling.

The earlier research runs used `SkipListMemtable`: their
[N=100 histogram](../logs/20260903-102943-many-tables-100t/histogram-02-after.txt)
contains 157 SkipListMemtable objects and no TrieMemtable objects. The old context
note's assumption about the test default was incorrect. The new harness records
the actual memtable class. A separate SkipList control is kept outside the main
Trie comparison. Selecting the node default also affects system memtables, so
that comparison does not isolate user-table object ownership.

## Validation

- Build and Checkstyle passed:
  [`20260904-171126-ai-build.log`](../logs/20260904-171126-ai-build.log).
- Final tests passed, `OK (4 tests)`:
  [`20260904-171224-many-tables-launch.log`](../logs/20260904-171224-many-tables-launch.log).
  These include 500 generated schedule/configuration cases, invalid-input checks,
  payload properties, and five real-cluster scenarios with full row verification.
- All 13 baseline/control/diagnostic runs at N=100 completed with no failed
  writes, verification failures, phase errors, or profiler warnings.
- Additional compatibility smoke runs passed:
  [original creation harness, N=1](../logs/20260904-172641-many-tables-1t/summary.json),
  [SkipList/BIG, N=3](../logs/20260904-172717-residency-written-flushed-3t/summary.json),
  [ShardedSkipList/BTI, N=3](../logs/20260904-172744-residency-written-flushed-3t/summary.json).
- The build wrapper previously treated the string `false` as a true shell test
  and skipped Checkstyle by default. The corrected branch runs both source and
  test checks and retains the raw build log.

## Results

All heap figures below are MiB (1,048,576 bytes), for the whole JVM after a GC
request and observed completion of current background work. Memtable counts are
for the 100 user tables.

| Scenario | Writes | Final heap MiB | Live / dirty memtables | SSTables | Flushes |
|---|---:|---:|---:|---:|---:|
| Never written, repeat 1 | 0 | 106.054 | 100 / 0 | 0 | 0 |
| Never written, repeat 2 | 0 | 106.321 | 100 / 0 | 0 | 0 |
| Written/flushed, repeat 1 | 1,600 | 111.498 | 100 / 0 | 100 | 400 |
| Written/flushed, repeat 2 | 1,600 | 111.611 | 100 / 0 | 100 | 400 |
| Idle/reactivate, 10 active, two 31-second pauses | 80 | 116.864 | 100 / 10 | 0 | 0 |
| Rotating bursts, 10 active per cycle, 40 touched | 160 | 146.887 | 100 / 40 | 0 | 0 |
| Trickle, 10 active, about 16 seconds | 160 | 116.022 | 100 / 10 | 0 | 0 |
| Trickle, 10 active, about 40 seconds | 400 | 116.654 | 100 / 10 | 0 | 0 |
| Written/flushed, profiling enabled | 1,600 | 112.194 | 100 / 0 | 100 | 400 |
| SkipList empty-table control, repeat 1 | 0 | 105.208 | 100 / 0 | 0 | 0 |
| SkipList empty-table control, repeat 2 | 0 | 105.207 | 100 / 0 | 0 | 0 |

The idle run verifies reads after both pauses and after settlement. The current
policy leaves the dirty data in memory. Explicit flushing clears user memtable
data but leaves 100 live replacement memtables. The explicit-flush runs also
complete compaction: 400 flush outputs become 100 live SSTables.

The two unprofiled written/flushed runs completed about 100.24 and 99.84 writes
per second within write windows. Their write service p99 values were 0.714 and
0.869 ms. Arrival p99 values were 0.783 and 2.606 ms. One repeat had a maximum
start delay of 91.35 ms, which the arrival measurements preserve. These short,
low-load runs are functional baselines, not saturation or latency guarantees.

The longer trickle run schedules 400 writes at 10/second, alternating across 10
tables. Each table receives a write about once per second, over a period longer
than the proposed 30-second age threshold. It has no intermediate read or GC
phases. This is a useful baseline for distinguishing future maximum-age flushing
from idle retirement.

## Confirmed first-write slab cost

The rotating workload writes just four small rows to each of 40 tables. Its
settled user memtable counters report 27,680 data bytes and 75,680 accounted heap
bytes, while whole-JVM heap rises by about 40 MiB after creation.

A separate diagnostic run captured live heap before and after those writes:
[`20260904-172558-residency-rotating-bursts-100t`](../logs/20260904-171404-residency-baselines/20260904-172558-residency-rotating-bursts-100t/).

| Heap dump | One-MiB byte arrays | Arrays reachable through slab Region.data → HeapByteBuffer.hb | Payload bytes in those slabs |
|---|---:|---:|---:|
| `created.hprof` | 5 | 5 | 5,242,880 |
| `settled.hprof` | 45 | 45 | 47,185,920 |
| **Increase** | **40** | **40** | **41,943,040 (40 MiB)** |

The extraction walks heap records, resolves class fields and superclass fields,
and follows those references. It validates record boundaries and instance field
lengths. The [extraction output](../logs/20260904-172651-count-hprof-arrays.log)
records the counts. This is direct array/reference evidence, not a full dominator
analysis of every table component.

The source matches the measurement:

- [`SlabAllocator.REGION_SIZE`](../src/java/org/apache/cassandra/utils/memory/SlabAllocator.java#L49)
  is 1 MiB.
- [`allocate`](../src/java/org/apache/cassandra/utils/memory/SlabAllocator.java#L81)
  charges the requested slice size to the pool.
- [`getRegion`](../src/java/org/apache/cassandra/utils/memory/SlabAllocator.java#L130)
  allocates an entire region when the allocator has none available.

Under this heap-buffer configuration, very small writes can therefore keep much
more memory resident than allocator counters report. First-write slab reservation
is a concrete target for idle flushing and for a separate smaller-initial-region
experiment. Lazy construction of never-written tables alone will not release
regions already allocated by cold dirty tables. No production fix is included.

## Allocation recording validation

The profiled written/flushed run produced 19 per-phase async-profiler recordings
and JDK recordings, with no reported phase errors or profiler warnings.
`ResourceProfiler` already requests `event=alloc,wall,cpu`, which includes the
allocation event selected by the async-profiler command-line `-e alloc` option.

`jfr summary` on `03-cycle-000-flush.ap.jfr` confirms 897
`jdk.ObjectAllocationInNewTLAB` events, 219 execution samples, and 1,924 wall-clock
samples. The allocation event is present; no additional run is needed just to
enable it. Use an allocation-specific view when rendering the recording.

The profiled run's write service p99 was 0.940 ms, compared with 0.714–0.869 ms
in the two controls. Its settled heap was 112.194 MiB versus 111.498–111.611 MiB.
The small sample does not establish an exact profiler overhead; keep controls.

The [recording validation log](../logs/20260904-171404-residency-baselines/20260904-172113-residency-written-flushed-100t/recording-validation.log)
also confirms 100 allocation events in the first write phase. Both recordings
converted successfully to allocation flame graphs:
[first writes](../logs/20260904-171404-residency-baselines/20260904-172113-residency-written-flushed-100t/03-cycle-000-alloc.html)
and [first flushes](../logs/20260904-171404-residency-baselines/20260904-172113-residency-written-flushed-100t/03-cycle-000-flush-alloc.html).

## Interpretation limits

In the main Trie runs, starting JVM heap varied from about 68 to 73 MiB, while
post-creation heap stayed near 106 MiB. Do not divide a single before/after delta by 100 and
call it exact retained bytes per table. Use matched repeats and heap ownership
analysis. First schema operations, system tables, driver activity, and shared
initialization contribute to the delta.

Allocator counters are not retained-size measurements. For example, the idle
run reports 13,840 bytes of user memtable data and 35,880 accounted heap bytes;
the whole-JVM heap increase is much larger. The empty and flushed cases report
zero user allocator bytes despite keeping 100 live memtables. Heap dominators
are still needed to assign that retained memory.

Only the written/flushed scenario forces a flush. Policy checkpoints precede GC;
settled checkpoints wait for observed flush/compaction/reclamation completion
without flushing the remaining dirty data. Periodic work can occur afterward.
Heap dumps are separate diagnostic runs because their pauses affect timing.

The sampler and client share the node JVM. The driver is paced and serial,
records overdue operations, and does not drop requests to maintain a nominal
rate. Sampled heap maxima do not establish exact peaks. Background allocation
must be read from flight recordings; the shared phase allocation delta covers
only the harness main thread. Existing test logging also contributes overhead.

## Reproduction and next comparison

The batch retains [the main recipe](../logs/20260904-171404-residency-baselines/run-residency-baselines.sh),
[the additional controls](../logs/20260904-171404-residency-baselines/run-residency-controls.sh),
[the comparison data](../logs/20260904-171404-residency-baselines/comparison.json),
and the extraction scripts. The [harness guide](../.build/memtable-residency.md)
documents every option independently of these local artifacts.

To reproduce the 40-second trickle case with a fresh node:

```bash
.build/sh/ai-profile-memtable-residency --scenario trickle --tables 100 --active-tables 10 --rows-per-table 4 --cycles 10 --rate 10 --payload-bytes 128 --seed 1 --hold-ms 1000 --no-profile
```

For the next implementation, compare never-written and written/flushed cases
first. Then compare idle retirement against the 31-second idle and rotating-burst
baselines. Use the longer trickle baseline for maximum-age flushing. Preserve
the distinction between empty object overhead, used data bytes, and reserved
slab capacity. Do not use allocator-accounted bytes alone as the memory budget.
