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

# Java histogram and meter allocation experiments

Updated 2026-09-05. Implementation is uncommitted. Both candidates are opt-in;
the original implementations remain the defaults. The user chose Java for this
experiment. No native library, new dependency, or JDK upgrade was added.

The compatibility constraint is exact recorded values. Differential tests feed
the same observations and controlled clock schedules to both implementations.
Independent benchmark runs measure different execution times and therefore do
not establish value equivalence on their own.

## Implementations

`LazyTombstoneHistogramBuilder` starts with the original builder configured with
a zero-capacity spool. This preserves empty snapshot capacity, hash codes and
release behavior. Its first observation creates the original builder with the
requested spool capacity. All insertion, merging, rounding and saturation still
run through the original algorithm. `releaseBuffers()` disables pending
initialization and preserves the reference's failure on subsequent updates.

`StreamingTombstoneHistogramBuilder` changes only its implemented interface.
`MetadataCollector` selects the candidate when
`cassandra.lazy_tombstone_histograms=true`. Constructor injection supports
side-by-side collector comparisons. The candidate preserves even the reference's
unrepresentable-capacity behavior by constructing the reference immediately for
spool sizes above 2^29. These exceptional inputs do not allocate positive huge
arrays. Both builders retain the existing single-writer contract.

The default spool rounds 100,000 entries to 131,072 slots. Its two backing arrays
contain 262,144 longs and integers: 3 MiB of payload per builder. A writer without
deletions or expiration observations avoids that allocation. A populated writer
still pays the full spool cost, plus a small initial empty delegate. Allocation
failure can therefore move to the first observation. Histogram precision and
draining thresholds do not change.

`GeometricThreadLocalMeter` preserves the original implementation independently
and doubles the shared rate-array capacity when required. Its counters, rate
arithmetic, tick boundaries, locking, offset reuse and cleanup match the original.
`Meter.create()` selects it when `cassandra.geometric_meter_arrays=true`. Both
registry meters and timers use the factory. `ThreadLocalMeter` remains unchanged.
The registry retains its Dropwizard meter return type.

Spare rate-array cells can increase retained memory by up to roughly the current
used capacity. The candidate retains the original copy-on-write registration
list, so it does not remove all quadratic allocation. The separate implementation
also duplicates maintenance: fixes to shared behavior must reach both classes
while this experiment remains in place.

The harness sets both properties before node startup, records requested and
effective values, and restores properties after shutdown. The switches affect
system and user tables. Production comparisons should select a mode at startup.

## Histogram comparison

The isolated HotSpot allocation probe measured the complete constructor,
optional update, snapshot and release lifecycle. All three eight-operation
samples returned the same per-operation counts:

| Exact calling-thread allocation per lifecycle | Reference | Lazy spool |
|---|---:|---:|
| No observations | 3,148,512 bytes | 2,816 bytes |
| One observation | 3,148,512 bytes | 3,149,928 bytes |

The empty path saves 99.91%; a populated lifecycle adds 1,416 bytes. The probe
does not measure retained heap or first-observation latency. Evidence is in
`logs/20260905-004401-ai-test-memtable-lazy/`, under the allocation test's log.

Batch: `logs/20260905-003831-java-allocation-histogram/`.
Analysis: `java-allocation-comparison.json` in that directory.
Recipe: `.build/sh/ai-compare-java-allocation histogram`.

Six fresh Java 21.0.12 JVMs each create 100 tables. Four unprofiled runs use
reference/candidate/candidate/reference order; a profiled reference/candidate
pair follows. All use eight processors, an 8 GiB heap limit, lazy TrieMemtable,
BTI SSTables, existing compaction settings, and cursor compaction disabled.
The geometric meter candidate remains disabled.

Each run writes ten active tables for two cycles, with four rows per table per
cycle, 128-byte values and 100 offered writes/second. Each run completes 80 writes,
300 verified reads, and 20 retirement requests. Each leaves 20 user SSTables and
zero initialized, dirty or flushing user memtables after reclamation.
No run reported a failure or profiler warning.

| Profiled allocation, both retirement phases | Reference | Lazy spool |
|---|---:|---:|
| Allocation samples | 182 | 53 |
| Weighted total bytes | 208,143,905 | 27,787,211 |
| Spool samples | 103 | 0 |
| Weighted spool bytes | 166,725,232 | 0 |

Estimated total allocation fell about 86.7%. Async-profiler weights are sampled
estimates, not exact allocated-byte counts. No sampled spool allocation remained
in the candidate's live-data retirement phases. CREATE TABLE also benefited:
weighted allocation fell from 4,124,597,089 to 3,195,548,823 bytes. System writers
that receive deletions still allocate spools; this does not eliminate all spools.

Unprofiled ten-table retirement groups took 335–462 ms with the reference and
255–303 ms with the candidate. These are four groups per mode, too few for a
general latency claim. Write p99 was 2.30–2.65 ms and 2.04–2.12 ms respectively.
The driver is paced and serial; this is not a saturation benchmark.

Settled heap overlapped: 107.81–107.91 MiB for the reference and 107.50–107.96 MiB
for the candidate. Sampled heap maxima varied and include unreachable objects.
The measured benefit is transient allocation during writer lifetimes, not a
material reduction in dormant table residency.

## Meter comparison

Batch: `logs/20260905-004614-java-allocation-meter/`.
Analysis: `java-allocation-comparison.json` in that directory.
Recipe: `.build/sh/ai-compare-java-allocation meter`.

The six-run order and JVM settings match the histogram experiment, but the
workload only creates 100 never-written tables. Lazy tombstone histograms stay
disabled. Each run completed 100 verified empty reads and left zero user SSTables.
No run reported a failure or profiler warning. The profiled pair also captures
baseline, created and settled heap dumps; timing comparisons use the unprofiled
runs separately.

| Profiled CREATE TABLE allocation | Reference | Geometric rates |
|---|---:|---:|
| Allocation samples | 5,088 | 3,981 |
| Weighted total bytes | 4,228,405,835 | 3,664,273,513 |
| Rate-array allocation samples | 1,135 | 0 |
| Weighted rate-array bytes | 595,065,745 | 0 |
| Weighted meter registration bytes | 141,557,490 | 171,441,849 |

Estimated total allocation fell 13.3%. Zero rate-array samples does not mean no
rate arrays were allocated; geometric growth moves that cost below this sample
resolution. The unchanged registration list remains visible and still copies
on each meter addition. Sampling variation affects attribution between runs.

Unprofiled CREATE TABLE phases took 9.09–9.26 seconds with the reference and
8.80–8.86 seconds with the candidate. There are only two runs per mode, so this
does not establish a general throughput improvement. Settled whole-JVM heap was
104.98–105.41 MiB and 105.14–109.76 MiB respectively. The candidate includes a
larger outlier; these observations do not establish a resident-memory saving.
Heap ownership gives a more specific comparison. Both profiled settled dumps
contain 8,650 live meters in one node classloader and no extra host meter copy.

| Settled node rate storage | Reference | Geometric rates |
|---|---:|---:|
| Double-array length | 25,983 | 49,152 |
| Array payload bytes | 207,864 | 393,216 |
| Allocated offset high-water mark | 25,983 | 26,547 |
| Free rate groups | 11 | 199 |
| Unassigned spare array slots | 0 | 22,605 |

The candidate retains 185,352 additional payload bytes (181.0 KiB). Of that,
180,840 bytes are unused geometric capacity; 4,512 bytes reflect a different
allocation high-water mark and cleanup/reuse timing. Independent JVMs need not
assign identical physical offsets. Both modes have the same live meter count.
Created and settled array capacities agree. Baseline payload was 101,352 bytes
for the reference and 196,608 for the candidate, with 4,108 live meters in both.
These counts include system metrics. The numbers exclude array object headers.

Evidence: `logs/20260905-005018-inspect-meter-rate-arrays.json` and its sibling
log, produced by `tmp/inspect-meter-rate-arrays.py` with `tmp/hprof_reader.py`.
The parser follows each meter class's static `rates` reference to its actual
primitive array and distinguishes classloader ownership. It does not infer the
array from a whole-heap size histogram.

## Validation status

Final clean build and Checkstyle passed:
`logs/20260905-004254-ai-build.log`.
All 23 histogram cases passed without skips in
`logs/20260905-004401-ai-test-memtable-lazy/`. These include eleven original
tests, seven candidate tests, one generated test covering 200 traces of 500
operations, three collector tests, and the allocation probe.
All 13 meter cases passed without skips in
`logs/20260905-004515-ai-test-memtable-lazy/`: six original tests, six candidate
tests, and four seeds of 1,024 generated events in one property case. Candidate
tests cover factory selection, exact rate bits, tick boundaries, long idle,
growth, concurrent registration/marking/ticking, and real GC/offset reuse.
All twelve N100 comparison runs passed. All ten harness cases passed in
`logs/20260905-005021-many-tables-launch.log`: six configuration/generated cases
and four scenario cases that start twelve three-table clusters. These include
the reference controls, each candidate separately, and both candidates together
during retirement and reactivation. Total: 46 targeted cases, plus twelve N100
comparison runs. No test, build, or benchmark process remains active.

The first build caught the registry's imported Dropwizard `Meter` shadowing
Cassandra's `Meter` interface. The registry now explicitly calls the Cassandra
factory and casts to its established return contract. A source edit during the
next compile also left one test class with six methods despite seven source
tests. A clean build after all source edits resolved the timestamp race.
Successful JUnit XML omits stdout, and the wrapper previously summarized away
the remaining test output. The allocation probe now logs through SLF4J, and
`ai-ci-test` retains the raw command output before summarizing it.

## Reproduction and limits

See [.build/memtable-residency.md](../.build/memtable-residency.md) for controls,
isolated test commands and measurement scope. The analysis script validates
completed writes and reads, retirement counters, effective selection, run order,
and allocation recordings. It writes both console output and a timestamped log.

These experiments do not enable automatic idle retirement or aggressive age/size
flushing. They do not shrink histogram precision, expire metrics, reset rate
history, or change metric registration availability. Per-table reservoirs and
registration objects remain a separate resident-memory investigation.

The next resident-memory candidate is lazy storage for empty metric reservoirs.
Preserve constructor-time clock state, exact bucket counts, decay arithmetic and
empty snapshot behavior. Keep the current implementation as the reference and
repeat differential tests before adopting a new representation. Separately,
meter registration-list copying remains a creation-allocation target. Neither
follow-up is implemented here.
