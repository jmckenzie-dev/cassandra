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

# Worker counter and metric-ID residency

At 1000 tables, final lazy IDs reduce whole heap by **7,742,512 bytes
(7.38 MiB, 6.20%)** against the final matched eager control. Eight census workers
use **86.3% less counter-array payload**. The optional implementation reduces
residency for untouched metrics.
The default uses the original eager recording methods and final ID fields.
Both modes retain dense worker arrays. The lazy path remains experimental and
off by default because its established recording path is measurably slower.

## Implementation and compatibility

Histogram/timer factories check for compatible registered metrics before
constructing reservoirs and counter IDs. Reused metrics still pass through the
existing registration path. Missing canonical MBeans, custom ObjectNames,
aliases, type-conflict behavior, profile-hidden recorders, and concurrent
registration remain covered by focused tests.

Set `-Dcassandra.lazy_metric_ids=true` before startup to select lazy IDs.
Remove the property or set it to false and restart for the eager control.
Cassandra's factories choose a concrete implementation; existing objects do
not change if the property later changes. Direct constructors retain eager
behavior. Factory overloads accept an explicit boolean for tests. Utility
counters can initialize before server YAML loads, so this uses a startup JVM
property rather than a YAML setting. It is independent of metric profile,
histogram reservoir, geometric meter storage, and JMX settings.

Lazy counters synchronize first allocation and publish their ID only after
registering cleanup. A negative sentinel identifies an unallocated ID; zero
values never identify unused slots. An explicit zero update allocates an ID.
Untouched reads, resets, histogram snapshots and meter ticks do not allocate
IDs. Lazy operations keep their owner reachable through each ID operation.

Each lazy histogram owns one LazyThreadLocalCounter. This reuses the counter
initialization/reset/cleanup code and preserves ClearableHistogram's subtype
and reservoir clearing. It adds one small helper object per histogram; final
whole-heap measurements include that cost. LatencyMetrics uses a lazy counter
subtype with the same child aggregation and release behavior.

Each lazy meter variant retains its existing rate storage and rate cleanup.
Its two counter IDs initialize together, and volatile publication follows both
cleanup registrations. Lazy meters register for background ticking only after
subclass fields initialize. Active lazy meters add two ID phantom references;
untouched meters retain rate cleanup but no counter IDs. The original eager
meter retains its combined cleanup object. Rate math is unchanged.

Dense array growth, worker-exit transfer, compensating summary subtraction on
reset, and the delayed recycler remain unchanged. The recycler's documented
lack of a formal happens-before edge for clearing reused worker slots is
pre-existing; this task does not claim to repair it. Memtable and JMX algorithms
are unchanged by this task.

## Matched node measurements

All node comparisons use Java 21.0.12, G1, an 8 GiB heap ceiling, eight available
processors/workers, 1000 tables or fewer, simple_metrics.yml, compact reservoirs,
lazy empty TrieMemtables, adaptive JMX history, transient JMX names, compact
registration, and the same property-query workload. Earlier JMX gains are
already in the control and are not counted here. The harness verifies metric
counts, registrations, successful scrapes, worker storage, and empty/uninitialized
user memtables. These are synthetic metric updates on empty tables, not query
throughput or table-capacity results.

| 1000-table implementation | Created heap B | Rescraped heap B |
| --- | ---: | ---: |
| Pre-task baseline | 118,606,008 | 125,131,960 |
| Registry-only repeat | 118,915,024 | 125,336,504 |
| First lazy design, rejected for default-path overhead | 114,052,600 | 116,846,640 |
| Final eager control | 118,493,432 | 124,851,688 |
| Final lazy implementation | 114,337,832 | 117,109,176 |

Registry lookup alone does not demonstrate a whole-heap reduction: its repeat
falls within the observed run variation. It does reduce the ID high-water mark
from 72,482 to 68,992, summary capacity from 77,580 to 70,528 longs, and each
census worker from 75,835 to 74,142 longs. Many temporary IDs already recycle.
The final active-ID saving must be attributed primarily to lazy initialization.

The final created-state reduction is 4,155,600 B (3.96 MiB); the populated,
rescraped reduction is 7,742,512 B (7.38 MiB). Against the pre-task baseline the
final reduction is 8,022,784 B, but the final matched eager/lazy pair is the
headline comparison. Startup post-GC heaps vary substantially: 49,683,592 B in
the final eager run and 44,498,432 B in the final lazy run. Do not subtract those
startup readings to infer per-table slopes. No 100-table slope or million-table
capacity is claimed.

| Final 1000-table census | Eager | Lazy |
| --- | ---: | ---: |
| ID high-water mark | 68,897 | 9,573 |
| IDs owned by live metric objects | 64,937 | 9,568 |
| Summary long slots | 70,528 | 10,488 |
| Summary payload B | 564,224 | 83,904 |
| Long slots per census worker | 74,136 | 10,183 |
| Payload B across eight census workers | 4,744,704 | 651,712 |
| Nonzero slots per census worker | 6002 | 6002 |
| MetricIdReference objects | 41,089 | 9,568 |
| MetricIdReference shallow B | 1,314,848 | 306,176 |
| MetricCleanerReference objects | 11,932 | 11,932 |
| MeterCleaner objects | 11,924 | 11,924 |

Primitive-array payload excludes headers. Reference counts/sizes come from
post-GC class histograms; these are shallow sizes, not dominator retained sizes.
They explain part of the whole-heap change and must not be added to that total.
Other node threads' arrays and registry cleanup-map entries also remain in the
whole-heap measurement.

Final lazy histograms own 11,261 helper counters, 24 B each: 270,264 B of helper
objects. Lazy clearable histogram objects add 17,536 B, lazy meter subclasses
add 95,392 B, and lazy latency-counter subclasses add 51,288 B relative to the
matching eager class populations. This measured object cost is included in the
7.38 MiB net whole-heap saving. The active lazy-ID census counts composed counter
owners once. It reports 7446 lazy counter IDs (including histogram helpers) and
1061 pairs of lazy meter IDs.

Final probes: `logs/20260907-202443-288531-worker-metric-probe.json` (eager) and
`logs/20260907-202444-480497-worker-metric-probe.json` (lazy).

## Counter recording and worker storage

The final matrix uses the same benchmark class with three implementations:
the saved registry-only JAR's original eager counters; current eager counters;
current lazy counters. Original mode calls the old no-argument constructor;
current modes call the configured factory. Metric infrastructure warms before
construction allocation measurement. Each case uses 1000 logical groups with
58 plain production counters per group and touches six counters per group.
Shared workers touch every group; partitioned workers touch disjoint groups.
Histogram/meter recording time is not measured by this counter-only benchmark.

Three fresh JVM forks reverse mode order on alternate forks. Round zero records
first-use work. Rounds 1 and 2 warm the loop; rounds 3 through 7 each record two
million updates per worker. Every case verifies all values after worker exit.
The JVM reports eight available processors through ActiveProcessorCount; that
flag does not pin 64 application threads to eight physical CPUs. No CPU affinity
or process isolation is claimed. Times include scheduling and host variation.

The table reports worker elapsed nanoseconds/update, mean +/- sample standard
deviation over 15 rounds from three JVMs. Rounds within a JVM are not independent
forks; these are descriptive statistics, not confidence intervals.

| Workers/access | Original eager | Final eager | Final lazy |
| --- | ---: | ---: | ---: |
| 8 shared | 4.146 +/- 0.193 | 4.340 +/- 0.339 | 4.941 +/- 0.252 |
| 8 partitioned | 4.204 +/- 0.056 | 4.371 +/- 0.358 | 4.682 +/- 0.161 |
| 64 shared | 7.042 +/- 0.159 | 7.128 +/- 0.137 | 7.976 +/- 0.167 |
| 64 partitioned | 6.918 +/- 0.211 | 6.926 +/- 0.093 | 7.831 +/- 0.152 |

Current eager means remain 0.1–4.7% above original, with overlapping fork
variation after restoring the original recording methods. Exact parity is not
claimed. Lazy means are about 11–19% above original. All final steady rounds
report zero allocated bytes/update. The initial shared volatile-field design
produced a repeatable default-path regression and was replaced, rather than
accepting that cost behind an option that did not contain it.

Aggregate wall nanoseconds/update (all worker updates in the denominator):

| Workers/access | Original eager | Final eager | Final lazy |
| --- | ---: | ---: | ---: |
| 8 shared | 0.569 +/- 0.094 | 0.620 +/- 0.111 | 0.731 +/- 0.108 |
| 8 partitioned | 0.570 +/- 0.027 | 0.600 +/- 0.090 | 0.651 +/- 0.065 |
| 64 shared | 0.269 +/- 0.008 | 0.266 +/- 0.007 | 0.298 +/- 0.014 |
| 64 partitioned | 0.264 +/- 0.010 | 0.255 +/- 0.008 | 0.291 +/- 0.008 |

First-use worker elapsed time and allocation, averaged over three fresh JVMs.
This includes worker-array growth, initialization contention, and thread-local
setup. Partitioned cases perform fewer first updates, so their per-update costs
are not directly comparable with shared cases.

| Workers/access | Eager ns/update | Lazy ns/update | Eager B/update | Lazy B/update |
| --- | ---: | ---: | ---: | ---: |
| 8 shared | 2025 | 1903 | 884.4 | 118.1 |
| 8 partitioned | 5994 | 6970 | 5963.7 | 773.7 |
| 64 shared | 4789 | 3373 | 884.4 | 98.8 |
| 64 partitioned | 58848 | 88525 | 30607.1 | 2203.8 |

Construction allocates roughly 15.7–15.9 MB for eager groups versus 6.3–6.5 MB
for lazy groups after infrastructure warmup. This allocation saving is distinct
from retained memory. Exact worker-array payload and post-GC whole benchmark
heap follow, averaged over three JVMs. The whole-heap reading uses Runtime and
includes harness state; it is not added to the node HPROF totals.

| Workers/access | Eager array payload B | Lazy array payload B | Eager heap B | Lazy heap B |
| --- | ---: | ---: | ---: | ---: |
| 8 shared | 3,936,256 | 416,896 | 24,386,136 | 16,068,651 |
| 8 partitioned | 3,746,376 | 371,592 | 23,700,547 | 15,862,387 |
| 64 shared | 31,490,048 | 3,335,168 | 56,584,640 | 20,659,579 |
| 64 partitioned | 30,362,984 | 2,652,328 | 52,733,429 | 18,283,120 |

Lazy allocation removes about 89–91% of worker payload in this counter workload.
Partitioned lazy storage still contains holes, but the remaining 64-worker
payload is about 2.65 MB at 1000 groups. Dense storage is retained for this task:
the measured benefit does not justify adding a second storage representation
and its lifecycle/read/write complexity now. Paging could target the remaining
bound, but no paging saving or large-table capacity is claimed.

## Verification and failures retained

- Final build passes `.build/sh/ai-build`, including Checkstyle.
- `run_tests.sh --metric-ids`: 27 tests pass, including racing first use,
  array growth, signed updates, concurrent compensating resets, worker exit,
  counter cleanup/reuse, both meter IDs and rate-group cleanup/reuse in both
  modes, untouched reads/ticks, configuration capture, histogram clearing,
  latency child/release aggregation, and factory compatibility.
- `run_property_tests.sh --metric-ids`: two suites pass, with 80,000 generated
  signed counter steps and 32,000 meter/tick steps comparing every rate bit.
- `PROFILE_LAZY_METRIC_IDS=true run_tests.sh --metric-profiles`: 35 existing
  profile, registry, latency, harness, and recording integration tests pass.
  This exercises both all/simple profiles, hidden aggregate inputs, real table
  construction, writes, and drop/release behavior with factory-selected lazy IDs.
- The missing/custom-MBean regression failed before the registration-path fix.
  Its first Ant run also exposed a test-only profile-resource path issue;
  the test now uses an absolute config path.
- Earlier test development caught an import-order Checkstyle failure and a
  Counter type mismatch. Both were corrected before passing validation.
- The first registry-only JVM completed, but its Bash wrapper exited 2 because
  the launcher source changed while running. The corrected repeat froze the
  JAR, skipped rebuilding, left its launcher untouched, and exited 0.
- A nested-distrobox benchmark driver failed before its matrix started. An
  escalation request was interrupted. The final driver runs inside dev and
  invokes the existing wrapper directly; all 36 final cases exit 0.

No full Cassandra test suite, new dependency, private-field mutation, hash-based
coupling, commit, or staging action is part of this task.

## Independent review

A clean-context implementation agent executed
`.plans/worker-metric-residency.md`. A separate principal-engineer reviewer
reviewed the 30-path task delta against saved pre-task working-tree sources,
then re-reviewed the fixes and final measurement evidence. The final gate was
COMPLETE / APPROVE / +1 YES, with no remaining findings. This approval covers
this task's changes, not all earlier uncommitted work on the branch.

Review prompted restoration of the original eager recording methods, meter
cleanup/reuse coverage, MBean registration retries on factory reuse, lazy
histogram clearing and latency-release tests, and correction of the heap
analyzer for lazy ID fields. The final reviewer independently parsed the
counter matrix and checked the successful build and test logs.

Final validation logs:

- `logs/20260907-201549-ai-build.log`.
- `logs/20260907-201646-run_tests.log` (27 focused tests).
- `logs/20260907-201735-run_property_tests.log` (two generated suites).
- `logs/20260907-202520-run_tests.log` (35 integration tests).

## Reproduction and artifacts

Build/test through Java 21 in the dev container:

```sh
distrobox enter dev -- .build/sh/ai-build
distrobox enter dev -- bash run_tests.sh --metric-ids
distrobox enter dev -- env PROFILE_SKIP_BUILD=true bash run_property_tests.sh --metric-ids
distrobox enter dev -- uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/benchmark-worker-metrics.py --original-jar tmp/worker-metric-lookup.jar --forks 3
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/benchmark-worker-metrics.py --summarize logs/20260907-201736-579239-worker-metrics-benchmark.json
```

`PROFILE_SKIP_BUILD=true` requires current compiled test classes and the chosen
JAR. `PROFILE_JAR` overrides the first production entry in the profile classpath.
The original JAR was saved before lazy-ID builds; its eager recording code is
the pre-task implementation. A fresh checkout must build/save that comparison
revision before building the candidate. No generated JAR is committed.

Final matrix: `logs/20260907-201736-579239-worker-metrics-benchmark.json` and
matching `.log`; summary `logs/20260907-201855-871294-worker-metrics-summary.json`.
Rejected initial matrix: `logs/20260907-200519-386974-worker-metrics-benchmark.json`.

Node profile command, add `PROFILE_LAZY_METRIC_IDS=true` for lazy:

```sh
distrobox enter dev -- env PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 132 --metrics-config simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/worker-metric-final-eager-1000
```

Use a different subnet/output directory per run. Final lazy uses subnet 133.
Run HPROF analysis only after all measurement JVMs have exited:

```sh
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/analyze-worker-metrics.py HEAP.hprof
```

The census handles eager IDs, lazy subclass IDs, and composed counter owners.
It distinguishes live-object IDs, ID high water, summary capacity and nonzero
worker occupancy. Negative sentinels are excluded; valid zero-valued IDs are
not discarded. Paging figures in census JSON are estimates, not measurements
of an implemented alternative.

Node directories:

- Baseline: `logs/worker-metric-baseline-1000/20260907-192808-heap-ownership-1000t/`.
- Registry-only corrected repeat: `logs/worker-metric-lookup-repeat-1000/20260907-193649-heap-ownership-1000t/`.
- Rejected first lazy design: `logs/worker-metric-lazy-1000/20260907-200603-heap-ownership-1000t/`.
- Final eager: `logs/worker-metric-final-eager-1000/20260907-201912-heap-ownership-1000t/`.
- Final lazy: `logs/worker-metric-final-lazy-1000/20260907-202139-heap-ownership-1000t/`.

Each contains summary.json, phase HPROFs and class histograms, scrape results,
and checkpoint assertions. Review uses only paths in
`tmp/worker-metric-residency-paths.txt` against copies in
`tmp/worker-metric-residency-baseline/files`, preserving earlier uncommitted work.
