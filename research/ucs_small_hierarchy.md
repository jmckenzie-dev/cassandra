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

# Smaller Unified Compaction Strategy hierarchy

Completed on 2026-09-09. Smaller hierarchy sizes cut compaction output by about
90% in the growing append workload, but more than doubled average reader counts.
Keep the legacy 1MiB default. The new option remains available for explicit
experiments; automatic idle flushing remains disabled by default.

## Question and implementation

Can earlier promotion of small SSTables reduce repeated rewriting after idle
flushes without raising resident heap too far? The approved
[experiment plan](../.plans/ucs-small-hierarchy-experiment.md) requires at least
30% less append compaction output, and checks file count, heap, and latency costs.

The optional Unified Compaction Strategy (UCS) table setting
`min_hierarchy_size` accepts 1B through 1MiB and defaults to 1MiB. It controls
both the hierarchy floor and the rounding quantum of the existing observed
flush-size estimate. The default retains the exact old computation and the
50% refresh rule. `flush_size_override` still requires at least 1MiB.
`min_sstable_size`, which controls sharding, stays at 100MiB.

The production change adds one scalar per Controller. It does not change
compaction selection, repair grouping, expiration safety, or scheduling.
Automatic idle flushing remains separately optional and disabled by default.
These experiments use lazy Trie memtables, the BTI file format, cursor
compaction, and the simple metric profile with the existing compact options.
Transient Java Management Extensions (JMX) handling, lazy metric identifiers,
and compact metric bookkeeping are enabled.
Lazy tombstone builders and geometric meter arrays remain off in both paths,
matching the baseline. This experiment isolates the hierarchy setting rather
than measuring the minimum allocation achievable with every branch option.

## Method and baseline

Each run starts a fresh Java 21 virtual machine (JVM). Small runs use 20 tables, one partition per
table, four 256-byte rows per cycle, a 2GiB heap ceiling, and a 256MiB memtable
pool. Explicit flushes first isolate the hierarchy effect. Each cycle settles
compaction, performs a full garbage collection, and verifies all expected
values. Append and overwrite are separate workloads. Write timing excludes
cycle zero; reads grow with the append partition, so these are service times
for this sequential harness, not production throughput estimates.

The trace asks real UCS instances for their computed levels, densities, flush
estimate, and base size for a representative table. On-disk level metadata is
not used to infer UCS membership. Every run logs live and sampled peak files,
pending compactions, process central processing unit (CPU) time, flush sizes, compaction history, and request
latencies. Sampled peaks are lower bounds. Compaction duration sums can overlap
across workers and do not equal process CPU or elapsed run time.

The pre-change baseline used the new trace and CPU measurement but the old
Controller implementation. At 192 cycles each workload completed 15,360 writes
and exactly 3,840 table flushes. Both verified their data, retired every user
memtable, and ended without a compaction backlog.

| Pre-change, T4/1MiB | Append | Overwrite |
| --- | ---: | ---: |
| Settled heap, MiB | 56.35 | 57.21 |
| Final SSTables | 60 | 60 |
| Compaction output / flushed bytes | 31.7214 | 0.3324 |
| Compaction output, bytes | 132,776,188 | 1,391,225 |
| Compaction jobs | 1,260 | 1,260 |
| Process CPU after table creation, seconds | 152.64 | 149.05 |

At cycle 48, the append baseline ratio was 7.7901. All files still occupied L0.
At cycle 192, the representative table had two 1,104-byte files and a
206,330-byte file, still in L0 under the 1MiB estimate.

The logs contain 3,840 nonempty writer outputs and 7,680 empty writer outputs.
Cassandra removes empty writers before updating the flush-size average; they
do not dilute the estimate. The nonempty outputs were 1,090–1,092 bytes.
A 1KiB rounding quantum therefore produces a 2KiB estimate for this workload.
The trace confirms that result.

Baseline artifacts: `logs/20260909-200153-ucs-hierarchy-baseline/`.
Initial validation: `logs/20260909-201739-ai-test-memtable-lazy/` (85 passing tests).

## Initial 48-cycle results

All ten runs completed 3,840 writes and 960 table flushes, verified every read,
retired every user memtable, and ended without a compaction backlog.

| T4 setting | Append output / flushed bytes | Final files/table | Mean settled files/table | Peak settled files/table | Final heap, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1MiB | 7.7862 | 3 | 2.0000 | 3 | 49.56 |
| 64KiB | 7.7864 | 3 | 2.0000 | 3 | 50.53 |
| 4KiB | 2.3486 | 6 | 3.3125 | 6 | 50.59 |
| 1KiB | 2.0390 | 6 | 3.5000 | 6 | 50.65 |
| 1B follow-up | 1.9976 | 3 | 4.0625 | 8 | 49.81 |

The 1B follow-up uses the same implementation and an already accepted option
value. It tests fine rounding because the 1KiB quantum rounded this workload's
roughly 1.09KiB flushes to 2KiB. No second production change was needed.

All overwrite cases retained three final files/table, averaged two, and had
output ratios between 0.31652 and 0.31655. The merged output stays small because
each cycle replaces the same four values. It therefore stays in L0 even with
the smaller hierarchy.

The file-count gate rejects 4KiB and 1KiB as default candidates. The 1B setting
also fails the residency intent: it doubles average reader counts and increases
the peak from three to eight files/table. Its final count alone conceals that
cost. The 64KiB setting has no useful disk-write reduction at this duration.

The 20-table whole-heap difference is small because fixed JVM, system-table,
metrics, and harness objects dominate. It does not show that extra readers are
free. The separate 1,000-table census measured roughly 17.3KiB per additional
file. Applying that estimate to five extra readers gives roughly 86.5KiB extra
per table at the observed peak; that is an extrapolation, not a new scale run.

The file-count failure invokes the plan's stop condition. The longer append
runs and automatic flushing below confirm it. No candidate advances to
promotion repeats or a 1,000-table experiment in this investigation.

Artifacts: `logs/20260909-202028-ucs-hierarchy-sweep48/` and
`logs/20260909-203621-ucs-hierarchy-exact48/`.

## Final 192-cycle append comparisons

Each run completed 15,360 writes and exactly 3,840 table flushes. All values
verified, all user memtables retired, and all user compactions settled.

| T4 setting | Output / flushed bytes | Final files/table | Mean settled files/table | Peak settled files/table | Final heap, MiB | Peak settled heap, MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1MiB | 31.7197 | 3 | 2.0000 | 3 | 57.42 | 57.42 |
| 64KiB | 31.7180 | 3 | 2.0000 | 3 | 56.55 | 57.30 |
| 4KiB | 3.3922 | 6 | 4.6406 | 8 | 57.36 | 58.41 |
| 1KiB | 2.9988 | 9 | 4.9063 | 9 | 58.43 | 58.98 |
| 1B | 2.9944 | 3 | 5.5156 | 11 | 56.62 | 60.11 |

The representative table first promoted a file at cycles 190 / 13 / 7 / 4 for
64KiB / 4KiB / 1KiB / 1B respectively. The 1MiB control never promoted a file.
The 64KiB promotion happened at the last compaction, so it avoided no repeated
rewrites within this window. The smaller settings cut output by 89.3–90.6%.

The final 1B heap and file count conceal the preceding peak: cycle 191 retained
11 files/table and 60.11MiB, then cycle 192 merged down to three files/table.
Mean and peak reader residency both reject it for the memory-oriented default.
The earlier matched census implies roughly 138KiB extra per table for eight
additional readers. That is a per-file extrapolation, not a measured million-
table heap or a new 1,000-table comparison.

| T4 setting | Compaction input/output, bytes | Jobs | Summed logged job time, seconds | Median/max job time, ms | Process CPU after creation, seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1MiB | 132,835,822 / 132,768,360 | 1,260 | 33.235 | 26 / 48 | 153.89 |
| 64KiB | 132,829,450 / 132,761,516 | 1,260 | 33.965 | 26 / 165 | 158.07 |
| 4KiB | 14,265,145 / 14,198,677 | 1,240 | 30.599 | 24 / 151 | 153.84 |
| 1KiB | 12,616,943 / 12,551,847 | 1,220 | 29.355 | 23 / 141 | 151.34 |
| 1B | 12,600,690 / 12,533,739 | 1,260 | 31.758 | 23 / 627 | 156.89 |

The 1B setting performs the same number of jobs as the control. Lower byte
output therefore leaves per-job work in place. Process CPU did not materially
improve. These counters measure Data.db bytes; they do not measure total device
writes, index/statistics components, the commit log, or logging overhead.

| T4 setting | First write p99, ms | Other write p99, ms | Read p99, ms |
| --- | ---: | ---: | ---: |
| 1MiB | 0.814 | 0.273 | 2.166 |
| 64KiB | 0.861 | 0.289 | 2.259 |
| 4KiB | 0.896 | 0.275 | 2.294 |
| 1KiB | 0.756 | 0.265 | 2.070 |
| 1B | 0.839 | 0.277 | 2.171 |

These single-run service-time distributions do not establish production latency
equivalence. The 4KiB first-write p99 is about 10% above the control, and the 1B
run contains a 627ms compaction outlier. The benchmark verifies reads after
settlement; its latency table does not measure queries under concurrent
compaction load. Any later promotion for disk-write savings needs repeated load tests and
investigation of those tails. The current experiment stops on residency first.

Artifacts: `logs/20260909-204009-ucs-hierarchy-long192/`; complete analysis:
`logs/20260909-211028-analyze-ucs-idle.json`.

## Existing T8 control

T8/1MiB completed the same 192-cycle append workload with a 13.8736 output
ratio, 540 jobs, 138.14 process CPU seconds, and 54.34MiB final heap. It averaged
3.9688 files/table, peaked at seven, and ended at three. Output fell 56.3% and
CPU fell 10.2% relative to T4/1MiB in these single runs. Its read p99 was 2.132ms.

Fewer jobs distinguish this control from the fine hierarchy: it writes fewer
compaction-history entries as well as doing fewer compactions. Node-wide pool
accounting after all user memtables retired was 3,035,089 bytes, versus
4,316,417 for T4. The whole JVM heap therefore includes other changing state;
it must not be treated as an isolated reader measurement. The average and peak
reader costs still matter for many tables.

Artifacts: `logs/20260909-211027-ucs-hierarchy-t8-control192/` and
`logs/20260909-211620-analyze-ucs-idle.json`.

## Automatic idle flushing

The 48-cycle T4 comparison used the real scheduler with a 300ms idle timeout.
Both settings completed 3,840 writes and exactly 960 table flushes. Data
verification passed; all user memtables retired and compactions settled.

| Setting | Output / flushed bytes | Mean / peak files per table | Final heap, MiB | Read p99, ms | Median idle drain, seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1MiB | 7.7901 | 2.0000 / 3 | 48.04 | 2.223 | 1.004 |
| 1B | 1.9978 | 4.0625 / 8 | 48.21 | 2.399 | 1.005 |

These match the explicit-flush rewrite and file-count results. Idle drain
includes the timeout, flushing, and reclamation. A separate settlement phase
then waits for compaction. The sampled pending-compaction estimate peaked at
one for the control and eight for 1B, then drained to zero. This paced workload
does not establish a sustained production throughput limit.

Artifacts: `logs/20260909-211619-ucs-hierarchy-automatic48/` and
`logs/20260909-212013-analyze-ucs-idle.json`.

## Allocation sampling

Separate 12-cycle append runs collected async-profiler allocation and wall
events. Each run completed 960 writes and 240 table flushes. The kernel did not
permit CPU sampling; process CPU measurements above use the JVM operating-system
counter instead. No kernel settings were changed.

The analyzer converted 48 cycle recordings per run with `jfrconv --alloc
--total`. It excludes startup/creation and counts stacks containing
`CompactionTask` separately.

| T4 setting | Weighted allocation estimate, bytes | Estimate on compaction stacks, bytes |
| --- | ---: | ---: |
| 1MiB | 3,282,067,646 | 272,106,937 |
| 1B | 3,275,775,886 | 269,485,498 |

Differences of 0.2% overall and 1.0% on compaction stacks do not establish a
material allocation improvement. These are sampled weights, not exact byte
counts. The profiler covers write, observation, retirement, and read windows;
unprofiled checkpoint/settlement gaps can contain background work. The results
are not a complete census of all compaction allocation. Do not use the profiled
runs' timings as normal service-time measurements.

Artifacts: `logs/20260909-212012-ucs-hierarchy-allocation12/` and
`logs/20260909-212253-analyze-ucs-idle.json`.

## Decision and scope of completion

All 22 fresh-JVM runs passed: 170,880 writes completed, 43,160 read queries
verified, exact flush counts matched, and final retirement/compaction checks
passed. No new run exceeded 20 tables. The earlier 1,000-table census supplies
the per-file ownership estimate; this experiment does not establish million-
table capacity.

The 4KiB and smaller settings exceed the 30% compaction output target but fail the reader
residency gate. The 64KiB setting does not meet that target at 192 cycles.
The 1B follow-up rules out coarse rounding as a way around the file-count cost.
Keep `min_hierarchy_size` at its existing 1MiB default for the memory goal.

The plan's conditional three-repeat promotion and 1,000-table stages were not
run because no candidate passed the residency gate. Extended mixed-burst
performance and repeated concurrent-load latency validation also remain outside
this stopped experiment. Burst, deletion, expiry, read-during-compaction, and
restart correctness were covered by the focused tests.

The next heap work stays with the census findings: trim finished empty tombstone
histograms, then assess immutable histogram-offset sharing and compact counters.
Those changes can reduce the cost of each reader while preserving statistics.
This experiment did not change those structures, so its comparisons retain the
same per-reader implementation.

## Correctness validation

Build and Checkstyle passed. The focused suite passed all 85 tests across eight
classes. The final strengthened Controller and real-data tests also passed
separately, through the same build/test wrappers.

- Controller tests cover option parsing, rejected sizes, preserved override
  behavior, zero and NaN startup estimates, exact rounding boundaries, the 50%
  refresh rule, and large finite estimates. Generated checks compare 20,000
  observations against the legacy computation and test 5,000 independently
  chosen rounding quanta and observations.
- UCS level tests cover exact density boundaries, empty intermediate levels,
  and local token coverage. Existing tests cover repair grouping and expiration.
- Real flush tests exercise tiny-to-large-to-tiny data, a file over 1MiB,
  multiple partitions, row/partition/range deletions, and time-to-live expiry at
  explicit read times. Queries run before, during, and after a real cursor
  compaction. A later write reactivates the retired Trie memtable.
- Distributed tests exercise automatic retirement, eligible/ineligible memtable
  and strategy combinations, indexes, reactivation, and restart. The new table
  option survives restart. The disabled-default test still passes.

Entry points: `./run_tests.sh --ucs-hierarchy` and
`./run_property_tests.sh --ucs-hierarchy`. Final additional test artifacts:
`logs/20260909-203557-ai-test-memtable-lazy/` and
`logs/20260909-2035-controller-hierarchy.xml`.

Test wrappers share output/data directories and must run sequentially. One
overlapping rerun lost its XML output; it was discarded and rerun successfully
in isolation. The workflow instructions now record that constraint.

## Reproduction

Use the repository build wrappers before benchmarking. Never rebuild the shared
JAR during a benchmark JVM's lifetime.

```sh
./run_tests.sh --ucs-hierarchy
./run_property_tests.sh --ucs-hierarchy
python3 .build/sh/benchmark_ucs_hierarchy.py sweep48 --sizes 1MiB 64KiB 4KiB 1KiB --cycles 48
python3 .build/sh/analyze_ucs_idle.py --brief logs/<batch-directory>
```

The runner supports `--workloads append overwrite`, `--repeats`, `--automatic`,
`--tables` (up to 1,000), `--heap-dumps` (final live heap only), and `--profile`.
Profile separately from timing comparisons: async-profiler records allocation
events as well as wall/CPU events where the kernel permits them. Short profile
runs avoid the cost of hundreds of cumulative Java Flight Recorder dumps.
