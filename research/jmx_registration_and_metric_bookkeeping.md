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

# Java Management Extensions (JMX) registration and table metric release bookkeeping

The pre-change control includes the earlier lazy metric-ID
implementation, with its optional lazy mode enabled throughout this task.
The earlier eager control remains untouched. No table population exceeds 1000.

## Current baseline

The fresh 1000-table control uses Java 21.0.12, the Garbage-First (G1) collector,
an 8 GiB ceiling, eight
workers/processors, simple_metrics.yml, compact reservoirs, lazy TrieMemtables,
lazy metric IDs, adaptive histogram history, transient JMX queries, and compact
registration. The actual node Java virtual machine (JVM) arguments and effective configuration are in
summary.json. Every created, scraped, populated, and rescraped assertion passed.

Final whole heap after garbage collection (GC) is 117,149,880 B.
The two disjoint JMX server subtrees
retain 32,238,032 B and 14,456 B, totaling 32,252,488 B. This exactly reproduces
the earlier JMX ownership total despite the later lazy-ID change.

These nested or bounded counts explain that ownership. They overlap the JMX
subtrees and are not additional savings:

| Current structure | Bytes | Scope |
| --- | ---: | --- |
| ObjectNames and canonical text/property graphs | 18,543,424 | Bounded graph, all 55,109 ObjectNames |
| Canonical text backing arrays | 6,768,168 | Nested in preceding graph |
| Property offset objects | 5,172,696 | 215,529 objects, nested |
| Property reference arrays | 3,516,528 | 110,217 arrays, nested |
| ObjectName headers | 1,763,488 | Nested |
| Canonical String headers | 1,322,544 | Nested |
| JMX repository map structure | 2,306,896 | Bounded collection structure, excludes entries' keys/values |
| NamedObject entries | 1,322,544 | 55,106 shallow objects |
| CassandraMetricsRegistry.metrics | 8,730,200 | Retained registry map; outside JMX subtree |
| Remaining standard adapters | 392,688 | 16,362 shallow objects, includes non-metric MBeans |
| Repeated meter/timer rate-unit text | 552,664 | 9869 strings plus backing arrays |
| TableMetrics.ownedMetrics structure | 2,773,568 | 1057 maps, 64 entries each; 2624 B/table |

Only 14,215 of the remaining adapters are metric wrappers: 4346 histograms,
9097 timers, 768 meters, and four gauge-compatible meters. Their removable
adapter cost is 341,160 B. Most metadata already belongs to shared Java
Development Kit (JDK) caches;
the baseline has only 133 MBeanInfo objects, not one per registration. Repeated
rate-unit labels offer a larger supported saving than additional adapters.
ObjectNames, repository keys/maps, and NamedObjects remain JDK-owned structures.
The task does not replace the server or repository to reduce those structures.

## Registration design

The existing compact_jmx_registration_enabled switch extends the field-free
DynamicMBean subclasses to histogram, timer, meter, and gauge-compatible meter
exports. The default remains false. Each subclass inherits its original metric
getters and owns no additional instance field. It returns legacy metadata and
uses the existing transient StandardMBean dispatch for attributes/operations.
Compact meters/timers share the constant events/second string. Legacy mode
retains its previous constructors and representation.

The intermediate registration-only run retains 116,257,808 B of whole heap,
892,072 B less than baseline. Its main JMX subtree falls to 31,344,208 B:
an exact 893,824 B reduction. The 14,215 removed adapters explain 341,160 B;
the old rate-label graphs explain 552,664 B. The one 56 B replacement graph
belongs to static state and is not retained by the JMX subtree. This distinction
explains why the subtree delta includes all old label bytes.

The intermediate ObjectName graph, repository map structure, NamedObject
population, and ownedMetrics structure are identical to baseline. Standard
adapters fall to 2147 non-metric instances. Four additional shared MBeanInfo
objects and associated metadata remain in fixed/class state. The whole-heap
delta includes these costs and run variation; it is not added to the subtree
delta. Intermediate artifacts:

- logs/jmx-bookkeeping-registration-1000/20260908-001113-heap-ownership-1000t/.
- logs/20260908-001357-rescraped-dominators.zip.
- logs/20260908-001358-255850-jmx-bookkeeping-probe.json.

## Bookkeeping design

The optional startup property cassandra.compact_table_metric_bookkeeping=true
selects an ordinary ArrayList containing alternating names and metric objects.
The default false keeps a HashMap control. Both live behind a small local
TableMetrics.OwnedMetrics holder. Setup does linear lookup/replacement in list
mode; reads and recording use the existing metric fields. The list reserves
128 slots for the observed 64 pairs and uses normal ArrayList growth for later
subclass additions. It does not freeze after the base constructor returns.

The holder preserves replacement by name, lookup, iteration, and clear. Table
release still removes each owned metric from its aggregate set, releases
latencies, and removes names/aliases through the registry. Its synchronization
and idempotent release guard are unchanged. Construction helpers remain
unsynchronized, as before. The default map control adds a 24 B holder/table;
the compact form measures 576 B/table including holder, list, and 128-slot
array. All 1057 captured tables have 64 pairs and no spare slots. The list can
grow for later additions; the lifecycle test adds 200 gauges after construction.

## Matched heap results

| Post-GC phase | Baseline B | Registration-only B | Combined B |
| --- | ---: | ---: | ---: |
| Created | 114,364,032 | 113,468,784 | 111,217,576 |
| Scraped | 114,532,816 | 113,641,752 | 111,313,336 |
| One worker | 116,052,880 | 115,161,352 | 112,879,984 |
| Eight workers | 116,633,080 | 115,741,064 | 113,460,128 |
| Rescraped | 117,149,880 | 116,257,808 | 113,977,128 |

The final reduction is **3,172,752 B (3.03 MiB, 2.71%)** against the fresh
baseline. Bookkeeping accounts for the second separately measured step:
2,280,680 B less whole heap than registration-only. Its exact structure saving
is 2,164,736 B: 2048 B/table across the captured 1057 tables. The Eclipse
Memory Analyzer (MAT) confirms that
TableMetrics disjoint retained size falls by that exact amount, from 6,925,464
to 4,760,728 B. The bounded ownedMetrics structure falls from 2,773,568 to
608,832 B. No registration saving is counted again as bookkeeping saving.

JMX retained size remains at 31,344,208 B in the main server plus 14,456 B in
the separate platform server. ObjectNames, repository maps, NamedObjects,
wrapper populations, and shared rate labels remain identical to registration-
only. The two exact structural savings total 3,058,560 B; the difference from
the whole-heap delta includes other startup/classpath/runtime variation.
Startup readings (44,889,808, 48,577,752, and 48,572,592 B) are not subtracted
to infer a per-table slope or table capacity.

Every run exports 52,914 metric MBeans, including 48,031 user-keyspace exports.
Full scrapes read 259,233 attributes, including 13,443 recent-value attributes,
with zero failures. All 1000 user memtables remain clean and uninitialized;
no user SSTables exist. Recording and aggregate assertions pass. The final
run's actual node arguments include cassandra.compact_table_metric_bookkeeping
and cassandra.lazy_metric_ids set to true. No recording algorithm changed.

The comparison uses frozen Java archive (JAR) files because the existing compact-registration
switch selected only gauges/counters before this task. It therefore uses a
baseline, a registration-only intermediate with the original map, and a final
combined implementation. It does not claim an independent bookkeeping-only
fourth quadrant under the older registration implementation.

Final artifacts:

- logs/jmx-bookkeeping-final-1000/20260908-002202-heap-ownership-1000t/.
- logs/20260908-002413-rescraped-dominators.zip.
- logs/20260908-002414-323055-jmx-bookkeeping-probe.json.
- logs/20260908-002523-204176-rescraped-heap-ownership.json.

## Monitoring cost

The same new benchmark class ran against the frozen baseline and registration
JARs in three fresh JVM forks each, alternating order across forks. Both modes
enable lazy IDs and transient name queries. Each JVM registers 1000 metric
MBeans (200 each of counter, gauge, histogram, timer, meter), not 1000 tables.
Each sample warms each operation, then measures 100,000 attribute reads,
10 full scrapes of 7600 attributes, and 100 queries per pattern. The table uses
samples 1–5 from each fork (15 samples/mode). Sample zero is retained in raw
logs but excluded below. Mean +/- sample standard deviation is descriptive;
samples within a JVM are not independent confidence intervals.

| Operation | Baseline ns/op | Registration ns/op | Baseline B/op | Registration B/op |
| --- | ---: | ---: | ---: | ---: |
| Counter Count | 143 +/- 25 | 154 +/- 30 | 265.6 | 265.6 |
| Gauge Value | 134 +/- 8 | 152 +/- 32 | 265.6 | 265.6 |
| Histogram Count | 125 +/- 25 | 166 +/- 27 | 217.6 | 265.6 |
| Timer Count | 119 +/- 21 | 173 +/- 14 | 212.8 | 260.8 |
| Meter Count | 121 +/- 22 | 141 +/- 6 | 212.8 | 264.0 |
| Full scrape | 4,029,145 +/- 357,900 | 4,217,032 +/- 282,634 | 14,419,502 | 14,766,371 |
| One matching scope | 73,931 +/- 5805 | 75,205 +/- 5702 | 51,584 | 51,584 |
| Missing keyspace | 47,514 +/- 1813 | 47,980 +/- 1908 | 49,944 | 49,944 |
| Broad domain, 1000 matches | 470,623 +/- 112,703 | 453,952 +/- 26,273 | 1,633,448 | 1,633,448 |

Temporary adapters increase monitoring allocation and histogram/timer Count
latency. Full-scrape allocation increases about 347 KB (2.4%); its mean time
increases 4.7%, with overlapping sample variation. No latency improvement is
claimed. Query allocation is unchanged; query times vary within the samples.
Counter/gauge code was already compact in the baseline. Their time variation
provides context for attributing smaller changes. Allocation includes the
calling thread only, with observed just-in-time (JIT) compiler differences of 24 B in some boxed-value
cases. These are local calls with empty metrics, not remote throughput or
recording-path measurements. The node profiles separately exercise populated
history and full property-query scrapes.

The result justifies retaining an optional resident-heap tradeoff with the
legacy switch still available; it does not justify enabling compact JMX by
default. No recording method changed in this task.

Raw matrix: logs/20260908-002131-957224-jmx-bookkeeping-benchmarks.json.
Summary: logs/20260908-002230-139098-jmx-bookkeeping-benchmark-summary.json.
All case result counts agree across modes and repetitions.

## Setup and release cost

The production OwnedMetrics holder was measured in three fresh JVMs. Each
round creates 1000 holders, performs a missing lookup plus insertion for each
of 64 names, then iterates and clears every holder. Modes alternate each round.
Rounds 0–19 warm the code; rounds 20–29 give 30 samples/mode across three forks.
Names and the shared metric value are created outside the measurements.

| Bookkeeping operation | Map | Compact list |
| --- | ---: | ---: |
| Construction mean +/- sample standard deviation | 1.67 +/- 0.33 us/table | 6.26 +/- 0.48 us/table |
| Construction allocation | 3144 B/table | 576 B/table |
| Iteration and clear | 303 +/- 157 ns/table | 240 +/- 37 ns/table |
| Resident structure with current holder | 2648 B/table | 576 B/table |

Map allocation includes transient resize arrays; the original pre-task map
resident structure was 2624 B/table without the new 24 B holder. Compact setup
is consistently slower by about 4.6 us/table in this workload. That is the
cost of a simple bounded linear scan. The release timing is only bookkeeping
iteration/clear; real aggregate and registration removal are verified by
lifecycle tests, not included in that microbenchmark. No release latency win
is claimed from the overlapping timings. Full 1000-table schema-creation phases
took 89.0, 91.0, and 92.1 seconds in baseline, registration-only, and combined
runs. Those include schema persistence and system-table flushes, so they do
not isolate bookkeeping latency.

An initial eight-round benchmark showed continuing JIT warmup. It remains in
logs/20260908-002712-855407-jmx-bookkeeping-benchmarks.json; the longer-warmup
matrix is logs/20260908-002848-625111-jmx-bookkeeping-benchmarks.json and its
summary is logs/20260908-002930-336188-jmx-bookkeeping-benchmark-summary.json.

## Validation

- Seven focused registration/history tests pass: all six metric wrapper
  interfaces, legacy MBeanInfo/ObjectInstance/queryMBeans equality, metadata
  array independence, getters, operations, invalid signatures, null/missing
  attributes, failing gauges, registration retry, cumulative/noncumulative
  history, resets, overflow, and independent alias history in both modes.
- Two generated registration suites pass, including histogram/timer/meter
  updates under both adaptive-history settings.
- Bookkeeping growth/replacement/no-op/clear and real subclass lifecycle tests
  pass in both modes. The latter adds 200 late gauges, reuses a duplicate name,
  removes aggregate membership and registry names, and calls release twice.
- 32,000 generated bookkeeping lifecycle steps agree with HashMap for lookup,
  replacement, iteration, and clear.
- The 35-test registry/profile/recording integration suite passes in both map
  and compact bookkeeping modes, with lazy IDs, transient queries, and compact
  JMX. Each run exercises all/simple profiles, hidden inputs, no-ops, recording,
  and drop/recreate.
- Local and real loopback remote boundary/operation/notification checks and
  Cassandra authorization checks pass. The generated query suite passes
  16,000 comparisons, 1600 lifecycle names, and 1000 concurrent cycles.

Logs include 20260908-001718-ai-ci-test.log (bookkeeping),
20260908-001828-ai-ci-test.log (subclass lifecycle),
20260908-001948-run_property_tests.log (bookkeeping properties),
20260908-001949-many-tables-launch.log (registration/history),
20260908-001958-run_tests.log (compact profile integrations),
20260908-003214-run_tests.log (map profile integrations),
20260908-002445-ai-ci-test.log (authorization), and
20260908-002836-996082135-jmx-names.log (query properties), all under logs/.
The final clean JAR build and main/test Checkstyle pass in
logs/20260908-002930-ai-build.log. git diff --check also passes.

## Artifacts and exact commands

Pre-edit working-tree source copies, Git status, and path manifest:
tmp/jmx-registration-bookkeeping-baseline/. This includes 1158 files. Frozen
control: tmp/jmx-registration-bookkeeping-baseline/control.jar. Task path list:
tmp/jmx-registration-bookkeeping-paths.txt. Earlier dirty changes are preserved.

```sh
distrobox enter dev -- .build/sh/ai-build
distrobox enter dev -- env PROFILE_LAZY_METRIC_IDS=true PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/jmx-registration-bookkeeping-baseline/control.jar .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 134 --metrics-config simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/jmx-bookkeeping-baseline-1000
distrobox enter dev -- bash .build/sh/ai-analyze-heap-dominators logs/jmx-bookkeeping-baseline-1000/20260907-235934-heap-ownership-1000t/rescraped.hprof
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/analyze-heap-ownership.py logs/jmx-bookkeeping-baseline-1000/20260907-235934-heap-ownership-1000t/rescraped.hprof --expected-tables 1000
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python tmp/probe_jmx_bookkeeping.py logs/jmx-bookkeeping-baseline-1000/20260907-235934-heap-ownership-1000t/rescraped.hprof
```

The profile JVM exited before heavy heap analysis started. Baseline artifacts:

- logs/20260907-235814-ai-build.log (successful control build/Checkstyle).
- logs/jmx-bookkeeping-baseline-1000/20260907-235934-heap-ownership-1000t/.
- logs/20260908-000157-rescraped-dominators.zip (disjoint dominators).
- logs/20260908-000315-rescraped-dominators.zip (proxy handlers/server ownership).
- logs/20260908-000258-719282-rescraped-heap-ownership.json (bounded census).
- logs/20260908-000313-822214-jmx-bookkeeping-probe.json (structure/units).

## Failures retained

The temporary heap probe initially passed a string instead of Path to the
reader, then called its object-array accessor on instances. Both errors were
corrected. Its first successful map walk included gauge values; the corrected
structure-only whitelist yields 2,773,568 B and excludes those values.
Registration test development caught an import-order Checkstyle failure and a
test-harness try-with-resources use on a wrapper without AutoCloseable. Both
were corrected before rerunning validation.

The first monitoring driver completed its baseline JVM but failed to parse
Cassandra's log-prefixed CSV rows. The corrected parser accepts that prefix;
the complete three-fork matrix was rerun sequentially. No profile launcher
changed while a measurement JVM was active.

Additional reproduction commands (run sequentially):

```sh
distrobox enter dev -- env PROFILE_LAZY_METRIC_IDS=true PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/jmx-registration-only.jar .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 135 --metrics-config simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/jmx-bookkeeping-registration-1000
distrobox enter dev -- env PROFILE_COMPACT_BOOKKEEPING=true PROFILE_LAZY_METRIC_IDS=true PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/jmx-bookkeeping-final.jar .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 136 --metrics-config simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/jmx-bookkeeping-final-1000
distrobox enter dev -- uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python tmp/benchmark_jmx_bookkeeping.py
distrobox enter dev -- uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python tmp/benchmark_jmx_bookkeeping.py --bookkeeping
distrobox enter dev -- bash run_tests.sh --metric-bookkeeping
distrobox enter dev -- env PROFILE_SKIP_BUILD=true bash run_property_tests.sh --metric-bookkeeping
distrobox enter dev -- bash run_tests.sh --jmx-registration
distrobox enter dev -- env PROFILE_SKIP_BUILD=true bash run_property_tests.sh --jmx-registration
distrobox enter dev -- env PROFILE_LAZY_METRIC_IDS=true PROFILE_COMPACT_BOOKKEEPING=true PROFILE_TRANSIENT_JMX=true bash run_tests.sh --metric-profiles
distrobox enter dev -- env PROFILE_LAZY_METRIC_IDS=true PROFILE_TRANSIENT_JMX=true bash run_tests.sh --metric-profiles
distrobox enter dev -- bash run_tests.sh --jmx-query
distrobox enter dev -- bash run_property_tests.sh --jmx-query
```

The benchmark driver runs inside dev and invokes the existing local launcher,
avoiding nested distrobox launches. Its outer log preserves all fork output.
Production controls are frozen JARs; compiled test classes must be refreshed
before using PROFILE_SKIP_BUILD. No dependency was added, no full test suite
ran, and no files were staged or committed. Default eager metric IDs, metric
profiles, histogram math, counter algorithms, and memtable retirement remain
unchanged. This task makes no million-table or maximum-capacity claim.
