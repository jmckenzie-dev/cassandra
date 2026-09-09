<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the License); you may not
use this file except in compliance with the License. You may obtain a copy
at http://www.apache.org/licenses/LICENSE-2.0 . Unless required by applicable
law or agreed to in writing, software distributed under the License is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
-->

# September 7 checkpoint: the path to one million tables

**The main task is reducing the memory cost of keeping a table present, even
when it is idle.** Memtable changes and selective metrics have reduced that
cost substantially. The remaining footprint is still far too large for a
million tables on a small heap.

The recent OpenTelemetry (OTel) experiments answered a narrower question:
could smaller histogram counters reduce memory further? They produced useful
evidence, but did not integrate OTel or reduce the server's current footprint.
There is enough evidence to pause that investigation and return to the larger
resident-memory owners.

## The intended system

The working design is a node with many logically available tables, where a
much smaller set needs writable ingest buffers at any instant. Reads of idle
tables should use their SSTables without creating writable memtable storage.
Writes should allocate that storage on demand. Flushing should let it go again.

This follows the proposed use of CommitLog plus memtables as an ingest buffer,
with disk/page-cache reads doing more of the work. Cursor-based, low-allocation
compaction is an assumption for that broader direction. The experiments here
have not validated that future compaction regime.

One million available tables and one million simultaneously dirty tables are
different capacity targets. The latter also needs a budget for their combined
ingest buffers and flush demand. Neither capacity has been demonstrated yet.
The target heap and concurrent writable working set still need to be explicit
before declaring success. The earlier 2 GiB example is not a measured capacity.

## What is implemented

| Area | Current behavior | What it buys |
|---|---|---|
| Profiling harness | Creates tables, records allocation and heap ownership, exercises write/flush/read cycles, and checks results | Repeatable evidence rather than inferred savings |
| Lazy TrieMemtables | Writable shard structures initialize on the first mutation; pure reads leave them dormant | Empty tables and fresh replacements avoid private writable structures |
| Explicit retirement | The harness requests existing production flushes; lazy replacements stay empty, and old storage is reclaimed after readers finish | Idle dirty-table buffers can be released safely |
| Compact recording histograms | Empty/sparse storage is lazy, stripes expand on contention, dense counters start at int width and widen | Smaller resident recording state; legacy remains selectable |
| Metrics profiles | YAML selects required/optional exports; unused recorders can share no-ops while internal and aggregate inputs remain real | Large reductions in registration and recording objects |
| Adaptive saved scrape history | An optional path stores previous histogram snapshots in narrow arrays, or no array when all zero | Less memory retained solely to answer recent-value queries |

Explicit retirement reclaimed **40 MiB of slab payload from 40 lightly written
tables** in a controlled test. That establishes reclamation of their data
buffers. It does not unload their table objects, schema, or metrics.

The lazy memtable implementation is **TrieMemtable-specific**. It is not a
general eviction system for all memtable types. There is also no automatic
30-second idle retirement policy yet.

The branch already has real read/write/flush correctness tests. The latest
large heap census, however, uses empty user tables and synthetic metric
updates. Its memory results are not a sustained database throughput test.

Sources: [memtable initialization](lazy_memtable_initialization.md),
[explicit retirement](explicit_memtable_retirement.md),
[compact recording metrics](compact_runtime_metrics.md),
[metrics profiles](metric_profile_runtime.md), and
[saved scrape history](adaptive_jmx_history.md).

## Where the memory stands

At 1000 tables, a matched comparison with compact recording already present
reduced final heap from **549.47 MiB with all metrics to 167.93 MiB with the
simple profile**. Adaptive saved history then reduced a matched simple-profile
run from about **167.91 MiB to 153.95 MiB**. A fresh repeat measured **153.83 MiB**.
These are successive measurements, not an exact upstream-to-current percentage.

The fresh optimized census attributes these major retained groups:

| Owner at 1000 tables | Approximate retained memory | Why it remains |
|---|---:|---|
| Java Management Extensions (JMX) servers | 65.1 MiB | Exported metric objects, names, and name caches |
| Table runtime objects (`ColumnFamilyStore`) | 17.3 MiB | Per-table machinery remains resident after memtable retirement |
| Registry map | 8.3 MiB | Enabled metrics still need registry entries |
| Table metrics objects | 6.6 MiB | Metric references and lifecycle bookkeeping |
| Worker counter contexts | 6.3 MiB | Per-thread arrays reserve slots for many metric IDs |

These owner groups omit other heap state and include some system/harness
objects. Do not divide each row by 1000 and treat it as a universal table cost.
The JMX result also includes name-property caches warmed by this local scrape
client. That extra cache growth is not inherent to every remote exporter.

An earlier size-series measurement still showed roughly **77 KiB per table
before scraping** on the simple path. Adaptive scrape history barely affects
that floor. At a million tables, every 1 KiB/table costs about 0.95 GiB; a
77 KiB/table slope would imply roughly 73 GiB before other costs. This arithmetic
shows the size of the gap, not a reliable million-table capacity forecast.

The central remaining problem is therefore the collection of objects retained
for every table. Smaller histogram arrays alone cannot close that gap.
See the [latest ownership census](optimized_heap_next_steps.md).

## Why the recent histogram experiments happened

There are three separate things that can easily get called “the histogram”:

| Component | Purpose | Status |
|---|---|---|
| Recording state | Stores observations used for distribution statistics | Compact Cassandra implementation exists |
| Saved scrape history | Stores a previous snapshot so a later scrape can compute a difference | Optional adaptive-width implementation exists |
| Registration/export objects | Let monitoring clients discover and query metrics | Profiles reduce their number, but each enabled metric still has overhead |

OTel supplied an adaptive array idea: use byte/short counters while counts are
small, then widen. The saved-history implementation already uses that idea.
The latest experiments asked whether it also fits **recording state**.

The storage benchmark found 216-byte byte-width arrays and 384-byte short-width
arrays, versus 712 bytes for the current Cassandra int-width atomic array at
165 buckets. Once OTel widens to int or long, that memory advantage disappears.
Its plain updates require exclusive ownership or external synchronization.
It also has a narrow weighted-add overflow incompatibility with Cassandra.

The follow-up ran actual Cassandra reservoirs over simulated time. It found:

- Cumulative counts can remain narrow for quiet tables. At one event/minute,
  a full day's concentrated count fits in a short.
- Decay-weighted counts can become huge despite small normalized counts. At
  one event/second, a stored bucket reached 93.5 billion after 30 minutes while
  its normalized percentile bucket contained 87.
- Rescaling reduces those weighted counts, then they grow again. OTel's
  `clear()` does not itself shrink its allocated array.

This supports treating cumulative and weighted counters separately. It does
not establish a ready replacement histogram, a new threading model, or a node
memory saving. A full OTel SDK/provider replacement, native library, and
worker-owned histogram design remain unimplemented.

The existing compact Cassandra recording path also has measured hot-path
tradeoffs: its earlier final microbenchmark was about 13.3% lower throughput
at one thread and 4.1% lower at four threads than legacy. Those costs concern
recording, not only a first write after memtable retirement. Future changes
need throughput checks alongside retained-memory measurements.

Sources: [OTel storage experiment](otel_compact_storage_benchmark.md),
[counter widths over time](histogram_width_over_time.md), and
[deferred threading design](metric_threading.md).

## What remains, in practical order

### 1. Address metrics registration and export

The follow-up [monitoring investigation](jmx_monitoring_name_retention.md)
attributes 33.47 MiB of the 65.11 MiB JMX total to avoidable ObjectName caches.
Reading the same attributes without warming those caches leaves 31.64 MiB
under JMX. Both local inspection and server-side property queries can populate
the caches, including queries from remote clients. Changing one scraper alone
does not prevent other callers from populating them.

The subsequent [query/export implementation](jmx_query_export.md) adds an
optional startup MBeanServerBuilder that avoids these caches for tested local
and remote monitoring paths, including Cassandra authorization. A separate
optional compact gauge/counter registration setting removes 38,699 persistent
JDK adapters at 1000 tables. Together they leave 30.76 MiB under the JMX server
subtrees. This is progress on the largest target, with registration work left.

The simple profile proved that fewer registrations save substantial memory.
The next question is how to expose the required names and behavior with fewer
resident objects per enabled metric. A shared metric catalog with exports
resolved on demand is a candidate, not an implemented design.

This is the largest measured target. Preserve discovery, aliases, aggregates,
and monitoring-client behavior. Keep the legacy path available for comparison.
Changing the histogram arithmetic again will not remove these registration
objects. More pluggable metrics may help this work, but pluggability alone
does not save memory.

### 2. Finish the bounded residency improvements already identified

Check the registry before constructing duplicate global histograms/timers,
then measure IDs allocated only when a counter is first updated. Those changes
may reduce worker-array holes and cleanup state. Avoided temporary allocation
alone is not enough; require a retained-memory reduction before counting a win.

Next, compact per-table metric release bookkeeping. The current map structure
has an estimated opportunity around 2 KiB/table without adding recording-path
work. These improvements do not address the JMX-owned memory above.

### 3. Reduce the table runtime that remains even with minimal metrics

`ColumnFamilyStore`, schema metadata, and supporting structures still exist
for every table. Examine which state can be shared or initialized on demand.
An unused compression-dictionary cache is one bounded candidate. Full table
runtime unloading is a broader lifecycle project and is not implemented.

This work becomes unavoidable if the target heap requires only a few KiB of
standing state per table. Metrics improvements do not establish that the
remaining database structures fit that budget.

### 4. Turn explicit retirement into a bounded automatic policy

Choose idle tables, schedule flushes, limit in-flight work, and handle failures
and reactivation. Avoid scanning a million tables on a short timer or flushing
them all at once. Preserve commit-log coverage, reader/writer ordering, and
base/index coupling. The existing retirement tests provide a starting point.

An aggressive ingest-buffer policy will also need write-burst, flush, and
recreation measurements. Automatic retirement alone does not remove schema,
metrics, or SSTable-reader state.

### 5. Validate populated-node scaling beyond heap at creation

After reducing the resident floor, test table-count and active-working-set
growth separately. Cover sustained reads/writes, retirement churn, creation
allocation, startup/schema operations, and background work. Populated tables
also add SSTable readers, file descriptors, mappings, and compaction/repair
state. Those are remaining validation areas, not all proven blockers today.

Keep runs at or below 1000 tables until the current cap is explicitly lifted.
Small runs can establish mechanisms and slopes; they cannot prove successful
operation at 100000 or one million tables. Future cursor compaction also needs
validation in the eventual combined workload.

## Recommended focus now

Pause further histogram-algorithm experiments. Continue reducing the remaining
JMX registration objects, with before/after heap measurements. The optional
query/export path now avoids the measured name caches and removes gauge/counter
adapters. Registered names, repository entries, and metric wrappers still
account for a substantial resident cost.

Judge progress by three separate quantities: resident cost per idle table,
additional memory for the active write set, and memory/CPU growth with workers
and monitoring. A memory saving at first write or during a scrape is useful,
but it does not automatically reduce the idle-table floor.

## Branch and configuration checkpoint

HEAD is `4192a00e1f`. The harness, lazy memtable/retirement work, and initial
compact recording optimizations are committed. Metrics profile runtime
loading, adaptive saved history, and the newer experiments/reports remain
working-tree changes. Historical reports describe their status when written;
their old “uncommitted” notes do not override current Git history.

The supplied `cassandra.yaml` selects `simple_metrics.yml` and enables compact
recording. Adaptive JMX history remains **off by default**; the 153.83 MiB
optimized census explicitly enabled it. All-metrics registration, legacy
recording, and legacy saved history remain available as separate controls.

Two newer controls are off by default. The startup property
`-Djavax.management.builder.initial=org.apache.cassandra.utils.TransientMBeanServerBuilder`
must precede platform-server initialization. YAML setting
`compact_jmx_registration_enabled: true` independently selects compact gauge
and counter exports. The [measurement report](jmx_query_export.md) records
query/call allocation costs and the arbitrary-MBean compatibility boundary.

This checkpoint file changes documentation only. The [continuation record](prosecute_memtable_tables.md)
retains implementation detail; this file records the overall direction.
