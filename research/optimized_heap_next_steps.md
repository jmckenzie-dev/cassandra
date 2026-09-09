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

# Heap profile after adaptive JMX history

Measured September 7, 2026. Runtime code is unchanged from the adaptive-history
checkpoint. This run identifies the next residency targets; it implements no
further optimization.

## Result

A fresh 1000-table run reproduced the previous result: final whole-JVM heap
was 161,302,224 bytes (153.83 MiB), versus 161,423,368 in the previous run.
JMX registration and name caches remain the largest owner. Worker counter
arrays remain sparse, but ordinary paging would recover much less memory than
their low nonzero occupancy suggests.

The later [monitoring investigation](jmx_monitoring_name_retention.md) changes
the recommended order: prioritize JMX query/export and registration residency.
It attributes 33.47 MiB to name caches and shows that remote property queries
can populate them too. The counter/metric-ID and release-bookkeeping candidates
below remain useful follow-ups, but do not address that JMX-owned memory.

## Reproduction and checks

```sh
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 122 --metrics-config simple_metrics.yml --adaptive-jmx-history --out logs/adaptive-history-next-census-1000
```

The run exited 0. The harness checked registrations, aggregate and table counts,
scrape failures, and clean/uninitialized user memtables. Configuration remained
Java 21, eight workers/processors, G1, 8 GiB heap ceiling, lazy TrieMemtables,
compact recording histograms, and BTI. These are empty user tables with synthetic
metric updates, not a loaded query-throughput or capacity test.

Artifacts: `logs/adaptive-history-next-census-1000/20260907-103350-heap-ownership-1000t`.

| Checkpoint | Whole-JVM heap bytes |
| --- | ---: |
| Startup | 49,678,344 |
| Created | 119,530,048 |
| Full scrape | 154,866,288 |
| One worker | 156,449,488 |
| Eight workers | 160,737,592 |
| Repeat full scrape | 161,302,224 |

After the profiling JVM exited, Eclipse Memory Analyzer (MAT) computed dominator
trees for created and rescraped heaps. Targeted field queries separated registry
and lifecycle owners. The existing bounded ownership analyzer and a temporary
read-only HPROF probe inspected the final snapshot. Heavy analyses ran after
the measurement JVM, not during it.

## Retained owners

These are disjoint top-level dominator groups, measured by MAT. A retained
subtree includes objects whose root paths all pass through its dominator. The
rows are not totals for every instance of the named class. The table omits
smaller groups and includes system-table and harness state.

| Group | Created bytes | Rescraped bytes |
| --- | ---: | ---: |
| JMX servers | 32,636,888 | 68,274,120 |
| ColumnFamilyStore | 18,159,288 | 18,175,512 |
| Class objects and dominated static state | 15,319,112 | 15,368,032 |
| ZipFile sources | 9,090,352 | 9,090,352 |
| Four concurrent maps | 8,766,104 | 8,766,104 |
| TableMetrics | 6,925,464 | 6,925,464 |
| ThreadLocalMetrics contexts | 1,747,600 | 6,633,360 |
| ThreadLocalHistogram | 1,801,016 | 2,265,016 |
| TableMetadata | 1,413,680 | 1,413,680 |
| LatencyMetrics | 951,056 | 951,056 |
| TopPartitionTracker | 720,000 | 720,000 |

ZipFile sources are classpath/harness overhead, not a per-table optimization
target. Do not extrapolate every row by dividing by 1000.

Nested field measurements explain some of those groups. They overlap the table
above and must not be added to it:

| Field/static owner | Rescraped retained bytes |
| --- | ---: |
| CassandraMetricsRegistry.metrics | 8,730,200 |
| ThreadLocalMetrics.phantomReferences | 4,204,136 |
| ThreadLocalMetrics.summaryValues | 620,672 |
| ThreadLocalMetrics.freeMetricIdSetTracker | 34,072 |
| ThreadLocalMetrics class | 4,859,824 |
| TableMetrics class | 2,706,744 |
| ThreadLocalMeter class | 722,640 |

The registry map accounts for almost all of the concurrent-map group. The
ThreadLocalMetrics class total includes its phantom references and summary.

## Worker occupancy

Each of the eight census workers has the same final counter array:

- Capacity: 76,336 long slots, 610,688 payload bytes.
- Nonzero: 6002 slots, or 7.86% of capacity.
- Values: 6000 slots contain one; two shared aggregate slots contain 1000.
- Highest nonzero slot: 72,958.

All eight arrays contain 4,885,504 payload bytes. The MAT context increase of
4,885,760 bytes includes the extra array/context headers. The node's ID generator
is at 73,057, while live metric objects own 64,937 distinct IDs. Its shared
summary array has 77,580 slots and 620,640 payload bytes.

Zero does not imply an invalid or disposable counter. The live-ID count, ID
high-water mark, and array capacity measure different things. The 8120-ID gap
includes lifecycle gaps; this probe does not attribute all of them to one cause.

Source accounting gives 58 live IDs per ordinary simple-profile lazy-Trie table:
28 plain counters, 10 histogram counters, and 20 meter counters. A timer owns a
histogram counter plus the meter's lifetime and unticked counters. The workload
touches six slots per table plus two shared aggregate slots. Rate arrays use
separate indexes, so splitting counter IDs from rate IDs would duplicate an
existing separation.

### Why plain paging is insufficient

The following estimates use the captured nonzero slot indexes. They include
primitive-array headers and a dense outer reference directory, with compressed
references and eight-byte alignment. They exclude pages used earlier and later
reset to zero, lifecycle metadata, spare capacity inside a new design, and CPU
cost. They are estimates, not measurements of an implemented alternative.

| Page slots | Occupied pages/worker | Estimated bytes/worker |
| ---: | ---: | ---: |
| 8 | 2768 | 259,624 |
| 16 | 2264 | 345,120 |
| 32 | 1659 | 460,808 |
| 64 | 1009 | 537,544 |
| 128 | 544 | 568,168 |
| 256 | 275 | 568,816 |

The current dense array occupies 610,704 bytes including its header. A 64-slot
page design saves only about 12%. Eight-slot pages save about 57.5%, but create
22,144 leaf arrays across eight workers. A compact sparse design deserves a
comparison with dense storage; tiny pages are not an automatic win. Other node
threads with only dozens of active slots could benefit much more from paging.

At 58 live IDs/table, dense payload alone is 464 bytes per table per worker.
That means 3712 bytes/table at eight workers, 29,696 at 64, or 118,784 at 256,
before capacity slack and historical gaps. Worker count therefore matters to
the million-table objective even though eight-worker storage is smaller than
JMX in this run. These are storage formulas, not capacity forecasts.

## Next implementation sequence

1. **Avoid duplicate global metric construction.** Histogram/timer factory calls
   construct candidates before registry deduplication. Each new table requests
   13 global histograms and six global timers: 31 temporary counter IDs plus
   reference/cleanup/rate state. Counter and meter factories already check the
   registry first. Match that pattern for histograms/timers, preserving type,
   alias, profile, and concurrent-registration behavior. Reprofile a fresh JVM.
   Accept a resident-memory claim only if ID high-water marks, summary/worker
   arrays, or retained cleanup state fall. Avoided allocation alone does not
   meet the current residency priority; many temporary IDs already recycle.

2. **Evaluate IDs allocated on first update.** Untouched counters could return
   zero without allocating an ID or phantom reference. This would reduce holes
   across every worker plus shared summary and cleanup storage. Reads and
   background meter ticks must not initialize empty counters. First-use
   synchronization, update checks, and safe publication need benchmarks. This
   can change the density substantially, so choose final worker storage after
   measuring it. Keep the dense implementation as an A/B control.

3. **Compact per-table metric release bookkeeping.** `ownedMetrics` keeps a
   HashMap mainly for construction-time lookup and release. The observed
   64-entry map structure costs 2624 bytes/table. A flat name/metric array needs
   about 528 bytes at exact capacity, an estimated 2096-byte saving before
   holder/spare-capacity costs. Preserve hidden recorders and aggregate removal.
   The protected creation helpers permit later subclass calls even though none
   exist in-tree; a compact append/lookup path is safer than blindly freezing
   the base constructor's result. This adds no lookup to metric recording.

These should be separate changes with pre/intermediate/post measurements. For
worker storage, compare the existing shared-table workload with partitioned
table access at eight and 64 workers, still at no more than 1000 tables. Measure
update throughput/allocation as well as heap. Test reset, drop/recreate, ID
recycling, count reads during growth, and worker exit before considering a
replacement correct.

## Larger residency work

**JMX representation remains the biggest target.** The full scrape increases
JMX retained size by 35,637,232 bytes. Prior and fresh bounded ownership show
about 32,120 bytes/table in ObjectName property-map graphs; adaptive history
is only 512 bytes/table in this workload. Name caches are nested within JMX,
not additional to its retained total.

The full harness deliberately calls ObjectName property accessors on locally
returned registered names. JDK 21 then retains parsed maps and strings. The
attribute-only control in the previous experiment did not warm those user
property maps. This cost matters for local exporters that inspect names, but
is not inherent to every remote scrape or getAttribute call. Copying names or
changing the test client is not proof of a general server-side solution.

Avoid warming registered-name caches where Cassandra controls the caller.
For a larger reduction that keeps existing external metric names, investigate
a shared metric catalog and an export adapter that resolves per-table state
on demand. Verify JMX query patterns, aliases, metadata, listeners, remote and
local clients before selecting that design. Replacing histogram storage again
will not remove the registration objects.

**Lazy empty dictionary caches are another bounded candidate.** CFS eagerly
creates a dictionary manager and its Caffeine cache even when dictionary
compression is disabled. Prior MAT evidence puts the whole manager subtree
at 1592 bytes/table, which is an upper ceiling for a cache-only change, not a
measured saving. Current graphs still contain the eager cache shells. Measure
the current cache alone before setting a target; preserve schema enable/disable,
dictionary loading, and close behavior. Do not sum this with the CFS group.

## Source and artifact references

Relevant source:
- `src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java`: histogram/timer factories around lines 359 and 415.
- `src/java/org/apache/cassandra/metrics/TableMetrics.java`: ownedMetrics at 298, constructor completion at 1024, release at 1054, helpers around 1104 and 1324.
- `src/java/org/apache/cassandra/metrics/ThreadLocalMetrics.java`: thread exit at 198, update/growth at 231/304, reset at 241, recycling at 353/418.
- `src/java/org/apache/cassandra/metrics/ThreadLocalHistogram.java:43`, `ThreadLocalMeter.java:185`, `ThreadLocalTimer.java:62`, `LatencyMetrics.java:139`, and `TrieMemtableMetricsView.java:56`: ID allocation.
- `test/distributed/org/apache/cassandra/distributed/test/HeapOwnershipCensusHarness.java:550`: synthetic worker updates.

MAT reports contain `pages/Query_Command2.csv`; timestamp-matched
`*-heap-dominators.log` files preserve commands:
- Created/rescraped groups: `logs/20260907-103619-created-dominators.zip`, `logs/20260907-103633-rescraped-dominators.zip`.
- Registry fields: `logs/20260907-103705-created-dominators.zip`, `logs/20260907-103730-rescraped-dominators.zip`.
- Lifecycle fields: `logs/20260907-103746-created-dominators.zip`, `logs/20260907-103759-rescraped-dominators.zip`.
- Static owners: `logs/20260907-103812-created-dominators.zip`, `logs/20260907-103837-rescraped-dominators.zip`.

Counter occupancy and paging estimates:
`logs/20260907-103903-430106-optimized-heap-probe.json`, produced by the temporary
read-only script `tmp/probe_optimized_heap.py` using the existing HPROF reader.
Bounded ownership and class census:
`logs/20260907-104132-504393-rescraped-heap-ownership.json`.
Bounded groups overlap and are not dominator retained sizes. MAT and HPROF
shallow accounting should not be combined into one additive table.
