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

# Runtime metric profiles

At 1,000 tables, the final simple profile reduces rested creation heap from
298.03 to 113.93 MiB (61.77%). After full scraping and metric activity, heap falls
from 549.47 to 167.93 MiB (69.44%). Both profiles remain selectable through
configuration. These measurements do not establish million-table
capacity or sustained read/write throughput.

## Configuration and scope

`metrics_config_file` in `cassandra.yaml` selects a startup profile. The supplied
configuration selects `simple_metrics.yml`. `all_metrics.yml` enables every
built-in catalog entry. Omitted or null configuration preserves all metrics,
including extension names outside the catalog. Bare filenames resolve on the
classpath; absolute paths and `file:` URIs select local files. Restart to change
the selection. `optimized_metrics_enabled` remains an independent choice.

The immutable policy uses existing SnakeYAML and a generated catalog bundled in
the Cassandra jar. The reference generator emits both that catalog and
`conf/metrics_ref.md` from Java declarations. No runtime compiler or new library
is required. Validation rejects unknown names, duplicate YAML keys, duplicate or
overlapping entries, malformed sections, and missing or misclassified required
entries. Unlisted optional names are disabled. Aliases follow the canonical name.

Selection covers Table, IndexTable, their legacy aliases, and Keyspace metrics.
Global Table aggregates, TrieMemtable metrics, Storage Attached Indexing (SAI),
and other node/service families retain their current behavior. The legacy
`system_views` metric tables retain their schemas but return no rows when their
metric is disabled. Missing exports do not represent measured zeros.

## Implementation increments

Step 1 suppresses registry and Java Management Extensions (JMX) entries while
retaining all recording state. Registry factory shortcuts for counters and meters
also obey the policy. Table cleanup owns metric references directly, since an
unregistered metric cannot be discovered through the registry. Format-specific
gauges retain their existing canonical-name deduplication without registration.

Cleanup now removes table latency children from their parents. The previous
cleanup depended on registry entries and did not call `LatencyMetrics.release`.
Parent release preserves the removed child's counts and distribution history.
It folds only the removed child into the parent's own reservoir; including live
siblings there would count those siblings again. Release is idempotent. Parent
membership permits concurrent iteration, while leaves share an empty collection.

Step 2 replaces unused recorder destinations with shared no-ops. Table histogram,
meter, and timer forwarding keeps enabled keyspace and global destinations.
Forwarding arrays exclude no-op destinations at construction. No profile lookup
runs on each observation. An unused latency pair shares a singleton only when
neither component nor an enabled parent needs it. Hidden latency children remain
real when a parent pulls their history.

Table counters and computed gauges remain real because global aggregates or
database functions can read them. The two anticompaction byte meters remain real
because the global ratio reads their table values. Required coordinator timers,
disk accounting, and the unregistered flush-size moving average remain active.
Disabled keyspace recorders have no database-control consumers and can use
no-ops. Computed keyspace gauges remain lightweight real objects.

## Measurement method

The pre-change run used commit `4192a00e1f`. Each subsequent run used the current
increment. Runs use Java 21.0.12, the Garbage First collector, an 8 GiB maximum
heap, 512 MiB initial heap, eight visible processors, and aggressive soft-reference
clearing. The harness uses lazy TrieMemtables and BTI SSTables, with row/key caches
disabled. All table counts remain at or below 1,000.

Eight metric workers exist before the first checkpoint and remain alive through
the final checkpoint. Each worker records actual table metrics for every user
table. This is a synthetic metric workload: no user rows or SSTables exist.
Separate integration tests exercise real writes, reads, and flushes. Both scrapes
query names and read every available attribute, including recent-value arrays.
Each rested checkpoint collects garbage and captures a live heap dump.

The harness checks exact registry keys and JMX names, enabled observation counts,
keyspace aggregates, the disabled independent meter, and uninitialized user
memtables. Activity still invokes disabled meters; only their stored value stays
zero. Heap figures describe the whole test JVM. The small-run startup baseline
contains transient system state, so subtracting it does not isolate a reliable
per-table slope. Ownership graphs can overlap and must not be added together.

## Pre-change and intermediate measurements

Raw heap bytes at 100 tables:

| Checkpoint | Pre-change all | Step 1 simple | Step 2 simple |
|---|---:|---:|---:|
| Startup baseline | 60,568,120 | 51,597,472 | 49,736,160 |
| Created, before scraping | 77,161,448 | 52,693,696 | 48,644,896 |
| First full scrape | 116,600,072 | 61,927,912 | 57,886,032 |
| One recording worker | 117,006,752 | 62,327,880 | 58,162,232 |
| Eight recording workers | 119,074,968 | 64,397,920 | 59,369,896 |
| Rescraped after activity | 119,132,024 | 64,393,280 | 59,375,400 |

Step 1 saves 23.33 MiB (31.71%) at creation and 52.20 MiB (45.95%) after
activity and scraping. User-table/keyspace MBeans fall from 25,001 to 4,831.
All metric MBeans fall from 41,831 to 9,714; attributes fall from 226,473 to
54,033. Both scrapes have zero failures.

The ownership census confirms unchanged recorder counts: 6,236 compact
reservoirs, 1,842 LatencyMetrics, 2,012 SnapshottingTimers, 3,619 ThreadLocalCounters,
3,880 ThreadLocalHistograms, and 8,650 ThreadLocalMeters in both runs. Per user
table, JMX wrappers fall from 249 to 48; recent-array storage falls from 67,632 to
13,680 bytes; name-property graphs fall from 166,744 to 32,120 bytes. The bounded
whole-JVM JMX graph falls from 53,437,304 to 12,489,320 bytes, and the registry graph
from 7,487,160 to 2,239,048 bytes. Table ownership bookkeeping adds 464,032 bytes
to its bounded graph. These overlapping graph sizes explain ownership, not
independent contributions to the total reduction.

Step 2 saves another 3.86 MiB (7.68%) at creation and 4.79 MiB (7.79%) after
activity and scraping, relative to step 1. The total reduction from the pre-change
run is 27.20 MiB (36.96%) at creation and 56.99 MiB (50.16%) after activity.
Exports remain identical to step 1. All enabled recording and aggregate checks
pass; the disabled ReadRepairRequests probe remains zero despite attempted
updates. This separates the additional recorder savings from registration savings.

The step-2 ownership comparison confirms that JMX graphs are unchanged while
recorders disappear:

| Whole-JVM class count, 100 tables | Step 1 simple | Step 2 simple |
|---|---:|---:|
| Compact reservoirs | 6,236 | 2,260 |
| ThreadLocalHistogram | 3,880 | 1,869 |
| ThreadLocalMeter | 8,650 | 2,924 |
| SnapshottingTimer | 2,012 | 857 |
| LatencyMetrics | 1,842 | 985 |
| MetricIdReference | 11,723 | 6,859 |

Bounded ThreadLocal state falls from 5,493,120 to 2,962,592 bytes; the bounded
TableMetrics graph falls from 2,133,080 to 1,090,200 bytes. These graphs overlap
other ownership categories. Comparison:
`logs/20260906-212246-700448-metric-profile-heap-comparison.json`.

Artifacts:

- Pre: `logs/metric-profile-pre-100/20260906-203333-heap-ownership-100t/`.
- Step 1: `logs/metric-profile-phase1-simple-100/20260906-210130-heap-ownership-100t/`.
- Step 2: `logs/metric-profile-phase2-simple-100/20260906-211444-heap-ownership-100t/`.
- Ownership comparison: `logs/20260906-210428-058960-metric-profile-heap-comparison.json`.

The first integration attempt exposed missing JMX test setup; enabling the
existing distributed-test JMX feature fixed it. The first filtered census exposed
an expected-key error in the harness: the registry uses lowercase `keyspace`,
while JMX uses `Keyspace`. Corrected expectations passed the complete rerun.

Before step 2 wiring, 32 targeted tests produced exactly two expected failures:
disabled histograms and unused latency families still had distinct real state.
That establishes that the new tests distinguish registration filtering from
recorder elimination. Evidence: `logs/20260906-210229-run_tests.log`.

## Validation

The runtime profile suite passed 34 tests. It covers configuration rejection,
aliases, real JMX exports, shared no-op counters/meters/histograms/timers,
callback and exception behavior, required inputs, exact histogram forwarding,
enabled parents with hidden children, virtual views, and repeated real table
create/write/read/flush/drop cycles. Log:
`logs/20260906-210632-run_tests.log`.

Four runtime property tests passed: 160 generated profile selections across
four seeds, duplicate/conflicting selection cases across the catalog, generated
no-op operations, and concurrent no-op updates. Log:
`logs/20260906-210836-run_property_tests.log`.

All 24 existing tests in `./run_tests.sh` passed without failures, errors, or
skips: TrieMemtable retirement (5), failed retirement (1), lazy initialization
(7), splittable and non-splittable flush ranges (4 each), and heap buffer
accounting (3). Results: `logs/20260906-210938-ai-test-memtable-lazy/`.

The reference generator passed 10 focused tests and its four-seed declaration
order/profile order property test. Its stale-output check confirms the reference
and jar catalog describe the same 126 table and 101 keyspace names.

An independent source review traced startup, registry aliases, direct consumers,
no-op forwarding, release, virtual views, and packaging. It found a missing
Debian install entry for the newly required default file; both profile files now
ship in that manifest. The RPM build already copies the configuration directory.
No remaining correctness findings were reported. Actual package installation
and an actual IndexTable creation scenario were not executed in this task.

The full clean build, jar, main Checkstyle, and test Checkstyle passed:
`logs/20260906-211258-ai-build.log`. The preceding build caught two test import
ordering issues, which were corrected without changing test behavior.

One optional diagnostic consequence remains explicit: the public
`StorageService.getOutOfRangeOperationCounts()` helper omits keyspaces whose
optional counters are disabled. No in-repository runtime caller or management
interface exposes that helper. Node-wide invalid-token counters remain active.

## Final matched pair at 1,000 tables

Both runs use the same final production implementation. Only the metric profile
and isolated test subnet/output directory differ. The launcher refreshes build
date/version resources before each serial run; no production source changes or
concurrent builds occur during either measurement JVM's lifetime.

| Rested checkpoint | All metrics, bytes | Simple profile, bytes |
|---|---:|---:|
| Startup baseline | 56,489,184 | 49,649,032 |
| Created, before scraping | 312,505,848 | 119,462,464 |
| First full scrape | 563,015,872 | 170,030,176 |
| One recording worker | 565,491,320 | 171,734,264 |
| Eight recording workers | 576,033,072 | 176,006,272 |
| Rescraped after activity | 576,164,192 | 176,083,616 |

The simple profile saves **184.10 MiB (61.77%) at creation** and
**381.55 MiB (69.44%) after scraping and activity**. Exact user-table/keyspace
registry and JMX counts fall from 249,101 to 48,031. Total metric MBeans fall from
265,931 to 52,914; scraped attributes fall from 1,396,473 to 259,233; recent-value
attributes fall from 61,216 to 13,443. Both scrapes have zero failures.

All 1,000 user memtables remain clean and uninitialized. Enabled selected metrics
retain eight observations per table and 8,000 per selected keyspace aggregate.
The disabled ReadRepairRequests recorder remains zero in simple mode. No user
SSTables exist. Actual database activity is covered by the separate integration
and flush tests, not by this synthetic metric workload.

Reproduce from the repository root, in the Java 21 development container:

```sh
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 113 --metrics-config all_metrics.yml --out logs/metric-profile-final-all-1000
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 114 --metrics-config simple_metrics.yml --out logs/metric-profile-final-simple-1000
```

Run the commands sequentially. Each creates a timestamped output directory.
Completed artifacts:

- All: `logs/metric-profile-final-all-1000/20260906-211702-heap-ownership-1000t/`.
- Simple: `logs/metric-profile-final-simple-1000/20260906-211924-heap-ownership-1000t/`.
- Verified comparison: `logs/20260906-213030-150953-metric-profile-heap-comparison.json`.

The final heap-dump census finds 400,079,736 fewer indexed bytes, within 840 bytes
of the checkpoint difference of 400,080,576 bytes. Bounded ColumnFamilyStore and
schema graphs are identical across the pair. No metric-name ownership conflicts
were found. Per-table JMX wrappers remain 249 versus 48, recent-value array bytes
67,632 versus 13,680, and name-property graph bytes 166,744 versus 32,120.

| Whole-JVM class count, 1,000 tables | All metrics | Simple profile |
|---|---:|---:|
| Compact reservoirs | 36,836 | 11,260 |
| ThreadLocalHistogram | 21,881 | 9,069 |
| ThreadLocalMeter | 49,151 | 11,924 |
| SnapshottingTimer | 10,112 | 2,657 |
| MetricIdReference | 72,054 | 41,095 |

Bounded JMX ownership falls from 339,271,240 to 68,078,192 bytes; registry ownership
from 48,146,768 to 11,432,656 bytes; ThreadLocal metric state from 27,345,352 to
11,224,536 bytes. These graphs overlap and are not dominator retained sizes.
They confirm removal of registration objects and unused recorder state without
attributing unrelated table/schema reductions to the profile.

## Limits and next work

This is one matched final pair, with earlier 100-table checkpoints separating the
two increments. Whole-JVM totals include system tables and startup activity.
The measurements cover empty user tables plus real metric observations, with
full name-inspecting scrapes. They do not measure query latency, burst ingestion,
dirty memtables, large SSTable sets, or maximum table capacity under a 2 GiB heap.

Selection does not bound metric history for all enabled children, remove schema
metadata, or unload ColumnFamilyStore. Global aggregates still require some
hidden table state. The remaining residency work starts with JMX recent-value
history and worker counter-array holes, then the surviving table/schema owners.
Automatic idle retirement remains deferred until resident metrics storage is
addressed. The all-metrics profile preserves the full built-in export catalog.
