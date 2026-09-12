<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Matched integration measurements

All 18 planned measurement cells passed: eight smoke cases, four profile
cases, four idle-policy cases, and two history census cases. One idle cell
required a longer harness watchdog; its original failed attempt remains
preserved. No production or test-source change was required.

All runs used the frozen combined binaries, Java 21.0.12.1, an 8 GiB heap,
eight active processors, and no competing build/test workload. Residency
runs used a 256 MiB memtable heap pool. Requested and effective controls
passed the checks recorded in the four integration-*-measurements.json files.
Each report links the raw summaries. Drivers retain revision and bytecode
hashes. See integration-measurement-audit.md for independent review scope.

## Metric selection

The matched profile cases use legacy metric storage and eager memtables.
Each completes four write/flush cycles. Both profiles produce the same final
SSTable and flush counts: 100/400 or 1,000/4,000. Each large case completes
16,000 writes and 5,000 verification queries.

| Tables | Profile | Post-GC created heap, MiB | Post-GC settled heap, MiB |
| ---: | --- | ---: | ---: |
| 100 | all | 106.11 | 111.37 |
| 100 | simple | 59.62 | 59.71 |
| 1,000 | all | 499.60 | 547.09 |
| 1,000 | simple | 170.59 | 202.06 |

At 1,000 tables, baseline-adjusted settled growth is 502,673,088 bytes for
all metrics and 157,037,896 bytes for simple metrics. The profiles also affect
system metrics, so the raw process difference is not all per-user-table cost.
The simple profile hides exports but the harness's direct flush/compaction
counters remain real. Their bytes exclude some SSTable components.

## Idle flushing

The matched idle cases use compact metrics, lazy memtables, all metrics, and
the same optional memory controls. Each completes four write/reactivate cycles.

| Tables | Idle flush | Settled initialized tries | Accounted memtable heap, bytes | Post-GC process heap, MiB |
| ---: | --- | ---: | ---: | ---: |
| 100 | off | 100 | 902,800 | 172.76 |
| 100 | on | 0 | 0 | 75.27 |
| 1,000 | off | 1,000 | 9,028,000 | 1,311.27 |
| 1,000 | on | 0 | 0 | 333.04 |

The 1,000-table enabled case completes 4,000 flushes and 1,000 compactions,
leaving 1,000 SSTables. It writes 4,372,000 data bytes through flushes and
4,361,376 through compactions. The disabled control performs neither operation.
Thus the memory reduction has a disk-work cost. Accounted allocator bytes
exclude other retained memtable objects; process heap includes unrelated state.

Enabled drain times are 9.68, 9.65, 9.56, and 14.90 seconds at 100 tables.
The successful 1,000-table retry takes 95.14, 94.77, 97.74, and 148.10 seconds.
The original fourth drain hit its 121-second watchdog while still progressing.
The retry changes only --settle-seconds from 120 to 300; the idle-off control
retains 120. See ../debug/integration-idle-watchdog.md. The runtime settings
stay unchanged. BTI forces normal compaction fallback despite the cursor flag.

## Management history census

Both 100-table cases use simple metrics, compact registration, transient
names, lazy metric identifiers, and compact bookkeeping. Only adaptive
histogram history changes. Each captures six live heaps and six histograms.
The existing analyzer successfully checks the created, first-scraped, and
final-scraped heaps from both cases. Its layout matches the recorded compressed
pointers and eight-byte alignment. No new dependency was installed.

Each scrape reads 41,875 attributes with zero failures. Each case verifies
3,033 user-keyspace metric registrations. The synthetic workers activate in
counts of zero, one, then eight. Each active worker records all 100 tables.
Disabled ReadRepairRequests remains zero. No user memtable initializes and
no user SSTable is written.

| User-table history arrays | Standard | Adaptive |
| --- | ---: | ---: |
| Before scrape, shallow bytes | 0 | 0 |
| First empty scrape, shallow bytes | 892,000 | 0 |
| After eight workers and rescrape, shallow bytes | 892,000 | 32,800 |
| Final allocated arrays | 800 long arrays | 200 byte arrays |
| Final null histories | 0 | 600 |

These totals cover 100 user tables and exclude keyspace/system wrappers.
All six heaps contain 3,000 user-table wrappers with 88,000 shallow bytes.
Their ObjectName property-cache graphs stay empty despite property inspection.
Array shallow sizes are not complete retained sizes. Bounded graph ownership
is not a dominator proof. Intermediate heaps remain available for later work.

## Interpretation limits

These are single, fixed-order observations. They do not establish confidence
intervals, production capacity, or stable throughput. Offered write rate is a
target; the reports preserve actual write lateness. Allocation counters cover
only the harness thread. Profiling was disabled. No Java Flight Recorder
files are expected. Whole-process heap snapshots vary with background state.

Standalone storage and export measurements already record lower memory use
with some update/scrape and broad-query cost increases. Read those branch
reports before changing defaults. Do not add their savings to these matched
process snapshots. All optional controls remain opt-in.
