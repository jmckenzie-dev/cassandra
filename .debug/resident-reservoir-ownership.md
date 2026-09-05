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

# Resident reservoir ownership

Each of the 100 untouched user tables retains **155,968 bytes (152.3125 KiB) of
all-zero histogram bucket arrays**, plus **20,320 bytes (19.84375 KiB) of private
bucket-offset arrays**. This is direct ownership evidence from live heap dumps.
Lazy empty storage and shared offsets deserve evaluation before disk backing.
No reservoir implementation changed during this investigation.

## Exact user-table ownership

The reference run is
`logs/20260905-004614-java-allocation-meter/20260905-004850-residency-never-written-100t/`.
Its baseline contains no user tables. Its created and settled dumps agree on
all counts below. Every user reservoir has zero in both its cumulative and
decaying backing arrays. Every reservoir uses two physical stripes.

| Owner | Reservoirs/table | Mutable long arrays/table | Payload bytes/table |
|---|---:|---:|---:|
| ColumnFamilyStore.metric / TableMetrics | 33 | 66 | 151,872 |
| TrieMemtable.metrics.contentionTime | 1 | 2 | 4,096 |
| Combined | 34 | 68 | 155,968 |

Across the 100 user tables, 6,800 distinct backing arrays retain 15,596,800
payload bytes. None of these arrays is shared between user tables. Per table:

- Twenty reservoirs use 127 offsets: forty arrays of 256 longs, 81,920 bytes.
- Ten reservoirs use 164 offsets: twenty arrays of 330 longs, 52,800 bytes.
- Four reservoirs use 165 offsets, including zero: eight arrays of 332 longs,
  21,248 bytes.

Each reservoir owns a cumulative AtomicLongArray and a DecayingBuckets holder
with another AtomicLongArray. The byte counts include their long-array payloads;
they exclude array headers, wrappers, reservoir objects, snapshots, timers,
metric names and registry/MBean structures. They are not full retained-size
estimates. Zero data here establishes an empty representation opportunity; it
does not prove that an arbitrary production reservoir has never received data.

There are also 2,002 distinct offset arrays referenced by user reservoirs:
2,000 private arrays of 127 longs, and the two shared default arrays of 164 and
165 longs. Their combined payload is 2,034,632 bytes. The private arrays account
for 2,032,000 bytes; the shared arrays account for 2,632 bytes once, not per
table. Sharing the low-count offsets would retain one small shared array while
removing these repeated copies, subject to an aliasing audit.

The source explains the smaller-than-default reservoir sizes:
[CassandraMetricsRegistry.java](../src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java:404)
uses LOW_BUCKET_COUNT=127 for timers measured outside nanoseconds. The reservoir
[constructor](../src/java/org/apache/cassandra/metrics/DecayingEstimatedHistogramReservoir.java:221)
shares offsets only when the count is 164. The original approximation of 33
default-size reservoirs per table overstates bucket payload in this workload.

At 100,000 equally untouched tables, the measured per-table bucket payload
projects to approximately **14.5 GiB**, with another **1.89 GiB** of private
offset payload. These are arithmetic projections from N100, not a 100k run or
proof of total heap use at that scale. Active workloads need additional analysis.

## Scope and accounting checks

Whole-JVM reservoir counts rise from 2,803 at baseline to 6,236 after creation.
Mutable backing-array payload rises from 12,605,440 to 28,352,864 bytes. The
3,433-reservoir increase exceeds the 3,400 directly owned by user tables; it
also includes shared keyspace/global initialization. The report does not charge
that remaining increase to individual tables.

Traversal follows TableMetrics fields through TableHistogram/TableTimer.cf,
LatencyMetrics.latency, timer.histogram, histogram.reservoir and ScalingReservoir.delegate.
It excludes parent/global metric links. Trie contention reservoirs are counted
separately through each TrieMemtable's metadata and metrics references. Backing
arrays and offsets are deduplicated by heap object identity. Inherited fields
with duplicate names require the most-derived declaration: OverrideTimer's
histogram field is live while the Dropwizard superclass field is null.

Reproduce with `venv/bin/python tmp/inspect-resident-reservoirs.py` followed by
the baseline, created and settled `.hprof` paths above. Full per-table and
per-metric-field results are in `logs/20260905-090932-inspect-resident-reservoirs.json`;
its `.log` sibling records console output. The script uses the shared HPROF reader.

## Exact-value requirements for a candidate

- Preserve the creation-time decay landmark. Allocating a normal reservoir at
  first update changes forward-decay weights and rounding. Empty snapshots and
  observations after the reset interval must advance landmark state as today.
- Preserve physical stripes and each integer bucket. The
  [rescale loop](../src/java/org/apache/cassandra/metrics/DecayingEstimatedHistogramReservoir.java:385)
  rounds each stripe independently. Collapsing stripes and later recreating them
  can change results even when aggregate counts initially agree.
- Keep cumulative counts after decayed counts reach zero. An idle or zero-decay
  histogram cannot discard historical getValues() data. Compressing a populated
  representation or placing it off-heap requires lossless state preservation.
- Preserve clear() and rebase() behavior, including their new/imported landmarks.
  The existing rebase operation explicitly does not support concurrent updates.
- Preserve concrete snapshot merge/rebase compatibility. LatencyMetrics
  [removeChildren](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java:127)
  casts to EstimatedHistogramReservoirSnapshot, merges a released child's
  history and rebases its parent. That snapshot class stores a concrete original
  reservoir; add() rejects other snapshot types. Percentile equality alone does
  not cover this contract.
- Preserve publication and rescale races. DecayingBuckets combines an immutable
  landmark with its mutable atomic counts to avoid inconsistent weighted updates.
  Lazy first updates must publish equivalent state without dropping observations.
- Audit offset aliases before sharing more arrays. buckets(length) exposes the
  offset array when lengths match; registry JMX adapters forward bucketStarts().
  The inspected production call sites show export/read paths, but the arrays
  escape the reservoir and are not structurally immutable. Existing sharing of
  default offsets does not prove that sharing previously private offsets has
  no compatibility effect.

Disk backing could hold losslessly encoded populated state later. It must retain
both histograms, physical stripe values and landmark state, and coordinate reads,
updates, snapshots and metric release. The measured empty arrays require no disk
representation at all. This evidence supports testing their lazy representation
first while retaining the original implementation for differential comparison.
