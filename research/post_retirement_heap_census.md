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

# Heap after memtable retirement

Measured 2026-09-09 on production checkpoint `fc4aa2ab43`.
This investigation changes measurement tools and documentation only.

## Finding

More SSTables materially increase resident heap even when table contents stay
fixed and every user memtable has retired. At 1,000 tables, one, three, and six
files per table retained 129.00, 162.98, and 213.25 MiB of whole test-JVM heap.
The one-to-six endpoint increase is **17,668 bytes per additional file**, about
17.3 KiB. This includes changing metric state and background system state; it is
an observed workload slope, not an exact exclusive reader size.

The clearest small optimization targets are the statistics attached to each
reader. Two dense histograms retain 4,528 bytes per file even though each has only
one populated bucket here. An empty tombstone histogram retains another 1,288
bytes. Sharing safe immutable offsets and trimming empty finished histograms
could recover roughly 3.3 KiB per file without changing the recorded values.
Those savings are a design estimate, not an implemented result.

JMX registration stays at 21.06 MiB across all three cases. Populated runtime
metrics and file-resource ownership both remain material. Memtable retirement
works, but it does not remove the remaining per-table or per-file structures.
These findings do not establish support for one million tables on a small heap.

## Controlled workload

Three fresh Java Virtual Machines (JVMs): Java 21, eight reported processors,
8 GiB maximum heap, 512 MiB initial heap, default G1 collector, compressed
references, eight-byte alignment, and the existing soft-reference policy.
The memtable pool limit is 256 MiB in every run.

Each run creates 1,000 lazy Trie tables with BTI files, cursor compaction, no row
or key cache, and UnifiedCompactionStrategy (UCS) T8. `min_sstable_size` stays at
100 MiB. All runs use the same optimized simple metrics profile, with
BytesFlushed, CompactionBytesWritten, and MemtableSwitchCount additionally
enabled for measurement. Legacy aliases are disabled; compact JMX, transient
name handling, first-use metric IDs, and compact release bookkeeping are enabled.

Each table contains the same 24 rows in one partition, with deterministic
256-byte values. Flush boundaries differ:

| Files/table | Write/retire cycles | Rows/table/cycle | Completed writes |
|---|---:|---:|---:|
| 1 | 1 | 24 | 24,000 |
| 3 | 3 | 8 | 24,000 |
| 6 | 6 | 4 | 24,000 |

Explicit retirement gives exact flush boundaries. Automatic idle flushing is
disabled for this census so a slow round of writes cannot create extra files.
T8 keeps the files below its compaction threshold. All runs finish with the
expected file count, zero user compaction jobs, zero initialized/dirty/flushing
user memtables, no pending compactions, and correct data after every cycle and
at final verification. This is not a natural T4 steady-state comparison or a
measurement of the automatic scheduler's candidate map.

Reads occur after every cycle, so read counts and metric values differ. The same
logical data does not imply identical serialized bytes: each additional file has
headers, partition framing, and compression boundaries. Final Data.db totals are
6,531,327, 6,588,570, and 6,638,784 bytes respectively.

Live heap dumps follow startup, creation, and final settlement. The harness's
post-GC heap reading precedes the live dump. Memory Analyzer (MAT) also removes
unreachable objects during analysis. These are different measurement instants;
the table below reports them separately. Some offline analysis overlapped later
runs, so timing from these runs is not used for performance claims.

## Whole heap and retained ownership

| Files/table | Created heap, MiB | Settled heap, bytes | Settled heap, MiB | MAT reachable heap, MiB |
|---|---:|---:|---:|---:|
| 1 | 102.827 | 135,266,152 | 129.000 | 124.434 |
| 3 | 103.122 | 170,899,672 | 162.983 | 158.389 |
| 6 | 103.345 | 223,605,880 | 213.247 | 208.073 |

The one-to-three increase is 17,817 bytes/additional file. The one-to-six
increase is 17,668 bytes/additional file. The similar slopes support a substantial
per-file cost in this workload, with the scope limits above.

The following MAT root dominator groups are disjoint. They include system and
harness objects as well as user tables. Nested field measurements below overlap
these groups and must not be added to them.

| Root group, MiB | 1 file/table | 3 files/table | 6 files/table |
|---|---:|---:|---:|
| BTI readers | 7.025 | 20.865 | 41.624 |
| ColumnFamilyStore | 27.297 | 27.738 | 27.983 |
| Main JMX server/proxy | 21.058 | 21.058 | 21.058 |
| Metric implementation roots, combined | 17.025 | 19.262 | 21.503 |
| Class objects and dominated static state | 13.626 | 14.776 | 16.440 |
| Reference cleanup GlobalState | 1.725 | 5.113 | 10.194 |
| Four concurrent-map roots | 5.902 | 5.902 | 5.902 |
| SSTable descriptors | 0.839 | 2.487 | 4.982 |
| ChannelProxy | 0.602 | 1.792 | 3.577 |
| CompressionMetadata | 0.532 | 1.585 | 3.256 |
| FileChannelImpl | 0.491 | 1.453 | 2.895 |
| TableMetadata roots | 1.348 | 1.348 | 1.348 |

Metric implementation roots exclude JMX and metric state retained through class
objects or other owners. The table is a selection of disjoint groups, not a full
partition of all heap by subsystem. Class-owned storage is not just class
metadata, and TableMetadata roots are not the full schema footprint.

From one to six files/table, each extra file adds about 7,256 bytes in the reader
root group, 1,776 in reference-cleanup GlobalState, 869 in descriptors, 624 in
ChannelProxy, 571 in CompressionMetadata, 520 in Ref.State, and 504 in Java file
channels. These disjoint changes explain why reporting only the reader's own
retained subtree understates file residency. Names, additional cleanup objects,
read meters, buffers, and static storage account for other increases.

## User reader statistics

Queries filter readers by the `memtable_residency` keyspace. MAT and the raw
HPROF index independently count exactly 1,000, 3,000, and 6,000 user readers.

| Nested field | Retained bytes/file | Observation |
|---|---:|---|
| Entire reader subtree | 7,256–7,280 | Excludes externally shared resource owners |
| StatsMetadata subtree | 6,544–6,568 | Dominates most of the reader subtree |
| Partition-size histogram | 2,560 | 156 counters; exactly one nonzero |
| Cell-count histogram | 1,968 | 119 counters; exactly one nonzero |
| Offset arrays inside those histograms | 2,216 combined | Separate copies per file |
| Tombstone histogram | 1,288 | 101 point/value slots; zero used |

Every inspected user reader has the histogram occupancy shown above. The
24-byte variation in whole-reader/statistics retention does not affect these
histogram sizes. At six files/table, user StatsMetadata subtrees retain
39,288,000 bytes; the two ordinary histograms retain 27,168,000 of those bytes.
The tombstone histograms retain a further 7,728,000 bytes.

Source explains the allocation:

- [MetadataCollector](../src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)
  creates fixed-size partition and cell histograms.
- [EstimatedHistogram](../src/java/org/apache/cassandra/utils/EstimatedHistogram.java)
  owns both a long offset array and dense AtomicLongArray counters.
- [StreamingTombstoneHistogramBuilder.DataHolder](../src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java)
  copies full-capacity arrays when building a finished histogram.
- [TombstoneHistogram](../src/java/org/apache/cassandra/utils/streamhist/TombstoneHistogram.java)
  serializes only used entries. Reopening a file can therefore have a different
  empty-histogram capacity from its freshly flushed reader. This census measures
  newly flushed readers, not a post-restart census.

These are SSTable statistics, separate from the branch's compact runtime metric
reservoirs. Disabling a table metric export does not remove these reader fields.

## Runtime metrics, table state, and allocators

The metric-root increase from creation to one-file retirement is about 5.22 MiB.
Further file/read cycles increase those roots by another 4.48 MiB at six files.
This includes per-file RestorableMeter objects: their disjoint root retention is
232 bytes per file, reaching 1,392,000 bytes at 6,000 files. Table read/latency
histograms also acquire more populated storage. Those counters must not be
discarded merely because the memtable retires.

The existing bounded-reference analyzer provides a complementary estimate for
the one-file run. Its table-metric ownership partition grows from 7,805,984 to
12,036,144 shallow bytes; shared thread-local state grows from 1,539,952 to
4,308,552 bytes. These estimates use field-layout sizes without class-histogram
calibration and exclude traversal back-edges. They are **not retained sizes** and
must not be added to the MAT figures. Their useful result is that recording
activates both local metric objects and shared worker/ID storage.

JMX registration retention does not change after writes. All 1,000 user tables
have 33 metric wrappers in this measurement profile, zero recent-history arrays,
and zero registered ObjectName property caches. No full external scrape runs in
this experiment. Monitoring history remains a separate workload to measure.

User ColumnFamilyStore retained subtrees total 10,032,312 bytes when empty and
12,402,016 / 12,876,896 / 13,120,592 after one / three / six files. The much larger
all-CFS root group includes system-table state. Nested user costs include:

- Compaction managers: 3,624 bytes/table empty, approximately 5,656–5,840 after
  activation, depending on file count and retained ownership.
- Trackers: 1,120 bytes/table empty; 1,424 / 1,832 / 1,976 with one / three / six files.
- Compression dictionary managers: 1,592 bytes/table throughout.
- Separately owned TableMetrics subtrees: 4,504 bytes/table throughout. This
  does not include all registered metric objects or shared worker storage.

User memtable live data and accounted on/off-heap bytes are zero at all final
checkpoints. Node-wide pool accounting remains around 11.1 MB because system
tables remain active; pending reclamation is zero. The 256 MiB pool limit is a
limit, not evidence that 256 MiB of Java heap has been reserved. Heap dumps do not
measure native buffers, mapped pages, kernel cache, or file-descriptor limits.

## Recommended order

1. Trim finished tombstone histograms to used entries, starting with the empty
   case. A separate empty snapshot with two zero-length arrays is about 72 bytes
   under this layout, versus 1,288 here: an estimated 1,216-byte reduction/file.
   Test exact serialization, equality, deletion-time boundaries, and reopened
   readers. Preserve mutable builder capacity where writes still require it.
2. Share the common histogram offset definitions safely. Their current cost is
   2,216 bytes/file. Audit array exposure first: `getBucketOffsets()` returns the
   backing array. Blind interning would introduce cross-object mutation risk.
   Preserve the existing API contract or add an explicit immutable snapshot.
3. Consider compact read-only counter storage for finished statistics. Each file
   currently has 2,200 payload bytes of counters with only two nonzero values
   combined. This is a larger API/serialization task than trimming unused
   capacity. Preserve exact histogram counts, overflow, and quantile behavior;
   do not transplant a new runtime aggregation system merely to store snapshots.

The first two ideas together suggest about 3,432 bytes/file before small shared
storage costs: 3.27 MiB at 1,000 files, or 19.64 MiB at 6,000. None of these
optimizations is implemented by this investigation. They leave most table/JMX
residency and substantial file-resource ownership intact.

The next compaction experiment is specified in
[the UCS hierarchy plan](../.plans/ucs-small-hierarchy-experiment.md). It preserves
the 1 MiB default and compares 64 KiB, 4 KiB, and 1 KiB bases. Both the hierarchy
floor and observed-flush rounding need attention; changing the floor alone has
no effect for tiny observed flushes. Measure rewrite savings against file count,
heap, and read latency. The census puts a concrete cost on a result that retains
more files. Normal-logging idle-drain profiling and byte-based admission remain
separate pending work.

## Reproduction and validation

Run against the already-built checkpoint in the existing Java 21 environment:

```sh
distrobox enter dev -- python3 .build/sh/benchmark_ucs_idle.py census
distrobox enter dev -- python3 .build/sh/analyze_sstable_residency.py logs/20260909-185023-ucs-idle-census
```

The benchmark validates completed writes, retirement, final file counts, and
absence of user compaction. The analyzer validates every user CFS/reader count
and rejects empty or possibly truncated field queries. Its MAT queries use a
10,000-row limit so the 6,000-reader result is complete. The wrapper's default
remains 1,000 rows; `MAT_QUERY_LIMIT` overrides it for this analysis.

All three benchmark runs and all ownership queries passed. The tool checks also
accepted the real completed control, rejected nine non-settled counter variants
and four invalid MAT limits, and passed Python parse and shell syntax checks.
No Java behavior changed after the checkpoint's 58-test validation, so that suite
was not rerun for the analysis-only edits.

Artifacts:

- Raw runs, effective settings, commands, checkpoints, and nine heap dumps:
  `logs/20260909-185023-ucs-idle-census/`.
- MAT queries, per-field retained sums/ranges, and histogram occupancy:
  `logs/20260909-190259-969811-sstable-residency.json` and its sibling log. The JSON
  records the exact CSV archive for every query.
- Bounded metric ownership:
  `logs/20260909-185932-020017-created-heap-ownership.json` and
  `logs/20260909-185651-332589-settled-heap-ownership.json`.
- Tool validation: `logs/20260909-190026-check-census-tools.log`.

Generated dumps, logs, and MAT indexes remain untracked.
