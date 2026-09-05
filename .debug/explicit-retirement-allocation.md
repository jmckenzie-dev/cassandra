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

# Explicit retirement: allocation follow-up

The dominant sampled allocation during retirement is the eager tombstone
histogram spool created for each SSTable writer. This is a separate flush cost
that remains after lazy memtable initialization. No production changes were made
for this investigation.

Evidence: [comparison JSON](../logs/20260904-230056-explicit-retirement-comparison/allocation-comparison.json)
and the [profiled retirement run](../logs/20260904-230056-explicit-retirement-comparison/20260904-230847-residency-idle-reactivate-100t/allocation-analysis/).
The two `03-cycle-NNN-retire-alloc-weighted_bytes.collapsed` files cover ten
active tables per cycle. Counts below are profiler samples, not allocation counts.
Weighted MiB are sums of sampled byte weights, not exact allocated or live bytes.

| Allocation stack family | Cycle 0 weighted MiB | Cycle 1 weighted MiB |
|---|---:|---:|
| `StreamingTombstoneHistogramBuilder$Spool` arrays | 85.00 | 83.00 |
| `SequentialWriterOption.allocateBuffer` | 5.00 | 7.00 |
| Other SSTable writer lifecycle work | 7.00 | 4.00 |
| Metrics construction/update | 3.00 | 1.00 |
| Logging | 2.00 | 2.00 |
| Cardinality metadata | 0.50 | 1.00 |
| Other stacks | 5.50 | 2.00 |
| Total | 108.00 | 100.00 |

Spool construction accounts for 55/101 and 53/87 allocation samples, and 78.7%
and 83.0% of weighted bytes: 89,129,840 of 113,247,042 weighted bytes in cycle 0
and 87,032,656 of 104,858,414 in cycle 1. Its call path is
`Flushing.createFlushWriter -> SimpleSSTableMultiWriter.create -> MetadataCollector
-> StreamingTombstoneHistogramBuilder -> Spool`.

The source establishes a fixed cost independently of sampling:
[SSTable.java:72](../src/java/org/apache/cassandra/io/sstable/SSTable.java#L72)
sets spool capacity to 100,000.
[MetadataCollector.java:123](../src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java#L123)
constructs the histogram eagerly. The builder immediately creates the spool at
[StreamingTombstoneHistogramBuilder.java:86](../src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java#L86).
[StreamingTombstoneHistogramBuilder.java:425](../src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java#L425)
rounds capacity to 131,072, then allocates `long[262144]` and `int[262144]`:
**3 MiB of array payload per collector**, excluding headers. The workload only
inserts live rows, but these arrays exist before the writer processes any row.

Start follow-up work with lazy or smaller initial histogram storage and empty
flush ranges. [Flushing.java:61](../src/java/org/apache/cassandra/db/memtable/Flushing.java#L61)
creates a writer for each disk range; `flushRunnable` constructs it before testing
whether the range produces data at
[Flushing.java:117](../src/java/org/apache/cassandra/db/memtable/Flushing.java#L117),
and [SimpleSSTableMultiWriter.java:128](../src/java/org/apache/cassandra/io/sstable/SimpleSSTableMultiWriter.java#L128)
creates its metadata collector. Empty writers are subsequently aborted at
[ColumnFamilyStore.java:1376](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1376).
The profiles include this abort path. It is normal empty-writer cleanup, not
evidence that the successful retirement runs failed. Histogram buffers have an
existing release path at [MetadataCollector.java:509](../src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java#L509).

Smaller contributors include writer buffers
([SequentialWriterOption.java:99](../src/java/org/apache/cassandra/io/util/SequentialWriterOption.java#L99)),
lifecycle log `BufferedWriter` arrays, file/path objects, metadata serialization,
log formatting/caller extraction, and replacement-memtable metrics.
Metrics samples reach `ThreadLocalMeter` registration's copied array and
`ThreadLocalMetrics.getNonStatic` counter-array expansion. These remain candidates
for the separate metrics investigation.

The unprofiled sampled heap maxima supplied by the comparison are approximately
171–327 MiB for retired idle/reactivation versus 121 MiB for controls, and
238–247 MiB for retired rotating bursts versus 165 MiB for controls. Flush writer
allocation and later reactivation are consistent with higher transient heap even
as retirement reduces settled resident storage. Heap-used samples also contain
uncollected garbage, and their maxima depend on sampling and collection timing.
The allocation recordings come from separate diagnostic runs; they do not
quantitatively attribute these maxima or establish a leak.

Cycle 1 write profiles contain zero slab-region samples in the control and eight
after retirement, consistent with allocating storage again on reactivation.
Neither retirement phase contains shard-construction samples; each retirement
write cycle contains one. Absence of samples is not proof of zero allocation.
Use direct state assertions and heap ownership evidence for the dormant-state
claim. Cursor compaction was disabled; making compaction allocation-free would
not itself remove the measured SSTable writer construction costs.
