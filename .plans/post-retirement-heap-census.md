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

# Post-retirement heap census

Checkpoint: `fc4aa2ab43`. Production behavior stays unchanged.

## Question

What owns the heap after memtable retirement? How much does each additional
SSTable cost when logical table contents stay fixed?

## Measurement

Run `.build/sh/benchmark_ucs_idle.py census` inside the existing dev container.
Use three fresh Java Virtual Machines (JVMs), each with 1,000 tables, 8 GiB maximum
heap, a 256 MiB memtable pool, lazy Trie memtables, BTI files, cursor compaction,
and the same optimized simple metrics profile used by the idle-flush comparison.
Keep the legacy-alias, compact Java Management Extensions (JMX), transient-name,
lazy metric-ID, and release-bookkeeping options fixed.

Each table receives the same 24 rows, one partition, and 256-byte values:

| Files/table | Cycles | Rows/table/cycle | Total writes |
|---|---:|---:|---:|
| 1 | 1 | 24 | 24,000 |
| 3 | 3 | 8 | 24,000 |
| 6 | 6 | 4 | 24,000 |

Use explicit flush/retirement boundaries and UCS T8 in all cases. Automatic idle
flushing stays disabled so a slow write round cannot create extra files. T8
keeps these files below its compaction threshold. This isolates post-retirement
ownership; it does not measure scheduler overhead or natural T4 steady state.
Validate exact final file counts, zero user compactions, correct data after every
cycle, successful writes, and zero initialized/dirty/flushing user memtables.

Capture live heap dumps at startup, after creation, and after final retirement.
Skip per-cycle heap dumps. Keep the heap ceiling, compressed references, object
alignment, collector, and soft-reference policy fixed. Report post-GC checkpoint
heap separately from Memory Analyzer (MAT) live-heap totals: they are different
instants and background system work can change between them.

## Ownership analysis

Use the existing MAT installation and `ai-analyze-heap-dominators` wrapper.
Compare disjoint root dominator groups, then inspect nested fields and retained
sets for SSTable readers, statistics histograms, file handles, runtime metrics,
thread-local metric arrays, JMX servers, schema, and memtable allocators.
Filter user-table objects by keyspace where possible. Report shared owners and
system-table state separately. Never add nested retained sizes to parent totals.

Reads occur after each cycle, so read counts and metric values differ across
cases even though final logical contents and write counts match. Attribute metric
storage directly before interpreting the whole-heap slope as a file cost.
This workload does not perform a full external metrics scrape. Heap excludes
native buffers, mapped pages, and kernel cache; these still need a separate
process-memory and file-descriptor budget.

## Deliverables

- Preserve raw runs and query outputs under timestamped `logs/` directories.
- Summarize remaining costs and the next maintainable targets in `research/`.
- Write a separate plan for an optional smaller UCS hierarchy base.
- Leave normal-logging drain profiling and byte-based admission as pending work.

## Result

Completed at 1,000 tables: 129.00 / 162.98 / 213.25 MiB settled heap for one /
three / six files. All 72,000 writes and exact final reader counts passed.
The histogram census found one populated counter in each dense statistics
histogram and zero tombstones in every 101-slot tombstone snapshot.
See [the report](../research/post_retirement_heap_census.md) for retained owners,
measurement limits, and proposed next changes.
