<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Matched integration measurements

Run ai-measure-many-tables-split only after combined tests finish. Do not build
or test while measurements run. Freeze compiled output and record its hash.
Use Java 21.0.12.1, an 8GiB heap, eight active processors, the default G1
collector, subnet 0, and the same 256MiB memtable pool for residency pairs.
Do not install dependencies or compare with historical measurements.

The driver preserves commands, logs, status, manifests, and effective node
settings. Each case runs in a fresh process. A failed case stops its batch.

## Batches

- --smoke: four three-table profile/storage combinations; idle disabled,
  enabled, and enabled with a 1KiB hierarchy under legacy metric storage;
  and a never-written legacy-storage case.
- --profiles: 100 and 1,000 tables, all versus simple profiles, legacy metric
  storage and eager memtables. Four write/flush cycles with equal row counts.
- --idle: 100 and 1,000 tables, idle disabled versus enabled, compact storage,
  simple-memory options, all metrics, and four write/idle cycles. Lazy IDs,
  compact bookkeeping, transient names, both JMX options, geometric meters,
  and lazy tombstone histograms remain enabled in both members of each pair.
- --census: 100 tables with standard versus adaptive JMX history. Both use
  simple metrics, compact registration, transient names, lazy identifiers,
  and compact bookkeeping. Six live heaps per case cover creation, scraping,
  and activation of one then eight metric workers.

## Interpretation gates

Check failure status, completed writes and reads, active profile, storage mode,
idle timeout, heap limits, processor count, and JVM startup arguments. Separate
allocator-owned memtable bytes from whole-process post-GC heap. Compare matched
workloads; resulting SSTable counts can differ because idle flushing is the
variable under test. In the profile pair, verify real SSTable state separately.

The simple profile disables BytesFlushed, CompactionBytesWritten, and
MemtableSwitchCount. Their zero values do not mean no disk work occurred.
Use SSTable counts/disk bytes and compaction history where available.
The old summary policy text always says cursor compaction is disabled; the
effectiveConfiguration.cursorCompaction field is authoritative instead.

Lazy identifiers, compact bookkeeping, and the transient builder appear in
recorded JVM arguments, not separate effective fields. Idle throughput and
rate-limit controls remain at defaults and lack explicit readbacks.

The census label standard refers only to long-array history. It does not mean
standard registration or builder. Inspect heap ownership with the existing
standard-library analyzer. No external dominator tool is required for that
bounded object-graph accounting; it is not a complete dominator analysis.

These fixed-order, single-process samples establish behavior and observational
memory/cost tradeoffs. They do not establish confidence intervals, production
capacity, or a stable performance effect size. Allocation counters cover only
the harness thread. Preserve raw heaps and logs for later repeated experiments.
