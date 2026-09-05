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

# Lazy memtable allocation evidence

2026-09-04. The actual async-profiler recordings confirm that deferred shard
construction moves to mutation handling. They do not establish an exact byte
reduction. The samples for this small object graph are sparse.

## Runs and verification

Both profiled runs use 100 tables, four write/flush cycles, four rows per table
per cycle, and the same configured workload. The experiment changes the default
memtable policy for system tables as well as user tables.

Artifact root: `logs/20260904-210325-lazy-memtable-comparison/`.

- Eager profile: `20260904-211030-residency-written-flushed-100t`.
- Lazy profile: `20260904-211215-residency-written-flushed-100t`.

After the benchmark batch finished, ran:

```text
distrobox enter dev -- venv/bin/python tmp/analyze-lazy-memtable-allocation.py logs/20260904-210325-lazy-memtable-comparison
```

Exit status: 0. Console and complete command output are in
`logs/20260904-211414-analyze-lazy-memtable-allocation.log`.
No dependencies were installed and no benchmark ran during this analysis.

The script checks run/phase failures, verifies allocation events in each actual
`.ap.jfr`, and converts create plus all four write and flush phases with the
existing async-profiler 4.2 `jfrconv`. It produces collapsed allocation sample
counts and `--total` byte weights, plus six HTML views for creation, first write,
and first flush. Java Flight Recorder's `jfr summary` confirms these example
event counts:

| Phase | Eager NewTLAB events | Lazy NewTLAB events |
|---|---:|---:|
| Create tables | 5,095 | 5,008 |
| First write cycle | 109 | 101 |
| First flush cycle | 884 | 879 |

All 18 inspected phase recordings contain `jdk.ObjectAllocationInNewTLAB`
events. They also contain CPU and wall-clock samples. Allocation views use
`--alloc` explicitly; the presence of other event types does not change the
selected view.

## Where shard construction occurs

Count allocation stacks that contain `TrieMemtable.generatePartitionShards`.
This identifies shard construction and the objects allocated below it, including
trie arrays, per-shard collectors, and write locks. These are sample counts, not
numbers of shards or calls.

| Phase | Eager shard samples | Lazy shard samples |
|---|---:|---:|
| Create tables, all node activity | 17 | 14 |
| Write cycle 0 | 0 | 1 |
| Flush cycle 0 | 2 | 0 |
| Write cycle 1 | 0 | 2 |
| Flush cycle 1 | 0 | 0 |
| Write cycle 2 | 0 | 1 |
| Flush cycle 2 | 1 | 0 |
| Write cycle 3 | 0 | 4 |
| Flush cycle 3 | 2 | 0 |

For eager user-table creation, two samples include this path:

```text
DistributedSchema.createTable
  ColumnFamilyStore.<init>
    ColumnFamilyStore.createMemtable
      TrieMemtable.<init>
        TrieMemtable.initialize
          TrieMemtable.generatePartitionShards
```

The other eager creation-phase shard samples occur through schema/system-table
flush replacement. The lazy creation phase has no sampled shard construction
under `ColumnFamilyStore.<init>`. Its 14 shard samples all occur during actual
schema/system mutations: `SchemaKeyspace.applyChanges` or
`SystemKeyspace.updateSchemaVersion`, followed by `ColumnFamilyStore.apply`,
`TrieMemtable.put`, `initialize`, and `generatePartitionShards`.

Creation still changes Cassandra's schema tables. A lazy creation profile can
therefore contain shard constructors even while every new user table remains
dormant. The absence of user-constructor samples alone is not proof of zero
allocation; the separately measured dormant state and heap graph establish
that part of the result.

The four eager flush phases include five shard-construction samples through
replacement memtable constructors. The four lazy flush phases include none.
The four lazy write phases instead include eight shard-construction samples
through `TrieMemtable.put`. Eager write phases include none. This matches the
implemented allocation timing.

The converter assigns 2,621,435 byte weight to those five eager flush samples
and 4,194,296 byte weight to the eight lazy write samples. Each of these sampled
weights is 524,287 bytes. Do not subtract these values to claim an allocation
regression or divide them by table count to estimate retained bytes. Sampling
at allocation boundaries, sparse observations, and different phase timing
prevent that inference. A zero sample count is also not a universal proof that
the method allocated zero bytes.

`SlabAllocator.getRegion` remains present in the first-write profiles for both
modes. The first eager write cycle contains 84 such allocation samples. Lazy
shard initialization does not remove the previously identified one-MiB slab
reservation on small first writes.

## Artifacts and limits

`allocation-comparison.json` in the artifact root contains event counts,
sample/byte-weight totals, and exact matching shard stacks for all 18 phases.
Each profiled run has an `allocation-analysis/` directory with JFR summaries,
collapsed views, and the three HTML byte-weight views.

This is whole-node sampled allocation evidence. It includes schema operations,
system tables, flush writers, compaction, client work, and sampling overhead.
It is distinct from the harness's main-thread-only allocated-byte counter and
from post-collection live heap. Use the heap ownership analysis for retained
memory claims and unprofiled runs for latency comparisons.
