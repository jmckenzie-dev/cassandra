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

# Compact runtime metrics measurements

The legacy implementation remains selectable with `optimized_metrics_enabled:
false` in cassandra.yaml. The configured default is true on this branch. Metrics
created before configuration loads use the legacy implementation so a later
explicit false setting remains effective. The setting applies to newly created
reservoirs at startup, not a live migration. Existing metric names, units, types,
and histogram exports remain available. SSTable histogram formats are unchanged.

All experiments use Java 21 in the dev container, an 8 GiB maximum heap and eight
available processors. Table workloads use N100. Standalone probes use 100
reservoirs for object-graph measurements. These graphs include shared reachable
objects once per batch; they are not heap-dominator retained sizes. Fixed-clock
probe timings exclude production clock/decay costs and are diagnostics. JMH
comparisons use the same update workload and production approximate clock.

## Optimization 1: empty and sparse storage

### Pre-change

Source baseline: c274d4232b, before reservoir implementation edits.

- Fresh N100 never-written: settled post-GC heap 110,334,416 bytes. All 100 tables
  remain logically available, with zero initialized TrieMemtables.
- Direct created/settled heap ownership: 3,400 user reservoirs, all empty;
  6,800 mutable backing arrays contain 15,596,800 payload bytes. Private bucket
  offsets add 2,032,000 bytes, excluding two shared default arrays.
- Standalone legacy reservoir: 543,744 reachable bytes per batch of 100, or
  5,437.44 amortized bytes per reservoir, at every tested occupancy.
- Existing JMH update benchmark, two forks and ten measured iterations: one
  thread 30.890 million updates/s; four threads 92.019 million updates/s. The
  four-thread samples ranged from 84.468 to 98.141 million/s.

Artifacts:

- `logs/compact-metrics-step1-pre/20260905-092941-residency-never-written-100t/`
- `logs/20260905-093613-inspect-resident-reservoirs.json`
- `logs/20260905-093445-many-tables-launch.log` (standalone probe)
- `logs/20260905-reservoir-before-t1.json`
- `logs/20260905-reservoir-before-t4.json`

The initial probe used printf, which the test logger split into separate records.
Later probe output uses a single println per record; measurement logic is unchanged.

### First iteration

The initial candidate allocates 16-counter pages on first use, preserves the
reference's two physical stripes and rounding, and shares private bucket offsets.
Snapshot construction hooks let both reservoirs return the existing snapshot
type without changing legacy histogram arithmetic or parent aggregation.

| Occupied buckets | Legacy bytes/reservoir | Initial candidate bytes/reservoir |
| --- | ---: | ---: |
| 0 | 5437.44 | 141.44 |
| 1 | 5437.44 | 701.44 |
| 4 | 5437.44 | 1661.44 |
| 64 | 5437.44 | 7005.44 |
| 164 | 5437.44 | 7005.44 |

Snapshots do not increase the reservoir's retained graph. Sparse storage works,
but densely populated reservoirs retain about 29% more memory because of page
headers and directories. Matched one-thread JMH measurements in the new benchmark
report legacy 33.242 million/s and candidate 29.125 million/s, a 12.4% reduction.
This iteration needs a dense representation before acceptance.

Artifacts: `logs/20260905-094158-many-tables-launch.log` and
`logs/20260905-reservoir-step1-peri-t1.json`.

The integration regression also reproduced pre-existing parent-release inflation:
five observations became ten after releasing a child. The optimized path now
merges released children into only the parent's own stored snapshot. The legacy
path retains its existing aggregation behavior. The failing regression is
`logs/20260905-094623-compact-release-regression.xml` (four cases, one failure),
with console output in `logs/20260905-094623-ai-ci-test.log`.

### Second iteration

Sparse mutations now share a monitor during promotion; dense updates use atomics.
Promotion occurs at 64 observations or half-capacity allocation. Fully populated
storage falls to 5,485.44 bytes/reservoir, within 1% of legacy. Empty remains
141.44 bytes; one and four occupied buckets use 717.44 and 1,677.44 bytes.
This lifetime observation threshold can make a slowly updated single-bin metric
permanently dense. Phase 2 will reduce unused stripe storage.

Four-thread JMH reports legacy 95.316 million/s versus candidate 94.839 million/s
(overlapping confidence intervals). One-thread JMH remains slower and variable:
33.581 versus 27.105 million/s. Despite its filename, the `post-t1` artifact below
records this intermediate result; the implementation needs another iteration.

Artifacts: `logs/20260905-095816-many-tables-launch.log`,
`logs/20260905-reservoir-step1-post-t1.json`, and
`logs/20260905-reservoir-step1-peri2-t4.json`.

### Final measurements

The third iteration separates small dense-update methods from synchronized
sparse helpers. Storage stays unchanged from the second iteration. The final
matched JMH batch reports the following medians across ten samples:

| Threads | Legacy million updates/s (range) | Compact million updates/s (range) |
| --- | ---: | ---: |
| 1 | 31.184 (29.379–33.504) | 29.620 (29.426–29.718) |
| 4 | 94.002 (87.284–97.940) | 94.009 (90.577–96.576) |

Single-thread median throughput is about 5% lower; four-thread throughput is
unchanged within observed variation. Dense storage adds 48 bytes/reservoir. These
are the remaining costs of the extra storage indirection. Accept this tradeoff
for the resident-memory objective; do not describe it as a general CPU speedup.

Eight fresh N100 runs alternate legacy/optimized/optimized/legacy per scenario:

| Scenario | Legacy settled heap MiB (range) | Optimized settled heap MiB (range) |
| --- | ---: | ---: |
| Never written | 105.091 (105.025–105.158) | 74.155 (73.683–74.628) |
| Written and flushed | 108.848 (108.785–108.910) | 77.853 (77.771–77.935) |

Each never-written run verifies 100 reads. Each written/flushed run completes
400 writes, 200 reads, and 100 SSTables. All runs report zero failures and clean
memtables. CREATE TABLE times span 8.93–9.15 seconds across these runs; two
samples per mode/scenario do not establish a timing improvement. Whole-JVM
savings include system/global metrics; do not project this full delta per table.

Direct user-table ownership establishes the incremental table benefit:

- Untouched tables: 15,596,800 counter payload bytes become zero at both created
  and settled checkpoints. The 2,032,000 private offset bytes also disappear;
  the optimized tables reference 3,648 bytes of shared offsets.
- Written/flushed tables: 15,596,800 counter payload bytes become 349,184 bytes
  across the same 3,400 reservoirs. Both modes have 900 populated reservoirs;
  the optimized implementation stores them sparsely. Object headers and page
  directories are excluded from these payload figures.

Final artifacts:

- `logs/20260905-101409-reservoirs-sparse-post-GJrFj4/` (both probes and JMH)
- Within that batch: `20260905-141715-041451-reservoir-summary.json`
- `logs/20260905-100638-metrics-sparse-post-HkqdIY/` (eight real-table runs)
- `logs/20260905-101409-137081-analyze-resident-metrics.json`
- `logs/20260905-101742-416660-analyze-resident-reservoirs.json`

The first analyzer invocation rejected the logger prefix on probe records after
all measurements completed successfully. The parser now accepts that prefix;
reanalysis of the unchanged raw artifacts passed. No benchmark samples were
discarded or rerun to repair this reporting defect.

Validation so far: clean build/Checkstyle (`logs/20260905-100417-ai-build.log`),
58 focused tests passed with one existing ignored legacy diagnostic
(`logs/20260905-095334-ai-test-memtable-lazy/`), and a passing focused rerun after
adding concurrent cumulative monotonicity assertions. The final harness suite
passed all 11 cases (`logs/20260905-101742-many-tables-launch.log`, 236.567 seconds).
Post-extraction reruns of the compact unit, generated-property, and integration
classes also passed. The first optimization is complete.

## Reproduction

Run commands through `distrobox enter dev --`. The repository wrappers write
timestamped console logs and preserve command failures.

```
env PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.ReservoirStorageProbe .build/sh/ai-profile-many-tables org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir 5 3 200000
env PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.ReservoirStorageProbe .build/sh/ai-profile-many-tables org.apache.cassandra.metrics.CompactDecayingEstimatedHistogramReservoir 5 3 200000
env PROFILE_MAIN_CLASS=org.openjdk.jmh.Main .build/sh/ai-profile-many-tables '.*CompactHistogramBench.update' -t 1 -f 2 -wi 3 -w 1s -i 5 -r 1s -prof gc -rf json -rff logs/reservoir-comparison-t1.json
.build/sh/ai-compare-resident-metrics --step sparse --checkpoint post --heap-dumps
```

Use distinct result filenames per invocation. Repeat JMH at four threads. Raw
heap dumps and build artifacts stay outside commits; each optimization commit
includes its measurement summary and reproducible tools.
