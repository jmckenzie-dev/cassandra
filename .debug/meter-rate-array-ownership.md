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

# Meter rate-array retained memory

The geometric candidate retains 185,352 more array payload bytes (181.008 KiB)
in the matched N100 creation dumps. This measures the capacity cost of avoiding
repeated array copying. It is not a reduction in resident memory.

Batch: `logs/20260905-004614-java-allocation-meter/`. Reference run:
`20260905-004850-residency-never-written-100t`; candidate run:
`20260905-004928-residency-never-written-100t`.

| State | Implementation | Live meters | Rate-array length | Payload bytes | Assigned slot high-water | Spare slots | Free groups |
|---|---|---:|---:|---:|---:|---:|---:|
| Baseline | Reference | 4,108 | 12,669 | 101,352 | 12,669 | 0 | 115 |
| Baseline | Geometric | 4,108 | 24,576 | 196,608 | 12,669 | 11,907 | 115 |
| Created and settled | Reference | 8,650 | 25,983 | 207,864 | 25,983 | 0 | 11 |
| Created and settled | Geometric | 8,650 | 49,152 | 393,216 | 26,547 | 22,605 | 199 |

The candidate has 180,840 bytes of capacity beyond its assigned slot high-water.
The remaining 4,512 bytes of its payload difference arise from 188 additional
assigned groups, all free by the settled dump. Group assignment depends on when
garbage collection and cleanup permit reuse; these independent runs need not
have identical offsets. Both settled weak-registration arrays contain 8,650
entries with no cleared referents. Both created dumps have the same live meter
counts as their settled dumps.

Each dump contains exactly one initialized meter implementation, owned by the
node's `InstanceClassLoader`. No second host-classloader copy contributes a rate
array. Object and loader identifiers may change between dumps when objects move.
These counts include all node meters, including system tables and global metrics.

The analysis follows each meter class's static `rates` reference to its primitive
double array, reads `rateGroupIdGenerator.value` and the free-group BitSet, and
counts live instances of the same class definition. It validates offset alignment,
array bounds, uniqueness and separation of live and free groups. Payload bytes
exclude the array header. This is direct reference evidence, not a dominator
analysis or an estimate from whole-process heap deltas.

Reproduce with `venv/bin/python tmp/inspect-meter-rate-arrays.py
logs/20260905-004614-java-allocation-meter`. The script records console output and
errors in timestamped logs. Full results:
`logs/20260905-005018-inspect-meter-rate-arrays.json` and its `.log` sibling.
It shares `tmp/hprof_reader.py` with the existing trie ownership inspector.
The refactored trie inspector reproduces the prior control's 100 user tables,
40 initialized memtables, 320 shards and 41,943,040 bytes in 40 slab arrays.
