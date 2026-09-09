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

# Optional adaptive JMX history

## Scope

`adaptive_jmx_histogram_history_enabled: true` selects compact saved snapshots
for Java Management Extensions (JMX) `RecentValues`. The default is false.
The setting applies when each wrapper is constructed at startup. It is
independent of `optimized_metrics_enabled` and `metrics_config_file`.

Recording structures, metric registration, and returned bucket layouts stay
the same. Each alias keeps its own cursor. The legacy option still calls the
existing `CassandraMetricsRegistry.delta` and retains the current `long[]`.

The optional path stores all-zero history as null. Other histories use a
`byte[]`, `short[]`, `int[]`, or `long[]`, based on the smallest signed width
that holds every cumulative count. It reuses narrow arrays when the length
and width match. Resetting counts can shrink or release storage. Full-width
history retains the snapshot array, as the legacy path does.

This uses the adaptive-width idea from OpenTelemetry Java's
`AdaptingIntegerArray`, inspected in `ref/opentelemetry-java`. It does not add
the SDK or copy its incrementer. Cassandra needs signed values, subtraction
with Java long overflow, and shrinking after resets. The existing synchronized
scrape method provides exclusive access. No metric recording method changes.

The implementation adds one boolean to each histogram/timer wrapper and changes
its saved-history field type from `long[]` to `Object`. It creates no history
holder object. Measurements must include any wrapper padding cost.

Source review of normal daemon startup found configuration loaded before the
affected histogram/timer wrappers are registered. The pre-configuration logging
listener registers meters, which have no recent-value history.

## Measurement method

Use the same working tree for each flag value, with `simple_metrics.yml` fixed.
The tree already contains the metric-profile work based on commit 4192a00e1f.
All table counts remain at or below 1000. Heap census workloads create empty
lazy TrieMemtables, scrape, record synthetic table metrics on eight workers,
and scrape again. These runs measure metric residency; they do not represent
a populated database or production query throughput.

Separate clients that inspect ObjectName properties from attribute-only
clients. Builds, heap analysis, and scrape microbenchmarks run separately.
The microbenchmark calls the production history helpers and local MBeanServer
attributes using an injected deterministic reservoir. It includes snapshot
allocation equally in both paths. It does not measure remote JMX transport or
production reservoir snapshot computation.

## Pre-change

Fresh 100-table simple-profile census, full attribute and name inspection:

| Checkpoint | Heap bytes |
| --- | ---: |
| Startup | 49,819,888 |
| Created | 48,803,168 |
| Scraped | 58,034,592 |
| One worker | 58,308,264 |
| Eight workers | 59,518,592 |
| Scraped again | 59,511,248 |

Command: `distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 100 --subnet 115 --metrics-config simple_metrics.yml --out logs/adaptive-history-pre-100`.
Exit status 0. Artifacts:
`logs/adaptive-history-pre-100/20260907-093459-heap-ownership-100t`.
Both scrapes found 4831 user metric beans, with zero attribute failures.
The harness checked exact registrations and recorder/aggregate counts.

## Iteration and final results

Final timing and heap ownership results are below.

The first implementation selected width by scanning the minimum and maximum.
Its first clean 200,000-operation benchmark is
`logs/20260907-094759-950-adaptive-jmx-history-benchmark.log`.
Median paired empty-history time ratios were 1.455 for the helper, 1.091 for
the histogram JMX call, and 1.346 for the timer JMX call. This is one JVM and
does not establish a durable production regression.

An experimental iteration accumulated the bits needed below each value's sign
bit, plus a separate nonzero check. Complementing negative values preserves
the signed width limits, including Long.MIN_VALUE. The separate nonzero check
distinguishes all-zero snapshots from values such as -1. The focused tests and
16,000-step property test passed again after this change. Its benchmark,
`logs/20260907-095011-415-adaptive-jmx-history-benchmark.log`, gave empty-history
median paired ratios of 1.491, 1.472, and 1.324 for helper, histogram, and timer.
This did not consistently improve the empty case. The final implementation
restores the simpler minimum/maximum scan. Cross-JVM movement also shows why
the final report needs repeated paired runs rather than one favorable result.

An earlier 100,000-operation exploratory run completed but Cassandra logging
split its CSV into fragments. The benchmark now reinstalls its console/file
streams after client initialization. That run is retained as an artifact,
`logs/20260907-094630-103-adaptive-jmx-history-benchmark.log`, but excluded from
the final timing summary.

### Final scrape timing

Three fresh JVMs ran the final implementation. Each used five warmup rounds
and nine measured paired rounds, 200,000 operations per round, with 160
buckets. Execution order alternated. All 567 measured pairs had matching
checksums; no samples were removed. JDK 21.0.12, G1 garbage collection, eight
reported processors, and an 8 GiB maximum heap matched the census launcher.

The table gives the median adaptive/legacy time ratio across the 27 paired
rounds. Values above 1 mean more time per operation.

| History | Helper | Local JMX histogram | Local JMX timer |
| --- | ---: | ---: | ---: |
| Zero length | 1.025 | 1.012 | 1.012 |
| All-zero, 160 buckets | 1.546 | 1.205 | 1.301 |
| Byte | 1.008 | 0.975 | 1.108 |
| Short | 1.062 | 0.916 | 1.068 |
| Int | 1.100 | 0.877 | 1.100 |
| Long | 1.316 | 0.941 | 1.122 |
| Widen/reset | 1.020 | 1.024 | 1.185 |

Empty timer-call JVM median ratios ranged from 1.283 to 1.341. The histogram
empty case varied more, from 0.950 to 1.326. Populated histogram calls showed
mixed results relative to the helpers and timers. These short operations are
sensitive to just-in-time compilation and host noise. Do not treat the
histogram speedups as a demonstrated production benefit.

For scale, median-of-JVM-median empty timer times were 218.6 ns legacy and
293.2 ns adaptive. Byte-history timer times were 212.0 and 241.2 ns. These
summaries use a different aggregation from the paired ratios, so their
quotients need not match the ratio column exactly.

Steady-width allocation was identical: 2,592 bytes per helper operation and
2,816 bytes per local JMX operation. Repeated widening/reset added 146 bytes
per operation on average. Both paths still allocate returned long arrays;
compact history reduces retained storage, not those returned-array sizes.

These are single-thread microbenchmarks with deterministic cached fixture
snapshots. They include fresh snapshot-value arrays, a constant-time checksum
sample, and volatile publication of the returned array. They exclude actual
reservoir snapshot/decay computation and remote JMX transport. The new
history work runs on scraping, not metric recording or table reads/writes.

Final logs:
- `logs/20260907-095219-045-adaptive-jmx-history-benchmark.log`
- `logs/20260907-095301-291-adaptive-jmx-history-benchmark.log`
- `logs/20260907-095350-320-adaptive-jmx-history-benchmark.log`

All samples, distributions, and JVM ranges are preserved in
`logs/20260907-095610-923485-summarize-jmx-benchmark.json` and
`logs/20260907-095610-923485-summarize-jmx-benchmark-aggregate.md`.

### Final heap comparison

Each row compares separate fresh JVMs using the same final binary and profile.
All six census runs exited successfully. Full clients read attributes and
inspect ObjectName properties; attribute-only clients omit the property
inspection. The final checkpoint follows eight workers recording metrics and
a second scrape.

| Tables | Client | Legacy final heap bytes | Adaptive final heap bytes | Saving |
| ---: | --- | ---: | ---: | ---: |
| 100 | Full | 59,668,040 | 56,504,000 | 3,164,040 (5.30%) |
| 100 | Attributes only | 53,396,272 | 50,033,680 | 3,362,592 (6.30%) |
| 1000 | Full | 176,068,760 | 161,423,368 | 14,645,392 (8.32%) |

The 1000-table pair used subnets 120 and 121. Its complete checkpoint series:

| Checkpoint | Legacy heap bytes | Adaptive heap bytes |
| --- | ---: | ---: |
| Startup | 49,584,576 | 49,575,152 |
| Created | 119,579,720 | 119,722,232 |
| Empty, scraped | 170,169,016 | 155,091,672 |
| One worker | 171,788,904 | 156,668,424 |
| Eight workers | 176,003,592 | 160,907,352 |
| Scraped again | 176,068,760 | 161,423,368 |

Both 1000-table runs exposed exactly 48,031 user metric beans and matching
registry entries. Each scrape successfully read 259,233 attributes, including
13,443 recent-value attributes. Selected table counts were eight; selected
keyspace aggregates were 8000. All 1000 user memtables remained uninitialized.

At 100 tables, the full empty-scrape MemoryMXBean reading was noisy:
58,202,216 bytes legacy versus 59,283,736 adaptive. The next adaptive
checkpoint fell to 55,344,312 despite recording more metrics. Heap-dump
indexed object bytes instead showed a 3,110,792-byte reduction at that empty
checkpoint. Those observations come from different instants; neither should
be substituted for the other. Direct history-array ownership below isolates
the intended effect.

### Saved-array ownership

The 100-table full-scrape dumps establish:

| Per-table state | Legacy | Adaptive |
| --- | ---: | ---: |
| Empty history arrays | 12 long arrays | None; 12 null histories |
| Empty history shallow bytes | 13,680 | 0 |
| Populated history arrays | 12 long arrays | 3 byte arrays; 9 null histories |
| Populated history payload bytes | 13,488 | 460 |
| Populated history shallow bytes | 13,680 | 512 |
| JMX wrapper count | 48 | 48 |
| JMX wrapper shallow bytes | 1376 | 1376 |
| ObjectName property-map graph bytes | 32,120 | 32,120 |

Populated history storage fell by 13,168 bytes per table, or 96.26%, in this
workload. ColumnFamilyStore, schema, registry, and recording-object graphs
matched between the two paths. No named-owner conflicts appeared.

The attribute-only 100-table pair confirmed the same history counts and
13,680-to-512-byte reduction, with zero user ObjectName property maps in both
modes. Its startup MemoryMXBean readings also varied (49,570,144 bytes legacy,
53,709,032 adaptive); the later checkpoints and direct ownership comparison
provide the useful evidence here.

The pre-change wrapper total was 1344 bytes per table. Adding the mode boolean
increased each histogram wrapper from 24 to 32 bytes; timer wrappers remained
48 bytes. That costs 32 bytes per user table in both final modes. Relative to
the original pre-change layout, the populated array saving minus that wrapper
cost is 13,136 bytes per table. This small cost also applies before scraping.

The final 1000-table dumps confirmed the same 13,680-to-512-byte reduction for
every user table. Whole-JVM history storage fell from 15,249,488 to 544,080
bytes. Registry, schema, recording, wrapper, and property-map graphs matched;
the bounded ColumnFamilyStore graph differed by only 72 bytes in total. There
were no owner conflicts. Indexed heap bytes differed by 14,645,528, close to
the final MemoryMXBean difference of 14,645,392 bytes.

Ownership comparisons:
- Empty 100: `logs/20260907-100307-230925-metric-profile-heap-comparison.json`
- Populated 100: `logs/20260907-100356-389712-metric-profile-heap-comparison.json`
- Attributes-only 100: `logs/20260907-100435-783113-metric-profile-heap-comparison.json`
- Populated 1000: `logs/20260907-100839-905996-metric-profile-heap-comparison.json`

Run artifacts, each containing the effective configuration, summary, class
histograms, and heap dumps:
- `logs/adaptive-history-post-legacy-full-100/20260907-095459-heap-ownership-100t`
- `logs/adaptive-history-post-adaptive-full-100/20260907-095544-heap-ownership-100t`
- `logs/adaptive-history-post-adaptive-attributes-100/20260907-095640-heap-ownership-100t`
- `logs/adaptive-history-post-legacy-attributes-100/20260907-095724-heap-ownership-100t`
- `logs/adaptive-history-post-legacy-full-1000/20260907-095808-heap-ownership-1000t`
- `logs/adaptive-history-post-adaptive-full-1000/20260907-100014-heap-ownership-1000t`

## Correctness checks

`distrobox enter dev -- ./run_tests.sh --jmx-history`: 17 Java tests and three
heap-analyzer unit tests passed. The Java checks cover both recorder backends,
both history settings, real JMX histogram/timer wrappers, independent alias
cursors, noncumulative metrics, resets, signed limits, overflow, length changes,
and mutation of returned arrays.

`distrobox enter dev -- ./run_property_tests.sh --jmx-history`: one Java
property test passed, comparing 16,000 generated scrape steps against the
legacy delta implementation. The analyzer property test also passed.

The first unit run failed in test setup/cleanup because the fixture called
`DatabaseDescriptor.setConfig(null)`. The metric-profile configuration API
requires a nonnull configuration. The fixture now initializes and restores a
valid configuration. No production change was needed for that failure.

Logs: `logs/20260907-094334-run_tests.log` and
`logs/20260907-094353-run_property_tests.log`.

Build and both main/test Checkstyle checks passed:
`distrobox enter dev -- .build/sh/ai-build`,
`logs/20260907-095104-ai-build.log` is the final build after the logging fix
and restored minimum/maximum scan. The first build found benchmark import
spacing and direct system-property access; both were corrected.

## Reproduce

Enable the optional path in `conf/cassandra.yaml` and restart:

```yaml
adaptive_jmx_histogram_history_enabled: true
```

Set false for the existing long-array history. Keep `metrics_config_file` and
`optimized_metrics_enabled` fixed when comparing the history paths.

Timing comparison (both paths in each JVM):

```sh
distrobox enter dev -- .build/sh/ai-benchmark-jmx-history --iterations 200000 --warmup-rounds 5 --rounds 9 --buckets 160
```

Heap comparisons use `.build/sh/ai-profile-heap-ownership` with
`--metrics-config simple_metrics.yml`. Omit `--adaptive-jmx-history` for the
legacy control; include it for compact history. Add `--attributes-only` to
exclude the client's ObjectName property inspection. Both clients still read
the recent-value attributes. The harness records the effective setting in
its summary and checks exact registration names and recorded counts.

The analyzer `.build/sh/analyze-heap-ownership.py` now measures each integral
array type and counts null history. Existing long-array reports remain valid.
Ownership graphs can overlap; use the saved-history array totals directly
instead of summing overlapping metric/JMX graphs.

## Limits and decision

Keep this path optional and disabled by default. It trades scrape work for
less retained history. It preserves exact cumulative snapshots and deltas;
it does not approximate buckets or percentiles.

It does not materially reduce the earlier roughly 77 KiB per-table slope
before scraping. That footprint has no saved JMX history to remove. It also
does not remove registry entries, ObjectName property caches, schema objects,
or worker counter-array holes.

Compression depends on the largest signed count in each saved snapshot.
Histories that need full 64-bit counts retain long arrays and receive no
array-space saving. Empty histories can release all backing storage; resets
can also narrow previously wide histories. Queries still return long arrays.
