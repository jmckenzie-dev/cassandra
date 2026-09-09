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

# Optional legacy metric aliases

## Behavior

The top-level metric-profile setting `include_legacy_aliases` defaults to true
when omitted. `all_metrics.yml` explicitly selects true; `simple_metrics.yml`
selects false. No configured profile still uses the legacy-compatible ALL
selection. The setting is read at startup, so changing it requires a restart.

False omits ColumnFamily/IndexColumnFamily exports and deprecated metric names
in Table/IndexTable and Keyspace scopes. It also omits global Table aliases;
global canonical exports remain independent of the metric-selection lists.
Unrelated metric families and metric groups remain unchanged.

Filtering uses the existing immutable MetricProfile and generated alias catalog.
The existing registry checks that profile before building either a JMX
(Java Management Extensions) registration or a registry entry. No additional
registry abstraction, dependency, cache, or recording-path branch was added.
The underlying recorders and aggregate membership remain unchanged. Monitoring
tools using legacy names must enable compatibility or migrate to modern names.

Both ReadTotalLatency and WriteTotalLatency remain selected in table and
keyspace scopes in both benchmark profiles. Their earlier omission is not
part of this comparison.

## Validation

- Full build and main/test Checkstyle pass: logs/20260908-234610-ai-build.log.
- All 11 reference-generator tests pass: logs/20260908-234814-run_tests.log.
  The generator validates the new boolean and preserves the canonical catalog.
- All 39 focused/integration tests pass: logs/20260908-234903-run_tests.log.
  They cover omitted/explicit settings, malformed options, scopes and global
  aliases, simple/all profiles with aliases on/off, registry and JMX presence,
  real recording, aggregate retention, and three drop/recreate generations.
- All four generated suites pass: logs/20260908-235027-run_property_tests.log.
  Random profile selections verify unchanged canonical decisions and suppressed
  aliases, including IndexTable and global Table names.

No full test suite, dependency install, staging, or commit is part of this task.
Existing uncommitted work is preserved in the pre-edit source snapshot at
tmp/metric-alias-baseline/. The previous total-latency profile edit left stale
test expectations; this task corrects them to the newly enabled exports.

## Measurement method

All runs use Java 21.0.12, G1, an 8 GiB heap ceiling, 1000 user tables, eight
recording workers, lazy TrieMemtables, lazy metric IDs, compact bookkeeping,
compact reservoirs, adaptive JMX history, transient JMX queries, compact JMX
registration, and the same property-query workload. Counts and aggregate
observations are checked by the heap census harness. Tables remain empty;
the workload records synthetic metric observations rather than database load.

Before edits, a frozen prior JAR and source-snapshot copy of simple_metrics.yml
established the baseline. That copy includes the enabled total-latency metrics
and omits the new flag, preserving legacy behavior. The run completed all
checkpoints and exited successfully. Final rescraped heap: 115,732,880 B.
Artifacts: logs/metric-alias-baseline-1000/20260908-234123-heap-ownership-1000t/.

Final on/off runs use the same frozen JAR, tmp/metric-alias-final.jar. Profiles
in tmp/metric-alias-profiles/ differ only in include_legacy_aliases. Runs are
sequential, with no builds or heavy heap analysis while a measurement JVM is
active. Whole-heap measurements include runtime and fixed costs; do not infer
a million-table capacity or per-table slope from these single-size runs.

## Results

Disabling legacy aliases saves **15,695,512 B (14.97 MiB, 13.57%)** of settled
whole heap at 1000 user tables. The final aliases-on control differs from the
pre-change baseline by only 72,328 B. Both final runs completed successfully.

| Measurement | Aliases on | Aliases off |
| --- | ---: | ---: |
| Whole heap after table creation, B | 112,876,328 | 97,415,472 |
| Whole heap after final scrape and GC, B | 115,660,552 | 99,965,040 |
| Metric MBeans, including system/global metrics | 55,044 | 33,790 |
| User-keyspace metric MBeans | 50,033 | 30,033 |
| Successful attributes per final full scrape | 261,363 | 180,475 |
| Failed attributes | 0 | 0 |

The saving already exists before the first scrape. It removes registrations,
names, and their associated storage; it does not depend on retiring tables.
The final scraped heaps include metric recording and monitoring history.

### Ownership checks

The heap census confirms that all **19,136 ColumnFamily/IndexColumnFamily
exports** and **2,118 deprecated names in modern table families** disappeared.
Their combined count, 21,254, exactly matches the drop in metric MBeans.

| Structure | Aliases on, B | Aliases off, B |
| --- | ---: | ---: |
| Main JMX server retained heap | 32,460,936 | 20,283,920 |
| All ObjectName bounded graphs | 19,250,776 | 12,002,296 |
| JMX repository map structure | 2,375,056 | 1,432,784 |
| Four top-level ConcurrentHashMap dominators | 9,073,912 | 5,497,808 |
| TableMetrics retained heap (1057 objects) | 4,760,728 | 4,760,728 |
| TableMetrics ownedMetrics bounded structure | 608,832 | 608,832 |

Eclipse Memory Analyzer (MAT) identifies the main server through its proxy;
an independent HPROF traversal verifies proxy -> TransientMBeanServerBuilder
handler -> JmxMBeanServer. The second server retains 14,456 B in both heaps.
The main JMX server saves 12,177,016 B (11.61 MiB), leaving **19.34 MiB**.
The ConcurrentHashMap group includes registry storage, but this grouped total
is not an exact attribution to Cassandra's registry alone. Bounded graphs
overlap the retained-heap measurements; these rows must not be added together.

Both modes record eight observations per user table for each enabled metric
checked by the harness, with 8000 observations in the selected keyspace
aggregate. Disabled ReadRepairRequests remains zero. All eight worker arrays
have 6002 nonzero slots and cover all 1000 tables in both heaps. Their combined
payload changes from 650,944 to 651,712 B, a 768 B increase; the process-wide
live ID count differs by three. Recorder memory is therefore not byte-identical
across these runs, but no user-table recording or aggregation was removed.
The unchanged TableMetrics ownership figures support the same conclusion.

### Interpretation and next steps

This result supports optional alias removal as a substantial resident-memory
reduction. It does not change recorder update code or add a hot-path branch.
This experiment did not measure query throughput or update latency. It uses
one matched pair, not repeated trials or a table-count scaling curve.

The compatibility cost is explicit: tools that query removed names must use
modern names or set include_legacy_aliases to true. The default for an omitted
flag remains true. Only the shipped simple profile opts out by default.

The remaining 19.34 MiB of main JMX retention still warrants investigation.
Next, attribute the surviving modern-name graphs, repository entries, and
wrappers on the aliases-off heap. The earlier alias estimate must not be
counted again as a remaining opportunity. Any further reduction should retain
the standard server and expose a measured, maintainable benefit before adding
another representation. Automatic idle memtable retirement remains a separate
pending task.

### Measurement artifacts

- Final on run: logs/metric-alias-on-1000/20260908-235057-heap-ownership-1000t/.
- Final off run: logs/metric-alias-off-1000/20260908-235340-heap-ownership-1000t/.
- Checkpoint/scrape comparison: logs/20260908-235816-448147-metric-alias-comparison.json.
- Exact alias counts and server verification: logs/20260908-235937-560122-metric-alias-owners.json.
- Bounded graphs, on/off: logs/20260908-235633-584417-jmx-bookkeeping-probe.json
  and logs/20260908-235715-059763-jmx-bookkeeping-probe.json.
- MAT dominators, on/off: logs/20260908-235633-rescraped-dominators.zip
  and logs/20260908-235715-rescraped-dominators.zip.
- Worker probes, on/off: logs/20260908-235857-792101-worker-metric-probe.json
  and logs/20260908-235856-621402-worker-metric-probe.json.

## Reproduction

Run these commands sequentially in the existing dev environment. The wrappers
write console output and dated logs while preserving command exit status.

```sh
distrobox enter dev -- .build/sh/ai-build
distrobox enter dev -- env PROFILE_LAZY_METRIC_IDS=true PROFILE_COMPACT_BOOKKEEPING=true PROFILE_TRANSIENT_JMX=true bash run_tests.sh --metric-profiles
distrobox enter dev -- env PROFILE_SKIP_BUILD=true bash run_property_tests.sh --metric-profiles
distrobox enter dev -- bash run_tests.sh --metrics-ref
distrobox enter dev -- .build/sh/ai-generate-metrics-reference --check
```

Copy simple_metrics.yml to two separate files and set include_legacy_aliases
to true in one and false in the other. Build/freeze the production JAR before
using PROFILE_SKIP_BUILD. Use different subnets/output directories per run.

```sh
distrobox enter dev -- env PROFILE_COMPACT_BOOKKEEPING=true PROFILE_LAZY_METRIC_IDS=true PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/metric-alias-final.jar .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 138 --metrics-config /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/metric-alias-profiles/simple-aliases-on.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/metric-alias-on-1000
distrobox enter dev -- env PROFILE_COMPACT_BOOKKEEPING=true PROFILE_LAZY_METRIC_IDS=true PROFILE_SKIP_BUILD=true PROFILE_TRANSIENT_JMX=true PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/metric-alias-final.jar .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 139 --metrics-config /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/metric-alias-profiles/simple-aliases-off.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/metric-alias-off-1000
```
