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

# Stock versus optimized Cassandra at 5000 tables

## Question and controls

Measure the combined resident-memory change from upstream commit
4c79cf739161985b6e066f40303154b9cd2783c1 to the current working branch,
using 5000 empty user tables and the optimized simple-metrics configuration.
This requested table count supersedes the earlier 1000-table limit for these
runs. The plan is .plans/stock-vs-optimized-5000-tables.md.

Stock production sources were exported with git archive to tmp/stock-5000
and built with .build/sh/ai-build. No stock production code was changed.
The build reused available dependencies; Accord's checked-out revision matches
the upstream submodule revision. The source export uses the current logged
build wrapper. The current branch includes existing uncommitted work.

Both configurations use Java 21.0.12, G1, eight available processors, an 8 GiB
heap ceiling, BTI SSTables, and TrieMemtables. TrieMemtables initialize eagerly
on stock and lazily on the optimized path. Both create the same keyspace and
tables through the same schemaChange calls, with SizeTieredCompactionStrategy
and row/key caching disabled. This holds the memtable type constant; it is not
a comparison of untouched shipped configuration files.

The optimized run enables compact runtime metrics, simple_metrics.yml with
legacy aliases disabled, adaptive Java Management Extensions (JMX) histogram
history, compact JMX registration, transient JMX queries, lazy metric IDs,
and compact table metric bookkeeping. Stock uses its full metric exposure and
original data structures. ReadTotalLatency and WriteTotalLatency remain enabled
at both table and keyspace scopes in the optimized profile.

The existing census harness now accepts --stock and up to 5000 tables. Stock
mode omits branch-only configuration and lazy-initialization checks. A nested
helper isolates the branch-only metric catalog from stock lambda deserialization.
Each run records the production JAR location. The launcher has no fallback
production classes directory and permits selecting the stock configuration
directory with PROFILE_CONFIG_DIR.

## Workload and validation

Eight workers exist before the baseline checkpoint. After table creation and
a full scrape, one worker and then the other seven update actual table metrics.
Each worker visits every table once. Workers remain alive for the final scrape
and checkpoint. The harness checks table counts, empty memtables, no user
SSTables, distinct worker stores, metric observations, keyspace aggregates,
registration counts, and successful readable JMX attributes. Scrapes also
exercise property-pattern queries and the objectName operation.

The workload records synthetic metrics. It does not insert user rows, measure
query throughput, trigger idle retirement, or represent a sustained data load.
ReadRepairRequests intentionally records eight events per table on stock and
zero on the optimized simple profile, where that metric is disabled. Other
selected per-table observations and the 40,000-event keyspace aggregates must
match. This compares complete configurations, including reduced exposure; it
does not isolate the cost of any single optimization.

Each checkpoint uses a full collection/class histogram and records settled
whole-JVM heap. Live heap dumps are retained. The two measurement JVMs run
sequentially, without builds or heavy heap analysis during either run.

The first stock smoke run exposed a harness-only NoClassDefFoundError during
lambda deserialization of a signature referencing MetricProfile. Isolating
the inventory helper fixed it. The second stock smoke run completed all
phases and passed its checks at two tables:
logs/stock-5000-smoke/20260909-001517-heap-ownership-2t/.

Stock build: tmp/stock-5000/logs/20260909-001121-ai-build.log.
Final current build and main/test Checkstyle: logs/20260909-001559-ai-build.log.
All eight focused harness tests pass, including exhaustive table-count bounds,
stock-option conflicts, profile inventories, and argument validation.

## Results

Both runs completed successfully at **5000 user tables** with the same 8 GiB
heap ceiling. The optimized configuration saves **3,187,340,216 B
(3039.68 MiB, 2.97 GiB), or 90.71%**, at the final checkpoint.

| Settled whole heap | Stock, B | Optimized, B | Reduction |
| --- | ---: | ---: | ---: |
| Created, before first scrape | 2,266,703,192 | 314,530,064 | 86.12% |
| After first scrape | 3,454,937,376 | 314,400,480 | 90.90% |
| After one worker | 3,463,334,560 | 321,907,432 | 90.71% |
| After eight workers | 3,522,068,272 | 324,667,672 | 90.78% |
| After second scrape | 3,513,653,552 | 326,313,336 | 90.71% |

In MiB, the creation checkpoint falls from **2161.70 to 299.96** and the final
checkpoint falls from **3350.88 to 311.20**. These totals include fixed JVM,
system-table, schema, and harness costs. They measure used heap after collection,
not committed heap, peak allocation, native memory, or process resident memory.
The optimized first-scrape decrease of 129,584 B is normal variation at this
scale; it is not a claim that scraping frees metric state.

| Monitoring check | Stock | Optimized |
| --- | ---: | ---: |
| Metric MBeans, including system/global metrics | 1,261,931 | 153,790 |
| User-keyspace metric MBeans | 1,245,101 | 150,033 |
| Successful attributes in final scrape | 6,596,473 | 796,475 |
| Failed attributes in either scrape | 0 | 0 |
| Observations in each checked keyspace aggregate | 40,000 | 40,000 |

The first stock scrape adds **1,188,234,184 B (1133.19 MiB)** of settled heap.
The optimized path has no comparable increase. Eight worker stores remain
distinct and alive in each final heap. All 5000 user memtables are empty in both
runs; the optimized run also verifies that all remain uninitialized.

### Class histogram evidence

The final class histograms show where much of the difference resides:

| Heap-wide class storage, MiB | Stock | Optimized |
| --- | ---: | ---: |
| long arrays | 1269.11 | 8.10 |
| byte arrays | 706.55 | 85.25 |
| String objects, excluding backing arrays | 333.85 | 15.01 |
| HashMap nodes | 195.54 | 6.53 |
| ObjectName property objects | 116.16 | 14.72 |

Stock has 14,586,235 String objects; optimized has 655,695. These are whole-heap
shallow totals by class, not ownership measurements of metrics alone. Byte
arrays include more than string backing storage. No new dominator analysis was
needed for this comparison; the saved heaps support that follow-up. These
results are consistent with the combined reduction in reservoir arrays,
registration count, and persistent monitoring-name state.

### Timing

Creating the tables took **761.05 seconds on stock and 676.23 seconds on the
optimized path**: 12m41s versus 11m16s, an 11.15% reduction in elapsed time.
The first full scrape took 26.61 versus 2.96 seconds; the second took 27.77
versus 3.62 seconds. Scrape work differs because the optimized profile exposes
fewer metrics, so these are end-to-end configuration timings, not equal-count
JMX operation microbenchmarks.

| 500-table batch ending at | Stock, seconds | Optimized, seconds |
| --- | ---: | ---: |
| 500 | 53.169 | 53.427 |
| 1000 | 56.016 | 52.258 |
| 1500 | 61.898 | 57.950 |
| 2000 | 65.929 | 60.748 |
| 2500 | 69.403 | 66.810 |
| 3000 | 78.886 | 67.453 |
| 3500 | 84.129 | 76.841 |
| 4000 | 88.203 | 77.819 |
| 4500 | 96.484 | 84.840 |
| 5000 | 106.932 | 78.081 |

Creation still becomes slower as table count grows. This run does not establish
the cause or an asymptotic growth rate. One sequential pair also cannot establish
a durable throughput change or hot-path contention cost. Allocation counters
in phase summaries cover the harness thread and must not be treated as total
CREATE TABLE allocation.

### Implications and remaining work

The combined configuration substantially reduces the resident-memory barrier
at 5000 tables, including the extra memory retained after monitoring. This
benefit comes partly from reduced metric exposure and the compatibility-name
opt-out. It does not preserve stock's complete monitoring surface.

The remaining **311.20 MiB** warrants an ownership census of the saved optimized
heap before choosing another memory change. Table-creation scaling remains a
separate problem. These empty-table, synthetic-metric runs do not prove a
million-table node, a smaller heap limit, or bounded memory under sustained
writes. Automatic idle retirement was not exercised.

### Artifacts

- Stock run: logs/stock-5000/20260909-001734-heap-ownership-5000t/.
- Optimized run: logs/optimized-5000/20260909-003245-heap-ownership-5000t/.
- Checked comparison: logs/20260909-004449-stock-5000-comparison.json and .log.
- Comparison script: tmp/compare_stock_5000.py; accepts stock and optimized
  run-directory paths in that order. It validates outcomes, configuration
  controls, table counts, observations, aggregate counts, and scrape failures.
- Each run includes summary.json, checkpoint-*.json, scrape-*.json,
  histogram-*.txt, and live *.hprof files.

## Reproduction

Use the existing production JARs and compiled harness after building. Run these
commands sequentially. The wrappers write dated logs and preserve exit status.

```sh
distrobox enter dev -- env PROFILE_SKIP_BUILD=true MANY_TABLES_XMX=8g PROFILE_JAR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/stock-5000/build/apache-cassandra-7.0-SNAPSHOT.jar PROFILE_CONFIG_DIR=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/tmp/stock-5000/conf .build/sh/ai-profile-heap-ownership --stock --tables 5000 --subnet 141 --property-queries --out logs/stock-5000
distrobox enter dev -- env PROFILE_SKIP_BUILD=true MANY_TABLES_XMX=8g PROFILE_COMPACT_BOOKKEEPING=true PROFILE_LAZY_METRIC_IDS=true PROFILE_TRANSIENT_JMX=true .build/sh/ai-profile-heap-ownership --tables 5000 --subnet 142 --metrics-config /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables/conf/simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/optimized-5000
```

No production source changes, dependency additions, staging, or commits belong
to this measurement task. The test harness and launcher changes remain available
for repeating this comparison.
