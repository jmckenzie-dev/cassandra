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

# Source path inventory

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Base: 50a10d42a5d633150fbf4ae518b4da85466472b2.
Scope: all 246 paths in the committed source delta; later planning files are outside it.

BASE is the shared branch. MEM, SELECT, STORE, EXPORT, and EXTRA are the five
feature branches. INTEGRATION holds combined fixtures/tools. ARCHIVE means
preserve the file in the original source checkout; it does not mean delete it.
Multiple owners require the method/hunk allocation in 08-shared-files.md.

This is a source ownership manifest, not a record of completed extraction.
Each executor must record actual destination paths, moved tests, and adaptations.

| Source path | Added | Deleted | Owner | Treatment |
| --- | ---: | ---: | --- | --- |
| .aftignore | 13 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .build/README.md | 23 | 0 | BASE + INTEGRATION | Hunks/methods; see 08-shared-files.md |
| .build/benchmarks/jmx-names/JmxNameRetention.java | 293 | 0 | EXPORT | Export diagnostics; adapt combined-only dependencies |
| .build/benchmarks/jmx-names/JmxServerBoundary.java | 499 | 0 | EXPORT | Export diagnostics; adapt combined-only dependencies |
| .build/benchmarks/jmx-names/README.md | 61 | 0 | EXPORT | Export diagnostics; adapt combined-only dependencies |
| .build/benchmarks/otel-storage/README.md | 88 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/io/opentelemetry/sdk/metrics/internal/aggregator/OtelArrayAccess.java | 32 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/io/opentelemetry/sdk/metrics/internal/aggregator/OtelBucketRangeBenchmark.java | 155 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/org/apache/cassandra/metrics/HistogramWidthOverTime.java | 399 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/org/apache/cassandra/metrics/OtelStorageBenchmark.java | 402 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/summarize-widths.py | 138 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/benchmarks/otel-storage/summarize.py | 138 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/memtable-residency.md | 301 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/metrics/GenerateMetricsReference.java | 456 | 0 | SELECT | Profiles/reference generation and packaging |
| .build/metrics/test_metrics_reference.py | 275 | 0 | SELECT | Profiles/reference generation and packaging |
| .build/sh/ai-analyze-heap-dominators | 43 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-benchmark-jmx-history | 21 | 0 | EXPORT | Export diagnostics; adapt combined-only dependencies |
| .build/sh/ai-benchmark-otel-storage | 64 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-build | 68 | 0 | BASE | Portable shared tools |
| .build/sh/ai-ci-test | 46 | 0 | BASE | Portable shared tools |
| .build/sh/ai-compare-java-allocation | 58 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-compare-resident-metrics | 118 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-generate-metrics-reference | 27 | 0 | SELECT | Profiles/reference generation and packaging |
| .build/sh/ai-measure-reservoirs | 74 | 0 | STORE | Storage diagnostics; adapt combined-only dependencies |
| .build/sh/ai-probe-jmx-names | 47 | 0 | EXPORT | Export diagnostics; adapt combined-only dependencies |
| .build/sh/ai-profile-heap-ownership | 21 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-profile-many-tables | 215 | 0 | BASE + SELECT + STORE + EXPORT | Hunks/methods; see 08-shared-files.md |
| .build/sh/ai-profile-memtable-residency | 21 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/ai-test-memtable-lazy | 143 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/analyze-heap-ownership.py | 520 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/analyze-java-allocation.py | 137 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/analyze-reservoir-measurements.py | 183 | 0 | STORE | Storage diagnostics; adapt combined-only dependencies |
| .build/sh/analyze-resident-metrics.py | 103 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/analyze-resident-reservoirs.py | 343 | 0 | STORE | Storage diagnostics; adapt combined-only dependencies |
| .build/sh/analyze-worker-metrics.py | 111 | 0 | STORE | Storage diagnostics; adapt combined-only dependencies |
| .build/sh/analyze_sstable_residency.py | 149 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/analyze_ucs_idle.py | 166 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/benchmark-worker-metrics.py | 123 | 0 | STORE | Storage diagnostics; adapt combined-only dependencies |
| .build/sh/benchmark_ucs_hierarchy.py | 101 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/benchmark_ucs_idle.py | 141 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .build/sh/hprof_reader.py | 133 | 0 | BASE | Portable shared tools |
| .build/sh/test_analyze_heap_ownership.py | 64 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| .debug/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/ai-ant-stub/git/install-git-defaults.sh | 4 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/explicit-memtable-retirement-lifecycle.md | 103 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/explicit-retirement-allocation.md | 90 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/explicit-retirement-failure-fixture.md | 28 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/lazy-memtable-allocation.md | 132 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/lazy-memtable-lifecycle.md | 169 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/lazy-memtable-ownership.md | 152 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/meter-rate-array-ownership.md | 57 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/rebase-origin-trunk-20260909.md | 125 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .debug/resident-reservoir-ownership.md | 125 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .gitignore | 7 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .opencode/skills/cassandra-table-creation-profile/SKILL.md | 401 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .opencode/skills/cassandra-table-creation-profile/evals/evals.json | 23 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .opencode/skills/cassandra-table-creation-profile/evals/trigger-eval-set.json | 82 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/adaptive-jmx-history.md | 51 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/bounded-idle-flush-admission.md | 57 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/branch-test-coverage.md | 36 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/compact-runtime-metrics.md | 81 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/explicit-memtable-retirement.md | 43 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/h1-metrics-scope.md | 74 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/heap-ownership-census.md | 45 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/histogram-width-over-time.md | 53 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/java-histogram-metrics-equivalence.md | 39 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/jmx-monitoring-name-retention.md | 55 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/jmx-query-export-handoff.md | 68 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/jmx-query-export.md | 77 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/jmx-registration-and-metric-bookkeeping.md | 199 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/lazy-memtable-initialization.md | 82 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/many-tables-harness-run-1.md | 58 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/many-tables-profiling-harness-context.md | 140 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/many-tables-profiling-harness-in-jvm-dtest-async-profiler.md | 153 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/many-tables-profiling-harness.md | 153 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/memtable-residency-baseline.md | 74 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/metric-profile-runtime.md | 47 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/otel-compact-storage-benchmark.md | 66 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/post-retirement-heap-census.md | 83 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/simplify-new-method-complexity.md | 155 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/stock-vs-optimized-5000-tables.md | 41 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/ucs-idle-flush.md | 75 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/ucs-small-hierarchy-experiment.md | 140 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .plans/worker-metric-residency.md | 153 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .prompts/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/compact-metrics-step1.md | 40 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/compact-metrics-step2.md | 121 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/compact-metrics-step3.md | 117 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/lazy-memtable-correctness.md | 92 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .reviews/otel-storage-benchmark.md | 63 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/bounded-idle-flush-admission.md | 28 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/compact-runtime-metrics.md | 48 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/many_tables_profiling_harness.md | 23 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/ucs-hierarchy-validation.md | 25 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| .worklog/ucs-idle-flush.md | 32 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| AGENTS.md | 7 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| README.asc | 57 | 1 | MEM + EXTRA + INTEGRATION | Hunks/methods; see 08-shared-files.md |
| TODO.md | 102 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| build.xml | 1 | 1 | BASE | JaCoCo version only |
| conf/README.txt | 213 | 0 | SELECT + STORE + EXPORT + INTEGRATION | Hunks/methods; see 08-shared-files.md |
| conf/all_metrics.yml | 276 | 0 | SELECT | Profiles/reference generation and packaging |
| conf/cassandra.yaml | 38 | 0 | MEM + SELECT + STORE + EXPORT | Hunks/methods; see 08-shared-files.md |
| conf/jvm-server.options | 4 | 0 | EXPORT | Optional JMX builder setting |
| conf/metrics_ref.md | 283 | 0 | SELECT | Profiles/reference generation and packaging |
| conf/simple_metrics.yml | 276 | 0 | SELECT | Profiles/reference generation and packaging |
| debian/cassandra.install | 2 | 0 | SELECT | Profiles/reference generation and packaging |
| doc/modules/cassandra/pages/managing/operating/compaction/ucs.adoc | 15 | 1 | EXTRA | UCS operator documentation |
| docs/.placeholder | 0 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/9_7_checkpoint.md | 261 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/adaptive_jmx_history.md | 322 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/bounded_idle_flush_admission.md | 216 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/compact_runtime_metrics.md | 554 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/create_slowdown.md | 251 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/explicit_memtable_retirement.md | 109 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/heap_ownership_census.md | 328 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/histogram_width_over_time.md | 251 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/internal_metric_dependencies.md | 307 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/java_histogram_metrics.md | 222 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/jmx_monitoring_name_retention.md | 304 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/jmx_query_export.md | 307 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/jmx_registration_and_metric_bookkeeping.md | 324 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/lazy_memtable_initialization.md | 301 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/lazy_metric_aggregation.md | 649 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/memtable_residency_baseline.md | 209 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/metric_aggregates.md | 366 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/metric_alias_exports.md | 181 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/metric_profile_runtime.md | 278 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/metric_threading.md | 199 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/optimized_heap_next_steps.md | 245 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/otel_compact_storage_benchmark.md | 252 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/post_retirement_heap_census.md | 264 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/prosecute_memtable_tables.md | 1171 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/stock_vs_optimized_5000_tables.md | 201 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/table_overhead.md | 229 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/ucs_idle_flush.md | 355 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/ucs_small_hierarchy.md | 320 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| research/worker_metric_residency.md | 310 | 0 | ARCHIVE | Preserve in source checkout; copy evidence selectively |
| run_property_tests.sh | 88 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| run_tests.sh | 98 | 0 | INTEGRATION | Combined measurement/selector tooling; retain original |
| src/java/org/apache/cassandra/config/CassandraRelevantProperties.java | 9 | 0 | SELECT + STORE + EXTRA | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/config/Config.java | 13 | 1 | MEM + SELECT + STORE + EXPORT | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/config/DatabaseDescriptor.java | 57 | 1 | MEM + SELECT + STORE + EXPORT | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/db/ColumnFamilyStore.java | 22 | 0 | MEM | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/db/compaction/unified/Controller.java | 39 | 5 | EXTRA | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/db/memtable/IdleMemtableFlusher.java | 280 | 0 | MEM | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/db/memtable/Memtable_API.md | 11 | 3 | MEM | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/db/memtable/TrieMemtable.java | 187 | 35 | MEM | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/db/virtual/TableMetricTables.java | 31 | 25 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/filter/BloomFilterMetrics.java | 11 | 6 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/filter/BloomFilterTracker.java | 3 | 3 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummaryMetrics.java | 1 | 0 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/keycache/KeyCache.java | 2 | 2 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/keycache/KeyCacheMetrics.java | 4 | 1 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java | 15 | 2 | EXTRA | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/AdaptiveCounterArray.java | 149 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/AdaptiveHistogramHistory.java | 110 | 0 | EXPORT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java | 214 | 21 | SELECT + STORE + EXPORT | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/metrics/ClearableHistogram.java | 18 | 2 | BASE + STORE | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/metrics/ClearableReservoir.java | 23 | 0 | BASE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoir.java | 537 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/DecayingEstimatedHistogramReservoir.java | 37 | 3 | BASE + STORE | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/metrics/GeometricThreadLocalMeter.java | 442 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java | 148 | 64 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/LatencyMetrics.java | 94 | 7 | SELECT + STORE | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/metrics/LazyClearableHistogram.java | 49 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/LazyGeometricThreadLocalMeter.java | 106 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/LazyThreadLocalCounter.java | 110 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/LazyThreadLocalHistogram.java | 49 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/LazyThreadLocalMeter.java | 106 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/Meter.java | 11 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/MetricProfile.java | 252 | 0 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/NoOpMetrics.java | 271 | 0 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/OverrideTimer.java | 1 | 1 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/TableMetrics.java | 303 | 104 | SELECT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/ThreadLocalCounter.java | 17 | 1 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/ThreadLocalHistogram.java | 18 | 2 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java | 53 | 7 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/ThreadLocalMetrics.java | 18 | 0 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/metrics/ThreadLocalTimer.java | 7 | 2 | BASE + STORE | Hunks/methods; see 08-shared-files.md |
| src/java/org/apache/cassandra/metrics/TransientMetricMBean.java | 66 | 0 | EXPORT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/ObjectNamePropertyPattern.java | 136 | 0 | EXPORT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/TransientMBeanServerBuilder.java | 318 | 0 | EXPORT | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/memory/BufferPool.java | 2 | 2 | STORE | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/streamhist/LazyTombstoneHistogramBuilder.java | 73 | 0 | EXTRA | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/streamhist/StreamingTombstoneHistogramBuilder.java | 1 | 1 | EXTRA | Feature-owned source; see branch plan |
| src/java/org/apache/cassandra/utils/streamhist/TombstoneHistogramBuilder.java | 32 | 0 | EXTRA | Feature-owned source; see branch plan |
| src/resources/org/apache/cassandra/metrics/metrics-catalog.properties | 246 | 0 | SELECT | Catalog shipped with profiles |
| test/distributed/org/apache/cassandra/distributed/test/HeapOwnershipCensusHarness.java | 696 | 0 | INTEGRATION | Combined fixture; branch-local subsets require a moved-test map |
| test/distributed/org/apache/cassandra/distributed/test/HeapOwnershipCensusHarnessTest.java | 165 | 0 | INTEGRATION | Combined fixture; branch-local subsets require a moved-test map |
| test/distributed/org/apache/cassandra/distributed/test/IdleMemtableFlushTest.java | 266 | 0 | MEM + INTEGRATION | Hunks/methods; see 08-shared-files.md |
| test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java | 309 | 0 | BASE | Feature-owned source; see branch plan |
| test/distributed/org/apache/cassandra/distributed/test/MemtableResidencyConfigTest.java | 286 | 0 | INTEGRATION | Combined fixture; branch-local subsets require a moved-test map |
| test/distributed/org/apache/cassandra/distributed/test/MemtableResidencyHarnessTest.java | 188 | 0 | INTEGRATION | Combined fixture; branch-local subsets require a moved-test map |
| test/distributed/org/apache/cassandra/distributed/test/MemtableResidencyProfileHarness.java | 766 | 0 | INTEGRATION | Combined fixture; branch-local subsets require a moved-test map |
| test/distributed/org/apache/cassandra/distributed/test/MetricProfileRecordingTest.java | 249 | 0 | SELECT | Adapt MEM/STORE assumptions; retain combined assertions in integration |
| test/distributed/org/apache/cassandra/distributed/test/MetricProfileRegistrationTest.java | 283 | 0 | SELECT | Adapt MEM/STORE assumptions; retain combined assertions in integration |
| test/distributed/org/apache/cassandra/distributed/test/ProfiledClusterHarness.java | 414 | 0 | BASE | Feature-owned source; see branch plan |
| test/distributed/org/apache/cassandra/distributed/test/ResourceProfiler.java | 493 | 0 | BASE | Feature-owned source; see branch plan |
| test/distributed/org/apache/cassandra/distributed/test/TrieMemtableLazyRecoveryTest.java | 197 | 0 | MEM | Standalone feature tests |
| test/distributed/org/apache/cassandra/metrics/AdaptiveJmxHistoryBenchmark.java | 439 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/microbench/org/apache/cassandra/test/microbench/CompactHistogramBench.java | 67 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/resources/org/apache/cassandra/metrics/test-metrics-profile.yml | 30 | 0 | SELECT | Profile fixture |
| test/unit/org/apache/cassandra/auth/jmx/TransientMBeanServerAuthorizationTest.java | 201 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/config/IdleMemtableConfigTest.java | 72 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/compaction/UnifiedCompactionStrategyTest.java | 31 | 0 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/db/compaction/unified/ControllerHierarchyTest.java | 145 | 0 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/db/compaction/unified/ControllerTest.java | 67 | 1 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/db/memtable/IdleFlushAdmissionBudgetPropertyTest.java | 60 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/IdleFlushAdmissionBudgetTest.java | 96 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/IdleMemtableFlusherTest.java | 432 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableLazyPropertyTest.java | 170 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableLazyTest.java | 304 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableRetirementFailureTest.java | 126 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/db/memtable/TrieMemtableRetirementTest.java | 253 | 0 | MEM | Standalone feature tests |
| test/unit/org/apache/cassandra/io/sstable/metadata/MetadataCollectorHistogramTest.java | 188 | 0 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/metrics/AdaptiveCounterArrayPropertyTest.java | 100 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/AdaptiveCounterArrayTest.java | 277 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/AdaptiveHistogramHistoryPropertyTest.java | 81 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/metrics/AdaptiveHistogramHistoryTest.java | 159 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/metrics/AdaptiveJmxHistogramHistoryTest.java | 332 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoirPropertyTest.java | 81 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoirTest.java | 585 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/CompactJmxRegistrationTest.java | 438 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/metrics/CompactMetricsAccuracyTest.java | 257 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/CompactMetricsIntegrationTest.java | 269 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/GeometricThreadLocalMeterPropertyTest.java | 88 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/GeometricThreadLocalMeterTest.java | 324 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/LatencyMetricsTest.java | 31 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/LazyMetricIdPropertyTest.java | 127 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/LazyMetricIdTest.java | 432 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/MetricFactoryReuseTest.java | 206 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/MetricProfilePropertyTest.java | 129 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/MetricProfileTest.java | 321 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/NoOpMetricsTest.java | 295 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/ReservoirStorageProbe.java | 247 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/ReservoirStripeProbe.java | 120 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/metrics/TableMetricBookkeepingLifecycleTest.java | 80 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/TableMetricBookkeepingTest.java | 148 | 0 | SELECT | Feature test; retain nested properties |
| test/unit/org/apache/cassandra/metrics/WorkerMetricResidencyBenchmark.java | 178 | 0 | STORE | Feature test/probe; adapt combined setup if present |
| test/unit/org/apache/cassandra/utils/ObjectNamePropertyPatternTest.java | 49 | 0 | EXPORT | Feature test/probe; retain boundary/property checks |
| test/unit/org/apache/cassandra/utils/streamhist/LazyTombstoneHistogramBuilderPropertyTest.java | 78 | 0 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/utils/streamhist/LazyTombstoneHistogramBuilderTest.java | 236 | 0 | EXTRA | Keep UCS and tombstone units separate |
| test/unit/org/apache/cassandra/utils/streamhist/TombstoneHistogramAllocationTest.java | 133 | 0 | EXTRA | Keep UCS and tombstone units separate |
