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

# TODO

- [ ] Census post-retirement heap with matched one-, three-, and six-SSTable cases. Separate populated metrics, SSTable readers, schema, and allocator reservations. Profile the observed 32–42 second drain of 1,000 tables under normal logging and assess byte-based admission for wide write bursts (research/ucs_idle_flush.md).

- [ ] Benchmark an optional smaller UCS minimum hierarchy base for tiny idle flushes. Preserve the legacy default and validate size transitions; T8 reduces rewrites but retains more SSTables, while T4,L10 leaves the tiny-file behavior unchanged (research/ucs_idle_flush.md).

- [ ] Evaluate bounded reductions to surviving modern JMX names/property offsets, repository entries, and metric wrappers. With optional legacy aliases disabled, the main JMX server retains 19.34 MiB at 1000 tables. Exclude removed aliases from remaining savings estimates; preserve the compatibility control without a server/repository rewrite (research/metric_alias_exports.md and research/jmx_registration_and_metric_bookkeeping.md).

- [ ] Revisit allocation-only improvements after residency blockers are addressed, including meter registration-list copying.

- [ ] Revisit worker-owned metrics, lazy aggregation, and safe snapshot/retirement protocols after current residency work (research/metric_threading.md and research/lazy_metric_aggregation.md).

# DONE

- [x] Add optional automatic idle flushing for lazy TrieMemtable + UCS, disabled by default. Preserve write/flush/reclamation ordering and metric history; cover CQL/JMX strategy changes and indexes. Pass 58 targeted tests and a real 30-second smoke test. At 1,000 tables, matched settled heap falls from 1,124.14 to 142.23 MiB (87.35%); report compaction and latency tradeoffs (research/ucs_idle_flush.md).

- [x] Rebase the seven branch commits onto origin/trunk at 88fd0f6a0e. Adapt the build/test entrypoints, fix isolated test startup and configuration reset, and validate all 36 branch test classes plus supporting checks and nine upstream regression classes (.debug/rebase-origin-trunk-20260909.md).

- [x] Compare upstream 4c79cf7391 with the full optimized simple-metrics path at 5000 tables. Add stock-compatible census mode and validate the raised limit. Both runs pass with an 8 GiB ceiling; final settled heap falls from 3350.88 to 311.20 MiB (90.71%). Preserve heap dumps and creation-batch timings (research/stock_vs_optimized_5000_tables.md).

- [x] Add include_legacy_aliases to metric profiles, default true when omitted and false in simple_metrics.yml. Preserve modern exports and recorders; validate registration, recording, aggregates, and lifecycle. Matched 1000-table whole heap falls by 14.97 MiB (13.57%), with 21,254 fewer metric MBeans (research/metric_alias_exports.md).

- [x] Enable ReadTotalLatency and WriteTotalLatency at table and keyspace scope in simple_metrics.yml; update the profile rationale and generated metrics reference.
- [x] Record the standing preference to summarize research reports in the conversation, including findings, measurements, tradeoffs, validation, and next steps; links supplement the summary (AGENTS.md).


- [x] Compact optional TableMetrics release bookkeeping while preserving hidden recorders, aggregate removal, late subclass additions, and both controls. Exact structure falls from 2624 to 576 B/table; quantify slower setup and retain map mode by default (research/jmx_registration_and_metric_bookkeeping.md).

- [x] Extend optional compact JMX registration to histogram/timer/meter exports and share rate labels. Matched JMX retention falls by exactly 893,824 B at 1000 tables; preserve metadata, aliases, queries, authorization, and lifecycle. Measure monitoring allocation/latency separately and retain the default legacy path (research/jmx_registration_and_metric_bookkeeping.md).

- [x] Reduce worker counter-array and metric-ID residency with optional first-use IDs and global histogram/timer reuse. Matched 1000-table whole heap falls by 7.38 MiB; eight-worker array payload falls 86.3%. Preserve the eager recording path; keep lazy IDs off by default because counter updates remain slower. Lifecycle/equivalence tests and principal review pass (research/worker_metric_residency.md).

- [x] Implement and measure an optional JMX query/export path that avoids persistent metric-name property caches across local platform access, remote connectors, and authorization; reduce gauge/counter registration adapters. Matched 1000-table JMX retention falls from 68,277,768 B to 32,252,488 B. Keep the remaining registration work explicit above (research/jmx_query_export.md).

- [x] Isolate monitoring name-cache retention with matched 1000-table heaps, local/remote JMX probes, and generated copy checks; attribute 33.47 MiB to property caches and identify server-side query triggers (research/jmx_monitoring_name_retention.md).

- [x] Record the high-level million-table checkpoint, implemented work, experiment limits, remaining blockers, and recommended focus (research/9_7_checkpoint.md).

- [x] Measure actual cumulative and decay-weighted histogram widths over two simulated hours at 1000 instances and 24 hours at 100, with concentrated/spread traffic, lifecycle observations, memory graphs, and legacy equivalence checks (research/histogram_width_over_time.md).

- [x] Benchmark actual OpenTelemetry adaptive counter storage against Cassandra atomic counters and plain long arrays; measure memory, updates, widening, and fixed-scale precision, and preserve the weighted-overflow compatibility probe (research/otel_compact_storage_benchmark.md).

- [x] Reprofile the simple/adaptive path at 1000 tables with dominator and worker-occupancy analysis; identify next resident-memory targets and paging limits (research/optimized_heap_next_steps.md).

- [x] Add optional adaptive-width JMX recent-value history while preserving the legacy path, independent alias cursors, and exact values; validate both scrape clients and benchmark through 1000 tables (research/adaptive_jmx_history.md).

- [x] Implement startup selection, registration filtering, and shared unused recorders for conf/all_metrics.yml and conf/simple_metrics.yml; preserve database and aggregate dependencies, validate lifecycle and flushing, and measure pre/intermediate/final heap through 1,000 tables (research/metric_profile_runtime.md).

- [x] Generate conf/metrics_ref.md from metric registration calls, field Javadoc, and shipped profiles; document every profile entry and validate catalog coverage, aliases, profile selection, and stale output with isolated tests.

- [x] Preserve the single-writer/lazy aggregation design discussion, memory tradeoffs, publication and retirement invariants, failure cases, and validation plan in research/lazy_metric_aggregation.md.

- [x] Inventory node-wide Table aggregates and Keyspace metrics by backing dependency; correct the omitted table-only ReplicaFilteringProtectionRowsCachedPerQuery profile entry (research/metric_aggregates.md).

- [x] Define all-metrics and simple allowlist profiles for Table/IndexTable and Keyspace metrics, with identical required entries and explicit optional/disabled partitions; document scope and the pending runtime implementation in conf/README.txt.

- [x] Inventory metric values used by database decisions, unregistered operational statistics, aggregate dependencies, and diagnostic-only consumers before designing a metrics allowlist (research/internal_metric_dependencies.md).

- [x] Attribute current compact-mode heap ownership at 100, 500, and 1,000 tables, including full scrapes, attribute-only controls, and one/eight recording workers (research/heap_ownership_census.md).

- [x] Preserve the deferred worker-owned metrics idea, existing Cassandra/OpenTelemetry implementations, lifecycle risks, and experiment criteria in research/metric_threading.md.

- [x] Use narrow dense reservoir counters with exact widening on overflow or contention; preserve histogram precision and validate pre/iteration/final memory and throughput measurements (results: research/compact_runtime_metrics.md).

- [x] Allocate reservoir stripes on contention, correct sparse promotion for smaller stores, and validate pre/iteration/final memory and throughput measurements (results: research/compact_runtime_metrics.md).

- [x] Implement empty/sparse runtime metric reservoirs with a YAML switch defaulted on, preserved legacy control, and pre/iteration/final measurements (plan: .plans/compact-runtime-metrics.md; results: research/compact_runtime_metrics.md).

- [x] Implement and compare Java lazy tombstone histograms and geometric meter storage against the unchanged reference algorithms (plan: .plans/java-histogram-metrics-equivalence.md; results: research/java_histogram_metrics.md).

- [x] Implement and validate explicit memtable retirement with isolated flush tests in run_tests.sh (plan: .plans/explicit-memtable-retirement.md; results: research/explicit_memtable_retirement.md).

- [x] Implement and validate lazy TrieMemtable initialization with existing flush policies (plan: .plans/lazy-memtable-initialization.md; results: research/lazy_memtable_initialization.md; continuation: research/prosecute_memtable_tables.md).

- [x] Preserve memtable residency decisions, evidence, and continuation details in research/prosecute_memtable_tables.md.

- [x] Extend the profiling harness for memtable residency and capture eager-allocation baselines (plan: .plans/memtable-residency-baseline.md; results: research/memtable_residency_baseline.md).
