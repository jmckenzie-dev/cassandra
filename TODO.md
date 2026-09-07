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

- [ ] Implement runtime selection and enforcement for the metric profiles in conf/all_metrics.yml and conf/simple_metrics.yml; preserve required state and enabled aggregate dependencies from research/internal_metric_dependencies.md, handle other metric families explicitly, and measure against research/heap_ownership_census.md.

- [ ] Compact JMX recent-value history, preserving independent alias cursors and empty scrape behavior; measure both attribute-only and name-inspecting clients.

- [ ] Reduce dense worker counter-array holes and metric-ID lifecycle overhead after the registration work; preserve counters across reset, ID reuse, and thread exit.

- [ ] Add bounded automatic idle retirement after resident metrics storage is addressed; preserve write/flush/reclamation ordering and metric history.

- [ ] Revisit allocation-only improvements after residency blockers are addressed, including meter registration-list copying.

- [ ] Revisit worker-owned metrics, lazy aggregation, and safe snapshot/retirement protocols after current residency work (research/metric_threading.md and research/lazy_metric_aggregation.md).

# DONE

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
