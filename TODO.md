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

- [ ] Reduce unused reservoir contention stripes after the first optimization commit; capture pre/iteration/final measurements and commit separately.

- [ ] Evaluate narrower reservoir counters with exact widening before overflow; retain current histogram precision if this saves more memory than coarser buckets. Capture pre/iteration/final measurements and commit separately.

- [ ] Add bounded automatic idle retirement after resident metrics storage is addressed; preserve write/flush/reclamation ordering and metric history.

- [ ] Revisit allocation-only improvements after residency blockers are addressed, including meter registration-list copying.

# DONE

- [x] Implement empty/sparse runtime metric reservoirs with a YAML switch defaulted on, preserved legacy control, and pre/iteration/final measurements (plan: .plans/compact-runtime-metrics.md; results: research/compact_runtime_metrics.md).

- [x] Implement and compare Java lazy tombstone histograms and geometric meter storage against the unchanged reference algorithms (plan: .plans/java-histogram-metrics-equivalence.md; results: research/java_histogram_metrics.md).

- [x] Implement and validate explicit memtable retirement with isolated flush tests in run_tests.sh (plan: .plans/explicit-memtable-retirement.md; results: research/explicit_memtable_retirement.md).

- [x] Implement and validate lazy TrieMemtable initialization with existing flush policies (plan: .plans/lazy-memtable-initialization.md; results: research/lazy_memtable_initialization.md; continuation: research/prosecute_memtable_tables.md).

- [x] Preserve memtable residency decisions, evidence, and continuation details in research/prosecute_memtable_tables.md.

- [x] Extend the profiling harness for memtable residency and capture eager-allocation baselines (plan: .plans/memtable-residency-baseline.md; results: research/memtable_residency_baseline.md).
