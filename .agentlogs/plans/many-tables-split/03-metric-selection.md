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

# Metric selection

Branch: many-tables/metric-selection. Worktree: wt_moar_metric_selection.
Owner code: SELECT. Parent: frozen many-tables/base.

You own only this branch and worktree. Other workers may edit other worktrees.
Do not revert their work or modify the original source worktree.

## Extract

1. Latency-child release correctness and explicit hidden-recorder ownership.
2. MetricProfile parsing/catalog, registration filtering, shared NoOpMetrics,
   aggregate-aware forwarding, KeyspaceMetrics, and virtual metric tables.
3. Legacy-alias selection and profile/reference generation and packaging.
4. Optional compact table release bookkeeping as a separate commit.

Keep real recording state for database decisions and enabled aggregate inputs.
Disabled exports must not appear as measured zero values in virtual tables.
Keep late subclass metric additions and repeated release correct.

Own field-documentation changes in BloomFilterMetrics, IndexSummaryMetrics,
and KeyCacheMetrics. Own both profile files, metrics-catalog.properties,
conf/metrics_ref.md, generator/tests, and Debian install entries.

Faithful extraction preserves metrics_config_file: simple_metrics.yml and the
profile alias defaults. The prior suggestion to preserve all exports by default
is a later revision proposal. Do not make that change during extraction.

## Shared files and tests

Use 08-shared-files.md. Registry factories must use legacy constructors on this
branch. Do not import STORE's lazy factories or EXPORT's registration classes.
The base provides ClearableReservoir and the timer constructor needed by no-ops.

Keep the source LatencyMetrics release/children/no-op changes. Exclude only the
lazy counter selector and LazyLatencyMetricsCounter subclass, which STORE owns.

Adapt MetricProfileRegistrationTest and MetricProfileRecordingTest to run
without optimized_metrics_enabled and lazy_initialization parameters.
Move TrieMemtable.isInitialized assertions into integration. Preserve actual
writes, reads, flushes, index behavior, and registry/aggregate checks locally.
Do not copy HeapOwnershipCensusHarnessTest if it requires the combined harness;
place it in integration and retain focused feature tests here.

## Verification

Run ai-build, then ai-ci-test for MetricProfileTest, MetricProfilePropertyTest,
NoOpMetricsTest, LatencyMetricsTest, MetricProfileRegistrationTest,
MetricProfileRecordingTest, TableMetricBookkeepingTest, and
TableMetricBookkeepingLifecycleTest. Preserve the nested NoOpMetrics and
bookkeeping property suites through their existing entrypoints.

Run the metrics-reference generator validation and stale-output checks.
Check profile files and catalog inclusion in distribution artifacts.

Exercise omitted/all/simple/custom/invalid profiles; aliases on/off; required
metrics; hidden parent inputs; extension names; index tables; create/drop/recreate;
client configuration loading; and empty virtual views for disabled metrics.
Measure all versus simple with legacy storage and JMX, and equal table workloads.

## Return

Record the exact source allocation, legacy-constructor adaptations, relocated
tests, build/tests, generator/packaging results, and remaining concerns.
Do not fold storage or JMX optimizations into this branch to fix compilation.
