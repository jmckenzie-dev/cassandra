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

# Metric recording and storage

Branch: many-tables/metric-storage. Worktree: wt_moar_metric_storage.
Owner code: STORE. Parent: frozen many-tables/base.

You own only this branch and worktree. Other workers may edit other worktrees.
Do not revert their work or modify the original source worktree.

## Review units

1. Compact reservoirs: empty/sparse representation, adaptive-width dense counters,
   contention-driven stripes, and compatible snapshot construction/rebase hooks.
2. GeometricThreadLocalMeter and Meter factory selection.
3. Registry histogram/timer reuse without eager allocation of discarded recorders.
4. Lazy identifiers, counters, histograms, meters, cleanup, and testing accessors.

Order geometric meters before LazyGeometricThreadLocalMeter. Preserve separate
commits because update overhead and spare retained capacity have different costs.
Keep factory reuse separate from lazy identifiers where practical.

Own AdaptiveCounterArray, CompactDecayingEstimatedHistogramReservoir, all new
Lazy* metric classes, GeometricThreadLocalMeter, Meter, ThreadLocalCounter,
ThreadLocalHistogram, ThreadLocalMeter, ThreadLocalMetrics, OverrideTimer,
and related shared-file hunks. Own counter-factory call sites in BloomFilterTracker,
KeyCache, and BufferPool.

Preserve optimized_metrics_enabled true and the opt-in system-property defaults.
Preserve legacy algorithms and eager hot paths. Compact storage retains cumulative
counts and bucket definitions, but recent-count rounding may differ with stripes.
It does not change SSTable histogram formats.

## Shared files and tests

Use 08-shared-files.md for registry, configuration, ClearableHistogram,
DecayingEstimatedHistogramReservoir, ThreadLocalTimer, and LatencyMetrics.
Registry reuse is unconditional with respect to profiles on this branch because
all exports exist; restore the profile guards when combining with SELECT.
LatencyMetrics gets the lazy counter selector/subclass only; do not import SELECT's
no-op/ownership/release changes to make tests pass.

Inspect CompactMetricsIntegrationTest for profile calls. Remove profile-only setup
from standalone tests and retain the combinations in integration. Any branch-local
test that exposes an existing defect owned by SELECT must be reported explicitly;
do not hide it with a skip or silently add SELECT code.

## Verification

Run ai-build and the array, compact reservoir, accuracy, integration, geometric
meter, lazy identifier, and factory reuse tests listed in 09-path-inventory.md.
Run the existing legacy reservoir, registry, histogram, meter, and latency tests.
Run generated array/reservoir/meter/identifier traces through their entrypoints.

Test clear/rebase, overflow, contention promotion, first-update races, worker exit,
cleanup, identifier reuse, signed updates, and legacy/eager controls.
Measure update cost and memory separately. Include empty/sparse/dense storage,
one and multiple writers, creation churn, and both geometric/lazy combinations.
Coordinate Java Microbenchmark Harness (JMH) runs to avoid competing workloads.

## Return

Give commit units, adaptations, test evidence, compatibility differences, and
performance results. Keep adaptive scrape history and JMX export objects out.
