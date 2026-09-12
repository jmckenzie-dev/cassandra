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

# Remaining code and experiments

Branch: many-tables/remaining. Worktree: wt_moar_remaining.
Owner code: EXTRA. Parent: frozen many-tables/base.

You own only this branch and worktree. Other workers may edit other worktrees.
Do not revert their work or modify the original source worktree.

This branch is an explicit inventory, not a claim that all remaining work belongs
in one eventual submission. Keep the following implementations in separate commits.

## UCS hierarchy option

Extract Controller's min_hierarchy_size field, option parser/validation,
constructor wiring, observed-flush rounding, and hierarchy floor.
Include ControllerTest, ControllerHierarchyTest, UnifiedCompactionStrategyTest,
and the operator documentation. Preserve the 1 MiB default and old arithmetic.

Run .build/sh/ai-build, then relevant classes through ai-ci-test. Include existing
ShardedMultiWriterTest. Use explicit ordinary flushes in standalone tests.
Automatic idle-flush/hierarchy combinations belong in integration, since MEM is
absent here. Keep test assertions when relocating the mixed distributed methods.

Previous small-hierarchy measurements trade lower compaction output for more
readers. Preserve that result and the stopped validation stages; do not change
defaults or extrapolate to larger populations during extraction.

## Lazy tombstone histogram construction

Extract TombstoneHistogramBuilder, LazyTombstoneHistogramBuilder,
StreamingTombstoneHistogramBuilder's interface change, MetadataCollector's
selection/injection, and LAZY_TOMBSTONE_HISTOGRAMS. Keep the option false.

Run StreamingTombstoneHistogramBuilderTest, LazyTombstoneHistogramBuilderTest,
LazyTombstoneHistogramBuilderPropertyTest, MetadataCollectorHistogramTest,
and TombstoneHistogramAllocationTest. Validate exact values, serialization,
release behavior, empty/populated allocation, and first-observation costs.

This changes writer allocation; it is independent of runtime metric reservoirs.

## Research and local tooling

The complete path inventory labels historical files ARCHIVE. Preserve them in
the original source worktree; copy relevant evidence into branch documentation
only with an explicit record. ARCHIVE means retained, not deleted.

OpenTelemetry experiments, width-over-time studies, and analyses that depend on
other feature classes belong to integration/research tooling. They must not make
this standalone branch depend on STORE or MEM. Keep cross-feature benchmark
drivers in the integration worktree.

List open ideas separately: SSTable snapshot trimming/offset sharing, additional
JMX object reductions, worker-owned metrics, lazy aggregation, and registration
list copying. No new implementation of those ideas is part of this split.

## Return

Give two independent production commit units, test results, a residual inventory,
and locations of retained research/tooling. No omitted production path is allowed.
