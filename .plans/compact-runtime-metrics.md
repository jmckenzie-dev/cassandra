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

# Compact runtime metrics

## Contract

Implement three sequential optimizations with one commit per accepted optimization.
Each commit includes reproducible pre-change, iteration, and final measurements,
the implementation, focused validation, and an account of limitations. Preserve
the existing reservoir implementation as a selectable reference. Add a
`cassandra.yaml` switch, defaulting the optimized implementation on as requested.
Do not change persisted SSTable histogram formats or remove existing metrics.
All actual table workloads must use at most 1,000 tables.

## Sequence and acceptance

1. Empty/sparse reservoir storage. Preserve current bucket and decay semantics
   initially. Allocate no mutable bucket arrays for untouched reservoirs; reduce
   retained storage for lightly populated reservoirs. Share immutable bucket
   definitions safely. Verify cumulative exports, snapshots, clear, child merge
   and release, and configuration selection. Measure N100 never-written and
   written/flushed residency plus reservoir update/snapshot performance.
2. Reduce permanent contention storage. Measure the first committed candidate
   before changing it. Reduce populated reservoir storage with a stripe policy
   that preserves exact cumulative counts. Measure single-thread and contended
   updates, and validate races across first allocation, promotion, and snapshots.
   Do not accept a material unexplained hot-path regression for a memory saving.
3. Evaluate counter-width changes against the second commit before changing
   histogram precision. Coarser recent buckets offer only about 9% dense payload
   savings for a 1.2-to-1.25 ratio change while complicating max and cumulative
   exports. Prefer 32-bit counters with exact promotion to 64 bits before overflow,
   subject to measurement and concurrency validation. Retain the existing buckets
   and accuracy when this representation delivers the larger memory benefit.
   Preserve stable cumulative export buckets and observation population. Require
   a measured resident-memory or computation benefit and bounded distribution
   error. Use controlled event time and raw-event expectations for median,
   p99, p99.99, extrema, mean, and distribution shape. Preserve recent/lifetime
   meaning and validate speculative-retry consumers. Select a conservative
   accuracy target from the existing bucket resolution and measured traces;
   do not silently trade away rare tails or expand memory for more precision.

## Measurement protocol

Record fresh baseline measurements before production edits for each step.
Use the existing Java 21 dev container and profiling wrappers. Use matched
fresh JVMs, fixed input seeds, and repeated alternating runs for comparisons.
Report structural retained bytes separately from whole-JVM post-GC heap and
sampled allocation. Timing probes include warm-up and repetitions; report
variation, not only the best sample. Run experiments sequentially to avoid
contamination from parallel builds or benchmarks.

Tests and diagnostics may be authored in parallel. Build and benchmark execution
has a single owner. Keep raw artifacts in timestamped logs; commit concise
results and scripts, not heap dumps or generated build artifacts. Existing
worktree documentation changes belong with the first optimization commit.

## Test scope

Production scope: new runtime reservoir/storage classes and their factory,
configuration, histogram-clear and latency-aggregation integration. Performance
tests compare the unchanged DecayingEstimatedHistogramReservoir with the new
implementation through supported constructors and public update/snapshot APIs.
Use generated event traces, concurrency tests, JMX export tests, and small real
table workloads. Keep test entry points in run_tests.sh and run_property_tests.sh.
