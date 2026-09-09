<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the License); you may not
use this file except in compliance with the License. You may obtain a copy
at http://www.apache.org/licenses/LICENSE-2.0 . Unless required by applicable
law or agreed to in writing, software distributed under the License is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
-->

# OpenTelemetry compact storage benchmark

Compare the existing working-tree Cassandra adaptive counter array with the
actual OpenTelemetry (OTel) adaptive array from the local reference checkout.
Git comparison basis: HEAD 4192a00e1f3f26bc92a7a56da7cb40debe79ff9b plus the
existing working tree. Do not change production code or add dependencies.

## Primary experiment

Use a plain long array as the single-owner control, Cassandra's existing
AdaptiveCounterArray as the atomic reference, and OTel AdaptingIntegerArray
as the candidate. Compile the actual source in ref/ without copying or editing
its implementation. Package-local test bridges may call intended methods.

Use identical bucket indexes and positive updates. Cover empty, byte, short,
int, and long counts; first update, each widening boundary, hot single-bucket
and spread updates, reset after widening, and cold histogram populations.
Keep populations at or below 1000. Use the current Cassandra bucket count
(164 offsets plus overflow, with zero handling checked against the source).
Report construction allocation separately from updates. Test correctness
outside timing against an exact long-array oracle, including seeded generated
positive updates, clear, copy, and overflow boundaries.

Measure update nanoseconds, thread-allocated bytes, and owned object-graph
bytes. Measure a population and a population of one fewer objects to exclude
shared enum/indexer state and the outer array from marginal memory. Keep
widths fixed during steady-state timing; reset or fresh fixtures outside
timing as needed. Include separately timed transitions. Alternate order,
warm up, retain every sample, and run three separate JVMs. Publish medians
and ranges, without machine-specific pass/fail timing thresholds.

## Secondary experiment and limits

If the actual OTel exponential bucket class can compile with existing local
dependencies, measure bounded bucket storage and downscaling accuracy. If its
build requires unavailable dependencies, do not install or substitute them.
Instead test the standalone circular counter and indexer at fixed scales and
report capacity/precision implications separately. No implementation of the
OTel downscaling algorithm may be copied into the benchmark.

This experiment excludes shared-writer contention, decay, parent aggregation,
JMX registration, and database requests. The atomic reference performs work
the plain arrays omit; do not claim a drop-in histogram throughput improvement.
Adaptive widths preserve counts but do not implement Cassandra's signed
updates, concurrency, or decay contracts. Low positive counts and long-lived
or weighted counts must be distinguished.

## Delivery

Provide a repeatable logged ai-* launcher, benchmark and property checks,
raw measurements, and research/otel_compact_storage_benchmark.md. Record
measurement limits and a recommendation. Update TODO.md and the continuation
notes. Do not commit unless the user asks.
