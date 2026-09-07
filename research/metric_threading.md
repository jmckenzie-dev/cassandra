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

# Deferred idea: metrics owned by recording workers

Recorded 2026-09-06. The user deferred this investigation to return to the
one-million-table heap-residency goal. This is a design hypothesis, not an
implemented optimization or measured performance result.

The later [lazy aggregation and retirement analysis](lazy_metric_aggregation.md)
compares worker-owned parent recorders with worker-owned active table
contributions. It records memory bounds, snapshot publication, exactly-once
retirement, and the per-table history contract. Both remain design candidates.

## Motivation

Shared histogram updates require coordination among writers. A monitor serializes
updates; atomic counters still require cache-line ownership and can suffer under
contention. A worker-private histogram can use ordinary primitive arrays and
combine its contribution with other workers only during collection.

More copies need not mean more memory if each copy is small and allocated only
for observed metrics. The important variables are the number of workers touching
each table, representation size, and how long inactive contributions remain.
Lazy allocation without retirement eventually retains every previously touched
worker/table pair.

The current compact reservoir remains the control. It saves standing memory but
retains adaptive-storage checks on its update path. Its final shared-reservoir
microbenchmark was 13.3% slower than legacy at one thread and 4.1% slower at four.
Those are sustained recording results, not memtable-reactivation costs or
end-to-end Cassandra throughput penalties. See [measurements](compact_runtime_metrics.md).

## Existing Cassandra implementations

- [ThreadLocalMetrics](../src/java/org/apache/cassandra/metrics/ThreadLocalMetrics.java)
  already stores scalar counter contributions in a plain long array per thread.
  Queries sum live threads and a summary of exited threads. Thread exit and metric
  ID recycling have explicit coordination. Its dense global-ID indexing is a
  residency concern: touching a high ID allocates slots for preceding IDs even
  when that thread never uses them. Its documented visibility assumptions should
  not be treated as a general protocol for reading a changing histogram structure.
- [ThreadLocalHistogram](../src/java/org/apache/cassandra/metrics/ThreadLocalHistogram.java)
  makes the observation count thread-local; bucket updates still reach a shared
  reservoir.
- [LogLinearDecayingHistograms](../src/java/org/apache/cassandra/metrics/LogLinearDecayingHistograms.java)
  provides single-threaded histograms, batches updates, and shares decay arithmetic
  across a group. It currently allocates bucket arrays and bounds the number of
  histograms encoded in a group; it is not an arbitrary sparse table registry.
- [ShardedDecayingHistograms](../src/java/org/apache/cassandra/metrics/ShardedDecayingHistograms.java)
  merges shard snapshots under caller-supplied locks. Accord supplies an executor
  lock. This is a relevant ownership pattern, not an existing lock-free collection
  implementation for table metrics.

Inspect these paths before creating another implementation.

## OpenTelemetry reference

Source inspection used the local `ref/opentelemetry-java` checkout at `c87b50ec4`.
The reference checkout is not a Cassandra dependency. No comparative benchmark
was run. Relevant paths below are relative to that checkout:

- `sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/internal/aggregator/AdaptingIntegerArray.java`
  widens ordinary byte arrays through short, int, and long arrays. It is not
  thread-safe. Histogram recording currently supplies synchronization.
- `AdaptingCircularBufferCounter.java` and
  `DoubleBase2ExponentialHistogramBuckets.java` in the same directory bound bucket
  storage and reduce resolution when the observed range does not fit. That is an
  optional histogram-algorithm change, separate from worker ownership.
- `sdk/common/src/main/java/io/opentelemetry/sdk/common/export/MemoryMode.java`
  describes reusable collection data. Retaining buffers and handles reduces
  allocation but can increase standing memory. Concurrent collection/export is
  restricted in reusable mode.
- `sdk/metrics/src/jmh/java/io/opentelemetry/sdk/metrics/internal/aggregator/HistogramBenchmark.java`
  uses thread-scoped histograms. Its multithread cases do not measure concurrent
  updates to a shared histogram. Its exponential configurations use scale zero.

The source files declare Apache-2.0. The small storage classes are package-private;
public aggregator classes are internal and have no API stability guarantee.
Extracting them leaves Cassandra responsible for integration and maintenance.
Using their supported SDK APIs instead does not supply Cassandra's decay, rates,
local percentile consumers, or existing exports automatically.

## Candidate structure

Use one sparse registry per stable recording worker, keyed by compact metric
identity. Avoid a separate Java ThreadLocal instance for each table metric and
avoid arrays sized by the highest global metric ID. Share metric definitions and
table identity. Allocate mutable state only for observed metrics.

Start with a fixed bucket scheme to isolate the ownership change. Compare existing
single-writer histogram arithmetic before adding adaptive counter widths or a
different sketch. A small cache of recent metric handles might reduce lookup cost,
but its memory and invalidation costs must be measured.

The intended memory model is:

```
shared definitions + retained historical summaries
  + storage for active worker/table/metric combinations
  + bounded publication and collection buffers
```

Worker ownership must be literal: hashing multiple concurrent writers onto a
small set of plain arrays is not single-writer confinement. Stable executor
ownership can reduce the number of copies, but introducing request routing solely
for metrics may cost more than it saves.

## Publication and reclamation

A single writer does not make a changing array safe for concurrent readers.
Approximate statistics still require safe publication and structurally consistent
reads. Do not let collectors inspect live non-thread-safe OpenTelemetry containers
while an owner widens arrays, changes bucket scale, or resets values.

A candidate protocol is owner-assisted publication:

1. The worker updates its private state.
2. At a task boundary or collection request, the worker publishes a completed
   contribution and starts using another buffer.
3. The collector merges the contribution exactly once.
4. The collector releases the buffer before the worker reuses it.

This needs a defined ownership and memory-ordering protocol. Double buffering alone
does not provide it. Bound outstanding buffers and define behavior when collection
falls behind. Account for coordination, copying, and buffer retention in benchmarks.

An idle worker must publish its final observations without waiting for another
metric update. A stopped or failed worker also needs cleanup. Collection must not
deadlock when requested from an owning executor or block requests indefinitely
waiting for another executor. Cached published snapshots are an option, but their
freshness contract must be explicit. An executor-lock approach like Accord's is
another control worth comparing.

Merge inactive contributions into retained history before removing private state.
Inactivity does not erase cumulative values. Specify metric generation/identity
rules for table drop and ID reuse. Readers must never count both a transferred
contribution and its former owner copy, or miss both.

## Accuracy and compatibility

- Merge distributions and then calculate percentiles; never average per-worker
  percentile values.
- Align decayed contributions to a common observation time. Define event-time
  handling for buffered updates and the freshness of idle-worker contributions.
- Compatible bucket layouts are required for merging; adaptive layouts may need
  downscaling or conversion. Validate the resulting tail error.
- Preserve cumulative history and meaningful recent statistics, including mean,
  standard deviation, and extrema for the intended time scope. A lifetime maximum
  does not replace a recent maximum.
- Preserve existing metric names, aliases, units, types and access paths. Retain
  the legacy path. Test percentile-based speculative retry, not only dashboards.
- A collector-owned reusable buffer is preferable to an unbounded permanent
  snapshot buffer per table, subject to concurrent reader requirements.

The user permits representative rather than numerically identical optimized
statistics. Numerical tolerance and allowed freshness differences remain open.
No observations should silently disappear to meet a memory limit.

## Experiment if this work resumes

Keep actual table workloads at or below 1,000. Use the user's pre-change,
iteration, and final measurement sequence, with one focused commit per accepted
optimization. Keep experiments selectable alongside the existing implementation.

Compare legacy shared reservoirs, current compact reservoirs, and a worker-owned
fixed-layout candidate. Add adaptive widths only after ownership works.

Measure:

- Uncontended recording, shared-table fan-out, and independent-table recording.
- Allocation after warmup separately from first use, publication, merge, and reuse.
- Retained state for never-used, lightly used, hot, and idle metrics.
- Memory after every worker touches every table, then activity contracts.
- Concurrent full scrapes, collection latency, and local percentile-query latency.
- Thread exit, metric drop/recreation, delayed collectors, buffer exhaustion, and
  repeated publication with exact post-quiescence count checks.
- Rare outliers, changing distributions, decay transitions, and p99.99 accuracy.

Count directories, handles, historical summaries, snapshots, and compatibility
adapters as well as numeric arrays. Existing OpenTelemetry benchmark results
cannot substitute for this ownership and contention matrix.

## Status

Deferred. No worker-owned histogram implementation, publication protocol, new
dependency, or metrics-provider refactor was added. Return first to attribution
and reduction of the current per-table resident graph; see
[the continuation record](prosecute_memtable_tables.md).
