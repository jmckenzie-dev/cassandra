<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Metric storage validation

Date: 2026-09-11

Java: OpenJDK 21.0.12.1

## Build

`.build/sh/ai-build` passed compilation, test compilation, and Checkstyle.
The final successful log is `logs/20260911-222435-ai-build.log`.

Two earlier build attempts found extraction errors and remain in the logs:

- `logs/20260911-222245-ai-build.log` found a missing `DatabaseDescriptor`
  import in the shared registry file.
- `logs/20260911-222326-ai-build.log` found a SELECT-owned test method in the
  standalone STORE test.

Both errors were fixed before the successful build.

## Focused tests

Each class ran through `.build/sh/ai-ci-test` with Java 21. The preserved XML
and HTML reports are in `metric-storage-test-results/` beside this file.

| Class | Cases | Passed | Skipped |
| --- | ---: | ---: | ---: |
| `AdaptiveCounterArrayTest` | 8 | 8 | 0 |
| `AdaptiveCounterArrayPropertyTest` | 1 | 1 | 0 |
| `CompactDecayingEstimatedHistogramReservoirTest` | 13 | 13 | 0 |
| `CompactDecayingEstimatedHistogramReservoirPropertyTest` | 1 | 1 | 0 |
| `CompactMetricsAccuracyTest` | 2 | 2 | 0 |
| `CompactMetricsIntegrationTest` | 4 | 4 | 0 |
| `GeometricThreadLocalMeterTest` | 6 | 6 | 0 |
| `GeometricThreadLocalMeterPropertyTest` | 1 | 1 | 0 |
| `LazyMetricIdTest` | 8 | 8 | 0 |
| `LazyMetricIdPropertyTest` | 2 | 2 | 0 |
| `MetricFactoryReuseTest` | 4 | 4 | 0 |
| `DecayingEstimatedHistogramReservoirTest` | 29 | 28 | 1 |
| `CassandraMetricsRegistryTest` | 9 | 9 | 0 |
| `ThreadLocalHistogramTest` | 2 | 2 | 0 |
| `ThreadLocalMeterTest` | 6 | 6 | 0 |
| `LatencyMetricsTest` | 3 | 3 | 0 |
| **Total** | **99** | **98** | **1** |

No test failed or reported an error. The one skipped case is an existing case
in `DecayingEstimatedHistogramReservoirTest`.

The nested-entrypoint audit found no nested `Properties` class in STORE tests.
The four top-level property classes reported 1, 1, 1, and 2 cases. The
`DecayingEstimatedHistogramReservoirTest` wrapper ran all 29 enclosed and
parameterized cases. No separate JUnit entrypoint was omitted.

## Reservoir measurements

The measurement ran with the source defaults through:

`.build/sh/ai-measure-reservoirs --step metric-storage --checkpoint post`

The command completed with exit code 0. The batch is under
`logs/20260911-223851-reservoirs-metric-storage-post-nPi2Zs/`. The analyzed
result is `20260912-024001-597858-reservoir-summary.json` in that directory.

The legacy reservoir retained 5437.44 bytes per instance at each measured
occupancy. The compact reservoir retained 157.44 bytes when empty, 653.44 bytes
with one occupied bucket, and 1581.44 bytes at 64 or 164 occupied buckets.

Median Java Microbenchmark Harness (JMH) throughput was 51599.23 operations per
millisecond for legacy and 50099.70 for compact with one thread. It was 24187.73
for legacy and 22963.54 for compact with four threads. Normalized allocation was
near zero and no garbage collection occurred. The samples overlap and the
four-thread measurements vary, so this record does not claim significance.

The first launcher attempt returned exit code 126 before a benchmark started.
Patch extraction had lost the source executable bit. The executable bit was
restored, and the complete rerun passed. All attempt output remains under
`logs/`.

## Lazy worker metric measurements

The benchmark used the corrected base JAR and three forks:

`python3 .build/sh/benchmark-worker-metrics.py --original-jar`
`../wt_moar_base/build/apache-cassandra-7.0-SNAPSHOT.jar --forks 3`

The benchmark and summary commands completed with exit code 0. All 36 cases
reported `verified_count`. The raw and summary records are:

- `logs/20260911-224025-335596-worker-metrics-benchmark.json`
- `logs/20260911-224054-120096-worker-metrics-summary.json`

At 64 workers with partitioned access, lazy allocation retained 18,430,987 heap
bytes versus 53,061,483 for eager allocation. It used 334,345 worker slots
versus 3,795,373. Its measured thread cost was 4.134 versus 3.329 nanoseconds
per operation.

At 8 workers with shared access, lazy allocation retained 16,230,637 heap bytes
versus 24,389,395 for eager allocation. It used 52,112 worker slots versus
492,032. Its measured thread cost was 3.091 versus 3.599 nanoseconds per
operation.

The source has no standalone geometric-meter performance harness. Correctness
and property tests cover eager and lazy identifiers with legacy and geometric
meter storage.
