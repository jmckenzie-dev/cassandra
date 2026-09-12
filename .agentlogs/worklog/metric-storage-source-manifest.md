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

# Metric storage source manifest

## Revisions

- Corrected frozen branch base: f7d5a7e2c0fcf769293ee27073630a3574584b06
- Final read-only source: 319995ce5c178f5eb0dbaf135b98d395c40a045c
- Geometric review state: f5f830e5d8

## Changed source inventory

- `.build/sh/ai-measure-reservoirs`
- `.build/sh/ai-profile-many-tables`
- `.build/sh/analyze-reservoir-measurements.py`
- `.build/sh/analyze-resident-reservoirs.py`
- `.build/sh/analyze-worker-metrics.py`
- `.build/sh/benchmark-worker-metrics.py`
- `conf/README.txt`
- `conf/cassandra.yaml`
- `src/java/org/apache/cassandra/config/CassandraRelevantProperties.java`
- `src/java/org/apache/cassandra/config/Config.java`
- `src/java/org/apache/cassandra/config/DatabaseDescriptor.java`
- `src/java/org/apache/cassandra/io/sstable/filter/BloomFilterTracker.java`
- `src/java/org/apache/cassandra/io/sstable/keycache/KeyCache.java`
- `src/java/org/apache/cassandra/metrics/AdaptiveCounterArray.java`
- `src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java`
- `src/java/org/apache/cassandra/metrics/ClearableHistogram.java`
- `src/java/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoir.java`
- `src/java/org/apache/cassandra/metrics/DecayingEstimatedHistogramReservoir.java`
- `src/java/org/apache/cassandra/metrics/GeometricThreadLocalMeter.java`
- `src/java/org/apache/cassandra/metrics/LatencyMetrics.java`
- `src/java/org/apache/cassandra/metrics/LazyClearableHistogram.java`
- `src/java/org/apache/cassandra/metrics/LazyGeometricThreadLocalMeter.java`
- `src/java/org/apache/cassandra/metrics/LazyThreadLocalCounter.java`
- `src/java/org/apache/cassandra/metrics/LazyThreadLocalHistogram.java`
- `src/java/org/apache/cassandra/metrics/LazyThreadLocalMeter.java`
- `src/java/org/apache/cassandra/metrics/Meter.java`
- `src/java/org/apache/cassandra/metrics/OverrideTimer.java`
- `src/java/org/apache/cassandra/metrics/ThreadLocalCounter.java`
- `src/java/org/apache/cassandra/metrics/ThreadLocalHistogram.java`
- `src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java`
- `src/java/org/apache/cassandra/metrics/ThreadLocalMetrics.java`
- `src/java/org/apache/cassandra/metrics/ThreadLocalTimer.java`
- `src/java/org/apache/cassandra/utils/memory/BufferPool.java`
- `test/microbench/org/apache/cassandra/test/microbench/CompactHistogramBench.java`
- `test/unit/org/apache/cassandra/metrics/AdaptiveCounterArrayPropertyTest.java`
- `test/unit/org/apache/cassandra/metrics/AdaptiveCounterArrayTest.java`
- `test/unit/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoirPropertyTest.java`
- `test/unit/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoirTest.java`
- `test/unit/org/apache/cassandra/metrics/CompactMetricsAccuracyTest.java`
- `test/unit/org/apache/cassandra/metrics/CompactMetricsIntegrationTest.java`
- `test/unit/org/apache/cassandra/metrics/GeometricThreadLocalMeterPropertyTest.java`
- `test/unit/org/apache/cassandra/metrics/GeometricThreadLocalMeterTest.java`
- `test/unit/org/apache/cassandra/metrics/LazyMetricIdPropertyTest.java`
- `test/unit/org/apache/cassandra/metrics/LazyMetricIdTest.java`
- `test/unit/org/apache/cassandra/metrics/MetricFactoryReuseTest.java`
- `test/unit/org/apache/cassandra/metrics/ReservoirStorageProbe.java`
- `test/unit/org/apache/cassandra/metrics/ReservoirStripeProbe.java`
- `test/unit/org/apache/cassandra/metrics/WorkerMetricResidencyBenchmark.java`

## Extracted semantic hunks

- `conf/cassandra.yaml`: STORE `optimized_metrics_enabled: true` setting.
- `Config.java`: STORE `optimized_metrics_enabled` field with the source default.
- `DatabaseDescriptor.java`: STORE `getOptimizedMetricsEnabled` accessor.
- `CassandraRelevantProperties.java`: STORE geometric-meter property.
- `CassandraMetricsRegistry.java`: STORE compact reservoir factory, reservoir
  selection, Meter factory, and unconditional histogram/timer reuse.
- `DecayingEstimatedHistogramReservoir.java`: STORE compact snapshot constructors,
  rebase callback, compact-storage accessor, and buckets-only snapshot access.
- `Meter.java`: STORE geometric factory selection.
- `OverrideTimer.java`: STORE default reservoir factory.
- `ThreadLocalTimer.java`: STORE default reservoir and Meter factory selection.

The list contains all 48 changed build, configuration, production, benchmark,
probe, and test paths. Shared files contain only STORE-owned hunks.

## Extraction adapters

- `CassandraMetricsRegistry.java` imports `DatabaseDescriptor`. The source import
  block was mixed with SELECT and EXPORT changes, so the STORE extraction added
  this required import explicitly.
- `conf/README.txt` appends the STORE options outside the absent SELECT profile
  section. The option defaults and behavior match the source.
- `.build/sh/ai-measure-reservoirs` keeps its source executable mode. Text patch
  extraction initially lost the mode and the first measurement attempt failed
  before any benchmark ran.
- The analyzer uses the existing `python3` executable. This host has no `uv` or
  executable `./venv/bin/python`, and the analyzer uses only the Python standard
  library.

## Standalone adaptations and moved assertions

| Source test | Source method | Destination | Reason | Preserved invariant |
| --- | --- | --- | --- | --- |
| `CompactMetricsIntegrationTest` | `resettingConfigurationAlsoResetsMetricProfile` | Integration test map | The method requires SELECT configuration and profile classes. | Resetting configuration resets both optimized storage and the profile in the merged branch. |
| `CompactMetricsIntegrationTest` | `optimizedParentPreservesExactBucketsWhenChildrenAreReleased` | Integration test map | The method requires SELECT child ownership and release fixes in `LatencyMetrics`. | Compact parent buckets stay exact across child release after branch merge. |
| `LazyMetricIdTest` | `lazyLatencyChildrenPreserveReleaseAggregation` | Integration test map | The method requires SELECT idempotent release and child ownership fixes. | Both eager and lazy latency counters preserve parent aggregation across repeated child release. |
| `MetricFactoryReuseTest` | `disabledFactoriesKeepIndependentHiddenRecorders` | Integration test map | The method requires SELECT configuration and hidden-recorder behavior. | Disabled factories keep independent real recorders without registry entries after branch merge. |

`LazyMetricIdTest.lazyLatencyCounterUsesSelectedImplementation` replaces the
combined method on this branch. It covers only STORE's lazy counter selector and
its basic recording behavior.

The standalone branch keeps registry reuse unconditional. The integration branch
must restore the final source profile guards around existing histogram and timer
reuse after it merges SELECT.
