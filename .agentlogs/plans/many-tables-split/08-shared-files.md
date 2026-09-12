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

# Ownership of shared files and methods

This allocation describes the delta from 50a10d42a5 to 319995ce5c.
Use semantic anchors below rather than offsets that change during extraction.
All unspecified unchanged code comes from the pinned base.
BASE, MEM, SELECT, STORE, EXPORT, and EXTRA refer to the branch plans.

## Configuration

| File | Owner and exact feature |
| --- | --- |
| Config.java | MEM: four memtable_idle_* fields. SELECT: metrics_config_file. STORE: optimized_metrics_enabled. EXPORT: adaptive_jmx_histogram_history_enabled and compact_jmx_registration_enabled. Final newline: coordinator reconstruction. |
| DatabaseDescriptor.java | MEM: validateIdleMemtableOptions call/method and four idle getters. SELECT: MetricProfile import/state, setConfig load, clientInitialization call through setConfig, getMetricProfile. STORE: getOptimizedMetricsEnabled. EXPORT: two JMX getters. |
| CassandraRelevantProperties.java | SELECT: COMPACT_TABLE_METRIC_BOOKKEEPING. STORE: GEOMETRIC_METER_ARRAYS and LAZY_METRIC_IDS. EXTRA: LAZY_TOMBSTONE_HISTOGRAMS. |
| conf/cassandra.yaml | Same ownership as Config fields. Preserve source defaults in each extracted feature. |
| conf/jvm-server.options | EXPORT: commented builder selection. |
| conf/README.txt | SELECT: profile/catalog/aliases/bookkeeping. STORE: recording/identifier/meter controls. EXPORT: JMX controls. Assign any combined example to integration. |
| README.asc | MEM: idle flushing/admission/operator behavior. EXTRA: UCS option/DDL. INTEGRATION: census and combined benchmark recipes. |

## Metric registry

CassandraMetricsRegistry.java:

- SELECT owns profile guards in counter, atomicLongCounter, meter, register, and
  histogram/timer lookup. Keep unexported required recorders real.
- STORE owns ThreadLocalCounter.create and Meter.create substitutions,
  histogram/timer existing-object reuse, createHistogramReservoir overloads,
  compact/legacy selection, and createReservoir/constructor changes.
- EXPORT owns registerMBean compact dispatch, DynamicJmx* classes,
  StandardMBean metadata, compact rate labels, adaptiveHistory fields, and
  getRecentValues packing/delta logic in JmxHistogram/JmxTimer.
- Standalone SELECT uses original constructors because STORE factories are absent.
- Standalone STORE has unconditional registry reuse because SELECT is absent.
  When merging, restore final source profile guards around reuse.
- Never copy the final registry wholesale into a standalone feature branch.

## Metric contracts and lifecycle

| File | Allocation |
| --- | --- |
| ClearableReservoir.java | BASE: complete new interface. |
| ClearableHistogram.java | BASE: field type and one-argument compatible constructors. STORE: metric-ID constructor, create factory, LazyClearableHistogram selection. |
| DecayingEstimatedHistogramReservoir.java | BASE: implemented interface. STORE: new snapshot constructors, rebaser callback, usesCompactStorage, rebase dispatch, DecayingBucketsOnlySnapshot visibility/construction. |
| ThreadLocalTimer.java | BASE: public/protected constructor delegation for OverrideHistogram. STORE: default reservoir and meter/histogram factories. |
| LatencyMetrics.java | SELECT: empty/concurrent children, no-op instance, isRecording, release idempotence, getSnapshotForRelease/withChildren, no-op timer constructor. STORE: lazy counter selector and LazyLatencyMetricsCounter. |
| TableMetrics.java | SELECT: all delta, including field docs, hidden ownership, aggregate release, no-op destinations, compact OwnedMetrics, constructor and lifecycle changes. |
| Meter.java and ThreadLocal* classes | STORE: all delta except the explicitly BASE timer constructor. |

## Memtables and compaction

TrieMemtable, IdleMemtableFlusher, and ColumnFamilyStore are MEM.
Controller is EXTRA. Neither production implementation needs the other feature.
Current IdleMemtableFlushTest mixes both; split test methods, not whole files.

## Shared tools and test fixtures

- BASE owns generic ResourceProfiler, ProfiledClusterHarness,
  ManyTablesProfileHarness, ai-build, ai-ci-test, hprof_reader.py, and generic
  ai-profile-many-tables behavior.
- SELECT owns PROFILE_COMPACT_BOOKKEEPING in that launcher.
- STORE owns PROFILE_LAZY_METRIC_IDS.
- EXPORT owns PROFILE_TRANSIENT_JMX.
- MemtableResidencyProfileHarness, HeapOwnershipCensusHarness, their combined
  fixture/config tests, ai-test-memtable-lazy, and the final root test selectors
  belong to INTEGRATION. Extract branch-local subsets only with a recorded mapping.
- Keep relevant simple wrappers on feature branches where useful, but never
  import selector entries that reference absent classes or configuration fields.
- Profile distributed tests belong to SELECT with MEM/STORE assumptions relocated.
- Existing LatencyMetricsTest changes belong to SELECT. STORE may add focused
  coverage of its counter variant without importing SELECT implementation.
- Store/export benchmarks that use combined configuration need an integration
  adapter. Preserve their original source for final combined verification.

## Recording adaptations

Each executor reports source file/method, destination file/method, reason,
preserved invariant, and test. Record newly introduced extraction adapters too.
Move no production hunk between owners without coordinator resolution.
