<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this work
for additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the License); you may not use
this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an AS IS BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License. -->

# Integration reconstruction

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Shared base: f7d5a7e2c0fcf769293ee27073630a3574584b06.
Merge order: memtables, selection, storage, exports, remaining.

All five final feature heads are merged. The working `src/` tree matches the
pinned source exactly. Shared configuration and registry conflicts were resolved
by assigned hunks and checked against that source. No production redesign was
made during reconstruction.

## Restored combined tests

The original classes retain the combined assertions; no replacement names hide
or duplicate the original methods.

| Class | Restored invariant |
| --- | --- |
| IdleMemtableFlushTest | Small UCS hierarchy, runtime alteration, and idle flushing |
| ControllerHierarchyTest | Lazy memtable initialization before and after writes |
| MetricProfileRegistrationTest | Compact storage and registration with lazy memtables |
| MetricProfileRecordingTest | Profile recording with compact storage and lazy memtables |
| CompactMetricsIntegrationTest | Profiles, aliases, factories, reuse, and release |
| MetricFactoryReuseTest | Disabled metrics keep independent hidden recorders |
| LazyMetricIdTest | Lazy identifiers with profile-aware factories |
| AdaptiveJmxHistogramHistoryTest | Both storage modes with history and registration choices |
| CompactJmxRegistrationTest | Compact storage with registration and lifecycle properties |

The separate expiration-test timing fix remains in
UnifiedCompactionStrategyTest. It changes mock timing only.

## Integration adapters

- Single-node runtime fixtures use subnet 0 on this host. Configuration parser
  tests keep their original subnet assertions.
- MemtableResidencyProfileHarness records effective profile, adaptive history,
  and registration settings after node startup.
- ai-test-memtable-lazy uses ai-build and supports frozen outputs with
  PROFILE_SKIP_BUILD=true.
- Root selectors use project-local Python when present, otherwise python3, for
  standard-library analysis. They do not install dependencies.
- Documentation keeps combined controls and marks historical source-only links.
- MetricProfileConfigurationMatrixTest adds focused live-node combinations.
  Its reused fixture checks effective settings and recorder implementation types.
- ai-validate-many-tables-split selects changed tests, domain smoke tests, and
  explicit nested property entrypoints. It preserves each test report.

## Verification status

Static production reconstruction, the Java 21 build with Checkstyle, and all
targeted combined tests passed. See integration-validation.md for counts and
raw evidence. Matched measurements remain pending.

## Path accounting

An independent working-file hash audit accounted for all 246 source paths:
157 imported paths and 89 archived paths. No source path is missing. All 89
archived paths remain in the original checkout. Its TODO.md has an existing
split-planning entry beyond the pinned revision; leave that user change intact.

The 13 content differences are named adaptations: the JMX probe README,
ai-generate-metrics-reference, ai-test-memtable-lazy, README.asc, conf/README.txt,
both root test selectors, IdleMemtableFlushTest, MemtableResidencyHarnessTest,
MemtableResidencyProfileHarness, MetricProfileRegistrationTest,
TrieMemtableLazyRecoveryTest, and UnifiedCompactionStrategyTest.

The JMX probe script intentionally has executable mode 100755 because its
portable instructions call it directly. The audit also found lost executable
modes on both reservoir analyzers. Storage commit 3e8478f751 restores those
original modes; integration merge 52aa4cc814 includes that correction.

## Exact moved methods

- IdleMemtableFlushTest restores smallHierarchyRetiresReactivatesAndRecovers
  and operatorCanAlterHierarchyAndScalingBeforeRestart.
- ControllerHierarchyTest restores initialization assertions in
  realFlushEstimatesPromoteLargeFilesAcrossLevelGaps and
  compactionPreservesDeletesAndTtlAtExplicitReadTimes.
- MetricProfileRegistrationTest restores combined controls and allocation
  assertions in exerciseProfile. MetricProfileRecordingTest restores its
  combined newCluster controls.
- CompactMetricsIntegrationTest restores resettingConfigurationAlsoResetsMetricProfile
  and optimizedParentPreservesExactBucketsWhenChildrenAreReleased.
- MetricFactoryReuseTest restores disabledFactoriesKeepIndependentHiddenRecorders.
- LazyMetricIdTest restores lazyLatencyChildrenPreserveReleaseAggregation in
  place of the standalone lazyLatencyCounterUsesSelectedImplementation.
- AdaptiveJmxHistogramHistoryTest restores
  configurationDefaultsOffAndDoesNotSelectTheRecorderBackend and
  realRecordersAndResetsMatchWithEitherHistogramBackend. Its registerViews
  helper retains the chosen recorder backend across export controls.
- CompactJmxRegistrationTest restores selected factories in
  metadataValuesOperationsAndFailuresMatchLegacy, generated main cases, and
  benchmarkMonitoring.

OpenTelemetry benchmarks require an external reference checkout. Heap dominator
analysis requires Eclipse Memory Analyzer. Neither dependency will be installed
as part of this split. Historical Linux-only drivers remain preserved tooling.
