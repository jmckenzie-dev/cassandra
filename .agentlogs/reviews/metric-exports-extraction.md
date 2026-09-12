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

# Extraction record

Owned production files match the pinned source. Shared registry edits contain
only export imports, registration adapters, and JMX wrappers. Config and
DatabaseDescriptor contain only the two export switches and getters.
The profiling launcher adds only PROFILE_TRANSIENT_JMX.

## Standalone test changes

AdaptiveJmxHistogramHistoryTest:
- configurationDefaultsOffAndDoesNotSelectTheRecorderBackend becomes
  configurationDefaultsOffAndLoadsFromYaml. Retains default and YAML loading
  assertions. Integration restores optimized_metrics_enabled independence and
  reservoir implementation assertions.
- realRecordersAndResetsMatchWithEitherHistogramBackend becomes
  realRecordersAndResetsMatchWithLegacyHistogramBackend. Uses the legacy
  reservoir directly. Keeps updates, clears, snapshots, deltas, and alias cursors.
  Integration restores both reservoir backends.
- configure and registerViews omit optimized_metrics_enabled. All eight
  adaptive/legacy history and compact/legacy registration views remain.

CompactJmxRegistrationTest:
- createHistogramReservoir(false) becomes a direct legacy reservoir constructor.
- Meter.create() becomes new ThreadLocalMeter().
- benchmarkMonitoring omits optimized_metrics_enabled.
  Metadata, values, errors, operations, generated checks, and benchmark assertions
  remain. Integration restores storage-factory and optimized-backend combinations.

The original combined methods remain in the pinned source and must return in
integration. These adapters alter only standalone fixtures, not production code.

## Harnesses and documentation

AdaptiveJmxHistoryBenchmark uses only the history switch and remains unchanged.
Name probes retain source behavior. Root selectors call the shared build wrapper
and preserve each test report. Historical research remains in the source branch.
