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

# Metric monitoring and export

Branch: many-tables/metric-exports. Worktree: wt_moar_metric_exports.
Owner code: EXPORT. Parent: frozen many-tables/base.

You own only this branch and worktree. Other workers may edit other worktrees.
Do not revert their work or modify the original source worktree.

## Review units

1. AdaptiveHistogramHistory and exact recent-value cursor packing.
2. TransientMetricMBean, compact registry registration adapters, and shared rate text.
3. TransientMBeanServerBuilder and ObjectNamePropertyPattern.

Own adaptive_jmx_histogram_history_enabled, compact_jmx_registration_enabled,
their getters and documentation, the optional JVM builder setting, and
PROFILE_TRANSIENT_JMX in the profiling launcher.
Keep all switches opt-in. Test these changes with legacy recording and all exports.

The builder must install before platform-server initialization. Preserve the
existing JDK repository and public behavior. Avoid private JDK state mutation.

## Shared files and tests

Apply only EXPORT hunks in registry registration and Jmx* classes.
Do not import MetricProfile, NoOpMetrics, lazy identifiers, or compact reservoirs.
AdaptiveHistogramHistory is separate from AdaptiveCounterArray.

Keep AdaptiveHistogramHistoryTest/PropertyTest, AdaptiveJmxHistogramHistoryTest,
CompactJmxRegistrationTest and its nested properties,
ObjectNamePropertyPatternTest, TransientMBeanServerAuthorizationTest, and
the JmxServerBoundary/JmxNameRetention diagnostics.
Move combined HeapOwnershipCensusHarness tests to integration.

## Verification

Run ai-build and the named unit tests using ai-ci-test. Run local/remote,
notification, protected-query, and early/late-install boundary programs using
the existing ai-probe-jmx-names entrypoint after adapting its dependencies.
Retain generated name-pattern and compact-registration tests.

Verify identical metadata, types, units, attributes, operations, exact/wildcard
queries, notifications, aliases, independent scrape cursors, registration/removal,
and authorization outcomes. Measure scrape allocation, latency, and retained
objects independently. Do not infer savings from merely disabling exports.

## Return

Report each unit, legacy-storage validation, boundary results, extracted harness
dependencies, logs, and any behavior differences.
