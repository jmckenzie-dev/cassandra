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

# Metric selection validation

Date: 2026-09-11

Base: `f7d5a7e2c0fcf769293ee27073630a3574584b06`

Source: `319995ce5c178f5eb0dbaf135b98d395c40a045c`

## Build and packaging

- `.build/sh/ai-build`: exit 0 with JDK 21.0.12.1. Main and test Checkstyle
  passed. Log: `logs/20260911-221747-ai-build.log`.
- `.build/sh/ai-generate-metrics-reference --check`: exit 0. The reference and
  runtime catalog contain 126 table metrics and 101 keyspace metrics. Log:
  `logs/20260911-221950-405545000-generate-metrics-reference.log`.
- `.build/sh/ai-generate-metrics-reference --test`: 11 passed. Log:
  `logs/20260911-222140-993850000-generate-metrics-reference.log`.
- `.build/sh/ai-generate-metrics-reference --property`: one passed. Log:
  `logs/20260911-222205-828899000-generate-metrics-reference.log`.
- `build/apache-cassandra-7.0-SNAPSHOT.jar` contains
  `org/apache/cassandra/metrics/metrics-catalog.properties`.
- The binary artifact target copies the full `conf/` directory. Debian package
  input names `conf/all_metrics.yml` and `conf/simple_metrics.yml`.

The generator runner uses `venv/bin/python` when present. This host has no
project environment, so it used the existing `python3`. The build supplied
`lib/snakeyaml-2.4.jar`. No dependency was installed.

## Cassandra tests

All tests used `.build/sh/ai-ci-test` with JDK 21. Preserved XML files are under
`.agentlogs/reviews/test-results/`.

| Class | Tests | Result | Log |
| --- | ---: | --- | --- |
| `MetricProfileTest` | 14 | pass | `logs/20260911-223058-ai-ci-test.log` |
| `MetricProfilePropertyTest` | 2 | pass | `logs/20260911-223128-ai-ci-test.log` |
| `NoOpMetricsTest` | 7 | pass | `logs/20260911-223144-ai-ci-test.log` |
| `LatencyMetricsTest` | 4 | pass | `logs/20260911-223202-ai-ci-test.log` |
| `TableMetricBookkeepingTest` | 1 | pass | `logs/20260911-223219-ai-ci-test.log` |
| `TableMetricBookkeepingLifecycleTest` | 1 | pass | `logs/20260911-223236-ai-ci-test.log` |
| `MetricProfileRegistrationTest` | 5 | pass | `logs/20260911-223255-ai-ci-test.log` |
| `MetricProfileRecordingTest` | 3 | pass | `logs/20260911-223356-ai-ci-test.log` |

The eight classes passed 37 top-level tests with no failures, errors, or skips.
The focused JUnitCore launcher also passed four metric-profile property
executions and one compact-bookkeeping property execution. Two executions
repeat the top-level `MetricProfilePropertyTest`, so the branch has 40 unique
Cassandra tests and 42 Cassandra test executions. Logs:
`logs/20260911-223639-run_property_tests.log` and
`logs/20260911-223649-run_property_tests.log`.

Generator and Cassandra checks passed 52 unique tests in total.

## Source and independence checks

- All 22 complete owned production, resource, reference, and unit-test files
  match the final source commit byte for byte.
- `TableMetrics.java` has blob
  `c2bd1eb410a10ee5ef7948b50b5183210e0ab02c`, equal to the source blob. This
  confirms that the review-unit split restored the exact code built and tested.
- Both distributed test differences are the recorded standalone adaptations.
- Shared files contain only metric-selection and compact-bookkeeping hunks.
- The registry keeps legacy counter and meter constructors. The branch has no
  STORE, EXPORT, or MEM feature dependency.
- `git diff --check` and shell syntax checks passed.

## Measurement disposition

The generic 100-table harness completed once with exit 0. It provides a smoke
run only. Its node loads `IInstanceConfig` parameters and does not load the
profile from `PROFILE_CONFIG_DIR`. Its MBean delta also measures the outer JVM,
not the node registry. The summary is under
`logs/metric-selection-simple/20260911-224219-many-tables-100t/` and records two
missing-node-log warnings.

The second generic run was stopped with exit 130 after this control defect was
confirmed. Log: `logs/20260911-224536-many-tables-launch.log`. No child process
remained after the stop. Do not use either run for an all-versus-simple claim.
The integration `MemtableResidencyProfileHarness` owns the deferred matched
comparison because it sets and asserts the effective node profile.
