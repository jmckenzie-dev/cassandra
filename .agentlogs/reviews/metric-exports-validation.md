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

# Metric export validation

Build: Java 21, ai-build exit 0. Main and test Checkstyle passed.
Log: logs/20260911-222929-ai-build.log. No production or test source changed
after this build. Later edits only changed selectors, documents, and records.

## Tests

Seven top-level classes passed 26 unique cases; the nested registration suite
passed two more. Total: 28 unique cases, zero failures, errors, or skips.
The registration class ran twice while adding the existing registry smoke test.

| Class | Passed |
| --- | ---: |
| AdaptiveHistogramHistoryTest | 7 |
| AdaptiveHistogramHistoryPropertyTest | 1 |
| AdaptiveJmxHistogramHistoryTest | 4 |
| CompactJmxRegistrationTest | 3 |
| ObjectNamePropertyPatternTest | 1 |
| TransientMBeanServerAuthorizationTest | 1 |
| CassandraMetricsRegistryTest | 9 |
| CompactJmxRegistrationTest$Properties | 2 |

Reports: logs/20260911-223140-run-tests-reports/ and
logs/20260911-223332-run-tests-reports/.
Nested suite: logs/20260911-223607-many-tables-launch.log, JUnitCore OK (2 tests).
The standard class wrapper does not run nested suites. run_property_tests.sh
now invokes this suite explicitly through the existing profiling launcher with
PROFILE_SKIP_BUILD=true.

## Standalone boundary programs

All commands use Java 21 and .build/sh/ai-probe-jmx-names.

| Arguments | Outcome | Log prefix |
| --- | --- | --- |
| --property-only | 16 seeds, 16000 name-copy cases pass | 20260911-223245 |
| --boundaries --properties | 16000 queries, 1600 lifecycle names, 1000 concurrent cycles pass | 20260911-223249 |
| --boundaries --late | Existing platform server unchanged; new server wrapped | 20260911-223252 |
| --boundaries 10 | Control local/remote queries, notifications, operations pass | 20260911-223308 |
| --boundaries --protected 10 | Protected local/remote queries, notifications, operations pass | 20260911-223311 |

The control retains 10 warmed names after notification inspection. The protected
server retains zero. Both preserve attributes and operation behavior. These are
correctness and object-graph observations, not isolated timing results.

## Limits

Isolated history, registration, and name measurements are pending.
Cross-feature storage/profile combinations return in integration.
No line or branch coverage percentage was computed for this extraction.
The build wrapper skips documentation generation and license auditing.
