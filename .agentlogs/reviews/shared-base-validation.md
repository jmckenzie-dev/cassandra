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

# Shared-base validation

JDK 21.0.12.1. Parent: 50a10d42a5d633150fbf4ae518b4da85466472b2.
Tested the extracted shared contracts and generic tools before committing.

Build and main/test Checkstyle passed through .build/sh/ai-build.
Log: logs/20260911-152611-ai-build.log.
Generated documentation and license auditing were skipped by the source wrapper.

| Test class | Passed | Skipped | Failures/errors |
| --- | ---: | ---: | ---: |
| ThreadLocalHistogramTest | 2 | 0 | 0 |
| DecayingEstimatedHistogramReservoirTest | 28 | 1 | 0 |
| ThreadLocalMeterTest | 6 | 0 | 0 |
| CassandraMetricsRegistryTest | 9 | 0 | 0 |
| LatencyMetricsTest | 3 | 0 | 0 |

All tests use org.apache.cassandra.metrics. The reservoir's existing
showHistogramOffsetOverflow diagnostic has a bare @Ignore annotation.
Preserved XML reports and the command-to-log map are in logs/base-validation/.
Every test wrapper exited zero.

Generic launcher smoke exited zero and reported success with these arguments:
--tables 3 --writes-per-table 1 --hold-seconds 0 --no-profile.
PROFILE_SKIP_BUILD=true used the verified build. All six phases completed.
Log: logs/20260911-173621-many-tables-launch.log.
Summary: logs/base-validation/profile-smoke/20260911-173621-many-tables-3t/summary.txt.

The distributed-test log API did not locate system.log. The launcher log retained
console output. Profiling was disabled for this smoke; this does not validate
async-profiler or Java Flight Recorder capture, or establish performance gains.
