<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Extend pager coverage

The passing cumulative report covers 393/576 pager lines and 229/368 branches.
Aggregation accounts for 68 missing lines, and paging-state parsing and compatibility
account for 55. The rest includes changed limits, exhaustion, range bounds, and guards.

1. Run existing paging-state and selected GROUP BY tests with JaCoCo. Distinguish
   missing measurements from missing tests.
2. Add small state round-trip properties and malformed-input cases. Cover optional
   fields, count boundaries, buffer views, and legacy row marks through public APIs.
3. Add real-storage pager tests for aggregation sub-pages, changed limits, range
   bounds, and exhaustion. Exercise enabled tombstone paging where supported.
4. Run focused tests, measure their runtime, and inspect the combined report. Add
   further cases only where they test a useful contract.
5. Record coverage gains, remaining gaps, and any valid failures. Keep the existing
   initial-empty-checkpoint GROUP BY regression visible as an unresolved defect.

Use finite datasets and bounded page traversal. Do not add sleeps, background test
threads, source injection, or a new cluster per small case. Do not force unreachable
assertions or memory-output failures merely to increase coverage.
