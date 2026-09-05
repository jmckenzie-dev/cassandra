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

# Java histogram and meter allocation experiments

Keep metric values identical and retain the existing implementations as defaults.
Use Java 21 and existing dependencies. Run no workload above 1,000 tables.

1. Add a separate lazy tombstone histogram builder, selected at MetadataCollector
   construction. Preserve spool capacity, insertion order, merging, rounding,
   saturation, snapshots, serialization, and release behavior. Keep the current
   algorithm intact. Allocate the large spool only on the first observation.
2. Compare both builders with deterministic unit and generated event sequences.
   Exercise empty/live data, deletion and expiration metadata, repeated snapshots,
   spool draining, overflow, and release. Compare exact values and bytes.
3. Expose selection in the residency harness and effective configuration. Run
   matched N100 explicit-retirement allocation profiles in fresh JVMs. Report
   transient allocation separately from settled heap and first-observation cost.
4. Add a separate meter candidate with geometric rate-array growth. Retain the
   existing meter as default. Preserve offsets, cleanup, ticking and arithmetic.
   Compare exact counts and rate bits under controlled clocks, generated events,
   array growth, reuse, and concurrent registration/updates.
5. Profile CREATE TABLE with the meter candidate independently of the histogram
   change. Record measured allocation, timing and retained capacity tradeoffs.
6. Run targeted tests and the build/checkstyle wrapper. Update research records,
   reusable test commands and TODO.md. Do not commit.

Subagents own bounded production/test work; the root owns integration, harness
selection, running builds and measurements, and the final evidence report.
