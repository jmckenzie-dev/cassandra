<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Heap ownership census

Authorized 2026-09-06. Measure the current compact-metrics branch before choosing
another residency optimization. Do not change production behavior.

1. Add a test harness using the existing profiled cluster runner. Create 100,
   500, and 1,000 tables in separate JVMs, with lazy TrieMemtables, compact metrics,
   Java 21, eight processors and a fixed 8 GiB heap limit.
2. Capture post-GC live dumps and class histograms at baseline, table creation,
   full readable metrics scrape, one recording worker, eight recording workers,
   and a repeat scrape. Keep the worker population alive throughout. Synthetic
   fan-out records real metric objects without creating user SSTables; distinguish
   this storage probe from end-to-end request traffic.
3. Smoke-test the harness before full runs. Validate table counts, effective
   configuration, metric updates, worker identities, and scrape completion.
4. Use direct HPROF reference ownership for metric arrays, names, wrappers,
   counters and snapshots. Report deduplicated shallow sizes and payload separately
   from retained size. Check size assumptions against JVM class histograms.
5. Use the existing Eclipse Memory Analyzer installation for retained-size
   dominators on representative dumps. Do not install dependencies.
6. Compare checkpoint deltas within each JVM and slopes across table counts.
   Identify fixed costs, table scaling and worker fan-out costs. Document benchmark
   limits, artifacts, reproduction commands and ranked optimization candidates.

The first measurements showed that natural ObjectName ordering and key-property
lookup warm a persistent JDK property cache. Add fresh 100/1,000-table
`--attributes-only` controls using canonical strings, with the same readable
attribute coverage. Report this client-dependent cost separately from histogram
history. Preserve the original full-scrape runs as measured evidence.

Existing modified documentation and the untracked reference checkout belong to
the user session. Preserve them. No commit is required for this measurement task.
