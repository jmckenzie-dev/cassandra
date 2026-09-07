<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Paging correctness and performance

Review scope: the complete feature diff from `e4d83b3bb74c4c9be3c8e5e1119709d1aa585eb7`
to `2761428ed3`, followed by any fixes from this execution. Inspect unchanged
callers as needed. No dependency changes, wire-format changes, or full-suite runs.

1. Trace coordinator tombstone exceptions through native-protocol translation.
   Establish which DigestResolver and AbstractReadRepair guards are reachable.
   Write reproductions before fixes. A failed read must retain its typed failure
   and tombstone reason; it must not return partial results as complete.
2. Review continuation ownership, empty-page semantics, LIMIT and GROUP BY
   accounting, replica disagreement, and mixed-version fallback. Check generated
   mutation models against independent controls. Record findings with source
   evidence and distinguish confirmed defects from unverified concerns.
3. Run a bounded performance comparison of enabled/disabled no-tombstone reads,
   matching checkpoints, and full-data reconciliation. Record latency and replica
   read counts. Use existing dependencies and explicit limits; exclude setup from
   measured queries and state the limits of in-JVM timing evidence.

Tests must permit empty pages with continuation, bound traversal and wall time,
and avoid sleeps or bytecode injection. Run builds and Ant test selections
sequentially. Preserve regression tests and record commands and results.
Move each TODO item to DONE when its implementation and validation finish.
