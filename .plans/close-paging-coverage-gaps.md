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

# Close paging coverage gaps

Use the feature diff from `0c202df78b^` through the current working tree.
The fresh baseline covers 554/579 pager lines and 340/378 pager branches.
Changed executable lines cover 275/306 lines and 234/268 branches.

1. Exercise a static-column tombstone cutoff before a regular clustering cursor
   exists. Establish replica and coordinator behavior with real reads, finite
   page traversal, explicit timestamps, and disabled controls. Keep any valid
   failing regression and document its reproduction.
   The native-protocol regression exposed an uncaught coordinator exception.
   Report the failed per-replica short read as `ReadFailureException` with reason
   `READ_TOO_MANY_TOMBSTONES`, then rerun the regression and related paging tests.
2. Cover aggregation empty-page iteration and range continuation without a row
   mark where public APIs admit those states. Do not construct invalid internal
   state merely to reach a branch.
3. Add focused continuation rejection, paging-state equality/hash, and management
   getter/setter tests. Check checkpoint fallback guards for production reachability.
4. Build once after edits and run affected tests sequentially with JaCoCo. Include
   fixed-seed properties. Avoid the unrelated two-minute digest stress method and
   the eight-minute speculative-read stress test in the focused selection.
5. Review assertions and cleanup, run targeted style checks, merge compatible
   coverage data, and record added tests, runtime, uncovered guards, and findings.

Production scope: `ReadCommand`, `TombstonePageContinuation`, `ShortReadRowsProtection`,
`DigestResolver`, `ReadCallback`, the pager package, and the tombstone-paging accessors
in `StorageService`. Preserve all existing changes. Use existing fixtures and test
entrypoints. Do not add sleeps, background races, bytecode injection, or dependencies.
Ant test invocations must not overlap because they clean shared test directories.
