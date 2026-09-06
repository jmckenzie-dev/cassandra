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

# TODO

# DONE

- [x] Implement digest and continuation agreement with full-data fallback; fix static-only output at empty checkpoints and exhaustion.
- [x] Add tests for matching, mismatching, legacy, remote-coordinator, static, and read-repair continuation paths.
- [x] Verify the reproductions, distributed fuzzer with two seeds, related regressions, coverage, and targeted style checks.
- [x] Record the implementation and validation results in `.debug/compare-tombstone-page-continuations.md`.
- [x] Map the distributed fuzzer's expected results to Cassandra mutation and read semantics.
- [x] Trace the fuzzer's production read path and check its assertions against plausible defects.
- [x] Check model accuracy, replica state, determinism, and false-failure risks with independent controls; seeds 18424, 1, and 8675309 pass.
- [x] Reduce a failing read to a deterministic distributed regression and identify initial digest checkpoint rejection.
- [x] Record confirmed findings, reproduction commands, and remaining assumptions in `.debug/paging-initial-checkpoint.md`.
- [x] Complete the paging coverage audit and record observed gaps in `.debug/paging-coverage-findings.md`; preserve failing distributed regressions for implementation follow-up.
- [x] Fix the checkpoint property's QuickTheories calls.
- [x] Exercise and assert synchronous reverse paging.
- [x] Run existing distributed paging tests with the feature enabled and disabled.
- [x] Add and execute a bounded native-protocol three-node fuzzer; retain the failing cases and reproduction evidence in `.debug/paging-coverage-findings.md`.
- [x] Restore the test entrypoint and add isolated JaCoCo reports and a property-test entrypoint.
