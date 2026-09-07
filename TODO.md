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

- [x] Use the numeric tombstone paging threshold independently and default it to 100000. Document that changing the configured failure threshold does not change this default. The regression, strengthened property, and three-node fuzz and mixed-configuration tests pass: 78 unit and 35 distributed executions. See `.debug/paging-correctness-and-performance.md`.
- [x] Measure bounded paging performance in two saved runs. Clean reads use 6 replica reads/query, matching checkpoints 9, and reconciliation 12 in memory and after flush. Record latency distributions and their limits in `.debug/paging-correctness-and-performance.md`. All 120 selected test executions and targeted style checks pass.
- [x] Review the complete paging feature diff and fuzzer models. Fix silent truncation in unpaged local reads and bound all current-version fuzzer traversals. Unit, distributed, and both mixed-version upgrade stages pass; record the threshold configuration mismatch for follow-up.
- [x] Audit coordinator tombstone failure paths. Fix native ServerError responses for no-clustering ONE reads and speculative transient checkpoints; all 32 selected distributed tests pass. See `.debug/paging-correctness-and-performance.md`.
- [x] Add nine focused paging tests and strengthen the state property; fix static-cutoff error reporting. All 110 selected executions pass. Pager coverage is 96.37% lines and 93.12% branches; see `.debug/close-paging-coverage-gaps.md` for remaining guards.
- [x] Add mixed-version paging fuzzing after the deterministic upgrade combinations pass. Seeds 18424 and 1 pass with one and two upgraded nodes, generated replica divergence, reverse reads, and IN queries.
- [x] Extend mixed-version paging tests through two upgraded nodes and cover GROUP BY, static columns, TTL, and partition-key IN together with paging limits. All four methods pass in 209.825 seconds.
- [x] Run all three real mixed-version paging tests with 5.0.10 and the current branch; all pass. See `.debug/mixed-version-tombstone-paging.md`.
- [x] Fix GROUP BY checkpoint state, unfinished-group counting, result-page exhaustion, and IN partition transitions. All retained regressions and the new three-node combinations pass; see `.debug/fix-grouped-checkpoint-paging.md`.
- [x] Fix short-read refill exhaustion after a grouped tombstone checkpoint; the regression previously returned deleted `ck=1` and overcounted its group.
- [x] Fix static GROUP BY suppression with tombstone paging disabled; the complete existing `SelectGroupByTest` class passes.
- [x] Fix read-only heap buffer copying; re-enable the crash regression and restore heap read-only views to the paging-state property.
- [x] Extend pager coverage to 95.7% lines and 89.7% branches with deterministic state, aggregation, limit, boundary, and exhaustion tests. New passing methods take about 0.6 seconds; see `.debug/extended-pager-coverage.md` for evidence and remaining gaps.
- [x] Fix stale checkpoint reuse in multi-partition IN paging; validate direct cursors, LIMIT combinations, unfiltered reads, and three-node IN fuzzing. Evidence: [.debug/fix-in-paging-checkpoint-reuse.md](.debug/fix-in-paging-checkpoint-reuse.md).
- [x] Add and validate concurrent-write, speculation, timeout/retry, shutdown, TTL, collection, and transient-replica paging tests.
- [x] Add disabled controls and retain the valid failing IN and GROUP BY regressions; cover safe range-checkpoint rejection.
- [x] Replace empty mixed-configuration tests with 32 passing cases and implement three real upgrade tests; all three upgrade tests now pass.
- [x] Record coverage, findings, commands, and artifact limits in `.reviews/remaining-tombstone-paging-coverage.md`.
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
