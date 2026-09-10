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

# Tombstone review reproduction results

Later test work: [test follow-ups](tombstone-test-followups.md) closes the
remaining FUL-006, FUL-023, FUL-024, and FUL-036 test items listed below.
Subsequent [ownership/error tests](tombstone-checkpoint-ownership-reproductions.md)
reproduce and fix FUL-019's native ServerError and test FUL-011's pager claims.

Review: `.reviews/review-full-tombstone-paging-branch-2026-09-08.md`.
Mechanical cleanup commit: `081999b31b`. The tests below first ran against
that commit's production behavior unless stated otherwise.

## Reproduced defects

- FUL-003: `QueryPagerTest.successfulCheckpointIsNotATombstoneFailure`
  failed because the failure counter advanced on a successful empty page.
  Checkpoints now increment `TombstonePages`; actual replica aborts increment
  `TombstoneFailures`. Non-aborted reads above the warning threshold increment
  `TombstoneWarnings`. Existing checkpoint assertions use the new counter.
- FUL-004: `QueryPagerTest.localRangePagerRejectsTombstoneCheckpoint` failed
  because local range execution returned without an exception after stopping
  on an unsupported checkpoint. Local range pagers now disable checkpoints.
  The retained test checks filtered and unfiltered execution.
- FUL-005: A marker-only partition with 20 markers completed despite crossing
  both configured thresholds. The retained regression now uses 2,200 markers
  to exercise the fixed allowance. A marker cannot supply a row checkpoint,
  so execution allows at most 1,024 additional tombstones past the paging
  threshold before a marker causes an abort. This fixed allowance keeps
  ordinary short marker runs working and bounds pathological scans. It adds
  at most about 1% to the default 100,000 cutoff. Deferred markers no longer
  format queries, increment failure counters, or log once per marker.
- FUL-013: `TombstonePageContinuationTest.invalidComponentCountsAreReadErrors`
  first failed with `IllegalArgumentException` for a zero count. The old code
  also allocated its array directly from the unsigned wire count. Decoding
  now rejects nonpositive Java counts with `IOException` and allocates as
  components arrive. After the fix, a count of `Integer.MAX_VALUE` with no
  payload returns a read error without attempting the giant allocation.
  The existing 100-example serialization property still covers nullable,
  empty, and populated clustering components. No table metadata is available
  in this serializer, so the review's proposed table-specific cap is not used.

## Claims not supported by reproduction

- FUL-001: `PagingAcrossTombstonesDistributedTest.emptyMismatchedCheckpointsRefillToCompletion`
  passed before repair changes. Every replica has twelve row tombstones, with
  different deletion timestamps, followed by three live rows. A bounded native
  read uses fetch size 2 and LIMIT 4. It asserts all three rows and a repair
  metric increase for NONE/BLOCKING, before and after flush. The flushed case
  reinstates divergence because blocking repair reconciles the first read.
  `DataResolver.shortReadProtectedResponse` extends checkpointed responses
  through short-read protection. The outer pager need not retain their cursor
  once the refill supplies complete data. No repair cursor patch was made.
- FUL-002: `ShortReadProtectionTombstonePagingTest.markerOnlyRefillTerminates`
  passed before fixes in memory and after flush, with a positive refill count
  bounded by six requests. The refill contains only range markers after two
  initial rows are deleted by another replica. Replica execution does not
  checkpoint on a marker. A zero-live-row checkpoint can contain a deleted
  Row, and `ShortReadRowsProtection.applyToRow` advances on that Row. The
  review conflated zero live rows with zero unfiltered Rows. No loop guard was
  added for a checkpoint shape that production does not emit.
- FUL-010: `QueryPagerTest.smallPagingThresholdsMakeProgress` verifies -2, 0,
  1, and 2 in both clustering orders, with an independent expected row set and
  a seven-page budget. Small settings cause small pages, not an infinite loop
  in these cases. Rejecting these settings is a configuration policy change,
  not a demonstrated correctness fix.
- FUL-012: CL ONE already has local and remote coordinator regression coverage
  in `PagingAcrossTombstonesDistributedTest`. The plan's blanket exclusion was
  stale; it now distinguishes ordinary paging from full-data reconciliation.
- FUL-026: `DigestResolverTest.tombstonePageCheckpointAcceptedByShortReadCapableDataResolver`
  already constructs an ACCEPT callback directly. Its retained-message check
  now also asserts that the original `TOMBSTONE_FAIL` parameter survives.

Passing tests refute the specific proposed reproductions, not every possible
failure involving these classes.

## Other review dispositions

- FUL-006: Confirmed empty test; explicitly ignored with a reason. This adds
  no coverage for its asymmetric mixed-version scenario.
- FUL-007, FUL-027, FUL-028, FUL-041: Historical commit-message suggestions.
  Existing history is preserved; the requested new commits describe their
  final changes and rationale.
- FUL-008, FUL-018, FUL-029–033, FUL-038–040: Mechanical cleanup committed.
- FUL-009: Release note now states the supported paged-read behavior.
- FUL-011: The proposed local-copy asymmetry has no reproduced user-visible
  failure here. Callback response metadata still carries the executed command's
  checkpoint. No ownership change was made on this speculative premise.
- FUL-014: Boolean-sounding accessor names remain. Their threshold contract
  is now documented. Renaming an operational interface is a separate API choice.
- FUL-015, FUL-016: State consolidation and reciprocal predicate documentation
  are design suggestions, not executable defect claims. No shared helper or
  state refactor was introduced.
- FUL-017: Confirmed no production users of USE_LOCAL_COMMAND. Removed it and
  migrated its tests to ACCEPT/REJECT; retained the existing wire failure flag.
- FUL-019: The existing direct repair unit test deliberately expects the local
  `TombstoneOverwhelmingException`. No native-client reproduction of a wrong
  error type was established; no exception-contract change was made.
- FUL-020: The extra local command copy is present. No allocation profile was
  collected and no copy guard was changed. Decision on 2026-09-10: decline the
  proposed optimization for this patch. Preserve command isolation and revisit
  the allocation cost during final end-to-end performance testing.
- FUL-021, FUL-022, FUL-034: Current validation is recorded below. Old run counts
  and coverage percentages are not treated as evidence for these new changes.
- FUL-023: The three older copied upgrade tests remain. They are not claimed as
  new tombstone-paging coverage in this report.
- FUL-024: The older validator loops remain a test-harness follow-up. New client
  regressions use bounded traversal or a timeout and request budget.
- FUL-025: The full paging suite reproduced a missing warning, caused by a
  stale fixture rather than the proposed logger race. The test changed only
  the failure threshold; paging uses its independent threshold. Set the paging
  threshold explicitly in this test, the collection test, and the shared
  validator. Added a preparation-contract assertion. The exact warning count
  remains; no logger-state manipulation or weaker assertion was introduced.
- FUL-035: The full run located six assumption-based skips in
  `PagingAcrossTombstonesTest`: the collection and warning tests each skip
  SYNC_FORWARD, SYNC_REVERSE, and ASYNC_REVERSE. The review's search for
  `@Ignore` missed these. The plan now labels old totals as historical claims.
- FUL-036: Corrected the replay property name in the cleanup commit. The existing
  logger sleep remains outside the regression fixes.
- FUL-037: Documented the harness's existing jamm reference sharing in the plan.

## Validation

Commands run through `distrobox enter dev -- bash tmp/paging-coverage-run.sh`.
That existing wrapper selects Java 17, writes a dated log in `logs/`, and
preserves the command's exit status. Ant invocations run sequentially.

Initial `bash run_tests.sh build -s` failed in the test-name checker because
stale test classes referenced the removed Accord `PreLoadContext`. Moved
`build/test/classes` to `tmp/test-classes-before-review-rebuild`, then rebuilt.
That backup remains recoverable. A new test also needed its concrete range-pager
type to call the unfiltered API; corrected before running reproductions.

Before-fix evidence:

- `build/review-before-unit`: all three defect regressions failed.
- `build/review-before-repair-corrected`: one method passed across four cases.
- `build/review-before-refill`: one method passed in memory and after flush.
- `build/review-before-decoder`: malformed-count regression failed.

The first marker fix aborted at the larger configured threshold. Fuzzing
rejected it (318 failed queries), so it was replaced by the fixed allowance.
The unchanged three-method fuzzer then passed with seed 18424 in 93.278 seconds:
`build/review-after-fuzz-bounded`.

The final current-version selection contains 256 test cases: 250 passed and
six skipped by the existing parameter assumptions described above. There are
no failures or errors. Reports are in `build/review-final-tests`:

| Class | Cases | Skipped |
| --- | ---: | ---: |
| QueryPagerTest | 26 | 0 |
| TombstonePageContinuationTest | 3 | 0 |
| DigestResolverTest | 14 | 0 |
| BlockingReadRepairTest | 11 | 0 |
| AbstractPagingValidatorTest | 23 | 0 |
| PagingAcrossTombstonesTest | 104 | 6 |
| PagingAcrossTombstonesDistributedTest | 14 | 0 |
| ShortReadProtectionTombstonePagingTest | 11 | 0 |
| PagingAcrossTombstonesInteractionTest | 16 | 0 |
| PagingAcrossTombstonesTransientTest | 2 | 0 |
| MixedConfigPagingAcrossTombstonesTest | 32 | 0 |

Each class ran with `bash run_tests.sh test <fully-qualified-class>
-Dno-build-test=true -DmaxMemory=4G
-Dbuild.test.output.dir=build/review-final-tests`. A failed initial warning
test was corrected and its entire class rerun; the table records the final run.

`git diff --check` passes. Production Checkstyle passes. Test Checkstyle
reports 25 existing violations in the old paging/upgrade helpers and
`RandomHelpers` (imports, raw property/environment access, and malformed
Javadoc). None points at a line introduced by this change. The full
`.build/check-code.sh -s` stops at four existing license failures:
`.aftignore`, `.build/git/git-hooks/post-switch`, `.build/run-ci.d/run_ci.py`,
and `.debug/.placeholder`. These files are tracked and unchanged. No checks
or suppressions were changed to hide these failures.

`ant dtest-jar` rebuilt `build/dtest-7.0.jar` successfully for the real upgrade
tests. All four methods in `MixedVersionPagingAcrossTombstonesTest` passed in
210.188 seconds with no skips, using 5.0.10 and the rebuilt branch JAR. The
suite covers one and two upgraded nodes. Its invocation was:

```text
bash run_tests.sh test org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=480000 -Dbuild.test.output.dir=build/review-final-upgrade
```

The Checkstyle XML compared against the pre-cleanup baseline confirms all 25
test violations are on unchanged lines (`tmp/review-style-baseline.py`, exit 0).

Coverage used only execution data collected after the final production build,
under `build/paging-coverage/review-final`. The selected coverage run passed
60 test cases with no skips. Two focused follow-up executions also passed:
the marker-abort test now asserts the exact scan count (1,027), one failure
counter increment, the wire `TOMBSTONE_FAIL` value, and absence of a checkpoint;
a new normal-response callback test asserts the returned data and no failure flag.
Only test code changed after the production build.

The report covers all 34 added executable production lines and 34 of 36
branches on those lines, compared with the cleanup commit. The two partial
conditions are in `ReadCommand.countTombstone`; this is not a claim of complete
branch coverage. `TombstonePageContinuation` has 24/24 lines and 16/16 branches
covered. The summary comes from the existing coverage-summary script, adjusted
to this report and the five changed production files, without importing any
historical execution data.

```text
ant -f tmp/review-coverage.xml selected jacoco-report -Dno-build-test=true -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/review-final -Dbuild.test.output.dir=build/paging-coverage/review-final/tests
ant -f tmp/review-coverage.xml follow-up jacoco-report -Dno-build-test=true -Dusejacoco=yes -Djacoco.export.dir=build/paging-coverage/review-final -Dbuild.test.output.dir=build/paging-coverage/review-final/tests
bash tmp/review-coverage-summary.sh
```

The final result is four reproduced safety/observability/parsing defects fixed,
plus test and documentation corrections. Specialized ownership/error-path
claims and the remaining performance/design suggestions are not declared fixed.
The report above records those limits explicitly.
