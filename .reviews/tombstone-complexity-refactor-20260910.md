# Tombstone method refactoring

The options and selected tradeoffs are in
[the implementation plan](../.plans/reduce-tombstone-method-complexity.md).

## Changes

- Extract synchronized request selection and BUSY requeue/backoff from `drain`.
  Keep the existing failure, cancellation, cooldown, and fatal-error handling.
- Extract cell-tombstone scanning from the retention test. Keep both older-data
  placements and all safety assertions, and close scanners/partitions on early return.
- Use `Util.consume` in the purge-count test. Keep both directions and all four
  eligibility combinations, with the exact expected count for each combination.
- Extend the root test runner with the missing changed unit class, batched test
  selection, incremental compilation, `--compaction`, and `--long`. Save each
  group's full helper log and JUnit XML before the next group clears build output.

## PMD 7.27.0 results

| Method | Before | After |
|---|---:|---:|
| TombstoneTriggeredCompactionManager.drain | 24 | 14 |
| CompactionsTest.testReactiveCompactionRetainsTombstoneOverOlderData | 23 | 3 |
| PurgeFunctionTest.testPurgeableRangeEvaluationCounts | 19 | 12 |

New helpers: `nextRequest` = 2, `retryBusy` = 3,
`hasCellTombstone(Iterable)` = 6, `hasCellTombstone(UnfilteredRowIterator)` = 8.
PMD scanned working-tree sources with `reportLevel=1` and no processing errors.
Raw report: `tmp/pmd-branch/refactored.json`.

## Validation

The incremental Java 21 build passed. The complete compaction and branch run
used `bash run_tests.sh --compaction --long --reuse` after that build.

- 88 unit test classes: 980 reported cases, including 10 skipped cases.
- One shutdown integration class: all three tests passed, on both runs.
- Queue-manager tests: all 19 passed, including 1,000 generated admission cases.
- Retention test class: 65 reported cases, five skipped, no failures or errors.
- Every unit and distributed test class changed on the branch was present.
- Production and test Checkstyle passed, with no errors in either XML report.
- Shell syntax, help, invalid flags, conflicting selections, and Git whitespace checks passed.

The broad unit run initially had four failures in the extra nodetool smoke test:
the subprocess loaded local Caffeine 3.1.8 instead of the build's resolved 3.2.4.
The runner now defaults to `.build/sh/cassandra-test.in.sh`, which sources the
stock configuration and puts resolved build libraries first. An explicit
`CASSANDRA_INCLUDE` still takes precedence. No dependency or library file changed.
Re-running `bash run_tests.sh org.apache.cassandra.tools.nodetool.CompactTest --reuse`
passed all four tests. No Java source changed after the broad test run.

Using the successful nodetool rerun for that class, the final unique-case total
is **973 passed, 10 skipped, zero failures, zero errors across 89 classes**.

Evidence:

- Build: `logs/run_tests_20260910T140835974556307Z.log`.
- Broad run: `logs/run_tests_20260910T141058367200870Z.log`.
- Broad unit/integration XML and helper logs: `logs/run_tests_20260910T141058367200870Z/`.
- Nodetool rerun: `logs/run_tests_20260910T143528822533922Z.log` and its matching directory.
- Final merged result summary: `logs/pmd-20260910-103611-259981.log`.
- Checkstyle: `logs/pmd-20260910-103609-867136.log` and `build/checkstyle/checkstyle_report{,_test}.xml`.

For future complete runs, use `bash run_tests.sh --compaction --long` with JDK 21.
The default `bash run_tests.sh` is the cheaper branch-unit selection.
Add `--long` to that default selection to include shutdown integration tests.

## Setup corrections

The initial Java 17 attempt failed against cached Java 21 classes. Java 21
resolved that mismatch. The initial unit selection used extended-regexp syntax;
the helper uses basic regexps. The runner now supplies its supported comma-separated
patterns. Both failures returned nonzero and were not counted as passing tests.

Editing the runner during the first integration invocation disrupted the shell's
remaining input after the tests passed. The complete run uses the corrected,
syntax-checked script without edits during execution.
