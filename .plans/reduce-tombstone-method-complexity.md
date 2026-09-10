# Reduce tombstone method complexity

## Scope

Refactor the three added methods with PMD cognitive complexity of at least 15.
Preserve behavior and test cases. Do not restructure classes.

## Options and decisions

### TombstoneTriggeredCompactionManager.drain (24)

1. Extract synchronized request selection and BUSY requeue/backoff operations.
   This names the queue transitions and keeps the existing exception handling
   in the drain loop. It adds two private methods without new state.
2. Move task execution and exception handling into a result-returning helper.
   This shortens the loop, but introduces another result contract for failure,
   interruption, and retry. Fatal exception propagation becomes harder to check.
3. Replace the loop with scheduled retries or an explicit state machine.
   This changes executor lifecycle and shutdown behavior beyond this refactor.

Choose option 1. Keep queue changes under the same monitor and sleep outside
the monitor. Preserve BUSY rotation, consecutive-BUSY reset, graceful shutdown,
cooldown ordering, direct/wrapped interruption, and fatal failure cleanup.

### CompactionsTest.testReactiveCompactionRetainsTombstoneOverOlderData (23)

1. Extract cell-tombstone scanning into helpers for SSTables and partitions.
   The test retains its setup, both older-data placements, and safety assertions.
   Each helper owns a clear resource/iteration boundary.
2. Split memtable and SSTable cases into separate tests with shared setup.
   This improves case naming but adds fixture plumbing to an already parameterized
   test class. It does not remove the scanner nesting by itself.
3. Replace nested loops with streams or materialize partitions.
   This makes resource closure less clear or changes memory use in the test.

Choose option 1. Preserve try-with-resources for scanners and partitions.
The test must still check tombstone retention, replacement of the original
SSTable, unchanged memtable identity, and an empty user-visible read.

### PurgeFunctionTest.testPurgeableRangeEvaluationCounts (19)

1. Replace manual iterator draining with the existing Util.consume helper.
   This removes repeated iteration/closure logic while retaining the matrix.
2. Extract one assertion helper per direction and eligibility combination.
   This shortens the test but adds a helper where an existing utility suffices.
3. Convert the class to parameterized tests.
   This gives separate case names but affects unrelated tests and fixtures.

Choose option 1. Keep both scan directions and all four eligibility combinations.
The exact callback-count assertion stays in the test.

## Test runner

Extend the existing root run_tests.sh. Keep single-class and --build usage.
Build incrementally and compile tests before running them. The default branch
selection must include all eight changed unit test classes plus the existing
nodetool compaction smoke test. Add --compaction for all compaction-package unit
tests and --long for the added shutdown integration class. Build once per run,
batch unit classes in one helper invocation, preserve logs and JUnit results
before the next helper invocation replaces them, and preserve command failures.
Keep run_property_tests.sh as the queue property-test entry point.

## Validation

- Re-run PMD on the changed working-tree files. Check helpers as well as targets.
- Run the updated root runner with --compaction --long.
- Include the existing generated queue-admission test and interruption/failure tests.
- Run production and test Checkstyle.
- Check shell syntax, argument errors, and documented runner selections.
- Record results and move the task to DONE only after validation completes.

## Outcome

Implemented the selected options. PMD scores are now 14, 3, and 12; all new
helpers score below 15. Java 21 compilation and both Checkstyle targets passed.
All compaction-package and changed branch test classes ran. After correcting
the nodetool subprocess classpath and rerunning its four tests, the final results
are 973 passed and 10 skipped. See
[the validation report](../.reviews/tombstone-complexity-refactor-20260910.md).
