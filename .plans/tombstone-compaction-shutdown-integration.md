# Tombstone compaction shutdown integration

## Purpose

Test the connection between the production compaction manager, its dedicated
reactive executor, and shutdown. Existing queue tests cover lifecycle rules in
isolation. This work should exercise real SSTable tasks without repeating the
tombstone-purging test matrix.

Baseline: `c1c6e79ecb`, following the JaCoCo run on 2026-09-06.

## Shutdown paths

Node drain is orderly at the service level, but it interrupts compactions.
`StorageService.drain` calls `CompactionManager.forceShutdown()`. It does not call
`finishCompactionsAndShutdown()`. Standalone verifier, upgrader, splitter, and
scrubber tools call the latter method.

Sources:

- `src/java/org/apache/cassandra/service/StorageService.java`: drain sequence.
- `src/java/org/apache/cassandra/db/compaction/CompactionManager.java`:
  `forceShutdown` and `finishCompactionsAndShutdown`.
- `test/unit/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManagerTest.java`:
  existing isolated shutdown tests.

The two uncovered graceful-shutdown calls in the coverage report belong to the
standalone-tool helper. They are not evidence that the node-drain calls were
unexecuted. Node-drain assertions still need to cover active and queued reactive
work explicitly.

## Test setup

- Use a disposable test node or forked test process. Shutting down the production
  singleton must not disable compaction for subsequent tests.
- Disable automatic compaction on the fixture table. Flush separate SSTables for
  distinct keys A, B, and C; avoid recent cooldown entries.
- Submit through the production `CompactionManager` API. Hold A after it acquires
  its real SSTable reservation, then queue B on the dedicated executor.
- Use latches and bounded waits. Reuse existing lifecycle test hooks where
  available. If a hook is necessary, use a narrow injectable collaborator; do not
  replace the compaction task with a fake runner or patch bytecode.
- Record reservations and task state before shutdown. Always release test gates
  in cleanup so a failed assertion cannot strand the test process.

## Case 1: Node drain

1. Start the normal drain operation with A active and B queued.
2. Observe reactive shutdown admission closing. Submit C and verify rejection.
3. Verify that A receives a stop or interruption request and B never executes.
   The test gate must permit interruption rather than hide it.
4. Verify bounded drain completion, termination of the dedicated executor, zero
   outstanding reactive requests, and release of A's reservations.
5. Check that cancellation leaves a valid SSTable set. If drain has closed the
   required storage services, reopen the fixture in a fresh process before
   checking data; do not assume reads remain supported on a drained node.

Do not require accepted requests to finish during node drain. Dropping queued
work and interrupting active compaction are the existing shutdown policy.

## Case 2: Graceful compaction-manager helper

Use a separate disposable instance and call the production
`finishCompactionsAndShutdown` method directly.

1. Start shutdown with A active and B queued.
2. Verify rejection of C and that shutdown has not returned while A is held.
3. Release A. Verify that A and B finish, reservations are released, and the
   dedicated executor terminates before the shutdown call returns.
4. In a separate BUSY case, hold a candidate SSTable with a real lifecycle
   transaction. Verify that shutdown drops the retry and terminates without
   cancelling that external owner or waiting for it to release its reservation.

Release work before the supplied shutdown timeout. This plan does not change the
helper's timeout contract or node-drain policy.

## Validation and completion

- Run only the new integration class and the existing queue-manager class through
  the repository test helpers. Repeat the integration class to check ordering.
- Run JaCoCo and confirm that both graceful-helper calls execute. Assertions must
  establish waiting, rejection, cleanup, and termination; line coverage alone is
  insufficient.
- Keep stdout, stderr, test results, and exit statuses in timestamped logs.
- Make a production change only if a failing regression demonstrates a defect.
- Move the TODO entry to DONE after the focused tests pass.

Multi-node repair, load testing, and compaction failure injection are separate
follow-ups. Do not add them to this task.

## Implementation

`test/distributed/org/apache/cassandra/distributed/test/TombstoneCompactionShutdownTest.java`
implements the three cases on separate single-node clusters with full startup.
The drain case restarts its node before checking live values and deleted cells.
The tests use the production singleton, executor, compaction tasks, and lifecycle
transactions. Production code is unchanged.

A table-configured test strategy decorates real SSTable scanners. The gate waits
at the first scanner iteration, outside the strategy manager's read lock. Holding
scanner creation instead would block drain's earlier `disableAutoCompaction`
step, before drain could interrupt the worker. The gate releases in cleanup and
checks interruption explicitly.

The graceful test detects closed admission through a rejected empty user-defined
compaction request. The helper closes that executor after it closes reactive
admission. This avoids adding a production accessor for private queue state.
Assertions check waiting, unchanged late-request SSTables, completed accepted
work, empty task state, released reservations, and worker exit.

## Validation results: 2026-09-07

Scope: the tombstone-compaction shutdown changes in `origin/trunk..HEAD`, with
`origin/trunk` at `51c5071b6b` and `HEAD` at `c1c6e79ecb`. The new test and this
write-up were uncommitted during validation.

Command: `bash -ic 'set_jdk 17 && bash tmp/run_shutdown_tests.sh'`.
The local logging script uses the repository's `run_ant` helper and test wrapper:

```bash
run_ant build-test
.build/run-tests.sh -s -a jvm-dtest -t '^org/apache/cassandra/distributed/test/TombstoneCompactionShutdownTest.java$'
.build/run-tests.sh -s -a test -t '^org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManagerTest.java$'
run_ant checkstyle-test
run_ant jacoco-report
```

Both final runs exited 0. Each passed all three integration tests and all 17
queue-manager tests, including the existing seeded queue-model checks. Test
compilation and test Checkstyle passed. JaCoCo marks both previously uncovered
reactive calls in `finishCompactionsAndShutdown` as covered: lines 381 and 384.
The report also covers the forced-shutdown call at line 347. These are focused
runs, not a rerun of the full compaction suite or a merged branch coverage report.
The coverage agent excludes the generated CQL parser, which exceeds the method
size limit when instrumented; no changed production class is excluded.

Local evidence:

- `logs/run_shutdown_tests_20260907T040025Z.log`: first passing run.
- `logs/run_shutdown_tests_20260907T040226Z.log`: repeat passing run.
- `tmp/shutdown-coverage-20260907T040226Z/index.html`: coverage report.
- The same coverage directory holds `integration.log`, `queue-manager.log`,
  test result XML files, and the integration node logs.

Earlier invocations of the same command found only fixture defects. The runs at
035400, 035428, and 035502 exited 1 during compilation. The 035533 run exited 1
after two tests exposed a partition-key read that advanced the shared buffer.
The 035716 run had one failing drain assertion because the gate held the strategy
lock. That wrapper exited 2 after an edit to the running local script caused a
shell parse error. The final runs used the corrected script without edits during
execution. No test assertion required a production repair.

All cases in this plan are complete. Multi-node and load tests remain outside
this scope.
