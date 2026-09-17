# Virtual-table threshold read failure

`VirtualTableTest` reproduced three failures on the branch before the fix:

- `testReadOperationsOnReadOnlyTable`
- `testDMLOperationsOnMutableCompositeTable`
- `testInsertRowWithoutRegularColumnsOperationOnMutableTable`

Each failure reported `Unknown keyspace test_virtual_ks`. The stack entered
`ReadCommand.executeLocally(controller, threshold)` through
`SinglePartitionReadQuery.Group.executeLocally`, then tried
`Keyspace.openAndGetStore`.

Both virtual command implementations override only the one-argument local-read
method. The threshold-aware overload was added to the read API, but these
implementations did not add the matching override. The same mismatch also made
old verb-handler test doubles bypass their fake read implementation.

Reproduction logs:

- `logs/run_tests_20260910T191324Z-3554743.log`
- `logs/run_tests_20260910T191413Z-3556286.log`

## Fix and verification

Both virtual command types now override the threshold-aware method and delegate
to their existing virtual selection path. This keeps virtual-table filtering
and limits unchanged. Virtual tables do not read storage tombstones, so the
storage tombstone threshold has no work to perform in this path.

- Java 17 JAR build passed: `logs/run_tests_20260910T191841Z-3571648.log`.
- `VirtualTableTest`: 15 passed, no failures or skips:
  `logs/run_tests_20260910T191900Z-3573480.log`.
- Verb-handler tests: 11 passed, no failures or skips. Result directory:
  `build/complexity-loop-20260910/virtual-fix-verb`.
- `RandomizedPagingStateTest`: 3 passed, no failures or skips:
  `logs/run_tests_20260910T191940Z-3575474.log`.
