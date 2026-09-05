# SRP tombstone paging TDD

## Pre-fix failures

- `remoteZeroLiveRefillContinuesAfterTombstoneCheckpoint` showed that a
  tombstone-limited remote response looked exhausted to short read protection.
  See `logs/run_tests_20260826T121442Z.log`.
- `fullDataShortReadDoesNotRewindOuterPager` exposed stale checkpoint reuse
  during full-data reconciliation. See
  `logs/run_tests_20260826T152516Z.log`.
- `checkpointedRepairAtConsistencyOneFailsSynchronously` and
  `fullDataRepairTransfersCheckpointBeforeLateLocalRead` exposed unsafe repair
  handling and late local checkpoint ownership. See
  `logs/run_tests_20260826T171625Z.log`.

## Fix shape

- Carry the tombstone checkpoint count in read-response message parameters.
- Continue supported finite single-partition reads through short read
  protection and reject unsupported paths.
- Transfer checkpoint ownership before full-data read repair and keep late
  local reads on command copies.

## Successful targeted test logs

- `logs/run_tests_20260826T172255Z.log`
- `logs/run_tests_20260826T173146Z.log`
- `logs/run_tests_20260826T174215Z.log`
- `logs/run_tests_20260826T174426Z.log`
- `logs/run_tests_20260826T174437Z.log`

The final build completed in `logs/run_tests_20260826T174505Z.log`.
