# Tombstone compaction coverage follow-up

Scope: feature commit 7f6e8e6cae and current worktree changes relative to d02415d0bd.

- Correct the QuickTheories compilation errors and rerun the focused classes.
- Exercise actual cursor support with a compatible table partitioner.
- Test queue identity across tables, executor rejection, shutdown before worker start, and generated admission/drain sequences.
- Test reactive compaction with older data in a memtable and in another repair group. Inspect persisted tombstones and verify that compaction does not flush the memtable.
- Test queued work across table drop/recreation, missing partitions, and subsequent successful work.
- Test two qualifying partitions in one range read.
- Test live capacity and task reporting through the production manager.
- Fix production defects only after a regression reproduces the failure.
- Record target names and return codes in test logs. Run all eight focused classes and the build/style wrapper before closing TODO.md.

No multi-node repair campaign or performance benchmark is included. The earlier plan explicitly deferred benchmark work. Existing queue unit tests cover graceful and forced shutdown; production singleton shutdown requires isolated process testing.
