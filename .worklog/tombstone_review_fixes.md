# Tombstone Review Fixes

- Added queue admission and graceful shutdown fixes for reactive compaction.
- Added read-path, compaction, and settings tests for policy and lifecycle cases.
- Added operator configuration and documentation for the queue capacity.
- Fixed QuickTheories compilation: use withFixedSeed and call inherited generators from instance methods.
- A fresh queue regression exposed stale draining state when forced shutdown cancels a worker before it starts. Clear that state when no request is active.
- Cursor tests require a compatible partitioner as well as the BIG format. The byte-ordered fixture exercised iterator fallback; a dedicated Murmur3 fixture now exercises cursor execution.
- Added persisted-tombstone checks for memtable and repair-group overlap, plus stale-table and production-capacity tests.
- Save the table ID before dropping a table: the metadata reference can follow a replacement table. Cell-only deletion fixtures must omit row markers when asserting no live row remains.
- The test wrapper's --reuse option skips compilation. Use it only after compiling every current source and test change.
- Do not edit a shell wrapper while it runs: the shell can reread later portions of the modified file. One diagnostic invocation hit this after reporting the expected regression failure; subsequent runs used the stable wrapper.
- Fixed ai-build's string-as-boolean check, which skipped Checkstyle even when its flag was false. Test logs now record target names and exit statuses.

## Validation on 2026-09-05

- Configuration, StorageService, SettingsTable, PurgeFunction, queue, and ReadCommand classes passed in `logs/run_tests_20260905T154137Z.log`. That run then exposed the integration fixture issues described above.
- Corrected integration fixtures and production admission/task-reporting tests passed: `logs/run_tests_20260905T171604Z.log`. CompactionsTest reported 60 cases, zero failures, and five existing skips.
- Manual partition compaction passed: `logs/run_tests_20260905T172308Z.log`.
- Build and production/test Checkstyle passed after correcting SettingsTableTest import ordering: `logs/run_tests_20260905T172558Z.log`.
- `git diff --check` and `bash -n run_tests.sh` passed.
- Multi-node repair, production singleton shutdown in an isolated process, log rate-limit assertions, and performance benchmarks remain outside this focused test pass.
