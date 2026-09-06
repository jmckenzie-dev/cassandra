# Tombstone compaction cooldown

Scope: worktree changes relative to 0b3774a7d1 in TombstoneTriggeredCompactionManager, its tests, and operator documentation.

- Keep recent completed attempts in a Guava cache keyed by the existing Request identity.
- Expire entries 60 seconds after completion or failure. Limit the cache to 1,024 entries independently of queue capacity.
- Use Cassandra's monotonic clock through an injectable Guava Ticker. Inject a smaller cache limit for eviction tests.
- Check cooldown after active/pending deduplication and before capacity rejection. Return COOLDOWN without refreshing expiry.
- Record completion and release active identity under the existing lock. BUSY retries retain current queue behavior.
- Test expiry boundaries, long execution, failure, repeated rejection, table isolation, copied keys, and eviction with pending/active work.
- Extend the seeded admission property model to cover completion cooldown and advancing time. Generate admission, drain, resize, and time-advance operations; shrink failures with QuickTheories.
- Verify the property model's ordered accepted requests and completion deadlines against actual admissions and execution, without modelling cache internals. Keep its key space below the cache bound; test eviction separately.
- Adjust the parameterized production-manager fixture to use distinct keys per case; assert that an immediate repeat submission leaves the completed SSTable unchanged.
- Document best-effort eviction/restart semantics. Run focused unit/integration tests and build/Checkstyle before marking the TODO complete.

## Validation

- Before suppression was implemented, four new unit tests and the extended property test failed as expected. Log: `logs/run_tests_20260906T013348Z.log`. Property seed: 1592639710.
- Queue class: 17 tests passed, including 1,000 generated examples and exact expiry boundaries. Log: `logs/run_tests_20260906T013524Z.log`.
- Compaction integration: 60 cases, zero failures, five existing skips. Immediate repeat submissions leave completed SSTables unchanged. Log: `logs/run_tests_20260906T013819Z.log`.
- ReadCommandTest passed. Log: `logs/run_tests_20260906T042929Z.log`.
- Build and production/test Checkstyle passed. Log: `logs/run_tests_20260906T042953Z.log`.
- Tests used JDK 21 and the root test wrapper. Reuse was used only after the current Java changes had compiled.
