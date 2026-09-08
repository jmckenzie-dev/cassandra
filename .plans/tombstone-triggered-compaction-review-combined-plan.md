# Tombstone-Triggered Partition Compaction — Combined Plan (Review Basis)

Combined plan for full review of the 4 commits on `tombstone_compact` ahead of
`origin/trunk`. It merges, without changing decisions, these sources:

- `.plans/tombstone-triggered-partition-compaction-plan.md` (feature plan)
- `.plans/tombstone-triggered-partition-compaction-review-fixes.md` (review-fix plan)
- `.plans/tombstone-compaction-cooldown.md` (cooldown addition)
- `.plans/tombstone-compaction-coverage-followup.md` (coverage expansion)
- `.plans/tombstone-compaction-shutdown-integration.md` (shutdown integration tests)

## Change set under review

- Branch: `tombstone_compact`, `HEAD` = `9115d242de00811cb62a9b932d4d3c0a78403c8d`
- Comparison basis: `origin/trunk` = `c1030321071ab9f10b2f19d63087ab9313035001`
- Commits (oldest first):
  1. `77e5d2a7bc` Add tombstone-triggered partition compaction
  2. `f0f76738fc` Fix and verify tombstone-triggered partition compaction
  3. `6b601bf7fa` Add a cooldown for tombstone-triggered partition compaction
  4. `9115d242de` Test tombstone-triggered compaction shutdown with real SSTable work
- Worktree clean at review time.

Production files: `Config.java`, `DatabaseDescriptor.java`, `ReadCommand.java`,
`CompactionManager.java`, `CompactionStrategyManager.java`,
`TombstoneTriggeredCompactionManager.java` (new), `PurgeFunction.java`,
`SettingsTable.java`, `StorageService.java`, `StorageServiceMBean.java`,
`conf/cassandra.yaml`, `conf/cassandra_latest.yaml`,
`doc/.../compaction/tombstones.adoc`.
Tests: `DatabaseDescriptorTest`, `StorageServiceTest`, `SettingsTableTest`,
`PurgeFunctionTest`, `ReadCommandTest`, `CompactionsTest`,
`TombstoneTriggeredCompactionManagerTest` (new), `TombstoneCompactionShutdownTest`
(new jvm-dtest). Repo helpers: `run_tests.sh`, `run_property_tests.sh`, `TODO.md`,
`.plans/**`, `.worklog/**`.

## Feature intent

Enqueue a bounded, deduplicated, per-partition compaction when a local read
purges more than `tombstone_warn_threshold` deletions from one partition under
normal purge rules (gc_grace_seconds, `only_purge_repaired_tombstones`,
overlap/memtable checks preserved).

- Node-wide FIFO queue, default capacity 10 counting queued and active work;
  `0` disables admissions. Hot-changeable via JMX and
  `UPDATE system_views.settings SET value='N' WHERE name='tombstone_compaction_queue_capacity'`.
  Runtime changes are not persisted to YAML.
- One dedicated single-thread executor (`TombstoneCompactionExecutor`);
  regular/manual compaction and repair behavior unchanged.
- Non-blocking admission from read paths: accepted / duplicate / cooldown /
  full / disabled / shutdown results; rejections never block the caller.
- Requests keyed by table ID + copied decorated key; CFS resolved at execution
  so dropped/recreated tables get no stale work.
- Busy tables (compaction/validation/anti-compaction own them) requeue to the
  FIFO tail with a fixed retry delay; use `getUserDefinedTasksIfAvailable`,
  never `runWithCompactionsDisabled`; tasks marked `TOMBSTONE_COMPACTION`
  priority; partial reservations closed on later reservation failure.
- Trigger fires exactly once per partition on first strict crossing of
  `count > tombstone_warn_threshold` inside the existing `PurgeFunction`
  purge traversal via a callback — no second cell scan, independent of the
  optional purgeable-metric granularity setting.
- Cooldown: completed/failed attempts suppressed for 60 s per table+key in a
  capped (1024-entry) Guava cache with injectable ticker; cooldown checked
  after dedup, before capacity; BUSY retries are not cooldown-recorded.
- Local system keyspaces follow the existing `respectTombstoneThresholds`
  exemption: they keep purging but never submit reactive work.
- Rejected admissions must not clone key bytes (clone only after admission).
- Graceful shutdown must terminate even if in-flight work returns BUSY
  (no infinite requeue loop); forced shutdown clears the queue.
- Coexistence with repair: never cancels or preempts anti-compaction or
  validation; waits behind them.

## Key invariants (from review-fix plan)

- Admit on first per-partition purge count strictly greater than
  `tombstone_warn_threshold`; reset per range-read partition.
- One node-wide capacity counting active and pending work; capacity 0 rejects
  new work without cancelling accepted work.
- JMX/settings-table changes are in-memory only; restart reloads YAML.
- Reactive path preserves CompactionTask/CompactionController behavior, cursor
  pipeline selection (cursor when enabled+supported, else iterator fallback),
  gc_grace_seconds, repaired-only policy, overlap/memtable checks, repair/disk
  grouping, rate limiting, cleanup.
- Manual partition compaction keeps `MAJOR_COMPACTION` + shared executor.
- SettingsTable: only `tombstone_compaction_queue_capacity` row writable, via
  the validated DatabaseDescriptor setter; generic Config read path (no
  special-case branch in `getValue`); other rows and deletes rejected.
- No raw partition key in logs; rate-limited acceptance warning.

## Accepted product decisions (do not relitigate)

- Default capacity `10` (zero available as opt-out) — approved.
- Single trigger threshold coupled to `tombstone_warn_threshold` — approved.
- No new JMH benchmark — deferred residual risk.
- CHANGES.txt entry only with a verified `CASSANDRA-#####` ID — none yet, so
  none added (report as maintainer metadata gap).

## Validation evidence on record

- Focused suites per `.plans/tombstone-triggered-partition-compaction-review-fixes.md`
  matrix via `./run_tests.sh`; eight focused classes + build/Checkstyle passed
  JDK 21 2026-09-05 (TODO.md DONE entry).
- Cooldown: queue class 17 tests incl. 1,000 seeded property examples;
  integration 60 cases zero failures; ReadCommandTest and build/checkstyle green
  (logs/run_tests_20260906T0133*/013524/013819/042929/042953Z.log).
- Shutdown integration (`TombstoneCompactionShutdownTest`, jvm-dtest): three
  cases passed twice, JaCoCo confirms previously uncovered
  `finishCompactionsAndShutdown` reactive calls (lines 381/384) now covered;
  logs/run_shutdown_tests_20260907T040025Z.log and T040226Z.log, JDK 17.
- Latest commit has a commit-time log `logs/commit_shutdown_tests_20260907T143320Z.log`.

## Deferred / out of scope

- Multi-node repair campaigns, load testing, compaction failure injection
  (explicitly deferred by the shutdown-integration plan).
- Full-suite runs and JaCoCo-of-branch (declined; focused evidence only).
- Broader compaction refactors or new abstractions (declined).
