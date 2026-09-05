# Tombstone-Triggered Partition Compaction Review Fixes

## Objective

Goal: Bring commit `7f6e8e6cae6539ddcc9eedc4092b49d096b0d227` and its current uncommitted follow-up work to a review-ready state with the smallest applicable correctness, lifecycle, test, documentation, and maintainability fixes.

- Preserve the approved behavior: default queue capacity `10`, strict `count > tombstone_warn_threshold`, one node-wide queue, and non-persistent runtime setting changes.
- Preserve the current tracked and untracked work. Do not reset, delete, or replace `TODO.md`, `run_tests.sh`, `run_property_tests.sh`, `.plans/`, or unrelated user files.
- Success means the applicable findings have focused regression coverage, the operator-facing configuration and documentation are complete, and fresh targeted test/build evidence exists. No commit is part of this work.

## Current-State Reconciliation

The reviewed commit is the current `HEAD`; six tracked files also contain uncommitted follow-up work.

| # | Status | Evidence and rationale |
|---|---|---|
| 1 | **Partially addressed** | The current diff adds default `10` to both YAML files and adds the operational section to `tombstones.adoc`. `CHANGES.txt` has no entry, and nearby entries all use a verified Cassandra issue ID. Add an entry only when the branch or pull request has such an ID. |
| 2 | **Partially addressed** | `getUserDefinedTasksIfAvailable` closes tasks collected before a later reservation failure. The worktree adds a one-SSTable anti-compaction retry test, but it does not force a successful first reservation followed by a failed later reservation, and its fixed sleep does not prove retry timing. |
| 3 | **Partially addressed** | The one-line worktree fix expects cursor execution when enabled for supported input. A reactive unsupported-input iterator fallback still needs coverage. |
| 4 | **Planned** | Existing tombstone warning/failure accounting excludes local system keyspaces through `respectTombstoneThresholds`; the new purge callback submits without that guard. This is a real policy mismatch. |
| 5 | **Partially addressed** | Production calls the hook only after purge eligibility checks, resets state per partition, and uses strict `>`. Existing end-to-end trigger coverage uses one old row-tombstone case. Add only the combinations needed to prove boundaries and the shared hook. |
| 6 | **Declined** | Existing Java Microbenchmark Harness read tests do not create purgeable tombstones. A useful benchmark requires disproportionate new state setup. Accept residual risk and minimize rejected-admission allocation instead. |
| 7 | **Planned** | `enqueue` clones before shutdown, disabled, duplicate, and full checks. Reorder work so rejected reads do not copy key bytes. |
| 8 | **Planned** | `SettingsTable.getValue` special-cases this setting although production and live mutation share the same Config field. Remove the special path and correct injected-versus-production Config tests. |
| 9 | **Planned** | BUSY work requeues after graceful shutdown and can prevent termination forever. |
| 10 | **Already addressed** | Current YAML and docs explicitly couple the trigger to `tombstone_warn_threshold`. Do not add another threshold. |
| 11 | **Planned** | Three SettingsTable deletion overrides only replace inherited rejection messages. Remove them if inherited errors meet the contract. |
| 12 | **Commit-time only** | Placeholder author metadata and newline churn do not require source fixes or history rewriting during this task. |
| 13 | **Planned** | Generate fresh class-specific evidence through the root wrapper. Do not add JaCoCo or run the full suite. |

Default `10` remains approved. The earlier plan made this an explicit product choice, zero remains available as an opt-out, and the review found no concrete Cassandra invariant that requires default-disabled behavior.

## Invariants to Preserve

- Admit on the first purgeable-deletion count strictly greater than `tombstone_warn_threshold`; reset for each range-read partition.
- Use one node-wide capacity. Count active and pending work. Capacity `0` rejects new work without cancelling accepted work.
- Keep runtime JMX and settings-table changes in memory only. Restart reloads YAML.
- Never call `runWithCompactionsDisabled` from the reactive path. Use `getUserDefinedTasksIfAvailable`, close partial reservations, and retry only during normal operation.
- Preserve CompactionTask/CompactionController behavior, cursor selection, `gc_grace_seconds`, repaired-only policy, overlap and memtable checks, repair/disk grouping, rate limiting, and cleanup.
- Do not alter manual partition compaction, normal compaction, validation, anti-compaction, or repair priority.
- Keep read admission non-blocking. Do not log raw partition keys.
- Continue purging eligible tombstones from local system reads; suppress only reactive submission.

## Ordered Implementation Plan

### 1. Preserve and validate the current uncommitted follow-up

- Add the review-fix task under `# TODO` in `TODO.md`. Preserve existing entries and user files.
- Record the tracked diff. Do not reset or regenerate current YAML, docs, cursor assertion, anti-compaction test, or generated queue-admission test.
- Run baseline `ReadCommandTest`, `CompactionsTest`, and `TombstoneTriggeredCompactionManagerTest`. Save fresh logs and treat failures as baseline evidence.
- Confirm docs retain default `10`, strict threshold coupling, node-wide scope, zero-disable behavior, live controls, non-persistence, full-SSTable rewrite cost, pipeline fallback, and repair safety.

### 2. Fix rejected-admission allocation and graceful BUSY shutdown

Files: `TombstoneTriggeredCompactionManager.java`, `TombstoneTriggeredCompactionManagerTest.java`.

- Add a failing latch-driven lifecycle test first: keep a task in flight, begin `shutdown(false)`, return BUSY, and assert bounded attempts, termination, zero outstanding tasks, and later SHUTDOWN admission.
- In `drain`, do not requeue BUSY work after graceful shutdown starts. Keep normal runtime rotation/retry and forced shutdown behavior unchanged.
- In `enqueue`, enter the existing monitor before cloning. Check shutdown and zero capacity, use the caller key only for duplicate lookup, preserve duplicate-before-full results, and clone only after admission succeeds.
- Store only the cloned request. Do not add a cloner abstraction solely for tests.
- Retain FIFO, deduplication, resize, generated admission-sequence, failure-isolation, graceful-drain, and forced-shutdown tests.

Expected result: rejected reads avoid cloning; accepted keys remain stable; graceful shutdown cannot loop forever on BUSY work.

### 3. Align local-system policy and add focused trigger coverage

Files: `ReadCommand.java`, `ReadCommandTest.java`, `tombstones.adoc`.

- Reuse one local-system-keyspace eligibility decision for threshold behavior and reactive submission.
- Gate only count/submission. Keep PurgeFunction active on local system reads.
- Add a regression that exercises the production `withoutPurgeableTombstones` wiring with local-system metadata. Prefer a real local-system read fixture with purgeable tombstones. If that is unsafe, test the transform with system metadata and observable admission state. Do not stop at an isolated predicate test.
- Keep the supported cursor assertion. Add one cursor-enabled reactive case with known unsupported metadata and assert iterator fallback.
- Add one range-read case that proves equality rejection, per-partition reset, young-tombstone exclusion, and one above-threshold eligible trigger.
- Add compact helper-driven cases for partition, cell, and range tombstones. The existing row case completes the deletion forms. Do not cross-product forms with cursor settings.
- Add one repaired-only case: an old unrepaired tombstone does not trigger under `only_purge_repaired_tombstones`; after marking it repaired through existing helpers, it triggers.
- Restore global settings, schema, repair metadata, and auto-compaction state in `finally` blocks.
- Document that local system keyspaces follow the threshold exemption and do not submit reactive compaction.

### 4. Prove real partial-reservation rollback and retry

Files: `CompactionManager.java`, `CompactionsTest.java`.

- Replace the fixed-sleep anti-compaction test with a latch-controlled integration test.
- If needed, make `executeTombstoneTriggeredCompaction` package-private for package-local testing. Do not add public API or another compaction path.
- Create at least two SSTables for the same key in deterministic repair/disk groups. Let the first reserve and hold a later group with an ANTICOMPACTION transaction.
- Submit through a local manager whose runner calls the production CompactionManager entry point.
- Observe the first real BUSY result with a latch. Before owner release, prove the earlier reservation was released by reserving and closing it independently. Assert anti-compaction still owns its SSTable.
- Release anti-compaction, allow retry, and assert both original SSTables are replaced.
- Use latches and bounded Awaitility only. Remove the fixed sleep.
- Keep the direct compaction/validation/anti-compaction ownership matrix.

### 5. Simplify SettingsTable without changing its live contract

Files: `SettingsTable.java`, `SettingsTableTest.java`.

- Remove the queue-capacity branch from `getValue`; use the generic Config Property path.
- Keep updates allowlisted to this row and routed through the validated DatabaseDescriptor setter.
- Remove partition, row, and column deletion overrides if inherited errors preserve rejection. Do not broaden mutability or truncate support.
- Add an injected-Config read assertion with a distinct capacity value.
- Exercise live mutation with a production-constructed SettingsTable backed by `DatabaseDescriptor.getRawConfig()`.
- Preserve JMX/CQL parity, invalid input rejection, protected rows, and delete rejection. Update only custom-message assertions.
- Restore global capacity and virtual-keyspace registration in `finally` blocks.

### 6. Finish operator and release documentation

- Preserve current YAML and `tombstones.adoc` edits. Add only the system-keyspace clarification and final wording corrections.
- Keep default `10`, strict coupling, node-wide capacity, zero-disable behavior, non-persistence, full-SSTable rewrite cost, pipeline fallback, and repair safety.
- If the branch or pull request has a verified `CASSANDRA-NNNNN` ID, add a matching 7.0 CHANGES.txt entry. If no ID exists, do not invent one or add an unnumbered entry; report the metadata gap for maintainer follow-up.

### 7. Run focused validation and close the work item

- Run every matrix command through `./run_tests.sh`; it delegates to the required `.build/sh/ai-*` wrappers and writes timestamped logs.
- Run `git diff --check` and inspect the final diff for intended changes only. Preserve unrelated helpers.
- Move the task from TODO to DONE with `[x]` only after all checks pass. Keep it under TODO with the failure recorded if a check fails.
- Do not commit.

## Focused Validation Matrix

Do not run the full suite. Do not call Ant directly.

| Scope | Command | Pass condition |
|---|---|---|
| Configuration | `./run_tests.sh org.apache.cassandra.config.DatabaseDescriptorTest` | Default 10 and value validation pass. |
| JMX | `./run_tests.sh org.apache.cassandra.service.StorageServiceTest` | Live getter/setter and rejection behavior pass. |
| Virtual settings | `./run_tests.sh org.apache.cassandra.db.virtual.SettingsTableTest` | Generic reads, live writes, protected rows, and delete rejection pass. |
| Purge hook | `./run_tests.sh org.apache.cassandra.db.partitions.PurgeFunctionTest` | Callback fires only for accepted purge decisions. |
| Read trigger | `./run_tests.sh org.apache.cassandra.db.ReadCommandTest` | System policy, boundaries, eligibility, deletion forms, cursor, and fallback pass. |
| Queue lifecycle | `./run_tests.sh org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManagerTest` | FIFO, capacity, deduplication, BUSY shutdown, and forced shutdown pass. |
| Reservation integration | `./run_tests.sh org.apache.cassandra.db.compaction.CompactionsTest` | Owner safety, partial rollback, and retry completion pass. |
| Manual compaction | `./run_tests.sh org.apache.cassandra.tools.nodetool.CompactTest` | Existing manual partition behavior passes. |
| Build/style | `./run_tests.sh --build` | Delegated ai-build exits 0 with build and Checkstyle gates. |

## Acceptance Criteria

- YAML and docs match final behavior, including local-system exclusion. CHANGES.txt is updated only with a verified issue ID.
- A deterministic multi-group test proves partial cleanup, ownership preservation, and retry after release.
- Supported input uses cursor; unsupported input falls back to iterator.
- Local system reads continue purging but cannot enqueue reactive work.
- Tests prove equality rejection, per-partition range counting, age and repaired-only exclusions, and all deletion forms.
- Rejected admissions do not clone keys; accepted requests preserve FIFO, capacity, deduplication, and key ownership.
- Graceful shutdown terminates after an in-flight BUSY result; normal retry and forced shutdown stay unchanged.
- SettingsTable uses the generic property path and keeps one validated writable row.
- Each targeted class and the build has a fresh target-identifiable successful log.
- TODO.md records completion only after validation. No unrelated work is deleted or overwritten.

## Declined or Deferred Feedback

- **Default capacity 0:** Declined. Default 10 is approved; zero remains an opt-out.
- **Second trigger threshold:** Declined. Existing coupling is intentional.
- **New JMH benchmark:** Deferred as accepted residual risk; current benches do not model the purge path.
- **JaCoCo:** Declined. The evidence gap is fresh target-specific execution.
- **Unnumbered CHANGES.txt entry:** Declined. Do not fabricate an issue ID.
- **Placeholder author and newline-only cleanup:** Commit-time only.
- **Broader compaction refactors or new abstractions:** Declined.

## Risks and Rollback Notes

- Graceful shutdown intentionally drops BUSY retry work after shutdown starts because external ownership may never release. Keep normal operation unchanged.
- Multi-group reservation setup can become order-dependent. Assert grouping and use latches; never replace evidence with timing sleeps.
- Async read tests can be timing-sensitive. Control auto-compaction and flushes, use bounded waits, and restore state in `finally` blocks.
- Apply the local-system guard only around submission so read-side purge remains active.
- Separate injected Config serialization tests from production live mutation.
- Use an un-cloned key only for lookup while holding the monitor; store only the clone.
- Roll back new edits by hunk against the recorded baseline, never with a broad reset.