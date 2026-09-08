# Full Review — tombstone-triggered partition compaction (4 commits vs origin/trunk)

- **Date:** 2026-09-07
- **Branch:** `tombstone_compact` @ `9115d242de00811cb62a9b932d4d3c0a78403c8d`
- **Comparison basis:** `origin/trunk` = `c1030321071ab9f10b2f19d63087ab9313035001`
- **Commits:** `77e5d2a7bc` (feature), `f0f76738fc` (fix+verify), `6b601bf7fa` (cooldown), `9115d242de` (shutdown tests)
- **Plan basis:** `.plans/tombstone-triggered-compaction-review-combined-plan.md` (combined from the five `.plans/tombstone-*.md` sources)
- **Method:** review-full — ten concurrent discipline reviews (principal-engineer, adversarial, structural, performance, coverage, tests, aislop, aislop-qualitative, overengineering, maintainer), each a fresh `codebase-analyst` child; parent verification of decisive claims (commit authors, `drain()` catch structure, interrupt-path cooldown).

# Review: Full

## Gate
- **Status:** COMPLETE
- **Verdict:** REQUEST CHANGES
- **+1:** NO
- **Blocker findings:** 0
- **Major findings:** 5
- **Minor findings:** 16
- **Nit findings:** 11

## Scope
- **Repository:** /home/jmckenzie/src/cassandra/cassandra_asf/wt_tombstone_compact
- **Plan:** .plans/tombstone-triggered-compaction-review-combined-plan.md (combined from the five `.plans/tombstone-*.md` sources)
- **Requested scope:** the 4 commits on this branch that differ from upstream trunk
- **Resolved scope:** `git diff origin/trunk...HEAD` = `77e5d2a7bc` (feature), `f0f76738fc` (fix+verify), `6b601bf7fa` (cooldown), `9115d242de` (shutdown tests); 38 files, +3075/−14. All ten reviewers verified match against the packet; the diff additionally carries `.aftignore`, `.gitignore`, `TODO.md`, `.plans/**`, `.worklog/**`, placeholder files, and root test wrappers — disclosed by every reviewer and folded into FUL-002/FUL-017.
- **Change-set fingerprint:** baseline `c1030321071ab9f10b2f19d63087ab9313035001` → head `9115d242de00811cb62a9b932d4d3c0a78403c8d` (verified by parent and by all children via `git rev-parse`; no fingerprint divergence)
- **Validation evidence:** plan "Validation evidence on record"; on-disk logs spot-checked by children (PER-001/COV-001 reviews read `logs/run_shutdown_tests_*`, `logs/run_tests_*`, JaCoCo artifacts under `tmp/shutdown-coverage-20260907T040226Z/`); parent verified commit authors and read `TombstoneTriggeredCompactionManager.drain()` directly.

## Findings

### Blocking

#### FUL-001 — Feature commit carries placeholder author identity
- **Severity:** Major
- **Confidence:** High
- **Sources:** review-principal-engineer PRI-001; review-maintainer F1
- **Requirement/invariant:** apache/cassandra attribution and commit conventions (repo AGENTS.md)
- **Evidence:** `git log --format=short origin/trunk..HEAD` (parent-verified): `77e5d2a7bcba Add tombstone-triggered partition compaction <Your Name you@example.com>`; the other three carry `Josh McKenzie <jmckenzie@apache.org>`.
- **Impact:** The core commit is unattributable and unmergeable upstream; `git blame` on the central new file is wrong.
- **Remediation:** Rebase to set the real author — fold into the single history rewrite with FUL-002/FUL-004/FUL-005/FUL-012.
- **Verification:** `git log --format=short origin/trunk..HEAD` shows the real identity on all four commits.

#### FUL-002 — Agent-workflow and tooling artifacts committed inside the upstream-targeted patch
- **Severity:** Major
- **Confidence:** High
- **Sources:** review-principal-engineer PRI-003; review-adversarial ADV-003; review-overengineering OVE-001; review-maintainer F2
- **Requirement/invariant:** Contributor patches contain only feature content; local tooling stays out of apache/cassandra history
- **Evidence:** `git diff --stat origin/trunk...HEAD` includes `TODO.md`, five `.plans/*.md`, `.worklog/tombstone_review_fixes.md`, `.debug/.prompts/.reviews/docs/.placeholder` files, `.aftignore`, and `.gitignore` +6 lines of personal tool ignores (`.serena/`, `.antigravitycli/`, `.codegraphcontext/`, `*wt_*`, `__pycache__`, `tmp/`) (all four reviews; parent diff-stat confirms). `docs/.placeholder` shadows the repo's real `doc/` tree (OVE-001).
- **Impact:** Roughly a fifth of the patch is non-feature material upstream will reject, or it becomes permanent maintenance debris in trunk; forces a history rewrite that re-breaks FUL-001 provenance if done late.
- **Remediation:** Ship only the 14 production/test/doc/yaml paths (plus CHANGES.txt later); keep plans/worklog/TODO/wrappers/ignores local (untracked or gitignored).
- **Verification:** `git diff --name-only origin/trunk...HEAD` lists only feature, test, config, and doc paths.

#### FUL-003 — Error escaping `drain()` permanently wedges the reactive queue
- **Severity:** Major
- **Confidence:** Medium
- **Sources:** review-principal-engineer PRI-002 (parent-verified by direct read of the method)
- **Requirement/invariant:** Feature must never hang its own machinery; codebase pattern is `catch (Throwable)` + `JVMStabilityInspector` around compaction execution
- **Evidence:** `TombstoneTriggeredCompactionManager.java:191-256` — `drain()` catches only `InterruptedException` and `Exception`; an `Error` (e.g. `AssertionError` — `assert !sstables.isEmpty()` is reachable in `SizeTieredCompactionStrategy.getUserDefinedTask`, enabled in long tests) exits the loop leaving `draining=true` and `active` set (parent read confirms structure). Consequence trace from PRI-002: `enqueue` never re-submits (`if (!draining)`), the wedged key returns DUPLICATE forever, `hasOngoingOrPendingTasks()` stays true, and callers such as `test/long/.../LongCompactionsTest.java:208` spin.
- **Impact:** Silent, permanent death of the feature until restart, with no log signal; long-test hang risk.
- **Remediation:** Catch `Throwable` in the drain loop: log, `JVMStabilityInspector.inspectThrowable`, clear `active`/`draining` in a `finally`, keep draining.
- **Verification:** New manager test whose `TaskRunner` throws `Error`; assert `outstandingTasks()==0` and a later different-key enqueue returns ACCEPTED.

#### FUL-004 — Commit `f0f76738fc` is a fixup over an unmerged sibling and narrates the review round
- **Severity:** Major
- **Confidence:** High
- **Sources:** review-aislop-qualitative ASQ-001
- **Requirement/invariant:** History reads as if the final state were the first and only version
- **Evidence:** Message: "Fix and verify…", "Complete the review fixes…", "Repair the test and build wrappers…" — yet `git log --oneline origin/trunk..HEAD -- run_tests.sh run_property_tests.sh` shows those wrappers are introduced by that same commit; nothing ever shipped broken (ASQ-001).
- **Impact:** Review-round bookkeeping and a fixup over sibling `77e5d2a7bc` make the published history misleading and non-bisectable in intent.
- **Remediation:** Squash `f0f76738fc` into `77e5d2a7bc` (author performs the rewrite) using ASQ-001's replacement message, which retains only facts verifiable in the final diff:

    ```
    Add tombstone-triggered partition compaction

    Queue bounded, deduplicated compaction work when a local read purges more
    tombstones from one partition than the warning threshold. Reserve selected
    SSTables without cancelling normal compaction, validation, or anti-compaction
    work, and retry requests that lose the reservation race. Local system keyspaces
    keep purging but never submit requests. Clone accepted keys only after
    admission. Stop BUSY retries at shutdown and clear queued work on forced
    shutdown.
    Key Items:
    - Add the dedicated FIFO queue and tombstone compaction executor.
    - Add live queue-capacity configuration through settings and JMX.
    - Count purgeable deletions per partition in the local read path.
    - Add concurrency, lifecycle, configuration, and integration tests.
    - Add run_tests.sh and run_property_tests.sh helpers.
    ```

- **Verification:** No subject or body references a sibling commit or a review round; the wrappers trace to one feature commit.

#### FUL-005 — Shutdown-test commit draws rationale against sibling commits and records plan bookkeeping
- **Severity:** Major
- **Confidence:** High
- **Sources:** review-aislop-qualitative ASQ-002
- **Requirement/invariant:** Same durable-history rule
- **Evidence:** Message opener "Queue tests alone do not verify…" and "real SSTable work" contrast tests added by earlier commits on the same unmerged branch; "mark the shutdown test plan complete" is `.plans/` bookkeeping; the "two passing runs… JaCoCo… Checkstyle" bullet is PR-description material (ASQ-002).
- **Impact:** After any squash the contrast references nothing; plan-file bookkeeping in `git log` is process noise.
- **Remediation:** Use ASQ-002's replacement message; move validation evidence to the PR description:

    ```
    Cover tombstone-triggered compaction shutdown with real compaction tasks

    Exercise both shutdown paths on a one-node cluster with real compaction tasks
    and SSTable reservations. Drain interrupts the active task and discards queued
    work. Graceful shutdown waits for active and queued work, then rejects new
    requests. A BUSY retry stops at shutdown without closing the owner of the
    reserved SSTable.
    Key Items:
    - Test drain interruption, queued-work discard, and data after restart.
    - Test graceful waiting and rejection of new requests.
    - Test BUSY retry shutdown without closing the external SSTable owner.
    - Add TombstoneCompactionShutdownTest with three cases.
    ```

- **Verification:** Each sentence of the new message stands without reference to another commit on the branch.

### Non-blocking

#### FUL-006 — Interrupted compaction attempt records a completion cooldown and can strand queued work
- **Severity:** Minor · **Confidence:** High · **Sources:** review-aislop AIL-001; review-principal-engineer PRI-005
- **Requirement/invariant:** Cooldown covers completed/failed attempts only
- **Evidence:** `TombstoneTriggeredCompactionManager.java:237-247` — `catch (InterruptedException)` calls `complete(request)`, which writes `recentlyCompleted` (parent read confirms). If the interrupt lands during the BUSY sleep (`active == null`), completion is skipped and the popped request can sit in `pending` with `draining=false` until an unrelated enqueue.
- **Impact:** A 60 s cooldown on a key that never got compacted; stray pending item during shutdown.
- **Remediation:** In the interrupt branch clear `active` without the cooldown write; treat sleep-interrupt like the forced path.
- **Verification:** Test with a `TaskRunner` that throws `InterruptedException` mid-run: same-key re-enqueue returns ACCEPTED, not COOLDOWN.

#### FUL-007 — Local-system-keyspace submission exemption has no dedicated regression test
- **Severity:** Minor · **Confidence:** High · **Sources:** review-coverage COV-001
- **Evidence:** Single boolean gates the only submission site (`ReadCommand.java:975-1024`); no test in `test/unit/…/db` or `test/distributed` drives a local-system read above threshold asserting no submission (COV-001 content search).
- **Impact:** A refactor dropping the guard silently submits reactive compactions for system tables.
- **Remediation:** Add the `ReadCommandTest` case mirroring `testPartitionCellAndRangeTombstonesTriggerCompaction` against a local-system table.
- **Verification:** Test fails if the `respectTombstoneThresholds` guard is removed.

#### FUL-008 — "BUSY attempts are not cooldown-recorded" invariant is unpinned
- **Severity:** Minor · **Confidence:** High · **Sources:** review-tests TST-001
- **Evidence:** Drain's BUSY requeue bypasses `enqueue`, so no existing test distinguishes recording vs not; mutation `recentlyCompleted.put(...)` in the BUSY branch passes the suite (TST-001 analysis).
- **Impact:** A regression wrongly suppressing re-admission of a BUSY'd key goes undetected.
- **Remediation:** Deterministic test (ManualExecutor/TestTicker) asserting ACCEPTED for a previously-BUSY'd key at the moment only BUSY attempts occurred.
- **Verification:** The mutation probe flips the new test red.

#### FUL-009 — Executed task's `TOMBSTONE_COMPACTION` operation type is never asserted
- **Severity:** Minor · **Confidence:** High · **Sources:** review-tests TST-002
- **Evidence:** `CompactionStrategyManager.java:1322` `setCompactionType(operationType)`; the constant appears in tests only as an input argument (TST-002 grep).
- **Impact:** Dropping the call passes the suite; operator-visible classification of reactive work (the repair-coexistence claim) silently regresses.
- **Remediation:** Assert `getActiveCompactions()` reports `TOMBSTONE_COMPACTION` during a gated run.
- **Verification:** Mutation probe removing the setter fails the new assertion.

#### FUL-010 — Timing-coupled negative and cooldown assertions can pass trivially
- **Severity:** Minor · **Confidence:** Medium · **Sources:** review-tests TST-003, TST-004
- **Evidence:** Negative "must not trigger" checks rest on a 1-second `pollDelay` on SSTable liveness (`ReadCommandTest.java:896-898`, `:1018-1020`); the CompactionManager-level cooldown assertion (`CompactionsTest.java:943-947`) passes under a broken cooldown if the no-op drain finishes first.
- **Impact:** False pass under loaded CI for the system-keyspace exemption and production-wiring cooldown regressions.
- **Remediation:** Additionally assert `CompactionManager.instance.hasOngoingOrPendingTasks()==false` in the window; add a Ticker seam to assert an explicit COOLDOWN result through the production route.
- **Verification:** Mutation probes (`respectsTombstoneThresholds()→true`, cooldown mis-keyed) fail deterministically.

#### FUL-011 — No recorded test execution at the shipped HEAD; validation predates the rebase/amend
- **Severity:** Minor · **Confidence:** High · **Sources:** review-performance PER-001; review-adversarial ADV-004; review-principal-engineer PRI-007; review-maintainer F4
- **Evidence:** `logs/commit_shutdown_tests_20260907T143320Z.log` records `5b6e1b0b72`, not shipped `9115d242de`; committer dates 16:44 vs author 10:45 show a post-validation rebase onto a trunk that moved with a large Accord refactor (PER-001). Children verified feature files byte-identical between tested tree and HEAD (`git diff` over feature paths empty).
- **Impact:** Integration with moved trunk files is unverified by any recorded run at the final SHA.
- **Remediation:** One focused re-run of the shutdown dtest + queue class + the plan's matrix at HEAD, archived in `logs/`.
- **Verification:** A post-rebase log referencing the final SHA with all passes.

#### FUL-012 — Commit metadata and CHANGES.txt below project convention pending a JIRA ID
- **Severity:** Minor · **Confidence:** High · **Sources:** review-principal-engineer PRI-006; review-aislop AIL-002; review-maintainer F3
- **Requirement/invariant:** AGENTS.md commit template (`patch by …; reviewed by … for CASSANDRA-#####`) and CHANGES.txt for user-discoverable behavior; plan's accepted decision: no entry without a verified ID.
- **Evidence:** All four messages carry only subject/body/`Key Items:`/`Assisted-by:`; no `CASSANDRA-#`; CHANGES.txt untouched (AIL-002, F3).
- **Impact:** Not committable upstream as-is; users discover default-on node-wide behavior only via CHANGES.txt.
- **Remediation:** File the JIRA, add the CHANGES.txt line, reformat trailers — same history rewrite as FUL-001/002.
- **Verification:** `git log --format=%B` shows conforming trailers; CHANGES.txt diff present.

#### FUL-013 — Reservation leak if a strategy throws mid-loop in `getUserDefinedTasksIfAvailable`
- **Severity:** Minor · **Confidence:** High (path exists), Low (trigger probability) · **Sources:** review-adversarial ADV-001
- **Evidence:** `CompactionStrategyManager.java:1315-1337` — the `available==false` branch closes tasks, but a throw from a later holder's `getUserDefinedTasks` leaves earlier reserved tasks unclosed (ADV-001).
- **Impact:** SSTables pinned as compacting until restart under an unexpected strategy-internal failure.
- **Remediation:** try/catch around the holder loop: `tasks.close()` and rethrow.
- **Verification:** Test injecting a throwing second holder; first holder's SSTable reacquirable via `tryModify`.

#### FUL-014 — BUSY retry polls the key index at a fixed 100 ms cadence during long busy periods
- **Severity:** Minor · **Confidence:** Medium · **Sources:** review-performance PER-002
- **Evidence:** Single-thread drain re-runs `sstablesWithKey` probes + `groupSSTables` + reservation attempts every 100 ms while BUSY (`TombstoneTriggeredCompactionManager.java:211-229`, delay wired at `CompactionManager.java:178-183`).
- **Impact:** ~100 key-index probes/s of pure spin for hours behind a repair campaign — bounded, but coexistence with repair is this feature's stated scenario.
- **Remediation:** Raise the fixed delay (1–2 s) or bounded exponential backoff while BUSY persists.
- **Verification:** Probe-rate log during a long repair with a saturated queue; unchanged recovery latency.

#### FUL-015 — `getUserDefinedTasksIfAvailable` near-duplicates trunk's `getUserDefinedTasks`
- **Severity:** Minor · **Confidence:** Medium · **Sources:** review-structural STR-002
- **Evidence:** `CompactionStrategyManager.java:1277-1295` vs `:1301-1337` — same reload/lock/grouping discipline, differing only in null-task policy (STR-002).
- **Impact:** Two loops must co-evolve; reactive vs manual semantics can silently diverge.
- **Remediation:** Extract one private helper returning tasks plus a "saw null reservation" flag; keep both public wrappers.
- **Verification:** Existing `CompactionsTest`/manager suites pin both policies.

#### FUL-016 — CQL capacity changes leave no log; JMX path logs
- **Severity:** Minor · **Confidence:** High · **Sources:** review-structural STR-003
- **Evidence:** `StorageService.java:4829-4838` logs the change; `SettingsTable.applyColumnUpdate` calls the same setter silently (STR-003).
- **Impact:** Capacity drift via CQL is undebuggable after the fact — the exact scenario this feature is examined under.
- **Remediation:** Emit the info line once inside the validated `DatabaseDescriptor` setter (covers every entry point).
- **Verification:** `SettingsTableTest` asserts the log line, or a one-line output check.

#### FUL-017 — Root-level `run_tests.sh`/`run_property_tests.sh` add a second undocumented test entry point
- **Severity:** Minor · **Confidence:** Medium · **Sources:** review-structural STR-004
- **Evidence:** Root wrappers over `.build/run-tests.sh` with different defaults (forced clean build, tee-to-logs), vs the documented `.build/` interface (STR-004).
- **Impact:** Two entry points drift; contributor/CI discovery ambiguity — mooted by FUL-002's strip, but relevant if the author keeps them.
- **Remediation:** Move under `.build/` with feature-neutral names, or keep untracked.
- **Verification:** Single documented interface remains; one smoke run.

#### FUL-018 — Reactive queue is invisible in metrics; CompletedTasks semantics change silently
- **Severity:** Minor · **Confidence:** Medium · **Sources:** review-maintainer F5, F6
- **Evidence:** `CompactionMetrics` varargs collectors feed only `CompletedTasks`, so `PendingTasks` never shows reactive depth; only signals are two NoSpam WARNs at 1/min plus the capacity getter (`CompactionMetrics.java:143-152`, `CompactionManager.java:178-191,1350-1368`) (F5/F6).
- **Impact:** "Why didn't my hot partition compact?" becomes 1-line/minute log archaeology; the node-wide CompletedTasks gauge now co-mingles reactive rewrites.
- **Remediation:** Small permanent addition: queue-depth and accepted/rejected/cooldown counters (or one MBean getter block); call out the CompletedTasks change in PR text.
- **Verification:** Counter advances observable in a test driving admission/rejection.

#### FUL-019 — Trigger counting unit is undefined by contract and differs from warning accounting
- **Severity:** Minor · **Confidence:** High · **Sources:** review-principal-engineer PRI-004; review-adversarial ADV-002; review-structural STR-001
- **Requirement/invariant:** Plan couples the trigger to `tombstone_warn_threshold`; operators tune that threshold
- **Evidence:** Hook fires once per purge-allowed predicate acceptance; a boundary range marker counts 1 or 2 per side (`PurgeFunction.java:46-61`, `:130-158`; pinned by `PurgeFunctionTest` boundary case) while the existing warning/metrics accounting counts differently (`ReadCommand.java:645-687`, `:1080-1098`) (STR-001, ADV-002, PRI-004).
- **Impact:** RT-heavy partitions trigger at roughly half the nominal deletion count; acceptance warnings won't reconcile with `PurgeableTombstoneScannedHistogram`; a future `PurgeFunction` refactor can silently shift trigger sensitivity.
- **Remediation:** One-sentence javadoc on `onPurgeableDeletion()` defining the unit, plus a sentence in `tombstones.adoc`. No behavior change.
- **Verification:** Extend the boundary test to pin the documented per-form counts.

#### FUL-020 — Log and allocation invariants on the read path are exercised but unasserted
- **Severity:** Nit · **Confidence:** High · **Sources:** review-coverage COV-004; review-tests TST-006, TST-005
- **Evidence:** No test asserts accepted-message content or key redaction (`CompactionManager.java:1350-1366`), nor the rejected-admission no-clone property (`TombstoneTriggeredCompactionManager.java:122-130`) (COV-004, TST-005/006).
- **Impact:** A regression leaking raw key bytes to logs, re-introducing log floods, or cloning per rejected read passes the suite.
- **Remediation:** Proportional: one `InternalAppender`-style assertion; optional allocation probe or documented exemption for cloning.
- **Verification:** Mutation probes (raw key in format string; eager clone) fail.

#### FUL-021 — Threshold re-read per purgeable deletion instead of cached per transform instance
- **Severity:** Nit · **Confidence:** High · **Sources:** review-performance PER-003
- **Evidence:** `ReadCommand.java:1003-1011` calls `getTombstoneWarnThreshold()` per deletion; siblings cache thresholds as finals (`:625-626`).
- **Impact:** Nanosecond-scale; consistency only.
- **Remediation:** Cache in a final field of `WithoutPurgeableTombstones`.
- **Verification:** N/A (no measurable effect claimed).

#### FUL-022 — Log arguments eagerly evaluated even when NoSpam suppresses output
- **Severity:** Nit · **Confidence:** High · **Sources:** review-performance PER-004
- **Evidence:** `CompactionManager.java:1350-1366` builds `keyspace.table` strings and re-acquires the manager monitor for `outstandingTasks()` before throttled statements are discarded.
- **Impact:** Bounded extra cost on threshold-crossing reads during exactly the incident state that triggers it.
- **Remediation:** Supplier-based/gated logging.
- **Verification:** N/A.

#### FUL-023 — Unnamed `100` positional argument sets the BUSY retry delay at the wiring site
- **Severity:** Nit · **Confidence:** High · **Sources:** review-structural STR-005
- **Evidence:** `CompactionManager.java:175-183` — bare `100` in the production constructor call.
- **Impact:** Intent and unit invisible at the one tuning site.
- **Remediation:** Named constant `TOMBSTONE_BUSY_RETRY_DELAY_MILLIS`.
- **Verification:** Compile plus existing BUSY test.

#### FUL-024 — Ignored `awaitTermination` result on the reactive executor
- **Severity:** Nit · **Confidence:** High · **Sources:** review-principal-engineer PRI-008
- **Evidence:** `CompactionManager.finishCompactionsAndShutdown` discards the boolean (mirrors pre-existing adjacent trunk style).
- **Impact:** A timed-out reactive drain is silent.
- **Remediation:** Warn when the await returns false.
- **Verification:** N/A (parity with existing code acceptable).

#### FUL-025 — `run_property_tests.sh` re-invokes the sibling wrapper and duplicates its logging
- **Severity:** Nit · **Confidence:** High · **Sources:** review-aislop AIL-003
- **Evidence:** `run_property_tests.sh:9` `bash run_tests.sh …` plus duplicated log-file creation producing two timestamped logs per run.
- **Impact:** Cosmetic; moot if FUL-017 removes/untracks the wrappers.
- **Remediation:** Direct invocation or drop duplicate logging.
- **Verification:** N/A.

#### FUL-026 — Duplicated `respectsTombstoneThresholds` derivation in two sibling read classes
- **Severity:** Nit · **Confidence:** Medium · **Sources:** review-aislop AIL-004
- **Evidence:** Same derived flag materialized twice (`ReadCommand.java` ~628 and ~978).
- **Impact:** Mild drift risk if one block's condition is edited.
- **Remediation:** Collapse to one shared field, or leave (preserves existing structure).
- **Verification:** Existing threshold tests stay green.

#### FUL-027 — Unrelated AutoRepair shutdown relocation rides inside a feature commit
- **Severity:** Nit · **Confidence:** High · **Sources:** review-adversarial ADV-005
- **Evidence:** `AutoRepair.instance.shutdownBlocking()` move in `StorageService.drain` attributed to the tombstone commits (`StorageService.java:3981-4045`).
- **Impact:** Slightly muddies bisectability of the feature.
- **Remediation:** Split into its own commit at finalization.
- **Verification:** Per-commit diffs show one coherent purpose each.

#### FUL-028 — Incidental EOF-newline hunks and residual uncovered defensive branches
- **Severity:** Nit · **Confidence:** High · **Sources:** review-overengineering OVE-002; review-coverage COV-003
- **Evidence:** EOF newline-only hunks in `Config.java`/`ReadCommand.java`; three residual branches (`SettingsTable.java:111`, one `Purger` compound branch, `Request.equals` self-identity) at 215/219 lines, 100/108 changed-line coverage (COV-003).
- **Impact:** Review noise only; guards' user-reachable behavior covered at higher level.
- **Remediation:** Optional; keep or revert either way.
- **Verification:** N/A.

## Discipline-specific analysis

### Reviewer gate table
| Required reviewer | Child status | Child verdict | Findings used | Scope/fingerprint | Result or blocker |
|---|---|---|---|---|---|
| review-principal-engineer | COMPLETE | REQUEST CHANGES | PRI-001–008 | Match | 3 Major / 3 Minor / 2 Nit |
| review-adversarial | COMPLETE | APPROVE | ADV-001–005 | Match | 3 Minor / 1 Nit; full requirements matrix satisfied |
| review-structural | COMPLETE | APPROVE | STR-001–005 | Match | 4 Minor / 1 Nit; no new-abstraction demands |
| review-performance | COMPLETE | APPROVE | PER-001–004 | Match | 2 Minor / 2 Nit; static conclusions, no benchmarks by plan decision |
| review-coverage | COMPLETE | APPROVE | COV-001–004 | Match | 2 Minor / 2 Nit; changed-line coverage 215/219, 100/108 branches |
| review-tests | COMPLETE | APPROVE | TST-001–006 | Match | 4 Minor / 2 Nit; strong trustworthiness, 4 unpinning gaps |
| review-aislop | COMPLETE | APPROVE | AIL-001–004 | Match | 2 Minor / 2 Nit; CLI denied by policy — deterministic checklist applied manually, disclosed |
| review-aislop-qualitative | COMPLETE | REQUEST CHANGES | ASQ-001–007 (+006b) | Match | 2 Major / 3 Minor / 3 Nit, with replacement commit texts |
| review-overengineering | COMPLETE | REQUEST CHANGES | OVE-001–002 | Match | 1 Major (tooling files) / 1 Nit; production structure judged proportionate |
| review-maintainer | COMPLETE* | request-changes* | F1–F7 | Match | Findings F1–F4 retained; see note below |

\* The maintainer child returned its discipline's narrative envelope rather than the normalized schema (no Gate block with four counts; finding IDs `F1–F7` without the skill prefix). Content is complete — scope verified, findings carry evidence, remediation, verification — so it is treated as usable with the schema deviation recorded under Limitations; its content verdict maps to REQUEST CHANGES.

### Cross-review consensus and conflicts
- **Consensus:** Patch presentation, not behavior, is the merge risk — FUL-001/002/004/005/012 corroborated independently by principal (PRI-001/003/006), adversarial (ADV-003), overengineering (OVE-001), aislop (AIL-002), and maintainer (F1–F3). The validation-evidence gap at the shipped SHA is confirmed by four sources (PER-001, ADV-004, PRI-007, MAINT-F4) with the mitigating verified fact that feature files are byte-identical to the tested tree. All reviewers who assessed the core mechanism found it implemented per plan (dedup, capacity, cooldown, BUSY requeue, no `runWithCompactionsDisabled`, shutdown wiring).
- **Conflicts:** Severity spread on the evidence gap: maintainer F4 held "Request changes" weight; three sources rated Minor; kept Minor since feature-file identity is verified and the fix is one re-run. OVE-001 rated Major while STR-004 rated the related wrapper issue Minor; split into FUL-002 (Major, committed tooling) vs FUL-017 (Minor, entry-point duplication) because remediation differs. AIL-004 judged the duplicated threshold derivation a Nit while STR-001's related-but-distinct counting-unit finding is Minor — kept separate for that reason.
- **Deduplication decisions:** Merged: PRI-003+ADV-003+OVE-001+MAINT-F2 (same root cause, same observable patch-shape failure); PRI-006+AIL-002+MAINT-F3; PER-001+ADV-004+PRI-007+MAINT-F4; AIL-001+PRI-005; COV-004+TST-006; TST-003+TST-004; PRI-004+ADV-002+STR-001 (same unit-of-count root cause, jointly remediated by doc+javadoc). Kept distinct: FUL-006 (interrupt records cooldown — a behavior defect) vs FUL-008 (BUSY non-recording unpinned — a test gap); FUL-007 vs FUL-010 (different guards, different tests); FUL-013 vs FUL-015 (leak vs duplication); OVE-002+COV-003 merged only as a convenience grouping of pure-noise items, each independently optional.

### Maintainer/change-risk synthesis
- **Compatibility and user impact:** Public surface strictly additive and convention-following (Config field, validated setter, JMX pair, both yamls) — maintainer "What earned its risk"; adversarial's traceability matrix found every requirement's runtime path wired. First-ever CQL-mutable row in `system_views.settings`; no trunk test asserts immutability, so no existing-user breakage, but the precedent must be stated for community weighing (MAINT-F7 → Recommendations/Document).
- **Patch shape and recovery:** Feature → fixes → cooldown → tests is coherent, but FUL-004/005 make the middle a fixup; kill switch is clean (capacity 0 stops admissions without cancelling accepted work; reverting the four commits leaves no state) — maintainer.
- **Maintenance surface and local benefit:** Permanent cost is proportionate except FUL-002 (personal tooling in history) and the observability gap FUL-018 (machinery earns its keep only if operators can see it — maintainer visible-cost principle).
- **Residual acceptance risk:** Upstream acceptance turns on one history rewrite (FUL-001/002/004/005/012 folded together) plus one verification re-run (FUL-011). FUL-003 is the only behavioral item a reviewer could reasonably make gating.

### Performance synthesis
- No measured regressions exist (no benchmarks added — accepted plan decision). Static conclusions from review-performance: read-path overhead equivalent (one virtual call + int increment per purgeable deletion inside the existing traversal, clone only after admission, bounded queue/cooldown retention); the two bounded resource items are FUL-014 (BUSY polling churn) and micro-nits FUL-021/022; monitor contention judged negligible because triggering reads already scan >1000 deletions. Residual measurement gap: post-rebase integration never executed (FUL-011).

### Validation and test-gap synthesis
- Recorded: 09-05 eight-class matrix + build/Checkstyle (JDK 21), 09-06 cooldown 17-test queue class incl. 1,000 seeded property examples and 60-case integration, 09-07 shutdown pair ×2 with JaCoCo covering `finishCompactionsAndShutdown` lines 347/381/384 — children verified artifacts exist and read junit counts. Changed-line coverage 215/219, branches 100/108 (COV-003 residuals).
- Gaps to close before upstream: FUL-007 (system-keyspace exemption untested), FUL-008/009 (unpinned invariants), FUL-010 (timing-weak negatives), FUL-020 (log/allocation invariants), FUL-003's Error-path test, and FUL-011's single re-run at final SHA. One unexplained flake: `CompactTest` 4/4 fail ×3 then pass with the failure detail overwritten — re-run a few times at final SHA or recover the cause (COV-002).

## Validation and limitations
- **Evidence inspected:** Combined plan + five source plans; ten child review outputs; parent verification of commit authors (`git log --format=short`) and direct read of `TombstoneTriggeredCompactionManager.drain()`/`complete()` corroborating FUL-003 and FUL-006; child inspections of full diffs, all production files, eight test classes, JaCoCo artifacts, and validation logs.
- **Commands run:** Parent: `git log/diff/rev-parse/status`, `ls`, file reads. Children (attributed): extensive `git log/show/diff/rev-parse`, log reads, JaCoCo HTML/CSV reads — all reported exit 0; denied and reported truthfully by children: `git merge-base`, `git grep`, `git cat-file`, reflog, any test/build/aislop-CLI execution (multiple children attempted re-runs and were blocked by policy — no child fabricated a re-run).
- **Limitations:** (1) No child or parent re-executed any test suite; green status rests on implementer-run logs plus children's byte-identity verification of feature files against the tested tree. (2) review-maintainer returned a non-schema envelope (usable, deviation recorded above). (3) review-aislop ran its checklist manually, not the CLI. (4) The FUL-003 wedge is a reasoned trace (parent-verified code structure), not an observed failure. (5) Pre-rebase `5b6e1b0b72` object existence unconfirmed (cat-file blocked); FUL-011 rests on log text versus shipped SHA. (6) `system_views.settings` write-authorization behavior not exercised; assumed trunk superuser convention. (7) Some children saw degraded semantic search; all code claims were re-verified by direct reads.

## Recommendations
- **Integrate:** Fold all history fixes into one rewrite before publishing: real author (FUL-001), strip non-feature paths (FUL-002), squash the fixup with ASQ-001/002 replacement messages (FUL-004/005), JIRA ID + CHANGES.txt + conforming trailers (FUL-012), split the AutoRepair hunk (FUL-027). Then: drain() `Throwable`/finally hardening + Error test (FUL-003), interrupt-path cooldown fix (FUL-006), `tasks.close()` on mid-loop throw (FUL-013), logging once in the validated setter (FUL-016), one focused re-run at final SHA (FUL-011), and the four cheap test additions (FUL-007/008/009/010).
- **Document:** State the `system_views.settings` mutability precedent and the CompletedTasks semantics change in the PR description (MAINT-F6/F7); record the trigger counting unit in `tombstones.adoc` + `onPurgeableDeletion` javadoc (FUL-019); classify the CompactTest flake (COV-002); defer the BUSY-delay tuning with a note (FUL-014); consider the queue-depth/counter observability addition as a tracked follow-up (FUL-018).
- **Decline:** Second threshold, default-0 capacity, new JMH benchmark (accepted plan decisions — never reopened by any reviewer); default-on is approved and recorded; EOF-newline and defensive-branch coverage nits (FUL-028) as vestigial; FUL-021/022 micro-nits acceptable to skip given no measurable effect; STR-002's helper extraction is optional now and can ride a future compaction-touching change.
