<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Review: Full

Aggregate output of the review-full skill (ten-discipline fan-out) for the
CASSANDRA-18424 tombstone-paging branch. Generated 2026-09-08.

## Gate
- **Status:** BLOCKED
- **Verdict:** NOT ISSUED
- **+1:** NO
- **Blocker findings:** 1
- **Major findings:** 8
- **Minor findings:** 23
- **Nit findings:** 9

## Scope
- **Repository:** /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424
- **Plan:** /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/.plans/tombstone-paging-overarching-plan.md
- **Requested scope:** "all the changes on this branch that have diverged since upstream/trunk (i.e. from commit including CASSANDRA-18424 to HEAD)."
- **Resolved scope:** Six commits `1ff90eee47, 08e09677bf, 5ec2c3bfb0, 7ec7490bc7, 0b3a07c55e, 4decacfc0d` — git range `1ff90eee47^..HEAD`, basis merge commit `c103032107`. 108 files, +18,460/−95; production delta 29 files +736/−89 (`src/java`, `conf/cassandra.yaml`, `CHANGES.txt`); 27 test paths +8,035/−6; `5ec2c3bfb0` is supporting infra. Verified by adversarial, structural, performance, coverage, tests, aislop, and aislop-qualitative reviewers; no drift.
- **Change-set fingerprint:** HEAD tree `b3511d58199fef31f9bf6ddc7a39d89ebe8f4615`, HEAD `4decacfc0d2d539f4f0a5e80a7602f0bf0a2ac45` — verified matching by every completing reviewer.
- **Validation evidence:** Commit-message claims (110/120 targeted executions; 78 unit + 35 distributed; 13 distributed regressions + 372 existing + 2,160 fuzz queries; pager coverage 96.37%/93.12%; changed-code 98.8%/87.7%) attributed to commit messages, not re-executed (review-coverage, review-tests, review-adversarial, review-performance). Committed artifacts corroborated: `./logs/` run and coverage logs, `build/paging-coverage/` JaCoCo reports, `logs/paging-performance-*.log` benchmark results (review-coverage, review-performance).

## Findings

### Blocking
#### FUL-001 — Checkpointed partial page accepted by the digest-mismatch full-data repair path is reported as a complete page
- **Severity:** Blocker
- **Confidence:** Medium (code-traced by review-adversarial; decisive premises independently re-verified by the orchestrator)
- **Sources:** review-adversarial ADV-001
- **Requirement/invariant:** Plan invariant 1 — a paged read with a checkpoint is never reported as complete; partial results are never returned as complete.
- **Evidence:** Replica emits `TOMBSTONE_PAGE_CHECKPOINT` + `TOMBSTONE_FAIL` (`src/java/org/apache/cassandra/db/ReadCommand.java:410-424` via `ReadCommandVerbHandler.java:129`). On digest mismatch, `AbstractReadExecutor.java:455-462` calls `readRepair.startRepair(...)`; `AbstractReadRepair.java:152-183` consumes the command's checkpoint (`:175`) and installs `TombstonePageCheckpointPolicy.ACCEPT`, so `ReadCallback.java:242-251` strips `TOMBSTONE_FAIL` from the truncated response. `setTombstonePageContinuation` has exactly one production call site (`DigestResolver.java:109`, digest-match path); `DataResolver` never re-establishes the cursor (grep: only that site plus the declaration; orchestrator-verified). `Pager.onClose` derives `hitTombstoneLimit` only from the pager's own `pageQueries` (`AbstractQueryPager.java:219-253`), so `exhausted = pageLimits.isExhausted(rowCounter)` (`:274`, orchestrator-verified) marks the truncated page exhausted and `SinglePartitionPager.state()` publishes no continuation.
- **Impact:** With a digest mismatch (uneven replicas or `read_repair='blocking'`) over a tombstone run exceeding the threshold, the client gets a short final page with no continuation and silently loses live rows after the run — no exception, no metric distinguishing it from real exhaustion.
- **Remediation:** Propagate the accepted response's checkpoint in the repair/DataResolver path onto the command or back to the pager (as `DigestResolver.getData()` does), or use `REJECT` in `AbstractReadRepair` so the read fails typed instead of truncating.
- **Verification:** Distributed regression: tombstone-heavy partition on every replica, forced digest mismatch, `0 < fetchSize < LIMIT`; assert the full expected row set. Existing `BlockingReadRepairTest.fullDataRepairTransfersCheckpointBeforeLateLocalRead` uses empty responses and cannot expose this.

### Non-blocking
#### FUL-002 — Short-read-protection refill can loop forever when a checkpointed refill yields no rows
- **Severity:** Major | **Confidence:** Medium
- **Sources:** review-adversarial ADV-002
- **Requirement/invariant:** Plan invariant 5 — checkpoint-driven retries must make clustering progress or fail safely.
- **Evidence:** `ShortReadRowsProtection.java:105-146` — three termination guards are disabled while `lastResponseTombstoneLimitedCount > 0`; `lastClustering` advances only via `applyToRow` (markers do not advance it); `makeFetchAdditionalRowsReadCommand` (`:193-207`) rebuilds a byte-identical command; `MoreRows.extend` (`MoreRows.java:46-49`) has no iteration cap; refill accepted at `ShortReadPartitionsProtection.java:194-203` without a `readQuorum > 1` requirement.
- **Impact:** A refill window containing only range-tombstone markers returns zero Rows; the coordinator reissues identical replica reads indefinitely on the client request thread.
- **Remediation:** Require the cursor or response checkpoint clustering to advance between refills; fail with `READ_TOO_MANY_TOMBSTONES` when it does not.
- **Verification:** New `ShortReadProtectionTombstonePagingTest` case with marker-only refill window; assert bounded `shortReadProtectionRequests` and termination.

#### FUL-003 — Graceful checkpoint pages increment `tombstoneFailures` and bypass `tombstoneWarnings`
- **Severity:** Major | **Confidence:** High
- **Sources:** review-adversarial ADV-003 (Major/High), review-structural STR-008 (Nit/High — severity disagreement retained)
- **Requirement/invariant:** Operational observability of a default-on feature.
- **Evidence:** `ReadCommand.java:793-846` — `metric.tombstoneFailures.inc()` at `:803` runs before the paging branch, so successful checkpoint pages count as failures; `tombstoneWarnings` at `:873-886` is gated against the paging threshold when paging is on (`:713-715`), so heavy reads record no warning either. Corroborated independently by review-structural.
- **Impact:** With paging on by default, `tombstoneFailures` becomes a false-positive alert signal, and genuine tombstone-heavy reads stop appearing in `tombstoneWarnings`; `nodetool tablestats` and dashboards change meaning.
- **Remediation:** Do not bump `tombstoneFailures` on the paging branch; gate `tombstoneWarnings` against the failure threshold.
- **Verification:** Assert `tombstoneFailures` unchanged across a graceful checkpoint page while a warning/dedicated counter advances.

#### FUL-004 — Checkpoint-stopped execution is reachable from the range pager, which never consumes checkpoints
- **Severity:** Major | **Confidence:** High (path verified; consequence argued, not executed)
- **Sources:** review-structural STR-001
- **Requirement/invariant:** Plan invariants 1 and 6 — unsupported paths fail safe.
- **Evidence:** `AbstractQueryPager.java:95-141` passes the global `tombstonePagingThreshold` into `executeLocally` for every subclass; `PartitionRangeQueryPager.java:37-128` never marks ownership and never populates `pageQueries`; the replica-side stop/record path (`ReadCommand.java:745-780, 789-846`) has no ownership check; `Pager.onClose` consumes checkpoints only via `pageQueries` (`AbstractQueryPager.java:219-253`), so a stopped range fetch is accounted as a complete page. The `TOMBSTONE_FAIL` fail-safe exists only on message paths.
- **Impact:** On local/range execution, rows after an over-threshold tombstone run can become unreachable (cursor advances past them) or the pager loops — on trunk the same read threw `TombstoneOverwhelmingException`, making this a silent-truncation regression risk.
- **Remediation:** Opt the range pager out: a protected threshold hook returning `Config.TOMBSTONE_PAGING_DISABLED` for `PartitionRangeQueryPager`, or gate the stop/record on checkpoint ownership. One-line-class fix, no new abstraction.
- **Verification:** Local range-query regression over an over-threshold tombstone run asserting typed failure or forward cursor progress; assert `tombstoneLimitedRow()` is null after each local range fetch.

#### FUL-005 — Default-on paging removes the tombstone fail-fast breaker for range-tombstone-bound-dominated scans and amplifies per-marker cost
- **Severity:** Major | **Confidence:** High
- **Sources:** review-performance PER-001
- **Requirement/invariant:** `tombstone_failure_threshold` as the default replica-side circuit breaker (`local_read_size_fail_threshold` is disabled by default, `conf/cassandra.yaml:2347-2348`; no iteration timeout in this tree's `ReadExecutionController.java:33-243`).
- **Evidence:** `tombstone_paging_threshold` defaults to 100000 — enabled; every replica read executes with it (`ReadCommandVerbHandler.java:57-61`, `StorageProxy.LocalReadRunnable`). In `countTombstone`, bound/boundary markers over the threshold take the "continuing iteration" branch and never stop or throw (`ReadCommand.java:818-833`), while baseline threw unconditionally at threshold; each marker additionally pays `toCQLString()`, tracing, `tombstoneFailures.inc()`, and `String.format` warn work (`:798-842`). The shipped benchmark covers only row-tombstone checkpoints (`PagingAcrossTombstonesPerformanceTest.java:83-88`).
- **Impact:** Range-delete-storm partitions that trunk aborted at ~100k tombstones now scan to partition exhaustion — unbounded replica CPU/disk/heap, replica continuing after client timeout, and O(tombstones) counter/logging work.
- **Remediation:** (1) A hard ceiling for the continue-on-marker case (stop/throw beyond a bounded margin) so scan work stays threshold-proportional; (2) hoist warn-side formatting/tracing/metric work to once per read on first crossing.
- **Verification:** Bounded regression with ≥ threshold+margin range-tombstone bound markers; assert replica stop and O(1)-per-read metric/log deltas versus baseline fail-fast.

#### FUL-006 — Empty `@Test` reports as passing while covering a named mixed-version failure mode
- **Severity:** Major | **Confidence:** High
- **Sources:** review-tests TST-001
- **Requirement/invariant:** Plan invariant 7 (old coordinators safe alongside new replicas); the method's own Javadoc names the mismatch-loop risk.
- **Evidence:** `MixedModePagingAcrossTombstonesTest.mixedModeMismatchReturn` (`test/distributed/.../upgrade/MixedModePagingAcrossTombstonesTest.java:77-80`) body is `//pass` — no cluster, query, or assertion; it passes unconditionally and counts toward the "13 distributed regressions" claim at `7ec7490bc`. The branch's own audit flagged it (`.debug/paging-coverage-findings.md:117-119`) and it remains at HEAD.
- **Impact:** The mixed-mode mismatch scenario has no deterministic regression — a vacuous green that violates the plan's "intentional skips must be visible" rule.
- **Remediation:** Implement reusing the `singleUpgradeToCurrentFrom(v50)` + asymmetric-tombstone pattern of `MixedVersionPagingAcrossTombstonesTest.upgrade(...)`, or convert to a visible `@Ignore` with reason.
- **Verification:** Mutation probe: accept a mismatched continuation when one replica reports under-limit — the implemented test must fail.

#### FUL-007 — Four commit bodies narrate defects that existed only on this unmerged branch
- **Severity:** Major | **Confidence:** High
- **Sources:** review-aislop-qualitative ASQ-001
- **Requirement/invariant:** Write history as though the final state were the first version; AGENTS.md/COMMIT.md commit conventions.
- **Evidence:** `git log 1ff90eee47^..HEAD`: `08e09677bf`, `7ec7490bc7`, `0b3a07c55e`, `4decacfc0d` each open by narrating defects introduced by this branch's own earlier commits ("could make…", "even when replicas agreed", "could reuse stale cursors", "After the threshold fix…").
- **Impact:** After merge/squash, trunk `git log` readers are told shipped behavior was broken, with contrasts pointing at intermediate commits that will not exist.
- **Remediation:** Squash the fixups into `1ff90eee47` or replace each narrating paragraph with capability text (full replacements drafted in the review-aislop-qualitative output). Human performs any history rewrite.
- **Verification:** Every "could"/"after the fix" sentence must still make sense with intermediate commits removed.

#### FUL-008 — MBean Javadoc describes a boolean flag for a method that returns an int threshold
- **Severity:** Major | **Confidence:** High
- **Sources:** review-aislop-qualitative ASQ-002
- **Requirement/invariant:** Javadoc states the actual contract.
- **Evidence:** `StorageServiceMBean.java:1081-1085` — "Gets whether the node is configured to gracefully page across tombstones" on `public int getTombstonePagingEnabled()`; implementation returns `DatabaseDescriptor.getTombstonePagingThreshold()` (`StorageService.java:4852-4863`); `-1` is the disable sentinel (`Config.java:635-640`).
- **Impact:** Operators cannot act on the contract: "whether" implies boolean, and the int semantics and `-1` sentinel are unstated on a permanent JMX surface.
- **Remediation:** State the int contract (threshold; `-1` disables) in both Javadocs; rename per FUL-014.
- **Verification:** Javadoc matches implementation at `StorageService.java:4852-4863`.

#### FUL-009 — CHANGES.txt entry uses internal mechanism words a release reader cannot act on
- **Severity:** Major | **Confidence:** High
- **Sources:** review-aislop-qualitative ASQ-003
- **Requirement/invariant:** Release notes state observable user-facing effect.
- **Evidence:** `CHANGES.txt:7` — "Implement graceful paging across tombstones with short-circuit on paging rather than throwing TombstoneOverwhelmingExceptions" — "short-circuit on paging" is internal mechanism vocabulary appearing on no user surface; copied verbatim from `1ff90eee47`'s subject.
- **Impact:** An operator comparing releases learns the name of a failure no longer raised but not what replaces it or which setting governs it (`tombstone_paging_threshold`).
- **Remediation:** Replace with the observable statement drafted in review-aislop-qualitative (paged reads stop at `tombstone_paging_threshold` and resume from a continuation; unpaged reads still abort at `tombstone_failure_threshold`).
- **Verification:** Replacement asserts only facts at `ReadCommand.java:808-816` / `Config.java:635-640`.

#### FUL-010 — `tombstone_paging_threshold` has no validation on the config or JMX path
- **Severity:** Minor | **Confidence:** High | **Sources:** review-adversarial ADV-004
- **Requirement/invariant:** Plan: separate paging threshold, default 100000, `-1` disables.
- **Evidence:** `Config.java:637-641` plain int; `DatabaseDescriptor.java:3421-3428` stores any int; `StorageService.java:4851-4860` forwards unvalidated. Only `-1` is treated as disabled, so `0`/`1` mean one-tombstone pages.
- **Impact:** An operator typo produces pathological empty pages with client spin and per-page warn logging.
- **Remediation:** Normalize any value `<= 0` other than `-1` to disabled (or reject) in `DatabaseDescriptor.setTombstonePagingThreshold` and on config load.
- **Verification:** Extend `StorageServiceServerTest:132-138` with `0`/`1` bounded-read-cost cases.

#### FUL-011 — Local-read copy guard is asymmetric, so a rejected checkpoint can still reach the pager
- **Severity:** Minor | **Confidence:** Low | **Sources:** review-adversarial ADV-005
- **Requirement/invariant:** Plan invariant 6 — unsupported paths never accepted.
- **Evidence:** `AbstractReadExecutor.java:174-180` copies for the local read only when `compareTombstonePageContinuations && readCommand == command`; otherwise `executeLocally` writes checkpoint state onto the shared command (`ReadCommand.java:593-596, 773-777`), and the pager reads `tombstoneLimitedRow()` with no policy check (`AbstractQueryPager.java:221-239`), so a `REJECT`-policy checkpoint can drive the cursor; conversely a legit local checkpoint is dropped if `maybeAllowOutOfRangeReads` returned a copy.
- **Impact:** Narrow (realistically top-k/out-of-range with an owned command): adoption of a rejected checkpoint or silent loss of a valid one.
- **Remediation:** Adopt checkpoints in the pager only from non-`REJECT`-accepted responses, or always copy for local reads when the command is a checkpoint owner.
- **Verification:** `QueryPagerTest` case asserting a `REJECT`-policy local checkpoint never advances `lastReturned`.

#### FUL-012 — DigestResolver widens supported checkpoints to read quorum 1, diverging from the plan
- **Severity:** Minor | **Confidence:** High | **Sources:** review-adversarial ADV-006
- **Requirement/invariant:** Plan: supported reconciliation requires read quorum > 1; CL ONE listed as fail-safe.
- **Evidence:** `DigestResolver.java:56-67` returns true with `readQuorum() == 1` when `canResumeTombstonePage()` holds, selecting COMPARE at `AbstractReadExecutor.java:93-101` and stripping `TOMBSTONE_FAIL` at `ReadCallback.java:244-251`; sibling predicates keep `>1` (`DataResolver.java:95-101`) or drop it (`ShortReadPartitionsProtection.java:194-196`).
- **Impact:** At CL ONE a single replica's checkpoint becomes authoritative for the pager cursor — contract drift from the stated invariant; no wrong result traced.
- **Remediation:** Restrict `canResumeTombstonePage()` to `readQuorum() > 1`, or record CL ONE as supported in the plan/commit text and add a CL ONE regression.
- **Verification:** A CL ONE checkpoint case (currently untested).

#### FUL-013 — Unbounded array allocation from wire-supplied count in the continuation deserializer
- **Severity:** Minor | **Confidence:** Low | **Sources:** review-adversarial ADV-007
- **Requirement/invariant:** Defensive parsing of new message parameters.
- **Evidence:** `TombstonePageContinuation.java:65-71` — `new ByteBuffer[in.readUnsignedVInt32()]` allocates from a supplied 32-bit count with no cap; the constructor precondition (`:37-41`) runs only after allocation; nullable component buffers reach `Clustering.make`.
- **Impact:** A malformed internode message can trigger a large allocation or downstream failure on the messaging path (partially trusted boundary).
- **Remediation:** Cap the component count against `metadata().clusteringColumns().size()` before allocating.
- **Verification:** `TombstonePageContinuationTest` negative case feeding a bogus count.

#### FUL-014 — JMX accessor named `…PagingEnabled` exposes an int threshold
- **Severity:** Minor | **Confidence:** High | **Sources:** review-adversarial ADV-008, review-structural STR-006 (Nit vs Minor disagreement retained; higher taken)
- **Requirement/invariant:** Consumer-hostile surface on a permanent operational contract.
- **Evidence:** `StorageServiceMBean.java:1080-1084` / `StorageService.java:4851-4861`: `int getTombstonePagingEnabled()` / `setTombstonePagingEnabled(int)`; neighboring knob follows `setTombstoneFailureThreshold(int)` naming. JMX names are unrenamable once released.
- **Impact:** Operators script against a boolean-sounding name that returns `100000`/"threshold" values with a `-1` sentinel.
- **Remediation:** Rename to `get/setTombstonePagingThreshold` before release.
- **Verification:** Updated `StorageServiceServerTest` exercises the renamed accessors.

#### FUL-015 — Per-execution mutable checkpoint state embedded in the query value object with a manual, distributed ownership protocol
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-structural STR-002
- **Evidence:** `ReadCommand.java:167-170, 392-442, 595-596, 773-777` (sticky owner flag, payload reset without flag reset); owner propagation across every copy factory (`SinglePartitionReadCommand.java:420-443, 485-500, 1369-1386`); reference-equality defensive copies (`AbstractReadExecutor.java:174-179`, `AbstractReadRepair.java:103-112`) added by later commits to fix leaked state; consumers across five classes.
- **Impact:** Protocol correctness depends on discipline spread across five classes; a future local execution site that forgets `copy()` reintroduces the stale/leaked-checkpoint bug class.
- **Remediation:** Consolidate the three fields plus accessors into one documented state holder owned by `ReadCommand` (no new indirection the plan declined).
- **Verification:** Grep audit that every writer/consumer routes through the holder; existing regressions re-run unchanged.

#### FUL-016 — "Checkpoint is safe" predicate duplicated with subtle variation across four sites
- **Severity:** Minor | **Confidence:** High | **Sources:** review-structural STR-003
- **Requirement/invariant:** Plan invariant 7 (predicates deliberately differ, but drift must be reviewable).
- **Evidence:** `DataResolver.java:95-100`, `DigestResolver.java:56-68` (near-identical cores, see FUL-012), `ShortReadPartitionsProtection.java:188-193`, replica-side emission at `ReadCommand.java:410-424`.
- **Impact:** Any future exclusion must be edited in 4+ coordinated places or the replica emits checkpoints the coordinator misclassifies.
- **Remediation:** Respect the plan's declined shared helper; add reciprocal Javadoc at each site naming siblings and the differing conjunct.
- **Verification:** Cross-reference comments present at all four sites; compile-only.

#### FUL-017 — Dead checkpoint policy `USE_LOCAL_COMMAND` retained in production enum after replacement
- **Severity:** Minor | **Confidence:** High | **Sources:** review-structural STR-004 (corroborated by review-tests TST-005 observation)
- **Evidence:** `ReadCallback.java:68-74, :300-312` — the `USE_LOCAL_COMMAND` branch is reachable only from tests (`DigestResolverTest.java:129, 457, 527`); no production constructor passes it; plan text says ACCEPT/REJECT "replaced" it.
- **Impact:** A maintainer can construct a callback expecting legacy local-checkpoint semantics through a path no production flow exercises.
- **Remediation:** Delete the constant and collapse the `failOnCheckpoint` parameter, migrating the three test usages; or document it as a deliberate test hook.
- **Verification:** `DigestResolverTest` passes after migration; zero non-test references.

#### FUL-018 — Sentinel-int threading of the paging mode with inconsistent use of the named constant
- **Severity:** Minor | **Confidence:** High | **Sources:** review-structural STR-005
- **Evidence:** `Config.TOMBSTONE_PAGING_DISABLED = -1` defined at `Config.java:637-641`, yet bare `-1` appears at `ReadQuery.java:166-190`, `ReadCommand.java:567-593, 685-688`, and one call in `SinglePartitionReadQuery.java:226-260` while the same file also uses the named constant.
- **Impact:** Mixed literal/constant forms invite silent divergence at future call sites.
- **Remediation:** Use the named constant at every internal call site; no API change.
- **Verification:** Grep for `executeLocally(.* -1` returns zero; Checkstyle passes.

#### FUL-019 — Same unsupported-checkpoint condition typed three different ways across coordinator paths
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-structural STR-007
- **Requirement/invariant:** Plan invariant 1 — single typed failure (`ReadFailureException` / `READ_TOO_MANY_TOMBSTONES`).
- **Evidence:** `DigestResolver.java:131-137` throws the typed `ReadFailureException`; `AbstractReadRepair.java:157-176` throws `TombstoneOverwhelmingException` or `IllegalStateException` for the same condition.
- **Impact:** Client-visible error shape depends on the resolving path; `IllegalStateException` is unclassifiable by downstream components.
- **Remediation:** Funnel both `AbstractReadRepair` branches through the typed failure the digest path builds.
- **Verification:** A read-repair-triggering checkpoint case asserts the same typed failure.

#### FUL-020 — Unconditional `ReadCommand.copy()` per coordinator-local data read even when the feature is disabled
- **Severity:** Minor | **Confidence:** High | **Sources:** review-performance PER-002
- **Evidence:** `AbstractReadExecutor.java:177-178` copies for every local data read when `compareTombstonePageContinuations` is true; that flag's `readQuorum() > 1` disjunct (`DigestResolver.java:56-68`) bypasses both the owner flag and the disabled-threshold check, so QUORUM-class coordinator-replica reads pay an extra `SinglePartitionReadCommand.copy()` even at `tombstone_paging_threshold: -1`. Committed benchmark bounds the enabled-mode delta at ~4% median (`logs/paging-performance-20260907T150653Z.log:17-18,37-38`).
- **Impact:** Small but universal allocation on the hottest read path, unmeasured against trunk.
- **Remediation:** Gate the copy additionally on the feature being enabled.
- **Verification:** Allocation profile (JFR `jdk.ObjectAllocationSample`) a clean QUORUM workload at `-1` and confirm the copy is gone.

#### FUL-021 — Distributed regression classes not re-executed after the final production edit at HEAD
- **Severity:** Minor | **Confidence:** High | **Sources:** review-coverage COV-001
- **Evidence:** Class-file mtimes show `ReadCommand.class`/`Config.class` recompiled 2026-09-07 20:45-46Z, after the last full distributed regression pass (14:56Z log); only the 78-unit + 35-distributed gates (Fuzz, MixedConfig) ran post-recompile; mixed-version suites last ran Sep 6/7 earlier. The two new typed-failure regressions from `4decacfc0d` have passing records only against pre-20:45Z bytecode.
- **Impact:** The final threshold-selection delta has no passing execution on record for transient/short-read/interaction/upgrade classes.
- **Remediation:** One final `run_tests.sh` pass over PagingAcrossTombstonesDistributed/Transient/Interaction, ShortReadProtectionTombstonePaging, MixedConfig, MixedVersion + upgrade fuzz against the HEAD build.
- **Verification:** Exit 0, Skipped: 0, logged.

#### FUL-022 — No changed-line coverage measurement reflects the final production state
- **Severity:** Minor | **Confidence:** High | **Sources:** review-coverage COV-002
- **Evidence:** All JaCoCo reports predate the 20:46Z recompile; none quantifies lines changed by `4decacfc0d`; independent CSV snapshot (`refresh-20260907-current-only/report.csv`) shows e.g. `ReadCommand` 145/172 lines, 54/86 branches for one selection.
- **Impact:** Branch coverage of the final delta (threshold selection, opt-in rejection, typed-failure branches) is unquantified.
- **Remediation:** Re-run coverage on the final selection and regenerate the changed-lines summary.
- **Verification:** Report whose exec and class dir both postdate the final compile, recorded in `.debug/`.

#### FUL-023 — Three MixedMode "regressions" are verbatim trunk copies with the feature never enabled
- **Severity:** Minor | **Confidence:** High | **Sources:** review-tests TST-002
- **Evidence:** `MixedModePagingAcrossTombstonesTest` methods at `:83-101`, `:104-125`, `:132-164` replicate trunk `CompactStoragePagingTest` / `MixedModeReadRepairDeleteTest` / `ReadDigestConsistencyTest` tests with no tombstone thresholds; audit concurs (`.debug/paging-coverage-findings.md:117-119`).
- **Impact:** Zero incremental defect detection; inflates the claimed regression suite.
- **Remediation:** Delete the copies or parameterize with low thresholds (pattern: `PagingTest` thresholds `{-1, 2}`), asserting counter deltas for anti-vacuity.
- **Verification:** Enabled-threshold variants produce a `tombstoneFailures`-style delta the disabled copies cannot.

#### FUL-024 — Unit-layer paging loops lack a finite budget; a cycling cursor manifests as a hang, not an assertion
- **Severity:** Minor | **Confidence:** High | **Sources:** review-tests TST-003
- **Requirement/invariant:** Plan test rules (finite budgets) and invariant 2.
- **Evidence:** Distributed harness enforces budgets/repeated-state rejection (`TombstonePagingTestUtil.java:42-72`, esp. `:66`); unit validators do not (`AbstractPagingValidator.java:1217-1232`, `:1151-1179`), and `seenCellState` silently dedupes re-served rows (`:306-345`).
- **Impact:** A unit-path-only rewind bug surfaces as a build timeout — slow, ambiguous diagnosis.
- **Remediation:** Port the `readPages` discipline (page cap, repeated-state assertion, strictly-advancing mark) into both validators.
- **Verification:** Mutate checkpoint adoption to reuse prior state; the unit suite must assert-fail, not time out.

#### FUL-025 — `testClientWarnings` asserts an exact count of NoSpamLogger-gated warnings — order/window-sensitive
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-tests TST-004
- **Requirement/invariant:** Determinism.
- **Evidence:** `PagingAcrossTombstonesTest.java:702-704` asserts exactly 1 warning while the premise is NoSpamLogger suppression (`:668`); sibling tests lower thresholds on the same logger keys (`:624`, `:682`).
- **Impact:** Flaky pass/fail under method-ordering or timing changes.
- **Remediation:** Assert warning content plus suppression behavior explicitly (two-phase baseline/second-query assertion or state reset).
- **Verification:** 20 repeated runs with varied seeds, stable green.

#### FUL-026 — The `ACCEPT` checkpoint policy has no direct `ReadCallback`-level unit test; tests still drive the legacy `USE_LOCAL_COMMAND`
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-tests TST-005 (corroborates FUL-017)
- **Requirement/invariant:** Plan/`4decacfc0d` claim that ACCEPT strips only the local `TOMBSTONE_FAIL` copy.
- **Evidence:** Production still branches on `USE_LOCAL_COMMAND` (`ReadCallback.java:68-73, :306`); the only checkpoint-policy unit tests construct callbacks with `USE_LOCAL_COMMAND` (`DigestResolverTest.java:457, :527`); no ACCEPT-level strip/preserve assertion.
- **Impact:** An ACCEPT strip regression is caught, if at all, only by slower fuzz/mixed-version suites.
- **Remediation:** Add two `ReadCallback` unit cases with `policy = ACCEPT` (strip on local checkpointed data; preserve typed failure under `REJECT` for digest).
- **Verification:** Mutation probe — no-op the ACCEPT strip must fail the new test.

#### FUL-027 — Core commit subject bundles mechanism words into a 20-word line and carries a "reviewed by TBD" placeholder
- **Severity:** Minor | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-004
- **Requirement/invariant:** Commit convention (`patch by …; reviewed by … for CASSANDRA-#####`).
- **Evidence:** `1ff90eee47` subject bundles mechanism terms with grammatically wrong plural "TombstoneOverwhelmingExceptions"; trailer "reviewed by TBD"; no body. (review-aislop-qualitative also noted missing `patch by`/`reviewed by` trailers on `08e09677bf` and `5ec2c3bfb0`.)
- **Impact:** Permanent log carries placeholder text and implementation phrasing.
- **Remediation:** Rewrite per the drafted subject/body; fill the reviewer placeholder before merge.
- **Verification:** `git log` shows trailer matching the project convention.

#### FUL-028 — Commit bodies carry audit and bookkeeping blocks referencing the branch's own review rounds
- **Severity:** Minor | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-005
- **Evidence:** Validation/Tests paragraphs with run counts and coverage percentages ("the preceding audit", "Earlier recorded runs", "Include plans, review notes…") in `4decacfc0d`, `0b3a07c55e`, `7ec7490bc7` — resolvable only against intermediate branch states.
- **Impact:** Permanent log carries unverifiable process bookkeeping.
- **Remediation:** Remove bookkeeping; keep one test-scope sentence per commit; percentages belong in review artifacts.
- **Verification:** Amended messages contain no run counts or coverage percentages.

#### FUL-029 — ReadQuery default `executeLocally` Javadoc narrates design rationale instead of contract
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-aislop-qualitative ASQ-006
- **Evidence:** `ReadQuery.java:169-176` "we fall back on throwing…" narrative; `:187` `@param` misattributes the short-circuiting actor and `@return` belongs to the other overload (default passes `-1`, `:179`).
- **Remediation:** Replace with the drafted contract text.
- **Verification:** Behavior at `ReadQuery.java:177-190`, `ReadCommand.java:722-724, 808-816`.

#### FUL-030 — `tombstoneLimitedRow` field comment omits the coordinator-side writer
- **Severity:** Minor | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-007
- **Evidence:** `ReadCommand.java:166-167` comment covers only the local stop; the coordinator also writes via `setTombstonePageContinuation` (`:403-408`, called from DigestResolver).
- **Remediation:** Drafted replacement naming both writers.
- **Verification:** Writers at `:403-408, 585-588, 772-779`.

#### FUL-031 — RT-boundary checkpoint comment buries its binding constraint in a clause pile
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-aislop-qualitative ASQ-008
- **Evidence:** `ReadCommand.java:820-822` — the actual rule ("never checkpoint on a range tombstone bound or boundary", guard at `:823`, RTBoundCloser dependency) is hard to extract; an editor could move the guard and break RT closure.
- **Remediation:** Drafted two-sentence replacement.
- **Verification:** Guard at `:823`.

#### FUL-032 — DataLimits base `Counter.onClose(boolean)` comment asserts behavior the base class does not implement
- **Severity:** Minor | **Confidence:** Medium | **Sources:** review-aislop-qualitative ASQ-009
- **Evidence:** `DataLimits.java:367-371` comment describes group-completion semantics while the base ignores the parameter; behavior exists only in `GroupableCounter` (`:1071-1086`).
- **Remediation:** Drafted replacement stating the subclass obligation.
- **Verification:** Override gate at `:1078-1086`.

#### FUL-033 — Cosmetic leftovers: stray blank line and unguarded debug string build
- **Severity:** Nit | **Confidence:** High | **Sources:** review-adversarial ADV-009
- **Evidence:** `ReadCommand.java:635-636` double blank line; `AbstractQueryPager.java:255` string concatenation inside `logger.debug` (orchestrator-confirmed at `:255`).
- **Remediation:** Remove the blank line; guard or parameterize the debug call.
- **Verification:** `.build/check-code.sh`.

#### FUL-034 — Artifact-to-revision provenance rests on inference
- **Severity:** Nit | **Confidence:** High | **Sources:** review-coverage COV-003
- **Evidence:** All six committer timestamps identical (rebase); every test log predates it; content-identity corroborated by class-count and XML cross-checks but not bound to SHAs.
- **Remediation:** Record the validated HEAD SHA in each `.debug/` note after the final green run (pairs with FUL-021).
- **Verification:** N/A.

#### FUL-035 — "Six unit cases intentionally skipped" not reproducible at HEAD
- **Severity:** Nit | **Confidence:** Medium | **Sources:** review-coverage COV-004 (corroborated by review-adversarial limitation)
- **Evidence:** No `@Ignore` in the new paging test files; final gates report Skipped: 0; likely referent is pre-existing skips recorded in `.debug/mixed-version-tombstone-paging.md`.
- **Remediation:** Cite the six skips by class or drop the clause at next message edit.
- **Verification:** N/A.

#### FUL-036 — Long-suite fuzz logger sleep and broken repro hint
- **Severity:** Nit | **Confidence:** High | **Sources:** review-tests TST-006
- **Evidence:** `PagingFuzzTest.java:254-255` `Thread.sleep(250)` (test-rule violation, failure path only); repro hint prints `cassandra.test.env=` but the consumed var is `cassandra.test.seed` (`:231`, `RandomHelpers.java:50-63`).
- **Remediation:** Remove the sleep; correct the hint string.
- **Verification:** N/A.

#### FUL-037 — Reflective jamm `Instrumentation` propagation is an undocumented deviation from the "no bytecode injection" commitment
- **Severity:** Nit | **Confidence:** Medium | **Sources:** review-tests TST-007
- **Evidence:** `AbstractCluster.java:155-225` reflects the outer JVM's jamm Instrumentation into each instance's private jamm copy — a harness fix, not checkpoint injection, but undeclared in the plan's test-rules text.
- **Remediation:** Document the exception; keep the null-safe fallback explicit.
- **Verification:** N/A.

#### FUL-038 — Pager catch-block comment uses colloquial register
- **Severity:** Nit | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-010
- **Evidence:** `AbstractQueryPager.java:243` "…what went down" (orchestrator-confirmed at `:243`).
- **Remediation:** Drafted STE replacement in review-aislop-qualitative.
- **Verification:** N/A.

#### FUL-039 — `tombstoneLimitedRow` Javadoc is passive and padded
- **Severity:** Nit | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-011
- **Evidence:** `ReadQuery.java:192-195`.
- **Remediation:** Drafted active-voice replacement provided.
- **Verification:** N/A.

#### FUL-040 — `ReadQuery.toCQLString` Javadoc is vague
- **Severity:** Nit | **Confidence:** High | **Sources:** review-aislop-qualitative ASQ-012
- **Evidence:** `ReadQuery.java:316-322` "more easily human-readable … and other data".
- **Remediation:** Drafted replacement provided.
- **Verification:** N/A.

#### FUL-041 — Infra commit subject is terse and informal
- **Severity:** Nit | **Confidence:** Medium | **Sources:** review-aislop-qualitative ASQ-013
- **Evidence:** `5ec2c3bfb0` "Add llm supporting infra", no body.
- **Remediation:** Drafted subject/body provided.
- **Verification:** N/A.

## Discipline-specific analysis
### Reviewer gate table
| Required reviewer | Child status | Child verdict | Findings used | Scope/fingerprint | Result or blocker |
|---|---|---|---|---|---|
| review-principal-engineer | Unusable — empty child result | — | None | Not assessed | Completed with no delivered result; unrecoverable from history |
| review-adversarial | COMPLETE | BLOCK | 9 (ADV-001…009) | Match | 1 Blocker, 2 Major, 4 Minor, 2 Nit |
| review-structural | COMPLETE | REQUEST CHANGES | 8 (STR-001…008) | Match (flagged plan drift: `USE_LOCAL_COMMAND` survives though plan says replaced) | 1 Major, 6 Minor, 1 Nit |
| review-performance | COMPLETE | REQUEST CHANGES | 2 (PER-001…002) | Match | 1 Major, 1 Minor |
| review-coverage | COMPLETE | APPROVE | 4 (COV-001…004) | Match | 2 Minor, 2 Nit; validated recorded evidence internally, re-execution denied by sandbox |
| review-tests | COMPLETE | REQUEST CHANGES | 7 (TST-001…007) | Match (corrected packet test-path errors: QueryPagerTest in `service/`, PagingFuzzTest in long suite, upgrade tests under `distributed/upgrade/`) | 1 Major, 4 Minor, 2 Nit |
| review-aislop | COMPLETE | APPROVE | 0 | Match | Zero findings reflect a NON-executed scan (engine blocked by permission policy) — APPROVE carries no signal; see limitations |
| review-aislop-qualitative | COMPLETE | REQUEST CHANGES | 13 (ASQ-001…013) | Match (noted plan file untracked — immaterial) | 3 Major, 6 Minor, 4 Nit |
| review-overengineering | Unusable — child result truncated in transport | — | None | Not assessed | 9,650-char result replaced by placeholder; unrecoverable from history |
| review-maintainer | Unusable — empty child result | — | None | Not assessed | Completed with no delivered result; unrecoverable from history |

Aggregate status is BLOCKED per the gate rule: three required reviewers (principal-engineer, overengineering, maintainer) delivered no usable envelope. This reflects orchestrator transport loss, not reviewer judgment, and no substitute was launched.

### Cross-review consensus and conflicts
- **Consensus:** Mixed-version fail-safe design is sound — review-adversarial "could not break" the `TOMBSTONE_FAIL` old-coordinator guard, review-coverage's traceability matrix marks invariant 7 Covered, review-tests verified `oldCoordinatorRejectsNewCheckpoint`. Checkpoint-state design risk corroborated across review-structural STR-002 and review-adversarial ADV-005 (different reference-equality copy sites added by two later commits). `USE_LOCAL_COMMAND` retention corroborated by STR-004 and TST-005. Recorded validation evidence is internally consistent but unbound to HEAD (COV-001/003, review-tests limitation note).
- **Conflicts:** Counter-semantics severity — ADV-003 Major/High vs STR-008 Nit/High on the same code; resolved to Major (only the Major version cites the warnings-gate bypass). MBean naming — ADV-008 Nit vs STR-006 Minor; resolved to Minor. ADV-001's Blocker is unconfirmed by any other lens (the three lenses most likely to cross-check it were lost); confidence stays Medium pending the reproduction test.
- **Deduplication decisions:** Merged ADV-003+STR-008 → FUL-003 and ADV-008+STR-006 → FUL-014 (same root cause and observable failure each). Kept distinct: FUL-002 vs FUL-005 (SRP refill loop vs replica-side continue-on-marker path — independently remediable); FUL-011 vs FUL-015 (concrete guard asymmetry vs design cohesion); FUL-017 vs FUL-026 (dead code vs test gap, cross-corroborated); FUL-001 vs FUL-004 (digest-mismatch repair path vs non-participating range pager — different silent-truncation entry points).

### Maintainer/change-risk synthesis
(review-maintainer was unusable; the following is corroborated evidence from other lenses, not a substitute maintainer verdict.)
- **Compatibility and user impact:** Default flips paged tombstone reads from fail-fast to paged (FUL-005), changes metric semantics operators alarm on (FUL-003), and adds a permanent JMX surface with misleading names/docs (FUL-008, FUL-014) plus an unvalidated config knob (FUL-010). Wire compatibility via `TOMBSTONE_FAIL` is consensus-sound. CHANGES.txt under-communicates the change (FUL-009).
- **Patch shape and recovery:** Four fixup-style commits narrate branch-local defects (FUL-007) and the rebase gave all six commits identical timestamps (FUL-034) — bisectability across the range is weak, and the squash plan ASQ-001 proposes must be weighed against per-commit bisect value before merge. `5ec2c3bfb0` mixes LLM supporting infra into the feature range.
- **Maintenance surface and local benefit:** Checkpoint mailbox in the query value object (FUL-015), quadruplicated eligibility predicates (FUL-016), retained dead policy branch (FUL-017), and committed supporting-infra files are durable costs; the design's restraint (declined shared helper, no wire cursor, no version negotiation) was respected throughout — no child found plan-scope drift in production code beyond FUL-012's undocumented CL ONE widening.
- **Residual acceptance risk:** The three lost lenses mean principal-engineer, overengineering, and maintainer judgments are missing entirely; combined with the unconfirmed FUL-001 Blocker, the branch is not ready for a merge verdict from this review.

### Performance synthesis
- review-performance: worst-case replica scan work unbounded for range-tombstone-bound-dominated partitions, default-on (FUL-005); small unconditional per-read command copy even when disabled (FUL-020). Measured evidence: committed in-JVM benchmark shows ~4% clean-read median delta (enabled vs disabled), 1.5–2× replica reads on checkpointed/reconciled pages — single run, no trunk baseline. Residual gaps: no trunk-vs-branch baseline, no RT-storm workload, production-scale threshold cost untested.

### Validation and test-gap synthesis
- No completing reviewer could execute builds or tests (permission-denied, truthfully reported by review-aislop, review-coverage, review-tests); all pass/coverage claims rest on archived `logs/` + `build/paging-coverage/` artifacts whose internal consistency the review-coverage reviewer independently verified. Closing checks before merge: full distributed re-run on HEAD (FUL-021), changed-line coverage of the final commit (FUL-022), Blocker reproduction test (FUL-001), refill-loop bounded test (FUL-002), empty-test implementation (FUL-006), ACCEPT-policy unit tests (FUL-026).

## Validation and limitations
- **Evidence inspected:** Ten child envelopes as received (seven usable); plan file; resolved git range, worktree state, and fingerprint (all completing reviewers matched the packet; orchestrator additionally re-verified `setTombstonePageContinuation` call sites, `AbstractReadRepair.java:90-194`, and `AbstractQueryPager.java:210-279` directly).
- **Commands run:** Orchestrator: `git log`, `git remote -v`, `git status --short`, `git diff --stat`, `git rev-parse HEAD^{tree} HEAD`, plus targeted source reads/greps cited above. Child-run commands are attributed in the findings and sections above; no orchestrator command executed builds or tests.
- **Limitations:** (1) review-principal-engineer, review-overengineering, and review-maintainer results were empty or truncated in transport and unrecoverable — aggregate forced to BLOCKED/NOT ISSUED with their lenses absent; a re-run of the full review (or those three disciplines) is the only cure. (2) The aislop deterministic scan never executed (script lacked the execute bit; wrapper-with-args and direct invocations denied) — its APPROVE is non-informative. (3) FUL-001/FUL-002 are code-traced, not reproduced; premises re-verified by the orchestrator but neither path was executed. (4) All claimed pass counts and coverage figures were validated for artifact consistency only, not re-executed; reports are unbound to HEAD post-rebase (FUL-034). (5) Several children reported `aft_search` semantic-lane unavailability and used lexical fallback.

## Recommendations
- **Integrate:** FUL-001 (repair-path cursor propagation or REJECT, with the completeness regression) and FUL-002 (refill progress guard) — the merge-gating defects; FUL-004 (range-pager opt-out); FUL-005 (marker-continue ceiling + hoisted per-marker work); FUL-003 (metric split); FUL-006 (implement or visibly `@Ignore` the empty test); FUL-010 (threshold validation); FUL-013 (decode cap); FUL-014/FUL-008 (rename and document the JMX accessors before release); FUL-019 (typed-failure funnel); FUL-009/FUL-027/FUL-028 (CHANGES.txt and commit-message rewrites); FUL-021/FUL-022/FUL-034 (final green distributed run and coverage pass bound to HEAD, logged in `.debug/`); FUL-017/FUL-018/FUL-026 and the FUL-029–FUL-033, FUL-038–FUL-040 comment fixes as cheap hygiene.
- **Document:** FUL-012 (decide CL ONE as supported or restrict it — record in plan/commit text); FUL-015 (checkpoint-state consolidation as a follow-up refactor, not this patch); FUL-016 (predicate cross-reference Javadoc); FUL-037 (jamm Instrumentation test-rule exception); FUL-035 (skip-claim wording); the squash-vs-bisectability tradeoff from FUL-007/FUL-034 for maintainer decision.
- **Decline:** Unifying the checkpoint-eligibility predicates into one helper and removing `TOMBSTONE_FAIL` from the wire (both already declined by the plan with sound rationale — review-structural's FUL-016 docs ask is the acceptable residue); introducing a wire cursor, version negotiation, or pager snapshot-semantics guarantees (plan non-goals); FUL-005's full solution need not add a new config knob — a bounded margin suffices.
