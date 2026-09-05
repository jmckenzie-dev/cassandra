# Address Full Review Findings for 7130977f

## Goal

- Remediate the applicable findings from commit `7130977f4cad6d473a05704d1fe919c14fe4419b` with the smallest safe change.
- Preserve tombstone paging checkpoints, supported short-read continuation, safe failure for paths without a valid continuation cursor, and pager ownership transfer before full-data read repair.
- Success means the remote-coordinator defect has a failing regression test and a narrow fix, the possible no-progress loop has a bounded test gate, owner-copy behavior has direct coverage, and all retained compatibility behavior is explicit.
- Do not add dependencies, change the CQL grammar or wire protocol, edit generated sources, or broaden pager ownership.

## Current State

- The reviewed commit is the current `HEAD`. Its tree is `34c8e7095fb87f2d822b8cc10ca9f76b8149edfa`, which matches the supplied fingerprint. Its intent is recorded in `.plans/review-full-7130977f-plan.md:10-28`.
- A replica emits both the new checkpoint parameter and the existing `TOMBSTONE_FAIL` parameter when local execution stops at a tombstone page checkpoint (`src/java/org/apache/cassandra/db/ReadCommandVerbHandler.java:125-136`). This makes old coordinators fail safely because message deserialization skips unknown parameters (`src/java/org/apache/cassandra/net/ParamType.java:35-41`, `src/java/org/apache/cassandra/net/Message.java:1151-1173`).
- `ReadCallback.onResponse` removes `TOMBSTONE_FAIL` only for `ACCEPT` and only from non-digest data responses (`src/java/org/apache/cassandra/service/reads/ReadCallback.java:240-258`). `AbstractReadExecutor` instead selects `USE_LOCAL_COMMAND` for an owned top-level page command (`src/java/org/apache/cassandra/service/reads/AbstractReadExecutor.java:83-100`). A remote data response therefore retains `TOMBSTONE_FAIL` and fails before `DigestResolver.preprocess`. This verifies FUL-002 by code trace.
- A remote checkpoint can lack a visible physical row after replica-side filtering. Metrics and checkpoint capture run before `RowFilter` and limits (`src/java/org/apache/cassandra/db/ReadCommand.java:593-634`), while `RowFilter` can remove a row (`src/java/org/apache/cassandra/db/filter/RowFilter.java:242-286`). `ShortReadRowsProtection` keeps the prior `lastClustering` and immediately reuses it for the next retry (`src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:76-80,181-205`). This makes FUL-001 plausible, but current tests do not reproduce it.
- If a checkpoint arrives before any regular clustering row supplies an anchor, `ShortReadRowsProtection` throws `TombstoneOverwhelmingException` with `STATIC_CLUSTERING` rather than resume unsafely (`src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:102-131`). This is the intended safe-failure invariant for FUL-003.
- Supported checkpoint reconciliation is intentionally limited to a finite, non-top-k, single-partition read with a read quorum greater than one (`src/java/org/apache/cassandra/service/reads/DataResolver.java:95-101`). A short-read retry asks one source a different question and therefore uses different eligibility conditions (`src/java/org/apache/cassandra/service/reads/ShortReadPartitionsProtection.java:189-219`).
- Full-data read repair checks support before consuming the outer command checkpoint, then uses copied commands for local reads so a late local response cannot restore the outer cursor (`src/java/org/apache/cassandra/service/reads/repair/AbstractReadRepair.java:95-131,152-190`). Existing tests cover this ownership transfer and unsupported consistency level (`test/unit/org/apache/cassandra/service/reads/repair/BlockingReadRepairTest.java:190-266`).
- The worktree has an uncommitted edit in `test/unit/org/apache/cassandra/db/ReadCommandTest.java`. It includes a relevant owner-propagation test for `copy()`, `withUpdatedLimit()`, and `withTransactionalSettings()` at lines 903-915. It also includes an unrelated property test at lines 849-901. Treat both as user work. Preserve both, but include only the owner-propagation test in this remediation scope unless separately approved.
- The existing distributed test covers repeated checkpoints, zero-live refill, descending reads, stale-row shadowing, protocol paging, and safe direct-read failure, but every helper coordinates through node 3 in the same three-node cluster (`test/distributed/org/apache/cassandra/distributed/test/ShortReadProtectionTombstonePagingTest.java:58-137,200-351`). The tests neither force the coordinator outside the replica set nor prove that a top-level checkpointed data response is remote. They do not cover FUL-002's precise path.
- `CHANGES.txt` already has the user-visible CASSANDRA-18424 entry at `CHANGES.txt:1-3`. The entry predates the reviewed commit.
- `run_tests.sh:7-30` supports `test <fully-qualified-class>` and `build`, forwards to the required AI wrappers, and writes timestamped logs under `logs/`. It is currently untracked user work and must not be overwritten.

## Assumptions

- Apply remediation on the present worktree. Inspect the dirty `ReadCommandTest.java` diff before editing and preserve unrelated changes.
- A four-node, two-data-center in-JVM distributed test can isolate a coordinator outside the replica data center by using the repository's existing `NetworkTopology` test API and `NetworkTopologyStrategy`. Confirm exact APIs against nearby tests before editing.
- FUL-001 remains a hypothesis until the filtering regression fails through repeated continuation on the unmodified production code. If the test cannot produce a checkpointed response with no visible physical row, do not add the proposed production guard.
- No historical log is pass evidence. Every accepted change requires fresh named output from the final tree.

## Finding Disposition

| Finding | Disposition | Evidence-based rationale |
|---|---|---|
| FUL-001 | Reproduce first | Replica filtering can remove the row captured by metrics before serialization, so the static claim is plausible. Add a hard-bounded regression. Add a no-progress guard only if the test fails through repeated retries. |
| FUL-002 | Integrate now | `AbstractReadExecutor` selects `USE_LOCAL_COMMAND`, while only `ACCEPT` strips the compatibility failure from remote data responses. The code path verifies the defect. |
| FUL-003 | Decline | The missing-anchor exception prevents unsafe continuation. Keep it as a safe-failure invariant. Do not invent a clustering cursor. |
| FUL-004 | Integrate now | Run fresh targeted classes and the final build through `run_tests.sh`. Retain each timestamped log name and final status. |
| FUL-005 | Document or defer | Current mixed-version behavior is fail-safe by design: old coordinators skip parameter ID 21 but process known `TOMBSTONE_FAIL`; old replicas cannot emit checkpoints. Add a concise compatibility comment. Do not add upgrade-matrix code in this patch. |
| FUL-006 | Decline | Production constructs `DigestResolver` only from the single-partition `AbstractReadExecutor`, so its cast is type-safe in the production path. Unsupported remote responses must fail in `ReadCallback` before this path. The different exceptions preserve the best available context. A helper would not unify the contracts. |
| FUL-007 | Decline | The branch already contains a CASSANDRA-18424 entry, inherited from the parent. Do not add a duplicate. |
| FUL-008 | Decline | Keep `TOMBSTONE_FAIL` on the wire. Direct branching without the compatibility parameter would let old coordinators accept an incomplete response. |
| FUL-009 | Decline | `DataResolver` answers whether merged reconciliation can continue. `ShortReadPartitionsProtection` answers whether a one-source retry callback can accept metadata. `AbstractReadExecutor` also requires pager ownership. These are related but different contracts. Do not create a shared abstraction. |
| FUL-010 | Decline | No concrete lost-owner path exists beyond the three command-copy methods. Keep the narrow owner bit, identity check, pager mark, and read-repair copy behavior. |
| FUL-011 | Integrate now | Preserve and use the already-prepared dirty test method that directly asserts owner propagation through all three command variants. Do not claim it is committed. |
| FUL-012 | Integrate now | Remove the ambiguous `USE_LOCAL_COMMAND` policy. Make local command identity handling independent, and document `ACCEPT` as remote/local non-digest acceptance while `REJECT` preserves failure signaling. |
| FUL-013 | Integrate now | Add concise JavaDoc that distinguishes local tombstone-limit state, transmitted page-checkpoint metadata, and pager ownership. Avoid a broad terminology rename. |

## Recommended Plan

### Phase 1: Add decision-gate regressions before production changes

1. Extend `test/distributed/org/apache/cassandra/distributed/test/ShortReadProtectionTombstonePagingTest.java` with a remote-coordinator regression for FUL-002.
   - Configure four nodes with nodes 1-3 in one data center and node 4 in a second data center. Alter the test keyspace to `NetworkTopologyStrategy` with replication only on nodes 1-3. Keep existing tests coordinated by node 3 so their topology remains local to the replica set.
   - Add `remoteCoordinatorAcceptsCheckpointedDataResponse` and coordinate it through node 4 at `ALL` with a finite single-partition limit.
   - Before loading divergent rows, derive the full-data target selected by `ReplicaPlans.forRead` on node 4 for the test partition. Put the threshold-crossing tombstone sequence on that replica and put non-checkpointing versions on the digest replicas. This avoids endpoint-order dependence.
   - Use the existing zero-live/stale-row shape: one source has live rows before and after more than two tombstones; another source has a newer deletion that shadows an early live row. The first remote data response must carry a checkpoint, digest comparison must enter the supported full-data resolution path, and short-read protection must return the requested live rows without exposing the shadowed row.
   - Assert exact final rows and an increase in `shortReadProtectionRequests`. Also prove node 4 is not a replica and the checkpoint came from a remote data response.
   - Expected pre-fix result: `ReadFailureException` occurs before resolver preprocessing because the remote response retains `TOMBSTONE_FAIL`. Expected post-fix result: the query returns all requested rows.

2. In the same distributed test class, add `filteredCheckpointWithoutVisibleRowDoesNotRetryForever` as the FUL-001 decision gate.
   - Keep node 3 as coordinator so FUL-002 does not mask this path. Create a table with a non-key value column and issue a finite `ALLOW FILTERING` single-partition query.
   - On one source, store two matching live rows, then more than the threshold of row tombstones, then another matching live row. On a second source, add a newer deletion that shadows the first live row. The initial merge must trigger short-read protection after the remaining early live row. The retry must hit the tombstone threshold while replica-side `RowFilter` removes all physical checkpoint rows from the response.
   - Install a scoped outbound `READ_REQ` counter or filter for the source retry and set a small test-only read timeout. Hard-cap retries so the test terminates deterministically.
   - Decisive pre-fix failure: retries exceed the cap while every retry starts from the same clustering anchor. A simple query failure does not reproduce FUL-001.
   - Decisive post-fix assertions, if reproduced: the coordinator returns no partial or stale rows, fails through the existing tombstone/read-failure path, and issues at most one checkpoint continuation after the last observed clustering anchor.
   - If the pre-fix code does not exceed the cap, retain the test only if it proves a useful bounded invariant. Otherwise remove the test-only experiment and make no FUL-001 production change.

### Phase 2: Fix supported remote checkpoint acceptance

3. Update `TombstonePageCheckpointPolicy`, `onResponse`, and `response(ReadResponse, ReadCommand)` in `src/java/org/apache/cassandra/service/reads/ReadCallback.java`.
   - Remove `USE_LOCAL_COMMAND`.
   - Keep `REJECT` as the default. It must leave `TOMBSTONE_FAIL` intact.
   - Keep `ACCEPT` limited to non-digest data responses that contain `TOMBSTONE_PAGE_CHECKPOINT`. It must remove only the local warning-map copy of `TOMBSTONE_FAIL`, then pass the response and checkpoint metadata to the resolver.
   - Make exact local ownership independent of policy: when `executedCommand` is the same object as the callback command and has pager ownership, synthesize the local response without `TOMBSTONE_FAIL`. Copied read-repair commands and unrelated local commands must retain fail-safe signaling.
   - Add concise comments that state local identity and remote acceptance semantics. Preserve the wire compatibility signal for every remote response.
   - Dependency: Phase 1's remote-coordinator test must fail for the expected callback reason first.
   - Observable result: an owned, supported top-level read accepts a remote checkpointed data response, while unsupported and digest responses still fail.

4. Update constructor policy selection in `src/java/org/apache/cassandra/service/reads/AbstractReadExecutor.java`.
   - Select `ACCEPT` only when all top-level continuation conditions hold: the command has pager checkpoint ownership, is finite, is not top-k, and the replica plan read quorum is greater than one. The method already accepts `SinglePartitionReadCommand`.
   - Select `REJECT` otherwise. A remote checkpoint at consistency level `ONE`, an unlimited direct read, top-k, or a non-owned command must fail safely.
   - Keep the condition local. Do not extract a shared eligibility helper because short-read retry and top-level acceptance have different ownership and quorum contracts.
   - Dependency: apply after the `ReadCallback` policy change.

5. Adjust policy-focused tests in `test/unit/org/apache/cassandra/service/reads/DigestResolverTest.java`.
   - Change `tombstonePageCheckpointFailsDigestResolverRead` to use `REJECT`; retain its assertions that the callback fails and the resolver receives no data.
   - Change `localCheckpointMetadataReachesTransientReconciliation` to use `REJECT`. Add an assertion that exact local ownership does not need `TOMBSTONE_FAIL` to reach transient reconciliation even under the rejecting remote-response policy.
   - Retain `tombstonePageCheckpointAcceptedByShortReadCapableDataResolver` as the direct assertion that `ACCEPT` strips the compatibility failure but preserves checkpoint metadata.
   - Do not weaken digest-response rejection.

### Phase 3: Add a bounded no-progress guard only if FUL-001 reproduces

6. If and only if Phase 1 proves repeated continuation from the same anchor, update `ShortReadRowsProtection` in `src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java`.
   - Track whether a checkpoint-driven retry produced any physical row that advances beyond the clustering anchor used to build that retry.
   - At the next `moreContents()` call, if the response still carries a checkpoint but produced no advancing physical row, throw `TombstoneOverwhelmingException` before another request. Use the current tombstone count and the last valid clustering anchor for diagnostics.
   - Preserve the existing `lastClustering == null` safe-failure branch. Do not resume from `STATIC_CLUSTERING`, infer a cursor from a count, or add clustering data to the wire.
   - Reset progress state for each retry. Preserve ascending and descending paging and existing repeated-checkpoint continuation when each response advances.
   - If the test does not reproduce repeated requests, skip this step and record FUL-001 as not reproduced.

### Phase 4: Complete ownership and documentation coverage

7. Preserve the current uncommitted `testTombstonePageCheckpointOwnerCopiedWithCommandVariants` in `test/unit/org/apache/cassandra/db/ReadCommandTest.java`.
   - Keep assertions for the unset initial state and propagation through `copy()`, `withUpdatedLimit()`, and `withTransactionalSettings()`.
   - Do not overwrite or reformat the adjacent dirty property test. Do not include that property test in this remediation change without separate approval.
   - Verify that no other command-copy method needs ownership by tracing pager and full-data read-repair call sites. Add no new owner propagation without a concrete call path.

8. Add concise compatibility and terminology documentation.
   - In `src/java/org/apache/cassandra/db/ReadCommandVerbHandler.java`, document why every remote checkpoint also carries `TOMBSTONE_FAIL`: old coordinators skip the unknown checkpoint and must fail the partial response.
   - In `src/java/org/apache/cassandra/db/ReadCommand.java`, document that `tombstoneLimitedRow` and `tombstoneLimitedCount` are local execution state, while message metadata is the transmitted tombstone page checkpoint. Document that the owner bit marks a pager-created command whose exact local execution can transfer that cursor.
   - In `src/java/org/apache/cassandra/service/reads/DataResolver.java`, keep the existing support predicate and add at most one concise method comment describing its finite single-partition reconciliation contract.
   - Do not rename public methods, introduce a checkpoint object, or alter `ParamType` IDs.

## Validation Plan

Do not use historical logs. Run only whole targeted classes because the wrapper does not support method filters.

1. `./run_tests.sh test org.apache.cassandra.distributed.test.ShortReadProtectionTombstonePagingTest`
   - Pass: existing continuation, stale-row, paging, safe-failure, remote-coordinator, and bounded no-progress cases pass.

2. `./run_tests.sh test org.apache.cassandra.service.reads.DigestResolverTest --reuse`
   - Pass: supported data checkpoints retain metadata and resolve; `REJECT` and digest checkpoints fail before data reaches the resolver; exact local ownership reaches transient reconciliation.

3. `./run_tests.sh test org.apache.cassandra.service.reads.repair.BlockingReadRepairTest --reuse`
   - Pass: full-data repair consumes outer ownership before dispatch, a late local copied command cannot restore it, and unsupported consistency level fails before reads are sent.

4. `./run_tests.sh test org.apache.cassandra.db.ReadCommandTest --reuse`
   - Pass: owner state propagates through the three relevant copy variants and all pre-existing tests pass.
   - If the unrelated dirty property test fails, preserve it and report the failure separately. Do not alter it as part of this remediation.

5. `./run_tests.sh build`
   - Pass: the wrapper exits zero and its timestamped log contains no compile, packaging, or reported style errors.
   - The current `.build/sh/ai-build:39-68` condition appears to take the `--no-checkstyle` branch even when `no_checkstyle=false`. Inspect the named build log. If checkstyle did not run, report that validation gap and ask a maintainer for the approved wrapper-only check. Do not call Ant directly or modify the wrapper in this change.

For each command, retain the emitted `logs/run_tests_<UTC timestamp>.log` path and record the exit status. Do not run the full suite.

## Acceptance Criteria

- A coordinator outside the replica set accepts a remote checkpointed data response only for an owned, finite, non-top-k read whose resolver can perform quorum short-read continuation.
- Remote digest checkpoints, consistency-level-one remote checkpoints, unlimited reads, top-k reads, and non-owned commands retain fail-safe failure behavior.
- Exact local pager execution can transfer checkpoint state. Copied full-data read-repair commands cannot retake ownership.
- The old-coordinator safety signal remains on every remote checkpoint response. Unknown new parameters remain skippable.
- The FUL-001 test either proves bounded safe failure after a reproduced no-progress retry or records that the issue did not reproduce and leaves production code unchanged.
- Supported ascending, descending, repeated-checkpoint, zero-live, protocol-paged, transient-replica, and stale-row-shadowing cases remain covered.
- Owner propagation has direct assertions for `copy()`, `withUpdatedLimit()`, and `withTransactionalSettings()`.
- The targeted test classes and final wrapper build have fresh, attributable logs. No historical log counts as evidence.

## Risks and Mitigations

- **Remote acceptance could permit an incomplete consistency-level-one result.** Gate `ACCEPT` on finite pager ownership and `readQuorum() > 1`; keep `REJECT` otherwise.
- **Removing `USE_LOCAL_COMMAND` could break local pager state.** Make exact object identity plus owner state independent of remote-response policy, and retain transient reconciliation coverage.
- **A no-progress guard could reject valid repeated checkpoints.** Add it only after reproduction. Detect absence of clustering progress, not merely a short or zero-live response. Run existing repeated and descending checkpoint cases.
- **A topology test could pass while node 4 is a replica or while the wrong source gets the data request.** Use two data centers with replication only in the replica data center, derive the selected data target, and assert both facts.
- **Mixed-version cleanup could weaken old-coordinator safety.** Keep both message parameters on remote responses. Any wire negotiation or parameter removal requires separate maintainer approval.
- **Dirty user tests could be overwritten or accidentally bundled.** Inspect the diff before edits, preserve both dirty methods, and scope this change only to the owner-copy test.
- **The final build wrapper might not execute checkstyle.** Inspect its named log and report the gap. Use only the repository-approved wrapper path.

## Non-Goals and Declined Feedback

- Do not add a wire-protocol cursor, version negotiation, new message parameter, dependency, grammar change, or public API.
- Do not unify all checkpoint eligibility checks. Their ownership, quorum, and retry contracts differ.
- Do not replace the safe missing-anchor exception with guessed resumption.
- Do not standardize all unsupported-path exceptions or add a shared exception helper.
- Do not redesign pager ownership or propagate it through unproven command variants.
- Do not add a duplicate `CHANGES.txt` entry.
- Do not remove `TOMBSTONE_FAIL` or rely only on new-coordinator policy branches.
- Do not include the adjacent dirty property test in this remediation without separate approval.
- Do not run upgrade tests or a full mixed-version matrix in this bounded patch. Retain the documented fail-safe behavior instead.

## Open Questions

None.