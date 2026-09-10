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

# FUL-011 and FUL-019 reproductions

Date: 2026-09-09. Baseline: `9525af44e0db0cdd8cb6e08056bdeb06c59389e3`.
Plan: `.plans/tombstone-paging-overarching-plan.md`.
Scope: FUL-011 checkpoint ownership and FUL-019 unsupported repair errors.
The worktree was clean before this work. Validation below preceded the
follow-up commit requested by the user.

## Results

FUL-019 is a confirmed native-client defect. The regression returned
`ServerError`, wrapping `TombstoneOverwhelmingException`, before the fix.
`AbstractReadRepair.startRepair` now reports `ReadFailureException` with
`READ_TOO_MANY_TOMBSTONES`, the requested consistency level, and the local
replica's address. It still rejects unsupported repair before scheduling reads.
The native regression passes for NONE and BLOCKING repair, in memory and after
flush. The production change does not change checkpoint ownership or copy policy.

FUL-011's claimed pager advancement or loss of an accepted local checkpoint
did not reproduce. A rejected local checkpoint leaves the pager state null and
the live-row budget unchanged, including on retry. A real local executor keeps
its checkpoint off the owned command until the resolver accepts the response.
The accepted response then supplies the correct clustering position and count.

The guard asymmetry does have an observable effect: a rejected local checkpoint
can remain on a shared, unpaged command and reach read repair. That is the
FUL-019 reproduction below. This result does not establish the separate claim
that a pager adopts that rejected checkpoint.

## Native failure reproduction

`PagingCheckpointOwnershipTest.rejectedLocalCheckpointWithRemoteDigestMismatchReturnsTypedFailure`:

1. Create three replicas with different live values at clustering 0.
2. Put three row tombstones before that row on the coordinator-local replica
   only. Set paging threshold 2 and failure threshold 10000.
3. Issue an unpaged, unlimited native QUORUM read with speculative retry ALWAYS.
4. Hold the two real remote responses through the message-filter interface.
   Wait for the local read to finish and assert its checkpoint metric increased.
5. Deliver the remote responses unchanged. Both succeed, but their data differ.
6. The remote quorum starts full-data repair. The shared command still has the
   rejected local checkpoint. Its unlimited limits prevent checkpoint repair.
7. Assert a native `ReadFailureException` and only tombstone failure reasons.

Before the fix, the assertion received:

```text
com.datastax.driver.core.exceptions.ServerError:
org.apache.cassandra.db.filter.TombstoneOverwhelmingException:
Scanned over 3 tombstones ... query aborted
```

The direct `BlockingReadRepairTest.checkpointedRepairAtConsistencyOneFailsSynchronously`
also failed before the fix when changed to require the typed contract. It now
checks consistency ONE, received 0, blockFor 1, no data, and exactly one local
`READ_TOO_MANY_TOMBSTONES` reason. It retains its assertions that the command's
checkpoint remains intact and no repair reads were sent.

## Other test cases

- `rejectedLocalCheckpointDoesNotAdvancePager`: a no-clustering table with three
  deleted cells and one live value. Two attempts on the same real coordinator
  pager must fail without advancing state. A native ONE read checks the client
  error too. Both storage states run.
- `localCopyPublishesCheckpointOnlyAfterResponseAcceptance`: run a real local
  executor at ONE with an owned command. Wait for local storage execution, check
  that the outer checkpoint is absent, accept the response, and assert an empty
  page with checkpoint clustering 2 and count 3. Both storage states run.
- `unsupportedRepairCheckpointReturnsTypedNativeFailure`: initial reads have
  different live values but no tombstones. Hold remote responses, finish the
  initial local read, then add tombstones before releasing the mismatch.
  Full-data repair must return the same typed native error as initial checkpoint
  rejection. Assert actual repair and checkpoint metrics. Run NONE/BLOCKING and
  both storage states with fresh partitions.
- `PagingAcrossTombstonesFuzzTest.localCheckpointCopiesMatchLiveRowModel`:
  QuickTheories generates threshold 1–4, additional deleted rows 1–4, live rows
  1–4, and fetch size 1–2. Every tuple runs in both directions and storage states.
  Replicas have identical timestamped data. Native ALL paging must return exactly
  the independently constructed live-row list and cross the local replica's
  checkpoint threshold. LIMIT is live rows + 2, strictly above fetch size.
  Existing native traversal checks bound rows, pages, repeated states, and time.
  Twelve tuples run per seed. QuickTheories shrinks failing tuples; each attempt
  uses a fresh partition. `cassandra.test.seed` controls deterministic replay.

## Limits

- The sole production override of `maybeAllowOutOfRangeReads` is
  `AccordInteropExecution`. Its `localReadSupported()` returns false. The review's
  combined out-of-range-copy/local-execution scenario does not follow that
  production path. No artificial coordinator override was added to force it.
- No top-k cursor-adoption failure was reproduced or claimed. The direct
  rejected-pager test uses the supported no-clustering rejection path.
- The former `IllegalStateException` fallback did not receive its own native
  reproduction. Both unsupported-checkpoint branches now use the same typed
  failure. No fabricated command state was used to reach the old fallback.
- Passing cases do not prove all checkpoint ownership interleavings safe.

## Validation commands and outcomes

Every command below has the prefix
`distrobox enter dev -- bash tmp/paging-coverage-run.sh`.
The wrapper selects Java 17, uses project-local temporary files, writes console
and dated logs, and preserves command exit status. Tests run sequentially.

| Command after prefix | Exit | Outcome | Log under `logs/` |
| --- | --- | --- | --- |
| `bash run_tests.sh build -s` | 0 | Baseline JAR built | `run_tests_20260909T225104Z-1531616.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -DmaxMemory=2G` | 1 | Two passed; direct pager fixture lacked warning-context initialization | `run_tests_20260909T225248Z-1535332.log` |
| `bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -DmaxMemory=2G` | 0 | Initial three tests passed; 25.688 seconds | `run_tests_20260909T225406Z-1539215.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -DmaxMemory=2G` | 0 | Four tests passed before production change; seed 18424, 108.956 seconds | `run_tests_20260909T225509Z-1542636.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -DmaxMemory=2G` | 1 | Added quorum reproduction received ServerError; another case timed out awaiting its filtered responses | `run_tests_20260909T225730Z-1548067.log` |
| `bash run_tests.sh test org.apache.cassandra.service.reads.repair.BlockingReadRepairTest -DmaxMemory=2G` | 1 | Ten passed; typed-error regression received TombstoneOverwhelmingException | `run_tests_20260909T225912Z-1551640.log` |
| `bash run_tests.sh build -s` | 0 | Rebuilt production fix | `run_tests_20260909T230008Z-1554151.log` |
| `bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -DmaxMemory=2G` | 0 | All four passed; 32.180 seconds | `run_tests_20260909T230028Z-1555820.log` |
| `bash run_tests.sh test org.apache.cassandra.service.reads.repair.BlockingReadRepairTest -Dno-build-test=true -DmaxMemory=2G` | 0 | Eleven passed after fix; 2.710 seconds | `run_tests_20260909T230148Z-1558880.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=2G -Dtest.jvm.args=-Dcassandra.test.seed=1` | 0 | Four passed after fix; 117.787 seconds | `run_tests_20260909T230313Z-1561920.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesDistributedTest -Dno-build-test=true -DmaxMemory=2G` | 0 | Fifteen passed after fix; 40.469 seconds | `run_tests_20260909T230526Z-1567851.log` |
| `ant build-test checkstyle checkstyle-test` | 1 | Tests compiled; production Checkstyle passed; 22 existing violations in unchanged test files | `paging-coverage-20260909T230636Z-1570095.log` |
| `ant -f tmp/checkpoint-ownership-checkstyle.xml` | 0 | All four changed Java files passed the repository Checkstyle configurations | `paging-coverage-20260909T230825Z-1574094.log` |

The direct-pager fixture now brackets each request with the normal
`CoordinatorWarnings.init()` / `done()` lifecycle. The initial-response filter
now excludes checkpoint responses without assuming every remote response is a
digest. Failure assertions and metric checks remain unchanged.

The final native coverage report is
`build/paging-coverage/20260909T230028Z-1555820/index.html`.
It confirms execution of all instructions on the new typed-failure lines in
`AbstractReadRepair`. The earlier three-test report did not enter that guard;
the subsequent quorum case was necessary to reproduce FUL-019.

All 34 selected tests passed after the fix, with no skips. The four-method fuzz
suite also passed before the fix with seed 18424. The new property executes 48
direction/storage comparisons per seed; those comparisons are not separate
JUnit tests. No full suite or mixed-version upgrade was run in this pass.
`git diff --check` passed. The targeted style command uses an ignored temporary
Ant file with explicit includes for the four changed Java files; it uses the
repository's existing configurations and suppressions without modifying them.
The broader style failures remain in `MixedModePagingAcrossTombstonesTest`,
`PagingFuzzTest`, `AbstractPagingValidator`, `PagingAcrossTombstonesTest`, and
`RandomHelpers`. This pass does not edit those files.

## Test Author Result

- Status: COMPLETE
- Domain: integration
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-011 and FUL-019 at `9525af44e0`
- Resolved comparison basis: `9525af44e0..working-tree`; review-era production paths inspected in place
- Production files reviewed: `AbstractReadExecutor.java`, `ReadCallback.java`, `DigestResolver.java`, `DataResolver.java`, `AbstractReadRepair.java`, `ReadCoordinator.java`, `AccordInteropExecution.java`, `StorageProxy.java`, `ReadCommand.java`, `AbstractQueryPager.java`
- Test files changed: `PagingCheckpointOwnershipTest.java`
- Production code changed: yes

## Tests Added or Updated

Four real three-node tests described above. No fabricated responses, internal
patching, or forced coordinator implementation. Message filters hold and
deliver actual responses unchanged.

## Commands and Outcomes

The command table records the failed baseline and passing post-fix tests.

## Findings for the Implementer

### integration-001

- Severity: Minor
- Production location: `AbstractReadRepair.startRepair`
- Behavior: a rejected local checkpoint survives a successful remote quorum;
  remote disagreement enters repair and exposes a local exception as ServerError.
- Evidence or failing test: `rejectedLocalCheckpointWithRemoteDigestMismatchReturnsTypedFailure`, baseline log `run_tests_20260909T225730Z-1548067.log`.
- Required implementation action: return a typed tombstone read failure. Applied
  after the failing regression under the user's explicit instruction to fix
  reproduced defects. All four native variants pass.

## Unresolved Test Gaps

See Limits. FUL-011's proposed top-k pager adoption and the former no-row fallback
have no native reproduction.

## Blockers

None.

## Test Author Result

- Status: COMPLETE
- Domain: unit
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-019's unsupported repair error contract
- Resolved comparison basis: `9525af44e0..working-tree`
- Production files reviewed: `AbstractReadRepair.java`, `ReadFailureException.java`
- Test files changed: `BlockingReadRepairTest.java`
- Production code changed: yes

## Tests Added or Updated

Strengthened the existing unsupported ONE repair test to require the typed
error and exact response fields. Kept the checkpoint and no-dispatch assertions.

## Commands and Outcomes

The baseline unit run had one expected contract failure. All eleven tests pass
after the separately authorized production fix. Exact commands are above.

## Findings for the Implementer

### unit-001

- Severity: Minor
- Production location: `AbstractReadRepair.startRepair`
- Behavior: unsupported checkpoint repair throws a local tombstone exception.
- Evidence or failing test: `checkpointedRepairAtConsistencyOneFailsSynchronously`, baseline log `run_tests_20260909T225912Z-1551640.log`.
- Required implementation action: the same typed failure fix as integration-001;
  applied and verified.

## Unresolved Test Gaps

The former no-row fallback has no naturally produced checkpoint fixture here.

## Blockers

None.

## Test Author Result

- Status: COMPLETE
- Domain: property
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-011 accepted local checkpoint completeness
- Resolved comparison basis: `9525af44e0..working-tree`
- Production files reviewed: `AbstractReadExecutor.java`, `ReadCallback.java`, `DigestResolver.java`, `AbstractQueryPager.java`, `ReadCommand.java`
- Test files changed: `PagingAcrossTombstonesFuzzTest.java`
- Production code changed: no

## Tests Added or Updated

Added `localCheckpointCopiesMatchLiveRowModel` to the existing property suite.
The domain, independent oracle, non-vacuity assertion, shrinking, and replay
details appear above. `run_property_tests.sh` already includes this suite.

## Commands and Outcomes

The full four-method suite passes with seeds 18424 and 1. The former ran before
the FUL-019 fix; the latter ran after it. Exact commands are above.

## Findings for the Implementer

None.

## Unresolved Test Gaps

This property checks accepted local copies, not top-k or transactional reads.

## Blockers

None.
