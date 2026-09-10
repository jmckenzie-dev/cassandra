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

# Tombstone checkpoint response-order tests

Date: 2026-09-09, America/New_York; command logs use UTC on 2026-09-10.
Baseline: `a4168d0170c78b0a5f25b5cb023eb85085ebff3d`.

Both requested response-order variants pass. No production change was needed.
The tests extend the [FUL-019 reproduction](tombstone-checkpoint-ownership-reproductions.md).

## Cases and assertions

The three replicas hold different timestamped values at clustering 0. Only the
coordinator-local replica has three preceding tombstones. Reads use native
QUORUM, unlimited limits, no native paging, and speculative retry ALWAYS.

| Local read timing | Remote response order | Required result |
| --- | --- | --- |
| Before initial remote quorum | 2 then 3; 3 then 2 | Typed tombstone ReadFailureException, as in the committed regression |
| After initial remote quorum starts repair, before repair responses arrive | Both orders, in each response round | Query remains pending after the local rejections, then returns the reconciled row when the remote repair quorum arrives |
| After the remote repair quorum and native result complete | Both orders, in each response round | Correct complete native result, followed by execution and completion of the late local reads |

Every case runs with NONE and BLOCKING repair, in memory and after flush.
The complete matrix has 24 cases: three local timings, two remote orders, two
repair modes, and two storage states. Twenty extend the four committed cases.

The native success oracle is exactly one row, `ck=0, v=3`, no paging state,
and a fully fetched result. The failure oracle checks the requested consistency
and a nonempty failure map containing only READ_TOO_MANY_TOMBSTONES reasons.
Repair metrics must increase in every case.

The timing-dependent difference between failure and success is intentional in
these tests. A local checkpoint already on the command prevents unsupported
repair from starting. Without that checkpoint at repair entry, two successful
remote replicas can complete repair despite a later rejected local response.
The tests do not require every replica to succeed at QUORUM.

## Ordering and cleanup

Message filters retain the real initial remote responses and, in late-local
cases, the real repair responses. The test delivers them unchanged through
`MessagingService.inboundSink`. Its handler runs synchronously, so each delivery
finishes before the test delivers the other replica's response.

The test fixture configures two read workers. Latch-controlled tasks occupy both
workers on node 1 through the public read-executor API. Initial local execution
and its later repair read therefore queue behind the tasks. The test asserts
unchanged local storage-read and checkpoint counters while the gate is closed.
It does not patch callbacks, mutate executor internals, or fabricate responses.

The DURING_REPAIR case waits for both remote repair responses to be held before
releasing local reads. After local execution drains, the test requires two
checkpoint crossings and a still-pending native result. Only then does it
deliver the remote repair responses.

The AFTER_RESULT case keeps local reads queued through native completion. It
then releases and drains them and requires both checkpoint crossings. Each gate
has a timeout; the test checks blocker futures and executor idleness. Finally
blocks release outstanding tasks and reset message filters, including on failure.

## Commands and outcomes

All commands below use this prefix:
`distrobox enter dev -- bash tmp/paging-coverage-run.sh`.
The existing wrapper selects Java 17, uses project-local temporary files,
records console output in dated logs, and preserves the exit status.

| Command after prefix | Exit | Outcome | Log under `logs/` |
| --- | --- | --- | --- |
| `bash run_tests.sh build -s` | 1 | Fixture compile failure: runnable method references could not throw checked exceptions | `run_tests_20260910T021323Z-1818988.log` |
| `bash run_tests.sh build -s` | 1 | The harness callable also excludes checked exceptions | `run_tests_20260910T021401Z-1820682.log` |
| `bash run_tests.sh build -s` | 0 | Gate helper handles checked exceptions and preserves interruption; build passes | `run_tests_20260910T023722Z-1848845.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -Dno-build-test=true -DmaxMemory=2G` | 0 | Initial six-test version passes, 30.462 seconds | `run_tests_20260910T023750Z-1850420.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -DmaxMemory=2G` | 0 | Seven tests pass with all three local timings, 28.290 seconds | `run_tests_20260910T023955Z-1853808.log` |
| `bash run_tests.sh test org.apache.cassandra.distributed.test.PagingCheckpointOwnershipTest -Dno-build-test=true -DmaxMemory=2G` | 0 | Repeat: seven tests pass, 26.250 seconds | `run_tests_20260910T024110Z-1856642.log` |
| `ant -f tmp/checkpoint-ownership-checkstyle.xml` | 0 | Established targeted style check passes on one production file and three test files | `paging-coverage-20260910T024217Z-1858569.log` |

No full suite, top-k investigation, allocation measurement, or upgrade testing
was added. The input domain here is the finite response-order matrix; no random
workload or unrelated property suite was added.

## Test Author Result

- Status: COMPLETE
- Domain: integration
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: late rejected local responses and reversed remote response order
- Resolved comparison basis: `a4168d0170..working-tree`
- Production files reviewed: `AbstractReadExecutor.java`, `AbstractReadRepair.java`, `Stage.java`, `SEPExecutor.java`, `ExecutorPlus.java`, `DatabaseDescriptor.java`, `MessagingService.java`, `InboundSink.java`
- Test files changed: `PagingCheckpointOwnershipTest.java`
- Production code changed: no

## Tests Added or Updated

Retained the local-first regression. Added reversed remote order, late local
execution during repair, and late local execution after native completion.
The seven-method class also retains the prior pager and repair regressions.

## Commands and Outcomes

See the command table above. Initial failures were fixture compilation errors,
not production defects.

## Findings for the Implementer

None.

## Unresolved Test Gaps

The local gate delays execution and therefore response delivery; it does not
pause only the callback after storage execution has already set a checkpoint.
Local execution between the two initial remote responses is outside the
requested late-local variants. Mixed versions and other query shapes remain
outside this pass.

## Blockers

None.

The separate user request added an AGENTS.md reminder not to open security
checkpoint or Checkstyle configuration/report files during routine work.
Established check commands remain enabled.
