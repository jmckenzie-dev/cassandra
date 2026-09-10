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

# TombstonePagingState — FUL-015

## Implementation

Each `ReadCommand` owns one eager, private, final `TombstonePagingState`.
The package-private final holder owns all checkpoint fields and mutations.
Its operations attach a pager, begin local execution, record a local stop,
accept a coordinator continuation, consume for reconciliation, and initialize
independent state for a new execution. Existing command methods delegate to
the holder so pager, resolver, and repair callers retain their interfaces.

Reset and consumption clear row and count together but retain pager ownership.
Local stops do not require a pager. Accepted continuations do. A null accepted
continuation clears both payload fields, preserving the existing contract.
Copy factories initialize the destination's already allocated holder from the
source's pager flag; they copy neither the payload nor the mutable holder.
There is no second holder allocation during this initialization.

The refactor adds one holder allocation per command, including copies and
commands that never checkpoint. It adds no per-row holder allocation, locks,
new eligibility rules, or changes to the FUL-020 local command-copy guard.
Existing execution and response-publication ordering remains required.

The implemented holder measures 24 shallow bytes on the local Java 17 runtime
with jamm 0.4.0. The temporary package-local probe `tmp/TombstoneStateSize.java`
instantiates the real class and calls `MemoryMeter.measure`. This does not
measure net parent-command layout changes or end-to-end allocation cost.

## Java diff before this refactor

Verified merge base with `origin/trunk`: `c1030321071ab9f10b2f19d63087ab9313035001`.
`git diff --numstat c103032107 -- '*.java'`, including the pre-existing worktree:

| Scope | Files | Added | Deleted | Net |
| --- | ---: | ---: | ---: | ---: |
| Production | 28 | 699 | 90 | 609 |
| Tests and helpers | 29 | 9431 | 7 | 9424 |
| Total | 57 | 10130 | 97 | 10033 |

These are Git line counts, including comments and blank lines, not executable
statements or cumulative commit churn.

After this refactor, including the two new Java files: production is 793 added,
90 deleted across 29 files; tests/helpers are 9559 added, seven deleted across
30 files. Total: 10352 added, 97 deleted, net 10255 across 59 Java files.

## Test Author Result

- Status: COMPLETE
- Domain: unit
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-015 state encapsulation and command-copy isolation
- Resolved comparison basis: `60e799656b` plus the existing FUL-014/016 worktree changes
- Production files reviewed: `ReadCommand.java`, `SinglePartitionReadCommand.java`, `TombstonePagingState.java`
- Test files changed: `TombstonePagingStateTest.java`, `ReadCommandTest.java`
- Production code changed: yes, separately authorized state encapsulation

## Tests Added or Updated

Four holder tests cover unowned local stops, publication rejection without
payload mutation, accepted publication, null-continuation clearing, repeated
consumption, ownership retention, and independent copied state for both owner
flag values. The real-read reset regression now checks ordinary copies,
updated-limit copies, and transactional-setting copies after a real local
tombstone stop; execution and consumption on each copy leave the original
checkpoint intact.

## Commands and Outcomes

Commands use `distrobox enter dev -- bash tmp/paging-coverage-run.sh`, which
selects Java 17, logs output, and preserves the command status.

- `bash run_tests.sh build -s`: passed.
- `ant -f tmp/tombstone-state-tests.xml state-tests -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp -Dbuild.test.output.dir=build/tombstone-state-tests`: exit 1 solely from the new fixture error described below. The other 246 cases yielded 240 passes and six existing skips.
- `bash run_tests.sh coverage org.apache.cassandra.db.TombstonePagingStateTest`: exit 0 after correcting the fixture; all four cases passed. The holder covered 24/24 executable lines, 4/4 branches, and 11/11 methods including its constructor.
- `ant checkstyle checkstyle-test`: production passed; exit 1 from the same 22 existing test violations in unchanged files. No new or modified file has a reported violation. No Checkstyle configuration or report file was opened.
- `git diff --check`: passed.

The first new rejection test constructed an invalid empty clustering before
calling the state holder. The continuation constructor rejected it, so the
test did not reach the expected ownership guard. Corrected the fixture to use
a valid clustering. This was a test-fixture error, not a production defect.

Final unique-case results after the corrected holder rerun:

| Selection | Passed | Skipped |
| --- | ---: | ---: |
| Holder lifecycle | 4 | 0 |
| ReadCommandTest, including the state property | 35 | 0 |
| QueryPagerTest | 26 | 0 |
| DigestResolverTest | 15 | 0 |
| BlockingReadRepairTest | 11 | 0 |
| PagingAcrossTombstonesTest | 98 | 6 |
| Renamed management accessor | 1 | 0 |
| Checkpoint ownership and response order | 7 | 0 |
| Short-read protection | 11 | 0 |
| Mixed configurations | 32 | 0 |
| Distributed fuzzer (119.236 seconds; 180-second limit) | 4 | 0 |
| Total | 244 | 6 |

Coverage report: `build/paging-coverage/20260910T133055Z-2739341/index.html`.
The new tests check lifecycle contracts through supported operations rather
than accessing private fields or modifying internals.

## Findings for the Implementer

None.

## Unresolved Test Gaps

End-to-end performance measurement remains deferred. No full suite or real
mixed-version upgrade was rerun for this state-only refactor.

## Blockers

None.
