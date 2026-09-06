<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Digest and tombstone continuation comparison

Implemented against HEAD `4e454fb560`, following
[the plan](../.plans/compare-tombstone-page-continuations.md).
The original five-row ALL reproductions pass in memory and after flush. The
corrected three-node fuzzer passes seeds 18424 and 1, including every enabled
query and its independent controls.

## Read behavior

`TOMBSTONE_PAGE_CONTINUATION`, message parameter 22, carries the clustering
components. The request supplies the partition and direction. Existing parameter
21 retains its integer tombstone count. Replicas still include `TOMBSTONE_FAIL`
with a checkpoint so coordinators without a consumer continue to reject it.

Eligible initial callbacks accept checkpoint responses for comparison. Matching
digests, early-stop status, and clustering continuations let the coordinator use
the data replica's response. Counts can differ at the same position. The resolver
adopts the selected cursor into the outer pager. An empty page with continuation
is a successful result.

A difference in digest, clustering, or completion status starts the existing
full-data reconciliation path. Missing cursor metadata also requires that path.
This supports finite single-partition reads at multiple-replica consistency even
when the coordinator's own pager cannot adopt a checkpoint. A count-only response
at ONE still fails safely. Ordinary failure parameters without checkpoint
metadata remain failures.

Local initial reads use command copies when comparing continuations. A late
local response therefore cannot replace the outer pager's selected cursor.
Transient responses retain the existing full-data reconciliation path.

## Additional regression found and fixed

With static `s=77`, deleted clustering rows 0–2, live rows 3–4, threshold 2,
fetch size 1, and LIMIT 2, accepting the initial checkpoint first produced a
static-only row. That row incorrectly consumed the query's LIMIT. The resolver
now suppresses static-only output until a checkpointed partition completes or
returns a live clustering row.

Deleting rows 3–4 as well exposed the opposite error: the pager suppressed the
real static-only row at exhaustion because earlier empty pages had advanced the
cursor. The pager now checks whether prior pages returned data, using its
remaining-per-partition count. The test expects exactly `(null, null, 77)`.

Both failures were observed before their fixes:

- Premature static row: `logs/run_tests_20260906T014016Z-693749.log`, exit 1.
- Missing static row at exhaustion: `logs/run_tests_20260906T014216Z-698183.log`, exit 1.
- Both cases pass: `logs/run_tests_20260906T014345Z-701967.log`, exit 0.

## Test scope and results

All commands use Java 17 and Ant through `tmp/paging-coverage-run.sh` inside the
existing `dev` distrobox. The source and test classes were rebuilt before the
final distributed regression run. Ant completed successfully for each selection
below. The outer-wrapper exception for the related suite is explained below.

| Selection | Result and evidence |
|---|---|
| `PagingAcrossTombstonesDistributedTest` | 13 pass; `logs/run_tests_20260906T015308Z-731252.log` |
| `PagingAcrossTombstonesFuzzTest`, seed 18424 | 2 pass; `logs/run_tests_20260906T015404Z-735992.log` |
| Same fuzzer, seed 1 | 2 pass; `logs/run_tests_20260906T015600Z-745294.log` |
| Eleven unit classes | 201 cases, 195 pass and 6 intentional skips; `logs/paging-coverage-20260906T014454Z.log` |
| Final `DigestResolverTest` rerun | 11 pass, including its concurrency test; `logs/run_tests_20260906T015542Z-743895.log` |
| Additional legacy metadata checks | 2 pass; `logs/paging-coverage-20260906T020258Z-770453.log` |
| Existing distributed paging and short-read tests | 372 pass: `PagingTest` 4, `ShortReadProtectionTest` 360, `ShortReadProtectionTombstonePagingTest` 8; isolated JUnit reports in `build/paging-coverage/continuation-distributed/tests/` |

The unit selection includes `ReadCommandTest`, the new continuation serializer
property, `DigestResolverTest`, `BlockingReadRepairTest`, `QueryPagerTest`,
`AbstractPagingValidatorTest`, `PagingAcrossTombstonesTest`, and the four CQL
classes `IndexQueryPagingTest`, `DistinctQueryPagingTest`, `PagingQueryTest`,
and `PagingTest`.

The deterministic distributed tests cover matching responses at ONE, QUORUM,
and ALL; threshold boundaries; memtables and SSTables; repeated checkpoints;
descending composite clustering with an empty text component; different digests
at the same cursor; different cursors; checkpoint versus normal exhaustion with
equal digests; static data; and blocking read-repair writes. Matching cases assert
that reconciliation metrics do not increase. Mismatch cases assert that they do.
A separate four-node cluster uses replication factor three and pins the client
to the non-replica coordinator, exercising remote data responses at ONE and ALL.

The new callback/resolver test checks seven agreement variants, including legacy
count-only data and digest responses and a coordinator without checkpoint
ownership. Separate tests confirm that a genuine tombstone failure remains a
`ReadFailureException`, and a legacy checkpoint at ONE still fails safely.
The serializer property checks 100 generated composite clusterings, including
null and empty components, at both the oldest supported and current message
versions. It checks round-trip equality, serialized size, and full consumption.

Each fuzzer seed generates six mutation histories. It checks 1,080 enabled
queries over three coordinators, both directions, memtables/SSTables, different
limits and fetch sizes, and matching/differing/partly disabled configurations.
The model uses uniquely ordered timestamps for inserts, row deletions, and range
deletions. Each history also passes a single-replica replay, unpaged ALL reads,
and disabled paging controls. There are 744 control checks per seed per method;
both methods run the controls. Empty pages are allowed. The final collected row
sequence must match the model. See the earlier investigation for the model audit.

## Coverage and checks

The [combined JaCoCo report](../build/paging-coverage/continuation-combined/index.html)
contains only execution data from passing tests in this implementation run.
Inputs are `continuation-unit`, `20260906T015308Z-731252`,
`20260906T015404Z-735992`, `continuation-distributed`,
`20260906T015542Z-743895`, and `continuation-legacy`, all under
`build/paging-coverage/`. Reports use the final production classes, with no
class-mismatch warnings. The aggregate does not include the earlier failing
static regressions or the failed method-list invocation.

Intersecting JaCoCo line records with added/replaced lines against HEAD, and
including the new continuation class, gives **85/86 executable lines (98.8%)**
and **93/106 branches (87.7%)** covered. These are execution measures.

| Scope | Lines | Branches |
|---|---:|---:|
| New/changed `DigestResolver` code | 27/27 | 40/44 |
| New/changed `AbstractReadExecutor` code | 6/6 | 8/8 |
| New/changed `ReadCallback` code | 11/12 | 16/18 |
| New continuation class | 21/21 | 12/14 |
| Entire `service.pager` package | 379/575 (65.9%) | 226/368 (61.4%) |

The missed changed line is the callback overload without an executed command.
Remaining partial branches include unsupported cursor shapes, eligibility and
invariant guards, and a pager continuation condition. The whole-pager figure
uses this test selection; it excludes the earlier long single-node fuzzer and
must not be compared as the same selection as the historical audit.

Aggregate and summary commands, logged through the local wrapper:

```sh
ant -f tmp/continuation-coverage.xml -Djacoco.export.dir=build/paging-coverage/continuation-combined
python3 tmp/continuation-coverage-summary.py
```

Targeted Checkstyle passes for all eight changed production files and the four
new or expanded test files. Shell syntax checks and `git diff --check` pass. The full repository check
was not repeated; the earlier audit records existing RAT and test-style failures.

JaCoCo 0.8.8 cannot instrument the generated CQL parser's oversized initializer.
The targeted read and pager classes do receive instrumentation. No dependency
upgrade or generated-source change was made.

One attempted method-list rerun exceeded the filesystem's filename length limit
because Ant embeds all selected method names in the JUnit output filename. Its
stuck JVM was stopped, and the whole resolver class was rerun successfully. That
attempt's execution data is excluded. Concurrent test wrappers also initially
shared a second-resolution outer log name; the local wrapper now includes its
process ID. Per-run `run_tests.sh` logs and isolated JUnit reports remain separate.
Changing that outer wrapper while the related suite ran caused a shell parse
error after Ant printed `BUILD SUCCESSFUL`; the outer process exited 2. All 372
JUnit results report zero failures/errors, and the complete Ant result is in
`logs/paging-coverage-20260906T015404Z.log`. Its JaCoCo data covers the completed
passing tests. Shell syntax validation and subsequent wrapper runs pass. Do not
edit an executing shell wrapper.

## Remaining scope

The multi-node fuzzer is bounded and uses quiescent, full replicas at ALL. It does
not establish behavior during concurrent writes, speculative retries, node loss,
TTL expiry, collection tombstones, GROUP BY, partition-range scans, or actual
mixed-version upgrades. Transient replicas have unit coverage, without a new
transient-replica cluster test. The existing mixed-version test factory issues
recorded in the earlier audit remain. Legacy metadata is tested at the callback
boundary; this is not a mixed-version cluster run.

## Reproduction

After building, run from the repository with Java 17 selected:

```sh
bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesDistributedTest -Dno-build-test=true -DmaxMemory=4G
bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=4G
bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=4G '-Dtest.jvm.args=-Dcassandra.test.seed=1'
bash run_tests.sh coverage org.apache.cassandra.service.reads.DigestResolverTest -Dno-build-test=true -DmaxMemory=4G
```
