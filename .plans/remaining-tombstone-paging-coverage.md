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

# Remaining tombstone paging coverage

## Scope

Repository: wt_cassandra-18424. Baseline: commit 168966f1c9.
Integration skill inputs: this plan and production changes
4e454fb560..168966f1c9. Applicable: real read, protocol, persistence,
replica failure, and upgrade interactions. Production paths are ReadCommand,
ReadCommandVerbHandler, TombstonePageContinuation, ParamType, ReadCallback,
DigestResolver, AbstractReadExecutor, and AbstractQueryPager.

## Scenarios

1. Hold a real checkpoint response at the network boundary while acknowledged
   writes change rows beyond that checkpoint. Validate subsequent pages against
   the committed mutations. Separately insert behind a consumed cursor and
   confirm it is not revisited. Paging does not promise a snapshot.
2. Drop a selected response to force server speculation, and drop enough
   responses to cause timeout. Retry the saved continuation after recovery.
   Stop a replica between pages and check QUORUM success and ALL unavailability.
3. Exercise real TTL expiry and collection-element tombstones. Preserve live
   rows whose collection cells were deleted. Check stale replica reconciliation.
4. Exercise grouped reads, partition-key IN, range reads, and transient replicas.
   Record supported behavior and preserve valid failing regressions.
5. Replace misleading current-version-only upgrade coverage with UpgradeTestBase
   tests using real historical versions. Configure only upgraded nodes with the
   new option. Cover upgraded and old coordinators and old-node failure.
   Run if historical dtest artifacts are available; otherwise record the exact
   missing-artifact failure. Do not substitute current versions.
6. Repair the current-version mixed-configuration fixture and replace empty
   test methods with bounded reads that cross thresholds.

## Validation

Build with Ant and Java 17. Run each new test class with isolated JUnit and
JaCoCo output. Use bounded waits and the repository's network filters; no
source patching or internal state replacement. Keep production unchanged in
this test-authoring batch. Record sound failures, evidence, commands, and
remaining scope. Run targeted Checkstyle and git diff --check.
