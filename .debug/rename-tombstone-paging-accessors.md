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

# FUL-014 accessor rename

Rename the management methods to `getTombstonePagingThreshold` and
`setTombstonePagingThreshold`. The configuration key and integer semantics
stay unchanged. No old-name alias remains. Historical review notes retain
their original names as evidence of the reviewed revision.

## Test Author Result

- Status: COMPLETE
- Domain: unit
- Applicability: APPLICABLE
- Plan file: `.plans/tombstone-paging-overarching-plan.md`
- Requested scope: FUL-014 management accessor rename
- Resolved comparison basis: `60e799656b8e82ba07e6880723b1e1e3d3a373a4` to worktree
- Production files reviewed: `StorageService.java`, `StorageServiceMBean.java`
- Test files changed: `StorageServiceServerTest.java`
- Production code changed: yes, the separately requested interface and implementation rename

## Tests Added or Updated

Update the existing test through `StorageServiceMBean`. Set and read -1, 0,
1, and 100; verify the underlying configured value and restore it in finally.
This test fails to compile if either renamed interface method is absent and
fails its assertions if the implementation reads or writes the wrong setting.

## Commands and Outcomes

All commands use `distrobox enter dev -- bash tmp/paging-coverage-run.sh`,
which selects Java 17, logs console output, and preserves the exit status.

- `bash run_tests.sh build -s`: exit 0.
- `ant testsome -Dtest.name=org.apache.cassandra.service.StorageServiceServerTest -Dtest.methods=testTombstonePagingManagementAccessors -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp -Dbuild.test.output.dir=build/paging-accessor-rename`: exit 0, one passed.
- `ant testsome -Dtest.name=org.apache.cassandra.db.ReadCommandTest -Dtest.methods=tombstonePageCheckpointStateIsThresholdBoundedProperty -Dno-build-test=true -Dtmp.dir=/var/home/jmckenzie/src/cassandra/cassandra_asf/wt_cassandra-18424/tmp -Dbuild.test.output.dir=build/paging-accessor-property`: exit 0, existing property passed.
- `rg` found no old accessor names in Java sources under `src` and `test`.
- `git diff --check`: exit 0.

## Findings for the Implementer

None.

## Unresolved Test Gaps

The accessor test calls the real MBean interface directly; it does not use a
remote management connection. No full suite was run for this name-only change.

## Blockers

None.
