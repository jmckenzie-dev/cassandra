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

# Tombstone continuation tests

- Require fetch size below LIMIT when a test needs a client pager. Accept empty
  pages with continuation and compare the complete ordered result.
- Check generated expectations against single-replica mutation replay and
  disabled paging controls before attributing failures to reconciliation.
- Compare completion status and clustering alongside digests. Tombstone counts
  do not identify a position.
- Test static-only output both before and at partition exhaustion. Cursor
  progress does not establish that an earlier page returned data.
- Give parallel runs separate logs and output directories. Do not edit a shell
  wrapper while it executes. Keep long method lists out of Ant output filenames.
- Use a single replicated mutation for matching TTL tests. Independent inserts
  produce different timestamps and expiry metadata and can legitimately cause
  full reconciliation instead of an empty checkpoint page.
- Keep builds separate from test execution. Ant rebuilds Accord artifacts;
  concurrent test startup can fail to load classes while those artifacts change.
- Check paging combinations with disabled controls. IN and GROUP BY controls
  pass while the corresponding tombstone-enabled reads expose state errors.
- Test repeated fetches through the same pager. Reconstructing it from serialized
  state on every fetch can hide retained-command bugs.
- Use separate assertions for filtered and unfiltered paging. Unfiltered counters
  include tombstoned rows; terminal paging state is undefined.
- Check the selected JAR when importing build.xml from a local Ant project.
  Its project name changes final.name and can select stale production classes
  when no-build-test is enabled.
- Measure reconciliation around enabled reads. Disabled model controls can also
  reconcile replicas and must not satisfy the enabled-path assertion.
- Separate missing coverage measurements from missing tests. Existing state and
  GROUP BY tests raised pager line coverage from 68.2% to 83.0%; new cases raised
  it to 95.7% with about 0.6 seconds of additional test-method runtime.
- Require complete groups from aggregation pages. A checkpoint after a live row
  can incorrectly complete a group even when initial-empty checkpoint tests fail
  differently. Check forward and reverse cases with disabled controls.
- Check buffer contracts before writing state properties. Deserialization
  consumes the input, and protocol fallback can accept a truncated legacy prefix.
  A buffer without an exposed array can still be heap-backed. The copy fix now
  allows the read-only heap reproducer to run in the regular suite.
- Do not count an unfinished group when a tombstone checkpoint closes a sub-page.
  Clear grouping state when a result page completes and when an IN child starts
  a different partition without returning rows.
- Static-row suppression must account for rows from an unfinished group. Completed
  group counts alone do not show whether earlier sub-pages returned data.
- Short-read refills can use a smaller row limit than the original GROUP BY read.
  After the first refill, determine exhaustion using that refill's limit. Applying
  the original limit can stop the deletion-bearing replica and expose stale rows.
- Upgrade tests load the versioned dtest JAR for the upgraded node. Rebuild that
  JAR from the feature branch; the main test classpath does not replace a copied
  trunk JAR with the same version number.
- Check upgrade behavior after each node restarts. A one-node upgrade does not
  exercise two checkpoint-capable replicas reconciling a legacy full response.
- Share a cluster across deterministic feature combinations to limit upgrade
  test cost. Keep separate checks for upgraded and legacy coordinators.
- Observe TTL expiration on each replica with a bounded condition. Do not use
  elapsed wall time as evidence that the TTL fixture has expired.
- Validate generated mixed-version histories with complete mutation replay on
  the old node, then unpaged and paged controls through each coordinator. Use
  the control only for IN partition order; derive membership from the model.
