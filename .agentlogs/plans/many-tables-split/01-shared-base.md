<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Shared base

Branch: many-tables/base. Worktree: wt_moar_base.
Owner: coordinator. Execute serially before assigning feature branches.

## Inputs and boundary

Start at 50a10d42a5d633150fbf4ae518b4da85466472b2.
Read final source at 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Do not branch from the coordinator checkout's older trunk HEAD.

Extract these reviewable commits:

1. Portable build/test wrappers and generic resource profiling.
2. JaCoCo 0.8.11 for Java 21 instrumentation.
3. Shared metric contracts with unchanged runtime defaults and algorithms.

Generic profiling includes ResourceProfiler, ProfiledClusterHarness, and
ManyTablesProfileHarness. These final Java sources contain no references to
the new metric or memtable feature names found by the planning inspection.
Verify compilation against the pinned base before treating them as independent.

The launcher has three optional feature hooks. Assign PROFILE_TRANSIENT_JMX
to EXPORT, PROFILE_LAZY_METRIC_IDS to STORE, and PROFILE_COMPACT_BOOKKEEPING
to SELECT. Add those blocks only in their owning branches. Keep the generic
launcher in BASE. Do not move the complete MemtableResidencyProfileHarness or
HeapOwnershipCensusHarness into BASE; those classes reference feature APIs.

Use the final ai-build and ai-ci-test wrapper revisions. Confirm their
build-jars.sh, check-code.sh, run-tests.sh, and ant-log-summary.py dependencies
exist in the pinned base. Preserve stdout/stderr logs and failing exit status.
Separate environment-specific workaround edits from the imported source so
reconstruction can account for them. Do not copy personal agent configuration.

Shared metric changes:

- Add ClearableReservoir.
- Change DecayingEstimatedHistogramReservoir to implement that interface only.
- Change ClearableHistogram's reservoir field and compatible one-argument
  constructors to use that interface.
- Add ThreadLocalTimer's protected OverrideHistogram constructor overload;
  preserve its existing public constructor as a delegating overload.
- Keep metric identifiers eager, meters original, reservoirs original, and
  exports unchanged. Snapshot hooks belong to STORE, not BASE.
- Do not add the ClearableHistogram metric-ID constructor or create factory;
  those need STORE classes.

## Verification

Run .build/sh/ai-build from wt_moar_base. Run ai-ci-test for the existing
DecayingEstimatedHistogramReservoirTest, ThreadLocalHistogramTest,
ThreadLocalMeterTest, CassandraMetricsRegistryTest, and LatencyMetricsTest
classes in org.apache.cassandra.metrics. Inspect available tests before selecting
additional constructor/clear coverage. Test any new behavior rather than writing
tests that only mirror declarations.

Run a small generic ManyTablesProfileHarness case after reading its actual
arguments. Record source revision, Java version, flags, table count, and result.
Do not run a large heap experiment for this gate.

The login shell currently selects Java 11. Select an installed compatible Java
version explicitly for builds and record it. Read the environment guide if
setup fails. Do not install dependencies.

## Output

Return the frozen base commit, named-file diff, build and test results, smoke
result, and any recorded extraction adaptations. Only then create all five
feature branches at that exact commit. If the base must change later, the
coordinator makes one change and updates every dependent branch.
