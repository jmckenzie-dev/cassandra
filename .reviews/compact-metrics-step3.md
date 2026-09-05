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

# Review: Adversarial correctness and integration

## Gate

- **Status:** COMPLETE
- **Verdict:** APPROVE
- **+1:** YES
- **Blocker findings:** 0
- **Major findings:** 0
- **Minor findings:** 1
- **Nit findings:** 0

## Scope

- **Repository:** /var/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables
- **Plan:** .plans/compact-runtime-metrics.md, step 3
- **Requested scope:** AdaptiveCounterArray, dense reservoir integration, and focused new counter and reservoir tests.
- **Resolved scope:** Uncommitted AdaptiveCounterArray.java, its two test classes, and changes to CompactDecayingEstimatedHistogramReservoir.java and its unit test. Includes widening after a failed narrow add CAS.
- **Validation evidence:** Source inspection. Root reports 30 focused cases passed; reviewer did not execute workloads or independently verify those logs. Root owns the final validation appendix.

## Findings

### Blocking

None.

### Non-blocking

#### ADV-001 — Contention test assumes concurrent scheduling

- **Severity:** Minor
- **Confidence:** High
- **Requirement/invariant:** Tests should distinguish an implementation failure from a valid execution schedule.
- **Evidence:** test/unit/org/apache/cassandra/metrics/AdaptiveCounterArrayTest.java:156 asserts widening after eight latch-started actors complete. The latch does not guarantee a failed CAS; a scheduler may serialize all actors.
- **Impact:** A correct narrow implementation can fail this assertion on a constrained or unusually scheduled test host. The exact final total assertion remains valid.
- **Remediation:** Keep the exact-total concurrency regression. Assert contention-driven widening only when the fixture can establish actual contention through a supported interface; otherwise report width behavior in the performance evidence.
- **Verification:** Exact totals must pass with both serial actor scheduling and overlapping execution. Integer-boundary tests independently verify mandatory widening.

## Discipline-specific analysis

### Requirements coverage matrix

| Requirement | Observable contract | Traced runtime path | Evidence | Status |
|---|---|---|---|---|
| Exact width promotion | No lost additions, sentinel values, or signed arithmetic changes | AdaptiveCounterArray get/set/add/CAS and promote | Atomic getAndSet captures each final narrow value; complete long array precedes volatile publication; boundary and AtomicLongArray oracle tests | Satisfied by source; runtime reported by root |
| Contention fallback | One addition per call when narrow CAS fails | addAndGet:97 to promote().addAndGet | Failed CAS performs no addition; wide operation applies it once | Satisfied |
| Preserve CAS semantics | Migration alone cannot return false for unchanged expected value | compareAndSet:103 | Failed narrow CAS detects frozen cell and retries wide CAS; migration test preserves unchanged cell | Satisfied |
| Dense arrays only | Sparse pages retain long storage | PagedBuckets dense field and promote | Only dense array type changes; sparse pages remain AtomicLongArray | Satisfied |
| Preserve exports and decay | Existing bucket geometry, weights, and cumulative population | Dense get/set/add callers, snapshot rebase and rescale | No arithmetic or geometry diff; new tests cross 32-bit limit through real merge/rebase and aged updates | Satisfied by source; runtime reported by root |
| Measured benefit | Memory saving without unacceptable throughput regression | External probes and table harness | Root owns measurements and acceptance | Outside this focused correctness verdict |

### Integration and failure-mode results

All narrow mutations use CAS. The synchronized promoter freezes cells with atomic
getAndSet and copies the returned value, so an update either precedes that cell's
freeze or retries against the wide array. A retained narrow reference cannot
overwrite a frozen cell. Readers of a frozen cell wait for volatile publication;
readers that obtained a normal value can linearize before its freeze.

The promoter allocates the long array before changing any narrow cell. Allocation
failure therefore leaves usable narrow storage. Copying performs no further
allocation or callbacks. Wide values use the existing signed long operations.
Invalid indices are checked before promotion. Negative deltas conservatively
widen even when the result would fit; normal histogram additions are positive.

Sparse-to-dense copying stays under the existing PagedBuckets monitor. Large
sparse values widen the private new dense array before it is published. Clear,
rebase, rescale, and contention routing keep their existing call paths. Migration
failure is hidden from the reservoir's CAS-based contention detector.

### Residual uncertainty

Concurrency tests use real threads and exact totals, but schedules are not
exhaustive or deterministic. Generated operation histories compare against the
JDK AtomicLongArray sequentially. No allocation-failure injection or formal
linearizability checker ran. Busy-waiting readers rely on the migration thread
being scheduled; this review found no lock cycle or fallible operation after
freezing starts.

## Validation and limitations

- **Evidence inspected:** Plan, current counter source, dense integration diff, both new counter test classes, and reservoir widening regression diff.
- **Commands run:** Read-only cat, sed, rg, git diff, and git status. No build, test, or benchmark execution.
- **Limitations:** Performance and final full focused-suite acceptance remain with root. No production or test files were changed by this review.

## Root validation

The final clean Java 21 build and style checks pass in
`logs/20260905-134846-ai-build.log`. The reusable reservoir suite passes 75 tests,
with one existing legacy ignore and no failures/errors. XML results are in
`logs/20260905-135010-ai-test-memtable-lazy/`. This includes all 30 compact/counter
cases and the unchanged legacy/export classes.

The contention-only stress test now requires more than one reported processor
and labels its assertion as a stress expectation. It still depends on scheduling;
the review's minor limitation remains. Serial boundary and generated arithmetic
tests independently verify mandatory overflow widening and exact values.

The first measured candidate had a material contended-update cost. Widening on
a failed narrow addition recovered four-thread throughput to its fresh pre-change
level while retaining the 44.7% quiet dense graph saving. The N100 iteration
also confirms 600 narrow dense user arrays and 40.7% lower user counter payload.
Full pre/iteration/final measurement evidence is in
`research/compact_runtime_metrics.md`.
