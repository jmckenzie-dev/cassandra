<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Worker counter and metric-ID residency

## Objective and scope

Execute the second TODO: reduce worker counter-array holes and metric-ID
lifecycle overhead. The user requested a clean-context implementation agent,
an independent principal-engineer review, integration of justified findings,
and a report of the plan, implementation, tests, and efficiency gains.

Resident memory takes priority over construction allocation. Preserve existing
metric values, reset behavior, registration/profile semantics, and the eager
dense control path. A new optional startup configuration can select lazy IDs.
Keep it off by default while evaluating correctness and recording-path costs.
Do not change histogram arithmetic, JMX implementations, memtable retirement,
or ownedMetrics release bookkeeping. Those are separate tasks.

Baseline is the current dirty working tree on `moar_tables`, HEAD `4192a00e1f`.
Earlier metric-profile, adaptive-history, and JMX changes must remain intact.
The parent captures source copies and Git status in
`tmp/worker-metric-residency-baseline/` before delegation. Review this task's
delta against that snapshot, not all changes since HEAD. Add any missing
baseline paths before editing them and identify new files explicitly.
Do not stage, commit, or delete a branch.

## Evidence to read

- `research/optimized_heap_next_steps.md`: ID allocation, occupancy, paging
  estimates, lifecycle hazards, and source references.
- `research/9_7_checkpoint.md`: million-table context and run limits.
- `research/jmx_query_export.md`: latest optional JMX controls, needed only to
  keep measurements consistent; do not include their gains in this task.
- Current metric factories, ThreadLocalMetrics, ThreadLocalCounter,
  ThreadLocalHistogram, ThreadLocalMeter, GeometricThreadLocalMeter, timer and
  histogram construction paths, and their focused tests.

The earlier eight-worker census had 76336 long slots per worker, only 6002
nonzero slots, and 4,885,504 B of worker array payload at 1000 tables.
Zero values may represent live counters. The current node must be measured
again before claiming a saving. Delayed ID recycling already avoids some
temporary gaps; do not assume each skipped allocation shrinks retained arrays.

## Implementation sequence

1. Establish a repeatable baseline before production edits. Capture idle
   construction, scrape-only, and populated metric states. Record worker and
   summary capacities, allocated/live IDs, cleanup objects, and whole heap.
   Use the same simple profile, reservoir, JMX options, worker count, and
   workload for each comparison. Keep all actual table runs <=1000.
2. Avoid constructing duplicate global histogram/timer candidates when a
   compatible registered metric already exists. Preserve alias registration,
   hidden recorders, type conflicts, concurrent registration, and return
   behavior. Measure this intermediate change separately. Avoid a new general
   registration framework for this bounded lookup improvement.
3. Implement/evaluate first-use metric-ID allocation as an optional path.
   Untouched metric reads, resets, scrapes, and background meter ticks must
   not allocate counter IDs. Publish initialization safely when writers race.
   Cover counter IDs inside histograms and meters as well as plain counters,
   or quantify and justify any retained eager subcategory. Preserve the old
   implementation as the selectable comparison. Minimize added objects and
   branches on established recording paths; avoid permanent helper objects
   whose overhead consumes the intended saving.
4. Re-measure occupancy after lazy allocation. Decide whether further sparse
   storage earns its complexity. Do not implement paging solely because most
   slots were previously zero. If lazy allocation solves most of the measured
   problem, retain dense worker arrays and document the remaining bound.
   An alternative storage representation requires its own measured benefit
   and lifecycle proof within this task, not an assumed gain.
5. Run final matched comparisons, document limits and tradeoffs, and hand
   the exact changed-path manifest plus evidence to the principal reviewer.
   Address substantiated findings and rerun affected validation. Re-review
   corrected production changes before reporting completion.

## Correctness requirements

- Preserve signed increments/decrements, signed-long overflow, counts,
  histogram observations, timer counts, and meter rate calculations.
- Reset must preserve concurrent updates according to existing semantics.
  Do not silently replace summary subtraction with clearing worker arrays.
- First use must allocate/publish a valid ID exactly once. Cleanup must not
  retain its owner or recycle an ID still used by an in-flight operation.
- Thread exit transfers values exactly once. Growing arrays and concurrent
  count queries must not lose or double count transferred values.
- ID reuse must not expose prior metric values, including after drop/recreate
  and worker exit. Preserve existing recycling safeguards and distinguish
  pre-existing visibility weaknesses from regressions introduced here.
- Configuration must be applied consistently before creation; existing metric
  instances must not change representation if test configuration later changes.
- Both all/simple profiles, hidden aggregate inputs, aliases, and legacy paths
  must remain functional. Preserve timer/meter construction options.

## Validation and performance evidence

Read `~/.config/opencode/TESTING.md` before authoring tests. Exercise actual
production classes through intended constructors/interfaces; no private-field
mutation, monkey-patching, or bytecode substitution. Add meaningful regression
tests before fixes when reproducing a defect.

Use focused existing ThreadLocalCounter/Histogram/Meter, registry, timer, and
profile tests as applicable. Add isolated unit and generated equivalence tests
for lazy initialization, untouched reads/ticks/resets, first-use races, reset,
signed arithmetic, array growth, cleanup, thread exit, and ID reuse. Expose
reusable root `run_tests.sh` and `run_property_tests.sh` options.

Measure actual JVM/table residency at 100 and 1000 tables where useful. Also
compare worker storage and recording at eight and 64 workers using shared and
partitioned access to at most 1000 logical tables/metric groups. Standalone
metric benchmarks need not create full Cassandra nodes. Report first-use and
steady-state recording costs separately, with allocations, repeated samples,
and variance. Do not infer a throughput improvement from allocation counts.
If a repeatable hot-path regression remains, quantify it and keep the new path
explicitly experimental/off by default; correctness defects are not acceptable
behind an option.

Acceptance requires a retained-memory improvement attributable to this task,
passing lifecycle/equivalence checks for both paths, and explicit steady-state
costs. If an experiment fails, preserve the result and explain the resulting
design choice. Do not present projected array savings as measured whole heap.

## Execution and ownership

The implementation agent owns relevant metrics sources, required config,
focused tests, benchmark support, and `research/worker_metric_residency.md`.
It announces its file scope and maintains a changed-path manifest. It must
not modify unrelated existing work. Parent owns this plan, baseline capture,
review coordination, and final integration decisions.

The review agent uses `review-principal-engineer` with exactly these inputs:
this plan's absolute path and the explicit task delta against the baseline
snapshot, narrowed by the implementation manifest. It reviews read-only and
returns its report without saving files. The parent records review outcomes.
No change-set hashes are needed or authorized.

Use Java 21 via `distrobox enter dev -- ...`. Build only through
`.build/sh/ai-build`; use existing ai-* test/profile wrappers. Never run the
full suite, direct Ant/Maven/Gradle, or install dependencies. Temporary files
belong in project `tmp/`. Python uses uv and `./venv`. Scripts tee stdout and
stderr to console and dated `logs/` files, preserving exit codes. No compound
shell commands. No builds or heavy heap analysis while a profile JVM runs.
Send meaningful progress and measurement results to the parent. Mark only
the completed worker-residency TODO done; leave unrelated TODOs intact.
