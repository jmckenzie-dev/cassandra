<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Split branch handoff

All five feature branches build independently on many-tables/base. Their
targeted tests and extraction reviews passed. The integration branch merges
all five and restores the combined fixtures. It is a verification branch;
use the individual feature branches for revision and submission.

| Branch suffix under many-tables/ | Worktree | Frozen head |
| --- | --- | --- |
| base | wt_moar_base | f7d5a7e2c0 |
| memtable-residency | wt_moar_memtables | 4c5eb8992c |
| metric-selection | wt_moar_metric_selection | 86c5f9b178 |
| metric-storage | wt_moar_metric_storage | 3e8478f751 |
| metric-exports | wt_moar_metric_exports | c6e2c0da6b |
| remaining | wt_moar_remaining | 9600252eb8 |

The shared base contains generic profiling and local tools, the JaCoCo
upgrade, shared metric contracts, and a test-only expiration timing fix.
The two requested fix commits are base f7d5a7e2c0 and remaining 1cfcbfab95.
Both contain the same correction. All feature branches include the corrected
base ancestry.

The independent feature results are 66 passing memtable cases, 40 unique
selection cases, 98 storage cases with one existing skip, 28 export cases,
and 82 remaining cases. Each branch has a validation record in this directory.
Do not add these counts: branches share tests. Storage's final commit changes
executable modes only; its prior build and test results still apply.

## Combined correctness

The combined Java 21 build and main/test Checkstyle passed. The focused test
set passed 337 unique cases with one existing skipped diagnostic. Ten profile
cases also passed with the alternate management server builder. Generator,
heap-analyzer, and management-name boundary checks passed separately.
See integration-validation.md and integration-test-results.tsv.

Independent coverage review approved extraction and correctness. Its minor
command-provenance finding is recorded with the exact invocation. No blocking
or major finding remains in that review. This is targeted validation, not a
full-suite or numerical coverage claim.

All src/ blobs match original commit 319995ce5c exactly. The source inventory
accounts for all 246 changed paths. Combined test, tool, and documentation
adaptations have explicit records in integration-reconstruction.md. Runtime
and test sources remain frozen at eeb3e6957e during measurements.

## Remaining work inventory

The remaining branch contains two separate production commits:

- 47d36ac487: configurable minimum hierarchy size for Unified Compaction
  Strategy. Review the tuning policy and compaction consequences separately.
- 92689444b6: lazy tombstone histogram construction. Review allocation savings
  and lifecycle behavior separately from compaction hierarchy policy.

These changes are thematically different. Keep the separate commits; split
this branch further if each needs its own submission. No production hunk is
unassigned. The 89 historical note/tool paths stay in the original checkout
as the archive inventory. External-tool prerequisites remain documented.

## Measurement status

All 18 cells passed with audited controls: eight smoke cases, four profile
cases, four idle cases, and two management-history census cases. Six selected
heaps also passed analysis. The original 1,000-table idle attempt exceeded its
121-second harness deadline while progressing. A watchdog-only retry passed
with a 148-second final drain and unchanged runtime settings. Both attempts
remain preserved. See integration-measurements.md and the raw-linked JSON
records for numbers and limits.

The large profile comparison settled at 547 MiB with all metrics and 202 MiB
with simple metrics under legacy storage. The large idle comparison settled
at 1,311 MiB with idle flushing off and 333 MiB with it on. Idle flushing
returned initialized user memtables and their accounted heap bytes to zero,
with 4,000 flushes and 1,000 compactions. These are single-run process values,
not production capacity claims. BTI uses normal compaction fallback here.

The 100-table census recorded 892,000 bytes of standard history arrays after
rescrape versus 32,800 bytes with adaptive history. Independent branch
measurements also show some update/scrape and broad-query costs. All optional
controls remain opt-in; review the branch tradeoff records before changing
defaults. No further extraction or planned validation task remains open.

## Preservation

The original branch and worktree remain intact, including their pre-existing
planning changes. Backup refs preserve the original committed and dirty states.
No branch was deleted or rebased. No branch was pushed and no pull request
was opened. Raw logs and heaps stay in the local integration logs directory;
committed records retain their paths and summarized evidence.
