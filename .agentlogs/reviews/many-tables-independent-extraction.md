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

# Independent extraction checks

Scope: each feature diff against corrected base f7d5a7e2c0 and pinned source
319995ce5c. These checks verify extraction boundaries and recorded test execution.
They are not a new comprehensive correctness or performance review.

| Branch | Reviewer | Result |
| --- | --- | --- |
| Memtable residency | Coordinator | Owned source hashes and shared hunks match. Two hierarchy methods moved; subnet-only portability adapter. Fourteen XML classes confirm 66 passed. |
| Metric selection | Coordinator | 22 complete source/tool/test paths match; one documented interpreter adapter. Shared constructors remain legacy. Two distributed adapters remove only cross-feature controls and initialization assertions. 37 top-level cases plus three nested cases passed. |
| Metric storage | MEM worker | All 17 owned production/call-site files and five diagnostic scripts match. Shared changes contain storage-only factories and lazy counters. Test adapters match the moved-test map. |
| Metric exports | MEM worker | Four owned production files match. Shared registration/history hunks match. Legacy-storage fixture adapters preserve export assertions. Branch-local selectors are intentional. |
| Remaining experiments | SELECT worker | Verification-and-coverage review APPROVE, no findings. Nine XML classes confirm 82 passed. Two production units remain independent of sibling features. |

No missing production hunk or hidden sibling dependency was found.
Config.java/README.asc final-newline normalization is accepted. Branch-local
selectors will be replaced by the combined selector in integration.
The STORE source manifest initially omitted later lazy-ID paths; its owner is
updating that record. Final branches retain their own validation records.

## Evidence and limits

MEM: wt_moar_memtables/.agentlogs/reviews/memtable-residency-validation.md
and logs/validation/. Final source code and tests are unchanged by record cleanup.

SELECT: wt_moar_metric_selection/.agentlogs/reviews/test-results/.
Build log 20260911-221747-ai-build.log. Nested entrypoint logs
20260911-223640-many-tables-launch.log and
20260911-223649-many-tables-launch.log. Generator 11+1 passed.

EXPORT: wt_moar_metric_exports/.agentlogs/reviews/metric-exports-validation.md.
Seven top-level classes plus the nested registration suite passed 28 unique cases.
Boundary properties ran 16000 name copies and 16000 query cases.

EXTRA review target: 92689444b6c7b0520a41a196cbe28ba50dfad02b.
Tree: 7ecf77b743af3769585f3dadc90851a500beefb5. Full owned production and
operator-document hashes match the pinned source. The test-only expiration
fix has baseline reproduction and three passing corrected class runs.

No numeric line or branch coverage was produced. Test counts are per branch;
shared classes overlap and must not be summed as unique repository tests.
Performance comparisons and combined reconstruction remain separate gates.

## Historical checkpoint

This record preserves the earlier task state. See
.agentlogs/reviews/many-tables-handoff.md for final branch and validation status.
