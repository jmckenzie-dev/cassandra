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

# Metric selection blocked checkpoint

Date: 2026-09-11

The coordinator stopped feature implementation and validation after
`UnifiedCompactionStrategyTest.testDropExpiredSSTables` failed on both the
remaining branch and the frozen baseline. The failure is outside SELECT
ownership. The failing assertion expected a non-null compaction pick.

The coordinator later corrected and verified the shared base. Work resumed from
this checkpoint on base `f7d5a7e2c0fcf769293ee27073630a3574584b06`.

The metric-selection worktree contains an uncommitted first extraction pass.
No metric-selection build or test wrapper ran. No commit was created. Resume by
checking the coordinator's baseline classification, reviewing the source
manifest, then compiling before making any extraction commit.

Pending work:

- Fix compile or test defects found in the first extraction pass.
- Run both metrics-reference generator test modes and stale-output checks.
- Check profile and catalog packaging.
- Run the planned build and eight targeted test classes.
- Run the small all-versus-simple profile comparison if host capacity permits.
- Review the final diff and split compact bookkeeping into its own commit.

## Resolution

The corrected base removed the blocker. Build, generator, packaging, and focused
tests passed. The generic harness could not control the in-JVM profile, so the
matched comparison moved to the integration harness. The branch review units
preserve the final tested source tree.
