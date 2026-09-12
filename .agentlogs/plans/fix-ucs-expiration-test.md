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

# Fix UCS expiration-selection test timing

Unit-test skill inputs:

1. .agentlogs/plans/fix-ucs-expiration-test.md
2. Test-only changes to UnifiedCompactionStrategyTest relative to shared base
   c6631dcef555d278cbb0f6734a887cc94ee90e1b, in wt_moar_base and
   wt_moar_remaining. Preserve the remaining worktree's added hierarchy test.

Production files to inspect: UnifiedCompactionStrategy.java,
CompactionController.java, and unified/Controller.java. No production edits.

The mock controller returns a zero expiration-check interval. The strategy uses
elapsed milliseconds > interval, so consecutive selections in the same clock
tick can omit the expiration scan. These fixtures test expired-file selection,
not scheduling. Set their injected mock interval to -1 so equal timestamps also
permit a scan. Keep all no-candidate, expired-only, and mixed-selection assertions.

The unchanged test has already failed on both worktrees. Build and run the full
affected class repeatedly in both after the fix. Preserve logs and reports.
Record the correction separately from the feature extraction; do not resume the
branch split or change production scheduling as part of this task.

## Historical checkpoint

This record preserves the earlier task state. See
.agentlogs/reviews/many-tables-handoff.md for final branch and validation status.
