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

# Fix the retained paging failures

1. Preserve the canonical empty grouping state until a live result supplies a
   partition key. Do not complete an unfinished group at a tombstone checkpoint.
2. Use internal grouping state to distinguish already returned rows from an
   initial empty checkpoint when suppressing repeated static-only output.
   Clear unfinished-group state when a result page completes. Do not transfer
   another partition's grouping state to an IN child after an empty checkpoint.
   Apply the original short-read exhaustion check only to the initial response;
   later refills use their own row limit.
3. Handle read-only heap sources in buffer-to-array copying without treating the
   heap offset as a native address. Enable the retained crash regression and
   restore read-only heap cases to the fixed-seed state property.
4. Run the retained forward/reverse, static GROUP BY, and three-node regressions;
   add combinations for grouping boundaries, limits, static data, and reconciliation.
   Run focused existing pager, buffer, and fuzzer tests. Keep all traversal bounded
   and use no sleeps.
5. Count distinct added test methods against the original implementation commit
   `0c202df78b`. Separate additions from expanded pre-existing methods and
   parameterized executions. Record each confirmed implementation defect fixed
   by the later commits and current worktree.
