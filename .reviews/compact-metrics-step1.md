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

# Compact metrics: first optimization review

Scope: the first optimization in `.plans/compact-runtime-metrics.md` against
baseline c274d4232b. Later stripe and counter-width work is excluded.

An adversarial source review found no blocking or major defects. It traced
configuration loading, legacy/compact snapshot merging, clear/rebase, cumulative
exports, and sparse-to-dense promotion. Sparse mutation and copying use the same
monitor; dense storage publishes before the sparse directory is removed. Readers
recheck dense storage after observing a removed directory.

The reviewer identified one minor test gap: the concurrent snapshot loop did not
assert per-bucket cumulative monotonicity during promotion. That assertion was
added and the focused reservoir class passed afterward. The earlier integration
regression reproduced parent-release inflation (five observations became ten).
The optimized-parent correction passed the full focused reservoir bundle. Legacy
parents retain the reference behavior, including with optimized children.

The reviewer authored the integration tests earlier, but did not implement the
production changes or execute validation. Runtime evidence comes from the root's
build and test logs. The reservoir suite reported 58 passes and one existing
ignored legacy diagnostic in `logs/20260905-095334-ai-test-memtable-lazy/`.

Limits: overlapping decay updates retain the reference's weak consistency;
rebase requires exclusion of concurrent updates. Configuration selects future
objects, not live migration. Performance acceptance is recorded separately in
`research/compact_runtime_metrics.md` after the final measurement iteration.
