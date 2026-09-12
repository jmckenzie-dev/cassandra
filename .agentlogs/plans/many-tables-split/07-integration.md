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

# Review, reconstruction, and combined verification

Branch: many-tables/integration. Worktree: wt_moar_integration.
Owner: coordinator/reviewer. Parent: frozen many-tables/base.

## Independent review gate

For each branch, check its diff against BASE and its ownership manifest. Verify
builds and tests ran from that worktree with the claimed source revision.
Inspect failures/skips and relocated assertions. Check hidden feature dependencies.
A disabled feature flag is not evidence of source independence.

Review shared-file edits and late fixes from 1096f47ed4 and 319995ce5c explicitly.
Compare the intended change with final source, not only early commit messages.
Preserve original defaults during extraction. Track proposed revisions separately.

## Reconstruction

Merge the five branches in the recorded order MEM, SELECT, STORE, EXPORT, EXTRA.
Only the coordinator resolves integration conflicts. It uses 08-shared-files.md
to combine assigned hunks; it must not choose an entire conflicting file blindly.

Restore the final combined harnesses, cross-feature test methods, benchmark
drivers, and root test-selector combinations assigned INTEGRATION.
Feature branches retain their independent fixtures; combined fixtures may need
new class names to avoid duplicate test classes. Record the original method,
destination, and invariant for every moved test.

Compare all src/ paths with 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Compare configuration, resources, packaging, and documentation separately.
The ideal production diff is empty. Any required adaptation must have a named,
reviewed explanation. No unexplained runtime difference is acceptable.
Match all 246 changed source paths against 09-path-inventory.md. Include archived
files in the accounting even though integration need not copy historical records.

This static comparison is necessary but insufficient. Build the recombined tree
and run relevant tests: merging individually passing branches can introduce faults.

## Combined test matrix

- Metric profiles with legacy and compact reservoirs; aliases enabled/disabled.
- Profiles with lazy memtables, retirement, indexes, and hidden aggregate release.
- Each JMX option alone and together, with both storage modes and alias settings.
- Lazy identifiers with original and geometric meters, plus table lifecycle.
- Idle admission with the original and configurable UCS hierarchy.
- Lazy tombstone construction with retirement/flush workloads.
- Full combined MemtableResidency and HeapOwnership harness tests.

Do not rely only on existing broad wrappers. Check that nested generated tests
and standalone boundary programs actually ran.

## Measurements

First establish small correctness/smoke cases. Then run matched 100- and
1,000-table experiments with equal Java, collector, workers, data, file counts,
heap limits, and scrape behavior. Record effective controls, not only requested
options. Measure retained heap separately from allocation and transient peaks.
Do not add historical savings from different baselines.

Run benchmarks without competing build/test workloads. Freeze jars and test
classes while measurement processes exist. Preserve command status, logs,
test output, and measurement manifests inside each worktree.

## Completion and later work

Deliver branch/base/integration revisions, per-branch test evidence, reconstruction
diffs, moved-test map, benchmark results and limits, and complete residual inventory.
Keep all original branches and worktrees.

Move to newer trunk only after extraction reconstruction passes. Record the new
common upstream revision and repeat checks. Default changes, redesigns, and
upstream submission preparation are separate revisions.
