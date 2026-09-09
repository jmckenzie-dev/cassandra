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

# JMX registration and table metric bookkeeping

## Objective

Reduce resident heap for many tables through three steps: measure the remaining
Java Management Extensions (JMX) registration ownership in the latest lazy-ID
implementation, simplify the largest avoidable registration structures, then
compact TableMetrics.ownedMetrics release bookkeeping. Report pre-change,
intermediate, and final measurements for each implementation step.

Maintainability and clarity take priority over the last increment of memory
saving. Prefer small changes to existing components, shared immutable metadata,
and ordinary Java collections or arrays. A smaller measurable improvement with
clear ownership is preferable to a bespoke monitoring framework.

Preserve existing metric names, values, visibility, and client behavior. Keep
the previous implementation available as a measurement control. Do not change
histogram arithmetic, worker counter algorithms, lazy-ID defaults, metric
profiles, or memtable retirement in this task. Do not stage or commit.

## Starting context

Read AGENTS.md and these reports before implementation:

- research/worker_metric_residency.md: final lazy-ID implementation and controls.
- research/jmx_query_export.md: previous JMX savings, compatibility, harnesses.
- research/optimized_heap_next_steps.md: ownership and ownedMetrics estimate.
- research/metric_profile_runtime.md: hidden recorders and aggregate contracts.

The current working tree contains substantial uncommitted earlier work. It is
the baseline, not HEAD. Preserve all existing changes. Capture relevant source
copies, Git status, and a path manifest in
tmp/jmx-registration-bookkeeping-baseline/ before editing. Include additional
original files before modifying them. Keep a separate changed-path manifest at
tmp/jmx-registration-bookkeeping-paths.txt. Do not use hashes for provenance.

The last matched 1000-table lazy-ID run retained 117,109,176 B of whole heap
and 651,712 B of payload in eight worker counter arrays. Its artifacts are:
logs/worker-metric-final-lazy-1000/20260907-202139-heap-ownership-1000t/.

The earlier detailed JMX census retained 32,252,488 B (30.76 MiB) in disjoint
JMX server subtrees. That census predates lazy IDs. It is neither an exact
current ownership figure nor an achievable saving. The earlier ownedMetrics
estimate was 2624 B/table for map structure versus roughly 528 B for an exact
flat name/value array, before holder and spare-capacity costs. Verify both
figures in the current baseline.

## Step 1: current baseline and ownership

Before production edits, build the current tree and save a control JAR. Run a
fresh 1000-table baseline using the existing heap-ownership harness. Hold fixed:
Java 21, heap/collector settings, simple_metrics.yml, compact reservoirs, lazy
TrieMemtables, lazy metric IDs, adaptive JMX history, transient JMX queries,
compact registration, eight workers, and the property-query workload.

Use the existing report's command and launcher flags; verify actual selected
settings rather than assuming an environment variable reaches the node JVM.
The fresh run must include created, scraped, populated, and rescraped states.
Keep all actual table counts <=1000. A 100-table run may help isolate fixed
cost, but do not assume noisy startup subtraction proves a per-table slope.

Analyze the last existing lazy heap immediately if useful, then use the fresh
run as the comparison baseline. Use the existing dominator and bounded-owner
analyzers. Attribute:

- ObjectNames, canonical strings, property arrays/offsets, and duplicate names.
- JMX repository entries and maps.
- Metric wrappers, standard adapters, and metadata reachable through them.
- Cassandra metric registry and TableMetrics.ownedMetrics structures.

Separate shallow sizes, bounded reachable graphs, and disjoint retained sizes.
Do not add overlapping groups or count all strings as avoidable. Separate fixed
JVM/classpath costs from table-associated state. Identify the largest supported
optimization and report the measured breakdown and proposed change to parent
before implementation. This is a progress checkpoint, not an approval gate.

## Step 2: registration structures

Implement the simplest measured opportunity first. Inspect existing compact
adapters and remaining standard adapters for repeated per-instance metadata
or wrappers before considering changes to JDK registration internals.
Reuse existing dispatch, configuration, interfaces, and tests when possible.

Do not fork or copy the JDK MBean server/repository, patch JDK internals, use
Unsafe/private reflective mutation, replace the monitoring protocol, build a
general pluggable metrics framework, or add an unbounded interning/cache map.
Do not introduce a dependency. Share metadata only when its public mutability
contract permits sharing; otherwise provide the required independent view.

Keep the old registration path selectable. Prefer the existing optional
compact-registration switch if its semantics cover the change. If another
option is required for a distinct representation, choose one clear startup
setting and document its scope/default. Do not change earlier defaults.

Preserve exact names and aliases, canonical-name equality, metadata and
descriptors, attributes and operations, queryNames/queryMBeans patterns,
QueryExp behavior, local/remote connector behavior, authorization, registration
notifications, duplicate registration, retry after removal, and drop/recreate.
Do not silently suppress metrics to obtain a smaller heap. Respect arbitrary
non-metric MBeans and existing custom-server compatibility limitations.

Add focused regression coverage for changed contracts, implement, iterate, and
measure a matched intermediate profile with ownedMetrics still unchanged.
Measure monitoring allocation and latency separately from resident heap:
representative attribute reads, full scrapes, narrow/missing/broad name queries,
and repeated monitoring. Use enough repetitions to distinguish a consistent
cost from scheduling variation. Do not claim a latency win from fewer bytes.

If the remaining large costs belong to JDK representations that cannot be
reduced within these maintainability constraints, document the boundary and
measured residual; continue to step 3. Do not claim the entire footprint is
avoidable or expand into a server rewrite to reach an arbitrary target.

## Step 3: ownedMetrics bookkeeping

Inspect all ownedMetrics readers, construction helpers, subclasses, and release
paths. Replace its map structure with a compact representation if the measured
population and usage support it. A flat name/metric array is a candidate, not
a prescribed implementation. Keep the representation local to TableMetrics
unless actual reuse justifies another component.

Preserve duplicate-name behavior, hidden recorders, no-op handling, aggregate
membership/removal, registration cleanup, and all/simple profile behavior.
Protected helper methods may be called after the base constructor by external
subclasses. Preserve supported late additions and lookups; do not assume the
constructor permanently freezes the collection. Match existing concurrency
and release semantics, including failed/partial construction where applicable.

Do not add lookup/allocation to metric recording. A linear lookup during setup
may be acceptable for a measured bounded collection; quantify construction and
release cost as well as resident capacity, headers, and spare slots. Avoid a
generic custom-map library or elaborate upgrade/downgrade state machine.

Keep the map control available through a small testable selection boundary or
startup option if needed for repeatable A/B runs. Preserve the default control
unless evidence supports choosing the compact representation and document any
default choice. Test both paths. Measure bookkeeping alone against the same
registration mode, then the combined final implementation.

## Validation and acceptance

Read ~/.config/opencode/TESTING.md before authoring tests. Use production
interfaces and dependency injection; no private-field/bytecode mutation. Extend
root run_tests.sh and run_property_tests.sh only where needed for reuse.

Run relevant JMX registration/query/authorization tests, registry/profile and
recording integration tests, and focused ownedMetrics lifecycle/subclass tests.
Exercise all/simple profiles and both controls. Add generated equivalence
tests where they provide useful coverage of patterns or lifecycle sequences.
No full test suite. Build with .build/sh/ai-build, including Checkstyle.

Acceptance for a retained optimization requires attributable resident savings,
passing compatibility/lifecycle tests, and measured costs for affected paths.
Reject mandatory recording-path regressions. Explain monitoring or construction
tradeoffs and repeat measurements if a claimed improvement is within noise.
If an experiment fails, record the result and retain the better implementation.

Report baseline -> registration-only -> bookkeeping-only -> combined results
where switches make these comparisons practical. At minimum, preserve separate
baseline and post-change evidence for each step; never attribute a combined
saving wholly to both changes. Use frozen JARs or documented startup switches
for exact controls, sequential runs, and unchanged launchers during execution.
Do not build or run heavy heap analysis while a measurement JVM is active.

## Deliverables and handoff

The implementation agent owns scoped production/config/test/harness changes,
the baseline snapshot and path manifest, and
research/jmx_registration_and_metric_bookkeeping.md. It is not alone in the
codebase; preserve others' edits and coordinate overlapping work. Parent owns
this plan and integration decisions. Do not spawn additional agents.

Report the baseline breakdown, each proposed bounded design, failures, tests,
intermediate and final measurements, exact commands/logs, defaults, remaining
costs, and tradeoffs. Send regular concise progress to parent and a final
summary of planned/built/tested/measured outcomes. Do not invent million-table
capacity from these runs. Move only completed TODO entries to DONE; if remaining
registration work is material, narrow its TODO instead of marking it solved.

Use Java 21 through distrobox enter dev and existing ai-* build/test/profile
wrappers. No direct Ant/Maven/Gradle, dependency installs, commits, or staging.
Use uv with ./venv for Python. Temporary files belong in project tmp/; scripts
must tee stdout/stderr to dated logs/ files and preserve command return codes.
Avoid nested distrobox launches: run benchmark drivers inside dev when they
invoke dev-local Java wrappers. Preserve existing work when a command fails,
diagnose the failure, and report a blocker promptly instead of waiting silently.
