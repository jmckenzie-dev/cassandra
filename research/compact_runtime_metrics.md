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

# Compact runtime metrics measurements

The legacy implementation remains selectable with `optimized_metrics_enabled:
false` in cassandra.yaml. The configured default is true on this branch. Metrics
created before configuration loads use the legacy implementation so a later
explicit false setting remains effective. The setting applies to newly created
reservoirs at startup, not a live migration. Existing metric names, units, types,
and histogram exports remain available. SSTable histogram formats are unchanged.

All experiments use Java 21 in the dev container, an 8 GiB maximum heap and eight
available processors. Table workloads use N100. Standalone probes use 100
reservoirs for object-graph measurements. These graphs include shared reachable
objects once per batch; they are not heap-dominator retained sizes. Fixed-clock
probe timings exclude production clock/decay costs and are diagnostics. JMH
comparisons use the same update workload and production approximate clock.

## Optimization 1: empty and sparse storage

### Pre-change

Source baseline: c274d4232b, before reservoir implementation edits.

- Fresh N100 never-written: settled post-GC heap 110,334,416 bytes. All 100 tables
  remain logically available, with zero initialized TrieMemtables.
- Direct created/settled heap ownership: 3,400 user reservoirs, all empty;
  6,800 mutable backing arrays contain 15,596,800 payload bytes. Private bucket
  offsets add 2,032,000 bytes, excluding two shared default arrays.
- Standalone legacy reservoir: 543,744 reachable bytes per batch of 100, or
  5,437.44 amortized bytes per reservoir, at every tested occupancy.
- Existing JMH update benchmark, two forks and ten measured iterations: one
  thread 30.890 million updates/s; four threads 92.019 million updates/s. The
  four-thread samples ranged from 84.468 to 98.141 million/s.

Artifacts:

- `logs/compact-metrics-step1-pre/20260905-092941-residency-never-written-100t/`
- `logs/20260905-093613-inspect-resident-reservoirs.json`
- `logs/20260905-093445-many-tables-launch.log` (standalone probe)
- `logs/20260905-reservoir-before-t1.json`
- `logs/20260905-reservoir-before-t4.json`

The initial probe used printf, which the test logger split into separate records.
Later probe output uses a single println per record; measurement logic is unchanged.

### First iteration

The initial candidate allocates 16-counter pages on first use, preserves the
reference's two physical stripes and rounding, and shares private bucket offsets.
Snapshot construction hooks let both reservoirs return the existing snapshot
type without changing legacy histogram arithmetic or parent aggregation.

| Occupied buckets | Legacy bytes/reservoir | Initial candidate bytes/reservoir |
| --- | ---: | ---: |
| 0 | 5437.44 | 141.44 |
| 1 | 5437.44 | 701.44 |
| 4 | 5437.44 | 1661.44 |
| 64 | 5437.44 | 7005.44 |
| 164 | 5437.44 | 7005.44 |

Snapshots do not increase the reservoir's retained graph. Sparse storage works,
but densely populated reservoirs retain about 29% more memory because of page
headers and directories. Matched one-thread JMH measurements in the new benchmark
report legacy 33.242 million/s and candidate 29.125 million/s, a 12.4% reduction.
This iteration needs a dense representation before acceptance.

Artifacts: `logs/20260905-094158-many-tables-launch.log` and
`logs/20260905-reservoir-step1-peri-t1.json`.

The integration regression also reproduced pre-existing parent-release inflation:
five observations became ten after releasing a child. The optimized path now
merges released children into only the parent's own stored snapshot. The legacy
path retains its existing aggregation behavior. The failing regression is
`logs/20260905-094623-compact-release-regression.xml` (four cases, one failure),
with console output in `logs/20260905-094623-ai-ci-test.log`.

### Second iteration

Sparse mutations now share a monitor during promotion; dense updates use atomics.
Promotion occurs at 64 observations or half-capacity allocation. Fully populated
storage falls to 5,485.44 bytes/reservoir, within 1% of legacy. Empty remains
141.44 bytes; one and four occupied buckets use 717.44 and 1,677.44 bytes.
This lifetime observation threshold can make a slowly updated single-bin metric
permanently dense. Phase 2 will reduce unused stripe storage.

Four-thread JMH reports legacy 95.316 million/s versus candidate 94.839 million/s
(overlapping confidence intervals). One-thread JMH remains slower and variable:
33.581 versus 27.105 million/s. Despite its filename, the `post-t1` artifact below
records this intermediate result; the implementation needs another iteration.

Artifacts: `logs/20260905-095816-many-tables-launch.log`,
`logs/20260905-reservoir-step1-post-t1.json`, and
`logs/20260905-reservoir-step1-peri2-t4.json`.

### Final measurements

The third iteration separates small dense-update methods from synchronized
sparse helpers. Storage stays unchanged from the second iteration. The final
matched JMH batch reports the following medians across ten samples:

| Threads | Legacy million updates/s (range) | Compact million updates/s (range) |
| --- | ---: | ---: |
| 1 | 31.184 (29.379–33.504) | 29.620 (29.426–29.718) |
| 4 | 94.002 (87.284–97.940) | 94.009 (90.577–96.576) |

Single-thread median throughput is about 5% lower; four-thread throughput is
unchanged within observed variation. Dense storage adds 48 bytes/reservoir. These
are the remaining costs of the extra storage indirection. Accept this tradeoff
for the resident-memory objective; do not describe it as a general CPU speedup.

Eight fresh N100 runs alternate legacy/optimized/optimized/legacy per scenario:

| Scenario | Legacy settled heap MiB (range) | Optimized settled heap MiB (range) |
| --- | ---: | ---: |
| Never written | 105.091 (105.025–105.158) | 74.155 (73.683–74.628) |
| Written and flushed | 108.848 (108.785–108.910) | 77.853 (77.771–77.935) |

Each never-written run verifies 100 reads. Each written/flushed run completes
400 writes, 200 reads, and 100 SSTables. All runs report zero failures and clean
memtables. CREATE TABLE times span 8.93–9.15 seconds across these runs; two
samples per mode/scenario do not establish a timing improvement. Whole-JVM
savings include system/global metrics; do not project this full delta per table.

Direct user-table ownership establishes the incremental table benefit:

- Untouched tables: 15,596,800 counter payload bytes become zero at both created
  and settled checkpoints. The 2,032,000 private offset bytes also disappear;
  the optimized tables reference 3,648 bytes of shared offsets.
- Written/flushed tables: 15,596,800 counter payload bytes become 349,184 bytes
  across the same 3,400 reservoirs. Both modes have 900 populated reservoirs;
  the optimized implementation stores them sparsely. Object headers and page
  directories are excluded from these payload figures.

Final artifacts:

- `logs/20260905-101409-reservoirs-sparse-post-GJrFj4/` (both probes and JMH)
- Within that batch: `20260905-141715-041451-reservoir-summary.json`
- `logs/20260905-100638-metrics-sparse-post-HkqdIY/` (eight real-table runs)
- `logs/20260905-101409-137081-analyze-resident-metrics.json`
- `logs/20260905-101742-416660-analyze-resident-reservoirs.json`

The first analyzer invocation rejected the logger prefix on probe records after
all measurements completed successfully. The parser now accepts that prefix;
reanalysis of the unchanged raw artifacts passed. No benchmark samples were
discarded or rerun to repair this reporting defect.

Validation so far: clean build/Checkstyle (`logs/20260905-100417-ai-build.log`),
58 focused tests passed with one existing ignored legacy diagnostic
(`logs/20260905-095334-ai-test-memtable-lazy/`), and a passing focused rerun after
adding concurrent cumulative monotonicity assertions. The final harness suite
passed all 11 cases (`logs/20260905-101742-many-tables-launch.log`, 236.567 seconds).
Post-extraction reruns of the compact unit, generated-property, and integration
classes also passed. The first optimization is complete.

## Second optimization: allocate stripes on contention

The first optimization is committed as `e4a19edd49`. Its fresh baseline is
`logs/20260905-103548-reservoirs-stripes-pre-4ivws2/`. The compact reservoir uses
141.44 bytes when empty, 717.44 with one occupied bucket, 1,677.44 with four, and
5,485.44 after dense promotion. These are reachable graph bytes amortized over
100 reservoirs, with shared offsets counted once. The legacy graph remains
5,437.44 bytes at each occupancy.

Fresh median throughput before the stripe change is 31.234/29.680 million
updates per second for legacy/compact at one thread, and 98.152/95.163 million
at four threads. The ten-sample ranges are 29.399–33.368/28.687–29.838 million
at one thread and 92.669–101.720/92.717–97.561 million at four threads.

An additional serial-handoff probe lets four distinct worker threads update
each of 100 reservoirs in sequence. After at least 512 observations, compact
storage remains 5,485.44 bytes at one, four, and 164 occupied buckets. Legacy
remains 5,437.44. Snapshots retain no additional storage. Raw records are in
`logs/20260905-104754-many-tables-launch.log` and
`logs/20260905-104811-many-tables-launch.log`; the records include actual
thread IDs and exact exported populations. This establishes the target:
thread handoff alone should not allocate a second full stripe.

### Implementation and first iteration

Each histogram starts with one logical-width counter stripe. Dense updates use
a compare-and-set operation until an update detects contention. That event
finishes both increments on its selected stripe; later events use the configured
thread stripe mapping. Secondary directories and stores allocate on demand.
Snapshots sum all allocated stripes. Rescale rounds each physical stripe before
summing, preserving that aspect of the existing algorithm. The contention flag
is permanent for the reservoir; this change does not retire historical stripes.

The first iteration reduces dense uncontended graphs to 2,861.44 bytes, including
after four serial worker handoffs. Empty graphs use 157.44 bytes (16 more than
phase 1); one/four sparse buckets use 653.44/1,613.44 bytes. The first iteration
batch is `logs/20260905-105724-reservoirs-stripes-peri-0v8LWh/`.

The focused bundle passed its first eight classes. JMX virtual-table tests then
failed on port 7012 being in use; an isolated rerun passed without source changes.
Review requested explicit coverage that contention actually activates a second
stripe. The added test records 800,000 concurrent observations, makes each
stripe's count odd through real writes, and checks nonzero half-life rounding,
rebase, clear, and reuse. The updated unit and accuracy classes passed all 12
cases in `logs/20260905-105639-many-tables-launch.log`. Accuracy checks also use
one million raw observations to verify median, p99, p99.99, maximum, exact bucket
counts, and mean/standard-deviation bounds from the actual quantization error.
The integration suite checks speculative-retry thresholds through registered
timers in both configurations, including empty fallback and microsecond units.

The first iteration's one-thread medians are 32.865 million/s legacy and 29.076
million/s compact; compact is about 2% below its own fresh pre-change median.
Four-thread medians fall to 78.691/74.277 million/s, with broad ranges of
57.963–82.941/58.190–92.324 million/s. Both controls slow substantially versus
the fresh baseline. Preserve these samples and use the final repeated comparison
to assess the candidate; this run alone cannot isolate a stable throughput cost.

### First candidate post-change measurements

The first candidate's post-change batch `logs/20260905-110146-reservoirs-stripes-post-ByXzHX/` repeats
the graph sizes above and the 2,861.44-byte serial-handoff result. Dense
uncontended graphs shrink 47.84% relative to the first committed compact path.
Counter payload halves from 5,280 to 2,640 bytes; object and directory overhead
explain the smaller graph percentage. Shared offsets are counted once over 100
reservoirs. Snapshots retain no extra storage in these probes.

| Threads | Legacy million updates/s (range) | Compact million updates/s (range) |
| --- | ---: | ---: |
| 1 | 29.033 (28.350–29.382) | 28.861 (28.494–29.231) |
| 4 | 85.855 (75.214–90.691) | 88.307 (84.127–91.342) |

Compact single-thread throughput is about 2.8% below its own fresh pre-change
median; the legacy control also varies substantially between runs. Matched
final ranges overlap at both thread counts. Accept the memory reduction without
claiming a throughput improvement or treating these shared-host timings as a
precise regression bound. Final clean build and Checkstyle passed in
`logs/20260905-110021-ai-build.log`.

Eight successful table runs are recorded in the completed manifest
`logs/20260905-111311-metrics-stripes-post-completed-ov95rK/` and summarized in
`logs/20260905-111436-702150-analyze-resident-metrics.json`. Settled heap medians
are 105.254/73.847 MiB for legacy/compact never-written tables and 109.038/77.997
MiB after writes and flushes. CREATE TABLE times span 9.32–11.89 seconds; this
does not establish a timing change. All completed workloads have the expected
read/write counts and clean settled memtables.

The first six final table runs completed successfully. The next legacy control
failed during startup because native port 9042 was in use, before creating user
tables. The failed JVM did not exit after several minutes; it was terminated
with SIGTERM (status 143). Its original manifest and failure summary remain in
`logs/20260905-110414-metrics-stripes-post-iUE8b9/`. The two missing legacy
controls are repeated in fresh JVMs; the completed manifest includes those
retries and the six successful original runs. No failed workload is reported
as a successful measurement.

### Second iteration: sparse promotion threshold

Heap ownership exposed a regression in lightly written tables: user counter
payload rose from 349,184 bytes in the first committed compact implementation
to 395,008 bytes in this stripe candidate. There are 110 dense user stores,
versus none in the earlier sparse layout. Untouched counters still allocate
zero arrays. See `logs/20260905-111532-467860-analyze-resident-reservoirs.json`,
which also validates the updated analyzer against both older layouts.

A 128-cell primary stripe reaches the 50% occupancy threshold after four sparse
pages. Those pages and their directories/wrappers use about 712 bytes, versus
1,056 bytes for dense storage. The candidate promotes before dense storage is
smaller. Raise occupancy promotion to 75%, while retaining promotion at 64
updates for hot histograms. This is a correction within the stripe optimization;
the first candidate's post-change artifacts remain recorded as iteration data.

The corrected threshold passed all 19 compact unit, generated-property,
accuracy, and integration cases. The new boundary test covers six pages in a
128-cell store and eight in a 165-cell store, including snapshot rebase.
Build/Checkstyle passed in `logs/20260905-112037-ai-build.log`.

A fresh N100 written/flushed iteration completes 400 writes, 200 reads, and 100
SSTables with clean settled memtables. User counter payload is now 330,752 bytes,
with 1,800 sparse stores and no dense user stores. The same 900 reservoirs are
populated and 2,500 remain empty. All user stores use one physical stripe.
Artifacts: `logs/compact-stripes-peri2/20260905-112510-residency-written-flushed-100t/`
and `logs/20260905-112655-980694-analyze-resident-reservoirs.json`.

### Final measurements for the corrected threshold

The final reservoir batch is
`logs/20260905-113448-reservoirs-stripes-final-post-NPjzs4/`. Graph measurements
retain the 2,861.44-byte dense uncontended result. One-thread JMH medians are
31.511/29.521 million/s for legacy/compact. Compact is within 1% of its fresh
pre-stripe baseline (29.680 million/s). Four-thread samples remain sensitive
to placement: compact's two forks average about 95.6 and 65.6 million/s.
The full unbound ranges are 86.245–100.220 million/s legacy and 64.137–95.990
million/s compact; these samples remain in the report.

This host has two L3 cache groups. A matched repeat pinned to physical CPUs
8–15, which share one L3 cache, stabilizes both forks. JMH means are 97.891
million/s legacy and 95.780 million/s compact, about a 2.2% cost. The ten-sample
ranges are 97.184–98.439 and 94.556–96.595 million/s. This supports accepting
the memory reduction with a small measured update cost; it does not establish
universal performance across placements. Artifact:
`logs/20260905-1143-stripes-final-t4-one-l3.json`.

The measurement wrapper now accepts optional `MANY_TABLES_CPUSET`, records it,
and runs probes/JMH through `taskset` when set. Future counter-width comparisons
will use the same affinity before and after the change.

Another table control failed to bind storage port 7012 before its workload
(`logs/20260905-112742-metrics-stripes-final-post-4E5jK6/`). The failed JVM was
terminated after remaining alive for several minutes. The specific source of
these intermittent port conflicts is unconfirmed. The test harness now exposes
the existing cluster builder's subnet option. `--subnet N` selects `127.0.N.1`;
default 0 preserves standalone behavior. Comparison runs use distinct addresses
starting at subnet 71, with requested and effective addresses recorded and
validated. Seven config tests pass, including real unstarted builder provisioning
at subnets 0, 37, and 255. Production networking code is unchanged.

The final isolated matrix completed all eight fresh N100 runs on subnets 71–78:

| Scenario | Legacy settled heap MiB (range) | Compact settled heap MiB (range) |
| --- | ---: | ---: |
| Never written | 105.175 (105.008–105.342) | 73.975 (73.830–74.119) |
| Written and flushed | 108.616 (108.523–108.709) | 78.008 (77.769–78.246) |

All never-written runs complete 100 reads. Each written/flushed run completes
400 writes, 200 reads, and 100 SSTables. All settled memtables are clean; all
requested/effective metrics modes and network addresses match. Creation times
span 8.91–9.14 seconds; no timing improvement is claimed. Whole-JVM heap remains
similar to the first compact optimization, as expected for this small workload.

User ownership confirms zero counter payload for untouched tables. The two
written/flushed heaps retain 339,968 and 333,056 counter payload bytes, with
1,800 sparse stores, no dense user stores, and one physical stripe per user
store. The small-table regression is resolved. These bytes exclude wrappers
and directories; the separate graph probe establishes the dense-reservoir saving.

Final table artifacts:

- `logs/20260905-114551-metrics-stripes-isolated-post-0M4HdF/`
- `logs/20260905-115151-052860-analyze-resident-metrics.json`
- `logs/20260905-115151-163892-analyze-resident-reservoirs.json`

Validation includes the final 19 compact cases, seven config/provisioning cases,
separately passing legacy/export tests, the successful JMX retry, direct old/new
heap attribution, and the eight final table workloads. The second optimization
is complete. The final clean build and style checks also passed.

## Third optimization: narrower dense counters

The fresh baseline uses commit `6daa0ce965`, Java 21, 8 GiB heap, eight active
processors, and CPU affinity 8–15 for the reservoir probes and JMH. The baseline
batch is `logs/20260905-132408-reservoirs-counters-pre-NFPAnM/`.
Compact reachable graph bytes per reservoir are 157.44 empty, 653.44 for one
occupied bin, 1,613.44 for four bins, and 2,861.44 dense. Legacy uses 5,437.44
throughout. Serial handoff does not allocate secondary stripes.

Baseline JMH medians (ten measured samples across two forks):

| Threads | Legacy million updates/s (range) | Compact million updates/s (range) |
| --- | ---: | ---: |
| 1 | 30.782 (28.894–32.782) | 29.316 (28.870–29.555) |
| 4 | 86.150 (84.942–86.949) | 91.507 (86.293–93.974) |

Separate aged probes create reservoirs at clock zero and update at 1,799 seconds,
just before the 30-minute reset. Compact remains at 2,861.52 bytes and legacy at
5,437.52 bytes for 1/4/164 bins. Aged clock state accounts for the extra 0.08
amortized bytes. Console launch logs are `logs/20260905-132951-many-tables-launch.log`
and `logs/20260905-133007-many-tables-launch.log`.
This checks the regime where decay weights can require 64-bit storage even for
small observation populations.

The table comparison runner now accepts `--rows-per-table` and `--rate` while
retaining its original defaults. The fresh N100 dense baseline uses 128 rows
per table and rate 5,000, with one fresh JVM per mode/scenario and distinct
subnets 101–104. Batch: `logs/20260905-133023-metrics-counters-pre-4S2jsy/`.
All four runs pass. Written runs each complete 12,800 writes and 200 reads,
retain 104 SSTables, and finish with clean memtables. Settled whole-JVM bytes
are 110,153,720/77,570,792 for legacy/compact never-written and
111,532,112/79,066,352 for written/flushed. Creation spans 9.44–9.76 seconds.
These are single-run baseline observations, not estimates of timing variance.

The compact written heap contains 3,400 user reservoirs, 900 populated and
2,500 empty. Its 600 dense counter stores comprise 400 arrays of 128 cells and
200 arrays of 165 cells. All user stores have one physical stripe. Total counter
payload is 827,968 bytes, including 673,600 dense bytes and 154,368 sparse bytes.
Legacy retains 15,596,800 counter payload bytes. Reports:
`logs/20260905-133325-770352-analyze-resident-metrics.json` and
`logs/20260905-133319-370042-analyze-resident-reservoirs.json`.

Production edits began only after these baseline runs and ownership checks.

### Candidate and correctness checks

`AdaptiveCounterArray` initially holds an `AtomicIntegerArray`. Its volatile
backing reference changes once to an `AtomicLongArray` when a write needs a
larger or negative value. Widening allocates the complete long array before
freezing narrow cells. Each atomic freeze returns the latest cell value for
copying. Narrow writes use compare-and-set, so stale writers cannot overwrite
the frozen marker. Marker encounters retry against the published long array.
Strong compare-and-set hides migration-only failures, which would otherwise
falsely activate reservoir contention stripes. Long arithmetic remains unchanged.

Only dense stores use this class. The 16-cell sparse pages and their promotion
thresholds remain unchanged. Counter-width changes do not alter bucket offsets,
observation populations, decay weights, or exported snapshot representations.

The first focused run passes all 29 cases: seven array unit tests, one generated
array property, 13 reservoir unit tests, one reservoir property, two raw-event
accuracy tests, and five integration cases. Generated array traces compare all
cells with `AtomicLongArray` after 80,000 operations. Concurrency cases exercise
increments, monotonic reads, set, strong CAS, and simultaneous widening requests.
Public snapshot merge/rebase doubles populations past the int boundary; controlled
aged updates widen decay counts before the landmark reset. Both compare with
legacy and check that migration alone does not activate contention.

The first pinned iteration batch is
`logs/20260905-133848-reservoirs-counters-peri-YvPbRb/`.
Dense graph bytes fall from 2,861.44 to 1,581.44 (44.7%), including the new wrappers.
Empty and sparse graph measurements are unchanged. One-thread JMH medians are
31.012/28.727 million updates/s for legacy/compact, with compact about 2% below
its fresh pre-change median. Four-thread medians are 97.640/79.289 million/s;
compact forks average about 88.3 and 70.8 million/s. Compact samples span
70.557–89.435 million/s versus legacy 96.457–99.929 million/s. This is a material
contended-update cost, so the initial candidate is not accepted as final.

The second iteration widens a dense array after a failed narrow atomic addition
as well as overflow. Busy arrays then use direct long atomic additions; quiet
arrays retain narrow storage. This trades the narrow saving on contended stores
for cheaper updates without changing count semantics. The failed CAS has not
applied its delta, so widening must copy the latest values and add that delta once.

The revised candidate passes 30 focused cases, including a new test that retains
narrow storage for 100,000 serial additions, then widens under concurrent additions
with an exact final count of 900,000. Its pinned second iteration is
`logs/20260905-134244-reservoirs-counters-peri2-peri-DOzLx9/`. Graph measurements
remain unchanged. One-thread compact reaches 29.162 million/s versus its fresh
29.316 baseline; four-thread compact reaches 91.809 million/s (89.583–94.932),
versus 91.507 before the width change. Matched legacy medians are 33.134 and
89.058 million/s. Control variation remains substantial across batches; the
results establish recovery from the first candidate's cost, not a speedup over
legacy. All measured JMH iterations report zero collections.

The aged compact probe retains 2,237.52 bytes per reservoir, compared with
2,861.52 before this change. Its decay store needs wide counters, while the
cumulative store remains narrow. Artifact: `logs/20260905-134510-many-tables-launch.log`.
Memory savings therefore depend on age, population, and contention. A fully wide
dense store has an extra wrapper relative to step two; it does not save counter
payload. Widening does not revert in place. Replacement decay generations can
start narrow again after rescaling.

The revised N100 iteration uses the same 128-row workload on subnets 111–114:
`logs/20260905-134548-metrics-counters-peri-u3Q99R/`.
All four runs pass. Written runs again complete 12,800 writes, 200 reads, and
104 SSTables with clean settled memtables. All 600 dense user stores remain
narrow. User counter payload is 491,168 bytes: 336,800 dense int bytes and
154,368 unchanged sparse long bytes. The reduction from the fresh compact
baseline is 336,800 bytes (40.7% of this workload's user counter payload).
The number and length of arrays, directories, and physical stripes match the
baseline. Extra wrapper bytes are excluded here and included in the graph probe.
Whole-JVM settled heap is 105.048/73.800 MiB legacy/compact never-written and
105.745/75.116 MiB written/flushed. Reports:
`logs/20260905-134838-828821-analyze-resident-metrics.json` and
`logs/20260905-134838-839938-analyze-resident-reservoirs.json`.

### Final validation

The clean Java 21 build and style checks pass:
`logs/20260905-134846-ai-build.log`. The reusable `./run_tests.sh --reservoirs`
run passes 75 tests with one existing legacy ignore and no failures/errors.
Its 11 classes cover the new counter, compact reservoir/property/accuracy and
integration tests, legacy reservoir, thread-local histograms, registry, latency
aggregation, and JMX virtual-table exports. XML and test logs are retained under
`logs/20260905-135010-ai-test-memtable-lazy/`.

The independent source review finds no production correctness defect. The
contention-only stress case now requires more than one reported processor;
thread schedules remain nondeterministic, so this is not an exhaustive
linearizability proof. See `.reviews/compact-metrics-step3.md`.

The final pinned reservoir batch is
`logs/20260905-135223-reservoirs-counters-post-vdaifO/`. Quiet dense graphs remain
1,581.44 bytes, with empty and sparse measurements unchanged. Final JMH results:

| Threads | Legacy million updates/s (range) | Compact million updates/s (range) |
| --- | ---: | ---: |
| 1 | 33.334 (33.196–33.604) | 28.898 (28.786–29.304) |
| 4 | 96.717 (96.220–97.143) | 92.753 (90.355–94.618) |

Relative to the fresh compact baseline, final throughput is about 1.4% lower at
one thread and 1.4% higher at four. No speedup is claimed. Against the matched
legacy control, the complete compact path is 13.3% lower at one thread and 4.1%
lower at four; this includes the earlier compact-storage costs and is not an
end-to-end Cassandra throughput measurement. Final JMH iterations report zero
collections and only negligible amortized background allocation.

The final table matrix repeats the dense N100 workload twice, alternating mode
order on distinct subnets 121–128:
`logs/20260905-135442-metrics-counters-post-8bb2gP/`.
All eight runs pass with matching requested/effective modes and addresses. Each
never-written run verifies 100 reads. Each written run completes 12,800 writes,
200 reads, and 104 SSTables; settled memtables are clean.

| Scenario | Legacy settled heap MiB (range) | Compact settled heap MiB (range) |
| --- | ---: | ---: |
| Never written | 105.138 (104.934–105.343) | 73.718 (73.639–73.797) |
| Written and flushed | 105.745 (105.535–105.954) | 75.116 (75.079–75.152) |

Creation takes 8.74–9.22 seconds across all runs; no creation-time improvement is
claimed. Both compact written heaps retain exactly 491,168 user counter payload
bytes, including 600 narrow dense arrays and unchanged sparse payload. Every
user store retains one physical stripe. Untouched user reservoirs retain zero
counter payload. The legacy written heap still retains 15,596,800 counter bytes
plus 2,032,000 private offset bytes. Final reports:

- `logs/20260905-140026-622435-analyze-resident-metrics.json`
- `logs/20260905-140026-603726-analyze-resident-reservoirs.json`

The third optimization therefore retains the 40.7% incremental payload reduction
on this dense N100 workload, with exact primitive ownership measurements separate
from whole-JVM heap. These runs do not establish 100,000-table capacity.

The final original sparse workload also passes: N100, four rows per table,
400 writes, 200 reads, and 100 SSTables with clean settled memtables. It retains
336,128 user counter payload bytes, within step two's 333,056–339,968 range.
All 1,800 populated counter stores remain sparse, with no dense user arrays and
one physical stripe per store. Whole-JVM settled heap is 82,033,944 bytes.
Artifacts: `logs/compact-counters-sparse-post/20260905-140054-residency-written-flushed-100t/`
and `logs/20260905-140141-815733-analyze-resident-reservoirs.json`.

All three planned metrics optimizations are complete. The optimized path remains
enabled by default and the legacy path remains selectable at startup. Remaining
table residency work includes bounded automatic retirement and the per-table
objects/registrations that these counter-storage changes do not remove.

## Reproduction

Run commands through `distrobox enter dev --`. The repository wrappers write
timestamped console logs and preserve command failures.

```
env PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.ReservoirStorageProbe .build/sh/ai-profile-many-tables org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir 5 3 200000
env PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.ReservoirStorageProbe .build/sh/ai-profile-many-tables org.apache.cassandra.metrics.CompactDecayingEstimatedHistogramReservoir 5 3 200000
env PROFILE_MAIN_CLASS=org.openjdk.jmh.Main .build/sh/ai-profile-many-tables '.*CompactHistogramBench.update' -t 1 -f 2 -wi 3 -w 1s -i 5 -r 1s -prof gc -rf json -rff logs/reservoir-comparison-t1.json
.build/sh/ai-compare-resident-metrics --step sparse --checkpoint post --heap-dumps
```

Use distinct result filenames per invocation. Repeat JMH at four threads. Raw
heap dumps and build artifacts stay outside commits; each optimization commit
includes its measurement summary and reproducible tools.
