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

# Compact metrics step 2 review

Date: 2026-09-05. Scope: the uncommitted adaptive-stripe implementation,
75% occupancy promotion, focused tests, resident-heap analyzer, harness subnet
selection, and optional benchmark CPU affinity. Contract:
[compact-runtime-metrics.md](../.plans/compact-runtime-metrics.md).

## Result

Source review is complete. No unresolved production correctness findings.
The earlier Major coverage finding is resolved. The parent reports that the
corrected tests passed. Final residency validation and performance acceptance
remain with the parent agent; source approval does not claim that the final
measurement matrix has completed.

## Production checks

- `update` chooses one stripe for both increments. A failed primary compare-and-set
  completes that increment exactly once through atomic addition. Both counters
  execute before the sticky contention flag changes subsequent routing.
- `StripedBuckets.stripe` publishes the secondary directory and each store through
  compare-and-set. Losing allocators read the published winner. Primary indices
  remain fixed when secondary stores appear. Existing counts do not move.
- Snapshots sum the primary and allocated secondary stores from the captured
  counter generation. Rescale reads each physical stripe separately and rounds
  before storing its replacement. Clear visits allocated cumulative stores;
  rebase clears them and places merged counts in the primary stripe.
- Bucket offsets, cumulative export layout, snapshot classes, and decay arithmetic
  remain unchanged. Consolidating initially uncontended histories changes the
  partition used for rescale rounding. Single-writer property traces therefore
  use the legacy one-stripe implementation as their exact arithmetic reference.
- Concurrent rescale retains the existing reservoir's weak consistency: an update
  racing replacement can remain in the old decay generation. This patch does not
  establish linearizable rescale or concurrent rebase.
- The heap analyzer follows inherited primary fields and all published secondary
  stores. It unions array identities across dense and sparse representations and
  includes both page and stripe directories. Older heap layouts remain supported.
  Physical allocation counts for phase-1 multi-stripe paged stores are explicitly
  unavailable rather than inferred from configured capacity.
- Update and set/rebase paths both promote at 75% allocated capacity. The fixed
  64-update threshold still promotes hot sparse stores. This changes storage
  choice only; the copy/publication protocol and counter arithmetic are unchanged.
  Length is bounded by the existing bucket limit, so the threshold's integer
  multiplications do not overflow for supported histograms.

## Measurement harness checks

- `--subnet` defaults to 0 and accepts 0 through 255. The harness invokes the
  existing builder's subnet option before node provisioning. Other harnesses
  inherit an empty hook. Requested subnet and effective listen/RPC addresses and
  ports appear in the summary; the analyzer validates the addresses.
- The comparison wrapper assigns one distinct subnet per fresh JVM. Its default
  two-repeat matrix uses 71 through 78, and range validation rejects a matrix
  that would exceed 255. Alternating implementation order and workload settings
  remain unchanged.
- `ai-measure-reservoirs` applies optional `MANY_TABLES_CPUSET` through a quoted
  `taskset` command array. Both probes and JMH inherit that affinity. An absent
  option preserves normal scheduling; a missing/failed `taskset` propagates its
  status through the existing log pipeline. The batch records requested affinity.
  Affinity does not reserve the selected CPUs from other processes.

## Coverage finding and resolution

The initial concurrent test could pass while every update remained in the primary
stripe. Its 30-minute reset also removed the small old counts, leaving physical
stripe rounding untested.

`contentionActivatesStripesAndRescaleRoundsEachStripe` now uses eight real writers,
checks observed contention, two allocated stripes, and exactly 800,000 events.
It then adds at most two real observations to make both physical counts odd. A
controlled 60-second reset checks each rounded physical count and proves the
result differs from rounding the combined count. Cumulative counts remain exact
through snapshot, rebase, clear, and reuse. The added diagnostic only reads counts;
it neither changes routing nor injects counter state.

`moderatelyOccupiedStorageStaysSparseThroughRebase` checks logical lengths 128
and 165 below and at the new occupancy threshold, including snapshot rebase.
The existing hot-storage test retains the 64-update boundary. Subnet tests
provision real unstarted clusters at 0, 37, and 255, inspect all four configured
addresses and ports, and reject missing, malformed, or out-of-range arguments.

## Validation and remaining work

- Parent-reported results: corrected compact tests passed (19 tests), subnet
  configuration tests passed (7 tests), and the Java Management Extensions
  integration retry passed after an earlier port conflict.
- Parent-reported final four-thread measurement, pinned to CPUs 8–15 in one
  shared last-level cache: legacy 97.891 million updates/second and compact
  95.780 million, a 2.2% throughput cost. Two forks stabilized with that placement.
  Raw measurement details and acceptance belong in the research record.
- The final isolated N100 comparison matrix remains in progress. Confirm its
  workload completion, requested/effective settings, heap results, and variation.
- Parse the new heap dumps and an older dump with the updated analyzer to verify
  physical allocation reporting and preserved old-layout support.

## Parent validation completion

The final isolated N100 matrix passed all eight runs on subnets 71–78. The
analyzer confirmed requested/effective addresses and metrics selection, exact
workload completion, and clean settled memtables. Old and new heap layouts parsed
successfully. Final written user payload is 339,968/333,056 bytes; untouched user
counters retain zero arrays. Artifacts are recorded in
`research/compact_runtime_metrics.md`. Source acceptance has no outstanding
correctness findings; the measured throughput tradeoff is documented there.

The reviewer ran read-only source/diff inspections, Python syntax parsing for the
analyzer, and `git diff --check`. Those checks passed. The reviewer ran no builds,
unit tests, heap analysis, or benchmarks.
