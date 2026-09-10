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

# Bounded idle flush admission

Status: complete. Build and Checkstyle pass. All 74 focused tests pass, including
100,000 generated budget steps and real scheduler rate/byte measurements.
See research/bounded_idle_flush_admission.md for results and coverage limitations.

Keep automatic idle retirement optional and disabled by default. Retain operator-controlled
UCS `min_hierarchy_size` and `scaling_parameters` in table DDL. Do not add adaptive tuning
or opportunistic compaction in this change.

## Implementation

- Reuse the initialized-memtable candidate set and existing concurrency/reclamation limit.
- Add positive node configuration values: `memtable_idle_flush_max_per_second` (100)
  and `memtable_idle_flush_throughput` (16MiB/s). These are initial safety limits,
  not measured optimum settings. Timeout remains 0s.
- Maintain two node-wide token balances under the scheduler lock. Accumulate at most
  one second of credit. Require an operation token and positive byte credit before
  admission. Charge only accepted flushes, using estimated live memtable data bytes.
- Permit one flush to overdraw the byte balance. Repay that debt before admitting
  further idle flushes, so large candidates cannot starve. This is estimated admission
  pacing, not a hard disk bandwidth limit. Index/output sizes and concurrent writes
  can differ from the estimate. Ordinary flushes and background compaction retain
  their current limits.
- Preserve candidate generation and write-time checks, incremental scanning, shutdown,
  failure handling, and no per-table budget state. Preserve iterator cleanup on close.
- Document CREATE/ALTER examples, restart-required node settings, soft idle deadlines,
  burst allowance, and the distinction between hierarchy floor and compaction threshold.

## Validation

- Deterministic budget tests: both limits, refill boundary, capped credit, oversized
  debt, zero estimated bytes, clock wraparound, rejected submissions, and invalid input.
- Generated budget traces: count/byte envelopes and eventual admission after debt repayment.
- Real memtable tests: budget gating, delayed new writes, generation replacement,
  normal forced flush while throttled, pinned readers, drop, failure, and recovery.
- Distributed tests: production configuration wiring and scheduler pacing, disabled
  defaults, DDL hierarchy/scaling changes and restart persistence, reads and writes.
- Run focused unit/distributed suites sequentially through run_tests.sh; property
  cases through run_property_tests.sh. Build and Checkstyle. Inspect available coverage
  instrumentation and map requirements to tests; report any quantitative coverage limits.
- Run a small admission measurement with real flushes; report elapsed drainage and
  observed file/row results. Keep all runs at or below 1,000 tables.
- Preserve existing uncommitted hierarchy/census work. No commit requested.
