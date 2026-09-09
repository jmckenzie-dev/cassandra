<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the License); you may not
use this file except in compliance with the License. You may obtain a copy
at http://www.apache.org/licenses/LICENSE-2.0 . Unless required by applicable
law or agreed to in writing, software distributed under the License is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
-->

# Histogram counter widths over time

Measure current cumulative and raw decay-weighted bucket counts through the
actual compact reservoir, with an injected monotonic clock. Preserve runtime
code and dependencies. Compare actual snapshots with the legacy reservoir in
verification. Repository basis is HEAD 4192a00e1f3f26bc92a7a56da7cb40debe79ff9b
plus the existing working tree; production measurement scope is
CompactDecayingEstimatedHistogramReservoir and DecayingEstimatedHistogramReservoir.

Use at most 1000 reservoir instances to represent one histogram per logical
table. Do not call this a Cassandra node workload or a production traffic trace.
Simulate two hours with one-second event steps. Include never-written, one
early event, first events at 7/15/29 minutes, one event/minute, one event/second,
periodic bursts, sustained 32 events/second, and traffic that stops after one
minute. Compare concentrated bucket updates and deterministic spread values.
Extend both patterns to 24 hours with 100 instances to measure cumulative
width transitions while keeping total update work similar to the 1000-instance
two-hour runs.

Sample every minute and around the strict 30-minute landmark reset boundary.
Use existing package-local decayingStripeValues to read raw stored weighted
counts without reflection. Capture raw maxima before and after scheduled
getSnapshot calls. Scrapes can trigger rescaling, so distinguish their effects.
Get cumulative counts from actual snapshots and validate them against recorded
event totals and exact bin ledgers. Normalized snapshot values must not be
mistaken for stored weighted values. Record allocated cells and selected actual
reservoir graph measurements separately from minimum-width projections.

Classify each array by its largest bucket: zero, byte, short, int, or long.
Report checkpoint distributions by cohort and overall, exact maxima, and
epoch changes. Do not interpret a minimum sufficient width as the retained
width of OTel's widen-only implementation. Do not extrapolate these synthetic
cohort proportions into a node capacity claim.

Verify boundary behavior with the legacy reservoir and known direct cases,
including sparse late writes and strict reset timing. Use generated event/time
sequences for equivalence and cumulative-count invariants. Expose isolated
verification through root test scripts. Provide logged repeatable commands,
raw CSV, and research/histogram_width_over_time.md. Update TODO and continuation
notes when complete. No commit requested.
