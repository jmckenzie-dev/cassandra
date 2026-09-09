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

# OTel storage benchmark review

Scope: new test fixtures under `.build/benchmarks/otel-storage`, the standalone
launcher, and `.plans/otel-compact-storage-benchmark.md`. Comparison basis:
working tree at `4192a00e1f3f26bc92a7a56da7cb40debe79ff9b`. No production edits.

An independent read-only review found no blocking defect in the default
165-bucket measurement. It confirmed actual source execution, identical
updates, fixed widths, untimed setup/checksums, rotating order, and marginal
memory accounting. The fixed-scale experiment reports storage and bucket
intervals; it does not claim to execute runtime downscaling.

One minor finding was corrected: stride 73 collapsed spread workloads at
optional bucket counts divisible by 73. Those sizes now use stride one.
The default measurement indexes remain unchanged. A 73-bucket smoke run passed.

Review also identified a compatibility failure outside the timed +1 workload:
OTel narrow weighted addition can overflow before widening. The benchmark now
preserves that comparison under `--weighted-overflow-check`; it fails for
byte, short, and int starting widths and agrees after prior long widening.
The source was not modified to conceal the failure.

The performance test author completed deterministic width/clear/copy checks
and 32000 generated operations across 16 seeds. Root ran those checks and
three final timing JVMs with 1000 objects, five warm-up rounds, and nine
measured rounds. All 1053 paired samples had matching checksums. Ordinary
verification passed again after review changes. The explicit compatibility
probe remains a failing test, with exit status 1.

Result: accept the benchmark as evidence about single-owner counter storage.
It does not establish concurrent update safety, decay compatibility, complete
histogram memory, JMX savings, or database throughput. See
[the full report](../research/otel_compact_storage_benchmark.md) for exact
commands, logs, distributions, and remaining integration work.

## Counter widths over time

A subsequent independent read-only review covered `HistogramWidthOverTime.java`
and `.plans/histogram-width-over-time.md`. It found no defects. Raw reads use
the actual reservoir accessor without triggering rescale. Cumulative snapshots
are checked against exact bin ledgers, and generated checks compare the actual
compact and legacy implementations. Marginal graph measurements remove shared
clock and offset storage.

The report preserves the review's limits: updates precede “pre” observations;
all checkpoints scrape; observed maxima do not establish between-sample peaks;
event endpoints are inclusive; and minimum logical widths differ from actual
retained backing widths. Root completed all four measurement runs and both
verification modes with exit status 0. See
[results](../research/histogram_width_over_time.md) for full evidence.
