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

# OpenTelemetry storage benchmark

Run from the Cassandra checkout with Java 21 and an existing `ai-build` output.
The launcher compiles the actual Cassandra adaptive counter source and three
small OpenTelemetry (OTel) source files from `ref/opentelemetry-java`. It does
not install a dependency, edit the reference checkout, or change the server.
Compiled classes go to `tmp/`. Console output and errors also go to `logs/`.

```bash
distrobox enter dev -- ./run_tests.sh --otel-storage
distrobox enter dev -- ./run_property_tests.sh --otel-storage
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --iterations 1000000 --warmup-rounds 5 --rounds 9
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --bucket-ranges
```

Repeat the timing command in three separate Java Virtual Machines (JVMs).
The default population is 1000 arrays, with 165 buckets each. `--population`
accepts 2 through 1000. `--buckets` changes the array length. The test runners
do not run timed performance tests.

The primary benchmark measures identical bucket updates through `long[]`,
Cassandra `AdaptiveCounterArray`, and OTel `AdaptingIntegerArray`. Cassandra
performs atomic updates; the other two require a single owner. Setup, clearing,
seeding, full checksums, and logging are outside timed intervals. Steady batches
cap updates at 64 per object so byte counts do not widen. Construction, first
touch, and width transitions have separate cases. Raw CSV retains every sample.

Memory rows report reachable graph bytes excluding the outer array, plus the
difference between populations of 1000 and 999 to exclude shared state. These
are storage objects, not complete histograms or registered table metrics.
The complete benchmark ends with `# benchmark=PASS`; reject truncated logs.

Summarize complete logs with `uv run --no-project --python venv/bin/python
.build/benchmarks/otel-storage/summarize.py LOG1 LOG2 LOG3`. The summarizer
checks settings, paired counts and checksums, and memory consistency. It writes
pooled statistics, per-JVM medians, and all raw samples to `logs/`.

`--weighted-overflow-check` is a separate compatibility probe that currently
exits with status 1. A narrow OTel counter containing 1 returns 0 after adding
`Long.MAX_VALUE`; Cassandra and Java long addition return `Long.MIN_VALUE`.
This probe preserves the failed comparison for a future implementation. The
normal verification and property suites cover the supported benchmark domain.

`--bucket-ranges` uses the actual OTel circular counter and indexer at fixed
scales. It selects a fitting scale offline and reports exact quantile bucket
bounds for synthetic positive inputs. It does not exercise the SDK aggregator,
runtime downscaling, or a percentile estimator. The full bucket class depends
on an AutoValue processor that the current Cassandra dependencies do not
provide; the standalone storage classes do not need that processor.

Results and limitations: [research report](../../../research/otel_compact_storage_benchmark.md).

## Counter widths over time

`--width-over-time` runs the actual compact reservoir with an injected clock.
It records raw weighted counts before and after scrapes, cumulative counts,
normalized percentile counts, and selected object graphs. It measures state
under synthetic traffic, not database throughput or a production trace.

```bash
distrobox enter dev -- ./run_tests.sh --histogram-widths
distrobox enter dev -- ./run_property_tests.sh --histogram-widths
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 1000 --seconds 7200
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 100 --seconds 86400 --spread
```

Add `--spread` to distribute events across 12 buckets. Without it, events use
one bucket. Each run has ten equal cohorts, so `--tables` accepts multiples of
10 through 1000. Every observation includes a scrape, which can reset decay
state. “Pre” observations follow that second's updates but precede its scrape.

Summarize complete logs with `uv run --no-project --python venv/bin/python
.build/benchmarks/otel-storage/summarize-widths.py LOG1 LOG2 ...`. Tables, full
JSON, commands, and interpretation limits are in the
[time-series report](../../../research/histogram_width_over_time.md).
