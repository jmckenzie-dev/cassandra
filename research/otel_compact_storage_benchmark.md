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

# OpenTelemetry compact storage benchmark

September 7, 2026. OpenTelemetry (OTel) adaptive counters save memory while
counts fit in a byte or short. The measured single-owner updates cost less than
Cassandra's atomic updates, but more than plain long-array updates. This is
evidence for the storage technique. It is not a tested concurrent histogram
replacement, and it does not reduce the JMX registration memory from the
[latest heap census](optimized_heap_next_steps.md).

No server behavior, metric registration, configuration, or dependency changed.
The benchmark compiles the actual source files from the local OTel checkout
and Cassandra working tree. The existing implementations remain intact.

## Measurement design

The three controls use identical bucket indexes and positive increments:

- Plain `long[]`: single-owner fixed-width control.
- Cassandra `AdaptiveCounterArray`: current atomic int storage that promotes
  to atomic long storage on overflow or update contention.
- OTel `AdaptingIntegerArray`: plain byte storage that promotes through short,
  int, and long arrays. A test-only package bridge calls its actual methods.

Each population contains 1000 storage objects. Each object has 165 buckets:
the default 164 Cassandra offsets plus overflow, without a separate zero
bucket. Zero-aware histograms require 166 slots. These objects are counter
arrays, not complete histograms, tables, or metric registrations.

The runs use Java 21.0.12, G1, a 2 GiB maximum heap, 512 MiB initial heap,
eight configured processors, and the existing Jamm instrumentation agent.
Only one thread records. Three separate Java Virtual Machines (JVMs) run
sequentially. Each has five warm-up rounds and nine measured rounds per case.
There are 1053 retained raw samples, with 27 samples per implementation/case.
Implementation order rotates each round. No samples or pauses are discarded.
The host is not dedicated or CPU-pinned; report ratios and JVM variation rather
than treating these nanosecond measurements as hardware-independent constants.

Steady cases request one million updates per sample. Each batch contains at
most 64 updates per object. Clearing and seeding occur before the timed and
allocation intervals, so byte counts remain byte counts. Hot cases update
bucket zero of each object; spread cases update up to 64 distinct buckets per
object. Full checksums run after timing. Those scans and resets affect cache
state; these are warm storage measurements, not random cold-table access.

Construction and transition cases measure 10000 operations per sample.
Construction includes creating each object and its backing storage. First
touch excludes construction. Each widening operation starts with a fresh
object seeded at the relevant maximum, then adds one. Setup and logging are
untimed. Allocation counters cover only the benchmark thread.

Memory uses reachable object graphs measured by Jamm through `ObjectSizes`.
The difference between populations of 1000 and 999, after subtracting each
outer array's shallow size, removes shared state. This yields owned storage
bytes per object, not garbage-collector dominator retained sizes.

## Retained storage

All three final JVMs returned identical memory results.

| Largest count / state | Plain long array | Cassandra adaptive | OTel adaptive | Saving against Cassandra |
|---|---:|---:|---:|---:|
| Empty allocated array | 1336 B | 712 B | 216 B | 69.7% |
| Byte: up to 127 | 1336 B | 712 B | 216 B | 69.7% |
| Short: up to 32767 | 1336 B | 712 B | 384 B | 46.1% |
| Int: up to 2147483647 | 1336 B | 712 B | 712 B | 0% |
| Long | 1336 B | 1368 B | 1368 B | 0% |
| Cleared after widening to long | 1336 B | 1368 B | 1368 B | 0% |

One large bucket widens the entire array. OTel's array constructor eagerly
allocates a byte array. Its enclosing histogram can defer that construction,
but this array is not itself lazy. Cassandra's empty compact reservoir also
defers counter storage; the empty row does not show a saving over an empty
reservoir that has allocated no array.

Clearing OTel's array does not shrink it. That differs from this branch's
adaptive JMX saved history, which can narrow or release its array on a scrape.
At full width, both adaptive objects have 32 bytes of overhead over `long[]`.

## Update time and allocation

Pooled median nanoseconds per update:

| Case | Plain long array | Cassandra adaptive | OTel adaptive | Paired OTel / Cassandra median |
|---|---:|---:|---:|---:|
| Hot byte | 0.928 | 2.891 | 1.796 | 0.630 |
| Spread byte | 1.076 | 3.189 | 1.856 | 0.590 |
| Hot short | 0.918 | 2.835 | 1.982 | 0.692 |
| Spread short | 1.054 | 3.146 | 2.021 | 0.652 |
| Hot int | 0.919 | 2.833 | 1.877 | 0.666 |
| Spread int | 1.093 | 3.237 | 2.068 | 0.646 |
| Hot long | 0.935 | 4.018 | 1.799 | 0.465 |
| Spread long | 1.081 | 4.327 | 2.119 | 0.480 |

OTel used about 31–54% less update time than the atomic reference, according to
the paired median ratios. It used about 74–115% more time than plain `long[]`.
All measured steady updates allocated zero bytes in all implementations.
The extra width checks are measurable; removing atomics does not make adaptive
storage cheaper than an ordinary long array in this workload.

The size of the timing improvement varied between JVMs. For hot byte updates,
OTel's per-JVM medians were 1.523, 1.801, and 1.824 ns, while Cassandra's were
3.214, 2.889, and 2.878 ns. Across every steady case and sample, paired
OTel/Cassandra time ratios ranged from 0.401 to 0.802. Every case favored OTel
in this single-owner experiment, but the exact percentage is not a production
throughput prediction. Full distributions and per-JVM medians are in the
summary artifact.

| Operation | Cassandra median ns | OTel median ns | Cassandra allocated bytes | OTel allocated bytes |
|---|---:|---:|---:|---:|
| Construct empty array | 29.122 | 9.323 | 712 | 216 |
| First update, construction excluded | 3.306 | 1.786 | 0 | 0 |
| Byte to short boundary | 3.292 | 57.528 | 0 | 352 |
| Short to int boundary | 3.249 | 63.969 | 0 | 680 |
| Int to long boundary | 593.908 | 69.481 | 1352 | 1336 |

Cassandra starts at int width, so its byte/short boundary rows are ordinary
updates. OTel pays an allocation and array copy at those boundaries. Its
int-to-long transition is cheaper than Cassandra's atomic freezing protocol,
but it offers no concurrent-update guarantee. Transition timings show wider
variation than steady updates; the summary retains all samples.

## Fixed-scale exponential bucket experiment

The full OTel bucket class depends on an AutoValue-generated implementation.
The required processor is absent from the existing Cassandra dependencies.
No dependency was installed and no substitute implementation was created.
Instead, a separate test compiles the actual standalone circular counter and
exponential indexer. It selects a fitting scale offline. It does not execute
OTel's runtime downscaling or measure its cost.

Four deterministic positive-integer corpora each contain 200000 observations.
The test checks exact bin counts, capacity rejection, independent copies,
clear behavior, and containment of median, p99, p99.99, and maximum in the
reported buckets. These are bucket intervals, not percentile estimates.
The reported maximum is also a bucket interval; retaining an exact maximum
would require a separate scalar, as the OTel aggregator can do.

| Synthetic distribution, 165-bucket budget | Fitting scale | Used index span, including empty bins | Adjacent-bound ratio minus one | Circular counter bytes |
|---|---:|---:|---:|---:|
| Values 1000–1999 | 7 | 129 | 0.54% | 416 |
| Log-uniform values across 1–10^12 | 2 | 161 | 18.92% | 416 |
| Mostly 1000–1999, 0.02% near 10^9 | 3 | 161 | 9.05% | 416 |
| Values 1000–1999 plus one 10^12 outlier | 2 | 121 | 18.92% | 744 |

The last case's concentrated counts require int storage. Narrower and wider
budgets of 64 and 256 also ran; all output remains in the range log. A
64-bucket budget forced scale zero for the broad corpus, with 100% between
adjacent bounds. These examples support testing precision independently from
counter width. A single extreme value can reduce resolution throughout the
distribution. The exact outcomes of runtime downscaling depend on recording
order and starting scale; this offline test does not establish them.

Circular-counter memory includes its bookkeeping and adaptive array. It
excludes the enclosing histogram, indexer, synchronization, min/max/sum,
export buffers, and any retained scratch counter used during downscaling.

## Compatibility and next steps

The measured array is useful for a future single-owner recorder or for data
that an existing lock already protects. It cannot replace Cassandra's atomic
array directly. Concurrent writers, collector publication, retirement,
thread-exit handling, and aggregate ownership remain separate design work.

Duplicating arrays per worker can erase the saving. Four byte-width copies
occupy 864 bytes, above one current 712-byte array. Two short-width copies
occupy 768 bytes. This calculation concerns storage only; lazy per-worker
creation and actual table access patterns determine whether duplication helps.

Counter semantics also need care. A separate compatibility probe reproduced
an OTel narrow-add overflow: start at 1, then add `Long.MAX_VALUE`. Cassandra
and Java long addition return `Long.MIN_VALUE`; OTel returns 0 because the
overflow happens before its widening check. Short and int starting widths
also differ; an already-wide array agrees. The probe remains an explicit
failing comparison under `--weighted-overflow-check`. The default checks cover
positive updates without that narrow weighted overflow, plus overflow after
the array has already widened. No claim of arbitrary signed or weighted
counter equivalence follows from the timing results.

Cassandra decay further limits how often narrow counts would help. Its
forward-decay weight doubles each minute relative to the landmark. At seven
minutes, even one observation contributes 128; at fifteen minutes, it
contributes 32768. Thus low event count does not necessarily imply byte or
short storage for the decaying counts. Cumulative counts and decaying counts
need separate occupancy measurements before predicting whole-table savings.

Recommended follow-up: measure the actual distribution of maximum cumulative
and weighted bucket counts over time, then evaluate byte/short storage within
a safe ownership model. Keep existing bucket definitions for that experiment.
The evidence does not yet justify replacing the histogram algorithm or
adopting per-worker copies. The larger JMX/registry and worker-ID residency
tasks remain on the existing TODO list.

## Validation, iterations, and artifacts

All paths below are relative to the repository root. All ordinary commands
returned exit status 0. The explicit weighted-overflow comparison returned 1,
which reproduces the compatibility defect described above.

1. `distrobox enter dev -- ./run_tests.sh --otel-storage`: boundary, clear,
   copy, and fixed-scale range checks passed. Launcher logs:
   `logs/20260907-124932-751506642-otel-storage.log` and
   `logs/20260907-124933-601240101-otel-storage.log`.
2. `distrobox enter dev -- ./run_property_tests.sh --otel-storage`: 32000
   generated operations across 16 seeds passed against an exact long-array
   oracle. Log: `logs/20260907-124950-801543492-otel-storage.log`.
3. Exploratory timing: `distrobox enter dev -- .build/sh/ai-benchmark-otel-storage
   --iterations 100000 --warmup-rounds 2 --rounds 3`. Log:
   `logs/20260907-125032-145432836-otel-storage.log`. This shorter warm-up had
   higher early-case timing; it is not pooled with the final measurements.
4. Final timing, executed three times: `distrobox enter dev --
   .build/sh/ai-benchmark-otel-storage --iterations 1000000 --warmup-rounds 5
   --rounds 9`. Logs:
   `logs/20260907-125102-565637739-otel-storage.log`,
   `logs/20260907-125224-127938798-otel-storage.log`, and
   `logs/20260907-125348-213000331-otel-storage.log`.
5. Independent review found a fixed-stride issue for optional bucket counts
   divisible by 73. The benchmark now selects stride one for those sizes.
   The default 165-bucket measurement path is unchanged. A smoke run with
   `--iterations 10000 --warmup-rounds 2 --rounds 3 --buckets 73` passed:
   `logs/20260907-125550-049019867-otel-storage.log`.
6. `distrobox enter dev -- .build/sh/ai-benchmark-otel-storage
   --weighted-overflow-check`: exit 1, with all four widths printed in
   `logs/20260907-125528-374454536-otel-storage.log`.
7. Summary: `uv --cache-dir tmp/uv-cache run --no-project --offline --python
   venv/bin/python .build/benchmarks/otel-storage/summarize.py` followed by the
   three final log paths. It verifies settings, counts, checksums, and memory
   consistency before aggregation. Output:
   `logs/20260907-125630-906084-summarize-otel-storage.json` and matching `.log`.

After the review changes, both root test commands passed again. Final logs:
`logs/20260907-125935-200793960-otel-storage.log` (deterministic),
`logs/20260907-125936-102381901-otel-storage.log` (ranges), and
`logs/20260907-125935-162778666-otel-storage.log` (properties).

The standalone launcher compiles the actual production storage sources and
test fixtures with `javac`. It does not build the OTel SDK. No Cassandra
production file changed, so no database tests or server rebuild were needed
for this storage experiment. Test entry points and reproduction instructions
are in [.build/benchmarks/otel-storage/README.md](../.build/benchmarks/otel-storage/README.md).
