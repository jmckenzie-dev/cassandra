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

September 7, 2026. Cumulative counters remain useful candidates for byte/short
storage under quiet traffic. The current decay representation often needs int
or long storage even when its normalized percentile buckets contain very small
counts. A uniform switch to the OpenTelemetry (OTel) adaptive array would not
retain its narrow-storage advantage throughout a decay cycle.

This experiment runs the actual compact reservoir with an injected clock.
It covers two simulated hours with 1000 histogram instances and 24 simulated
hours with 100 instances. Both durations run concentrated and spread inputs.
These are deterministic component workloads, not a node with real tables or
a production traffic trace. No runtime code, configuration, or dependencies
changed. No commit was requested.

## Stored weighted counts grow while normalized counts stay small

For one event per second into one bucket:

| Simulated time | Cumulative count | Stored weighted count | Minimum weighted width | Normalized percentile bucket |
|---|---:|---:|---|---:|
| 0 | 1 | 1 | Byte | 1 |
| 5 minutes | 301 | 2697 | Short | 84 |
| 10 minutes | 601 | 89064 | Int | 87 |
| 15 minutes | 901 | 2852779 | Int | 87 |
| 25 minutes | 1501 | 2921338186 | Long | 87 |
| 30 minutes | 1801 | 93482824795 | Long | 87 |
| 30 minutes, 1 second | 1802 | 87 | Byte | 87 |
| 31 minutes | 1861 | 170 | Short | 85 |
| 2 hours | 7201 | 90298456683 | Long | 87 |

The drop after 30 minutes comes from the real reservoir's rescale. Its update
at second 1801 triggers the reset before that checkpoint's scrape. The same
pattern repeats through the run.

The reservoir adds forward-decay weights that grow with time since its
landmark. It divides the stored values by the current weight when producing
percentile snapshots. At second 1800 the weight is roughly a billion, so a
stored count around 93 billion normalizes to 87. These normalized values are
the snapshot's percentile bucket counts, **not the metric's cumulative Count**.

At the two-hour concentrated checkpoint:

| Activity | Largest cumulative bucket | Largest stored weighted bucket | Largest normalized percentile bucket |
|---|---:|---:|---:|
| 1 event/minute | 121 | 2074332472 | 2 |
| 1 event/second | 7201 | 90298456683 | 87 |
| 100 events every 5 minutes | 2500 | 107062321200 | 103 |
| 32 events/second | 230432 | 2889550613858 | 2786 |

Spreading observations across 12 buckets reduced the two-hour weighted
maximum for one event/second to 8012134019. It still required long storage;
the normalized maximum was 8. Spreading counts helps, but does not remove the
weight multiplier.

## A first write can already need a wide weighted counter

Each of these reservoirs was created at time zero and received no earlier
events. Scheduled observations preserved the original landmark before the
30-minute reset threshold.

| First event time | Cumulative count | Stored weighted count | Minimum weighted width | Normalized bucket |
|---|---:|---:|---|---:|
| 7 minutes | 1 | 128 | Short | 1 |
| 15 minutes | 1 | 32768 | Int | 1 |
| 29 minutes | 1 | 536870912 | Int | 1 |

Low event count therefore does not imply narrow weighted storage. The
29-minute single event still fits in int; long width is not required merely
because the landmark is old.

In the 24-hour concentrated run, the one-event/minute cohort reached an
observed weighted maximum of 2147483648, one beyond the signed-int limit.
Rounding and epoch timing can decide whether that store needs to widen.

## Cumulative counters have more headroom

Largest cumulative bucket after 24 hours:

| Activity | Concentrated value / width | Spread across 12 buckets: value / width |
|---|---|---|
| 1 event/minute | 1441 / short | 121 / byte |
| 1 event/second | 86401 / int | 7201 / short |
| 100 events every 5 minutes | 28900 / short | 2409 / short |
| 32 events/second | 2764832 / int | 230403 / int |
| 32 events/second for first minute, then idle | 1920 / short | 160 / short |
| One event, then idle | 1 / byte | 1 / byte |

Events occur at both second zero and the final second. Thus the one-event/
second case contains 86401 observations. It crosses from short to int at
second 32767, when its cumulative bucket reaches 32768. The extra checkpoints
at seconds 32767 and 32768 capture that transition directly.

The ten equally sized cohorts include four single-event cohorts and one
never-used cohort. At 24 hours, the concentrated run's cumulative arrays were
10% zero, 40% byte, 30% short, and 20% int. The spread run was 10% zero, 50%
byte, 30% short, and 10% int. These proportions reflect the chosen workload
mix. They are not estimates of a production node's traffic or memory saving.

The summary also reports equal-weight minute-checkpoint distributions,
excluding extra boundary checkpoints. They describe the sampled population;
they are not continuous-time residency fractions.

## Inactivity and actual resident storage

Elapsed time does not itself change the stored counters. For the cohort with
one event at time zero, raw weighted storage still contained 1 immediately
before the scrape at second 1801. That scrape reduced it to zero. The
cumulative counter remained 1.

The cohort that recorded 32 events/second for its first minute still retained
a raw weighted maximum of 2688 before that scrape. Rescaling removed those
decayed values, but preserved its 1920 cumulative observations.

Measured marginal **whole-reservoir** object graphs in the concentrated run:

| State | Bytes per reservoir |
|---|---:|
| Never written | 144 |
| One event, before decay rescale | 640 |
| One event, after decay rescale | 392 |
| First-minute traffic stopped, before rescale at 1801 | 1568 |
| Same stopped traffic, after rescale | 856 |
| 1 event/second at 30 minutes | 2224 |
| 1 event/second after its update-triggered rescale at 1801 | 1104 |
| 1 event/second at 2 hours | 2224 |

These are actual current structures, including sparse pages or dense arrays,
not hypothetical OTel objects. The stopped cohort retains a dense cumulative
array after its decaying storage disappears. A zero logical array and an
unallocated array are different states; the CSV reports allocated cells too.

Jamm measures reservoir-only root graphs. Marginal subtraction between
identical cohort populations removes shared clock/offset storage and the
outer reference arrays. Shared fixed storage was 1352 bytes per measured
cohort graph. Do not add that shared amount repeatedly when estimating a
larger graph. These measurements exclude table objects, metric wrappers,
registration, parent aggregates, and JMX.

## Implications

Treat cumulative and decay-weighted storage as separate optimization targets.
Byte/short cumulative storage could preserve exact values and existing bucket
definitions for quiet histograms. Its benefit depends on activity per bucket
and the counter's lifetime. A concurrency-safe implementation is still needed;
the OTel array tested previously requires exclusive ownership.

For weighted storage, investigate limiting the stored magnitude before
assuming narrower arrays will deliver durable savings. Candidates include
initializing a decay landmark on first use, or an optional decay representation
with more frequent normalization or a different scale representation. Each
requires an A/B test of percentile accuracy, rounding, merge behavior, and
update/scrape costs. Storing the already-normalized integer snapshot directly
is not an equivalent algorithm: it discards information used in future decay.

Minimum sufficient width is not OTel's retained width. Its `clear()` preserves
the widest allocated backing. Recompression or replacement at a rescale would
need explicit implementation and lifecycle validation. No OTel recorder was
integrated or measured in this time-series experiment.

The larger registration/JMX and worker-ID residency tasks remain unchanged.
This experiment does not revise the whole-node capacity estimate or establish
that one million tables fit in a given heap.

## Method and verification

The standalone launcher compiles the actual compact and legacy reservoir
sources, the existing adaptive counter, and test-only fixtures. The existing
launcher uses Java 21, G1, a 2 GiB maximum heap, eight configured processors,
and the Jamm agent. The injected monotonic clock advances by seconds; no
two-hour or 24-hour wall-clock wait is required. This measures value and object
state, not throughput or latency.

There are ten cohorts: never used; one event at zero; first event at 420, 900,
or 1740 seconds; one event/minute; one event/second; 100 events every 300
seconds; 32 events/second; and 32 events/second for seconds 0 through 59.
Each cohort has identical histories. Concentrated input is value 1000.
Spread input cycles through the actual offsets for bucket indexes 40–51.
There are 165 logical cells and two configured stripes, with only the primary
stripe active. No concurrent recording occurs.

Each second's updates precede observations. Every checkpoint calls
`getSnapshot`, including the regular 60-second checkpoints and extra boundary
checkpoints at 1801, 3602, 5403, 32767, 32768, 65535, and 65536 seconds.
Those scrapes can trigger rescaling. “Pre” means after updates but before that
scrape; an active cohort may have rescaled during its update already. No
inference about an unobserved or differently scraped workload follows.

The existing package-local `decayingStripeValues` method reads raw stored
counts without reflection or rescaling. The sampled values cover all bins
this workload can update. Every reservoir's cumulative snapshot is checked
against an exact bin ledger at each checkpoint. Reported maxima and widths
are observations at those checkpoints; they do not establish unobserved peaks
or all exact crossing times between samples.

Deterministic checks passed for delayed first writes, raw bucket access
including the overflow bucket, and the strict 30-minute reset threshold
through both updates and scrapes. Generated tests passed 32000 steps across
16 seeds, comparing actual compact and legacy cumulative buckets, normalized
buckets, and landmarks. They include time advances, recordings, and clears.
An independent read-only review found no measurement defect in the Java
fixture and plan.

## Reproduction and artifacts

All commands returned exit status 0:

```bash
distrobox enter dev -- ./run_tests.sh --histogram-widths
distrobox enter dev -- ./run_property_tests.sh --histogram-widths
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 1000 --seconds 7200
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 1000 --seconds 7200 --spread
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 100 --seconds 86400
distrobox enter dev -- .build/sh/ai-benchmark-otel-storage --width-over-time --tables 100 --seconds 86400 --spread
```

| Run | Raw log under logs/ |
|---|---|
| Verification | 20260907-153815-537244596-otel-storage.log |
| Generated checks | 20260907-153857-790938382-otel-storage.log |
| 2 hours, concentrated | 20260907-153857-835099576-otel-storage.log |
| 2 hours, spread | 20260907-154040-526736178-otel-storage.log |
| 24 hours, concentrated | 20260907-154503-189249286-otel-storage.log |
| 24 hours, spread | 20260907-154608-183257968-otel-storage.log |

Summarize the four measurement logs with:

```bash
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/benchmarks/otel-storage/summarize-widths.py logs/20260907-153857-835099576-otel-storage.log logs/20260907-154040-526736178-otel-storage.log logs/20260907-154503-189249286-otel-storage.log logs/20260907-154608-183257968-otel-storage.log
```

The summarizer verifies completion, populations, width distributions,
aggregate sums/maxima, minute checkpoint coverage, and cumulative preservation
across scrapes. It retains every raw observation in JSON. Final artifacts:
`logs/20260907-154847-186285-histogram-width-summary.json` and matching `.log`.

Source fixture: [HistogramWidthOverTime.java](../.build/benchmarks/otel-storage/org/apache/cassandra/metrics/HistogramWidthOverTime.java).
Previous storage benchmark: [otel_compact_storage_benchmark.md](otel_compact_storage_benchmark.md).
