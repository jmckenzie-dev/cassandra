<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Independent measurement audit

## Smoke and profile batches

Independent review approved the eight smoke cases and four profile cases.
It found no blocking, major, minor, or nit finding. All 12 linked summaries
exist. Field-by-field comparison found no mismatch in copied checkpoints,
operation counts, heap deltas, write lateness, or phase/drain durations.
Both drivers record the frozen revision, Java version, JAR and class hashes,
commands, per-case PASS markers, and batch completion.

All requested and effective controls agree: subnet, heap, processor count,
memtable pool, profile, storage mode, initialization mode, idle timeout, and
optional switches. All cases have no failure, failed write, phase error, or
warning. Real SSTable counts and flush counts match each workload.

Reviewed report SHA-256 values:

- integration-smoke-measurements.json:
  937df87b3e0f3bf4c2370b3936b593002c11c7466694e1cec1535a77997300a0
- integration-profile-measurements.json:
  8f63bfbecadcef43a8b962e38664f21cff6ad09289690dd46595b2801839f8ee

The review used the two driver logs under
logs/20260912-002857-split-measurements---smoke and
logs/20260912-003304-split-measurements---profiles, plus every report-linked
summary. It ran no competing build, test, heap analysis, or measurement.

## Limits

Fixed-order single samples do not establish confidence intervals, stable
effect sizes, production capacity, or throughput. Profile runs recorded
399-404 late writes at 100 tables and 2,642-2,759 at 1,000 tables. Maximum
lateness was 16.01-37.66 ms. No sampled CPU profile was available. Allocation
counters cover only the harness thread. Profiling was intentionally off.

The cursor-compaction setting is enabled in the idle smoke cases, but BTI
forces normal compaction fallback. The stale policy string is documented in
the measurement plan. Neither field supports a cursor-compaction claim.

The smoke/profile audit above excludes the large idle and history census
cases. Separate audits follow.

## Idle batch

Independent review approved the four successful idle cases. All referenced
summaries exist. Field-by-field comparison found no mismatch in checkpoints,
operation counts, write lateness, drain durations, or watchdog values. Every
successful case has no failure, failed write, phase error, or summary warning.
Both drivers record the frozen revision and binary hashes, Java 21.0.12.1,
and commands. The original driver records three PASS markers, then stops on
the fourth case's watchdog failure. The retry records its PASS marker and
batch completion.

All cases use subnet 0, an 8 GiB heap, eight active processors, a 256 MiB
memtable pool, all metrics, compact storage, lazy memtables, both JMX options,
lazy metric identifiers, compact table bookkeeping, transient names, geometric
meters, and lazy tombstone histograms. Profiling is off. Requested values match
the effective configuration and JVM arguments.

| Tables | Idle | Watchdog | Writes / reads | Final dirty | SSTables / flushes / compactions | Drain seconds |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 100 | off | 120 s | 1,600 / 500 | 100 | 0 / 0 / 0 | none |
| 100 | on | 120 s | 1,600 / 500 | 0 | 100 / 400 / 100 | 9.558-14.901 |
| 1,000 | off | 120 s | 16,000 / 5,000 | 1,000 | 0 / 0 / 0 | none |
| 1,000 | on | 300 s | 16,000 / 5,000 | 0 | 1,000 / 4,000 / 1,000 | 94.766-148.104 |

Idle-off cases retain initialized dirty memtables. Idle-on cases settle with no
dirty, flushing, or pending memtables and return to dormant lazy memtables.
Post-GC heap deltas from baseline are 122,330,280 versus 20,339,912 bytes at
100 tables and 1,316,436,400 versus 290,744,264 bytes at 1,000 tables for idle
off versus on. These are single observations.

The preserved original 1,000-table idle attempt completed 16,000 writes and
3,000 reads. Its first three drains took 99.497, 101.560, and 103.155 seconds.
The fourth reached the 121.071-second watchdog with 211 dirty memtables and
raised the recorded drain failure. The retry changed only `--settle-seconds`
from 120 to 300 as a workload control; its output path also changed. All
Cassandra settings, workload arguments, effective settings, and startup flags
match. The retry completed four drains in 95.136, 94.766, 97.744, and 148.104
seconds. Its driver contains normal shutdown messages, no shutdown error or
exception, a PASS marker, and batch completion. It also contains the standard
startup warning about the missing JVM out-of-memory termination option.

Cursor compaction is requested and recorded enabled in all four successful
cases. The BTI SSTable format forces normal compaction fallback. The summary
policy text that says cursor compaction is disabled is the documented stale
string and does not change the effective setting.

Late writes were 413 and 399 at 100 tables, then 1,401 and 2,262 at 1,000
tables for idle off and on. Maximum lateness was 11.04-29.09 ms. These fixed-order
single runs do not establish throughput, confidence intervals, or production
capacity. Process CPU sampling is unavailable, and allocation counters cover
only the harness thread.

Reviewed `integration-idle-measurements.json` SHA-256:
`aabcbf2bc89e63f0b5cd96afdfc9c45005078e940074c62ae1f508b5c6532e85`.

## Census and heap analysis

Independent review approved both census records and all six linked heap
analysis records. The copied effective settings and checkpoints exactly match
their summaries. The four copied scrape records exactly match their source
JSON files after adding the phase name. All copied analyzer fields and all
per-table aggregate totals match the linked analyzer outputs.

Each analysis found the expected 100 tables. The recorded layout uses
four-byte references, four-byte class pointers, eight-byte alignment, and a
16-byte array header. This agrees with the effective compressed-pointer and
alignment settings. Every heap contains 3,000 user-table JMX wrappers with
88,000 shallow bytes.

Standard history has no user-table arrays before the scrape. Its first and
final scrapes each contain 800 long arrays with 892,000 shallow bytes. Adaptive
history has no arrays after the first empty scrape. After eight workers and the
final scrape, it contains 200 byte arrays with 32,800 shallow bytes and 600
null histories. These values agree with the table in
`integration-measurements.md`.

These are shallow array sizes for bounded user-table ownership. They are not
retained sizes or dominator results. Ownership groups can overlap and must not
be summed. The census excludes native and metaspace bytes, class-dump mirrors,
and traversal beyond its approved bounds. Class histograms and heap dumps can
represent different instants. One fixed-order sample per case does not establish
a stable effect size.

Reviewed `integration-census-measurements.json` SHA-256:
`a24f3d363af5b551bffd82299af8b1134744175667100073bd90f8e01a3d3657`.
