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

# Lazy metric aggregation and retirement

Recorded 2026-09-06 on branch `moar_tables`, production code at `24714c59fd`.
This document preserves the design discussion. It does not select or implement
a new recorder, retirement protocol, or metrics provider. No performance result
below establishes the performance of these candidates.

## Assessment

Single-writer metric contributions, lazy parent snapshots, and merging retired
contributions into shared history form a viable design candidate. Normal updates
can avoid modifying a shared parent histogram. Collection and retirement then
pay the cost of combining contributions.

The prerequisites are actual writer ownership, safe snapshot publication, and
an exactly-once transfer of each retired contribution. Volatile fields alone do
not provide those properties. A table is not a single writer: concurrent requests
can update the same table's metric objects.

The intended benefit is low contention during ordinary recording, with memory
for active contributions and a bounded amount of merged parent history. The
main costs are multiple contributions for active tables, collection work,
publication buffers, and retirement coordination. An idle timeout is not a bound
on the number of active contributions.

The immediate branch priority remains registration residency. The YAML profiles
exist, but runtime loading and enforcement remain unimplemented. This document
is a candidate for subsequent recording work, not a prerequisite to adding the
registration filter.

## Context and contracts

The target is one million logically available tables on a node. All executed
table workloads remain limited to 1,000 tables. A million simultaneously busy
tables and a million available tables with a small active subset have different
memory requirements; retirement primarily helps the latter.

The current implementation retires TrieMemtable backing structures, not metric
history. Keeping a table readable from disk does not require keeping its
optional histogram in memory, but it does require preserving database state and
any statistics used for database decisions.

The [heap census](heap_ownership_census.md) identifies registration as a major
cost: about 147 KiB/table of incremental Java Management Extensions (JMX) server
retained memory and about 40 KiB/table in the metric registry map before scraping.
These are measurements of the current branch. They do not predict the savings
of the designs in this document. The census recording workers were activated
sequentially; that experiment is not a contention benchmark.

The operator can select optional metric exports through the proposed allowlist.
Required internal state must survive every selection. Enabled distributions must
remain representative, including median and tail percentiles. Exact numerical
identity is no longer required for the optional optimized implementation, but
acceptable error and publication delay have not been specified. This does not
authorize silently resetting an enabled table's cumulative history on sleep.

Related documents:

- [Internal consumers and dependencies](internal_metric_dependencies.md).
- [Full table and keyspace aggregate inventory](metric_aggregates.md).
- [Earlier worker-owned metrics and OpenTelemetry research](metric_threading.md).
- [Compact recorder measurements](compact_runtime_metrics.md).
- [Memtable and metrics continuation record](prosecute_memtable_tables.md).

## Current aggregation is not a single hierarchy

There are 126 canonical table names, 101 keyspace names, and 106 node-wide Table
names in the current source catalog. These counts exclude aliases and individual
timer/histogram attributes. They include conditional and built-in storage-format
metrics. Keyspace and node-wide metrics describe the local node.

The aggregate inventory separates their inputs:

| How values are obtained | Node-wide Table names | Keyspace names |
|---|---:|---:|
| Read table counters or gauges | 41 | 22 |
| Combine table latency children | 10 | 20 |
| Receive observations in separate recorders | 41 | 41 |
| Read storage state directly | 14 | 8 |
| Record keyspace operations directly | 0 | 10 |

The first two rows depend on table metric state today. These are output counts,
not independent backing-object counts. One table contribution can feed both a
keyspace output and a node-wide output.

[LatencyMetrics](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L179)
reads its children's counts, rates, distributions, and total latency. The global
Read, Write, Range, KeyMigration, and RangeMigration families have table children;
keyspaces also have other latency families. The node parent can read table
children directly. It is not necessarily a parent of the keyspace recorder.

[TableHistogram, TableMeter, and TableTimer](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L1205)
already send observations to separate table, keyspace, and node-wide recorders.
Removing a table recorder does not remove observations already sent to those
parents. Other enabled gauges can still need that recorder's values; for example,
MutatedAnticompactionGauge reads two table meter counts.

Sleeping a table must not remove its disk usage from a current-state aggregate.
Storage gauges and historical event distributions therefore need different
retirement policies.

## Two different worker-owned designs

### A. Record directly into worker-owned aggregates

Each worker records an observation in its own keyspace and node-wide aggregate
histograms. It also updates a table-local recorder when required.

```text
operation on table T, executed by worker W
  -> W's keyspace K / metric F contribution
  -> W's node-wide / metric F contribution
  -> T's optional or required local recorder

query -> merge worker contributions for the requested aggregate
```

A worker's node-wide histogram combines observations across all tables it serves.
It stores bucket totals, not a list of tables or per-table contributions. For one
metric family, 64 stable workers serving one million tables need at most 64 live
worker aggregate histograms, plus retained history and collection storage.
They do not need one million worker aggregate histograms.

This design decouples parent history from table residency from the start.
However, it duplicates recording across enabled destinations, and optional table
history is an additional cost. Workers also need distinct keyspace contributions
when they serve multiple keyspaces.

### B. Record worker-owned table contributions and aggregate lazily

Each worker records into a contribution associated with an active table and
metric. Parents read those contributions when queried. Retired contributions are
merged into each applicable parent's historical state.

```text
worker W -> active contribution for table T / metric F
                         |
                         +-> keyspace snapshot reads it
                         +-> node-wide snapshot reads it

sleep -> freeze contribution -> merge into parent histories -> reclaim
```

For a selected parent and a common observation time:

```text
parent distribution = retired history + active contribution snapshots
```

Here, querying builds a result from the active snapshots. It must not repeatedly
add complete active snapshots into persistent history. Persistent history receives
a contribution once when that contribution retires. This distinction prevents
each scrape from counting all earlier observations again.

This is the user's latest proposal. It can avoid duplicate parent updates on
each observation. Its cost grows with active worker/table combinations, rather
than only with recording workers. A hot table touched by all workers can need
one contribution from every worker.

These alternatives must not be combined accidentally. If design A already sends
events to the parent, design B's later merge of the same child events would count
them twice. Different metric families can use different designs, but each
family needs an explicit rule for which component records each observation.

## Memory model

Let W be the number of recording workers, K the number of relevant keyspaces,
F the number of histogram families, and B the bytes per contribution. Let P be
the number of populated, active worker/table/family combinations. B must include
arrays and recorder metadata; it can change as a compact histogram fills.

| Design | Approximate live recorder storage, excluding exports |
|---|---|
| One shared histogram per active table/family | Active tables * F * B |
| Worker-owned node aggregates | Populated worker/family pairs * B, at most W * F * B |
| Worker-owned keyspace aggregates | Populated worker/keyspace/family triples * B, at most W * K * F * B |
| Lazy worker-owned table contributions | P * B |

Add parent historical summaries, worker directories, metric identities, cached
handles, publication buffers, and collection results to every applicable row.
Enabled table history and required control distributions are additional costs
when the aggregate representation does not retain them.

With a fixed bucket layout, parent historical summaries combine retired tables
into the same bucket arrays. Their object count need not grow with tables ever
touched. Counter widening and overflow still need defined behavior. Retaining a
map of frozen histograms keyed by every retired table would defeat this bound.

Exited workers must also transfer their contributions and release their state.
An ever-growing list of all historical worker objects has the same problem.
The bound should refer to stable physical workers, not per-request tasks or an
unbounded population of short-lived threads.

An idle timeout only bounds how long unused contributions remain eligible for
retirement. It does not bound their number. If every worker touches every table
within the timeout, P can approach W * active tables * F. A broad burst can create
that state before any retirement starts. Memory budgeting, admission, or another
representation may be necessary; dropping observations is not an implicit
fallback.

## What single writer means in Cassandra

The [table write path](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L1516)
updates table metrics on the calling request thread. A table has no universal
single-worker owner. Reads, writes, coordinator completion, and background work
can also execute in different contexts.

Possible ownership arrangements are:

- A contribution owned by one stable worker. This preserves request routing but
  can create several contributions for a table.
- A table metric owned by one designated executor. Other workers must hand off
  observations. Queueing, backpressure, and scheduling then become recording
  costs.
- A single shared table histogram with atomic or locked updates. This keeps
  fewer copies, but does not provide single-writer semantics.

Hashing concurrent writers onto a small number of ordinary arrays does not make
those arrays single-writer. Colliding writers still require atomic updates or
mutual exclusion. Ownership transfer between workers also needs an explicit
handoff before the new owner mutates the old owner's state.

For a first candidate, use existing worker execution contexts. Avoid changing
database request routing solely to make metric updates single-writer. Audit
every update call site for the selected family before claiming that ownership is
complete.

## Visibility and snapshot consistency

With a true single writer, a bucket increment need not use compare-and-set (CAS).
Cross-thread access still needs a defined protocol. A volatile array reference
does not make its elements volatile. Java's
[VarHandle access modes](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/invoke/VarHandle.html)
provide atomic element access and acquire/release or volatile ordering.

Volatile visibility does not turn several fields into one atomic histogram
snapshot. A reader can obtain a bucket before an update and a total count after
it. The [Java memory model](https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.3)
explicitly distinguishes ordered individual accesses from groups of operations
that must appear atomic. A publication store protects preceding writes; it does
not freeze subsequent mutations to the same buffer.

For fixed-layout cumulative buckets, a read spanning a short interval may be
acceptable. Compute a returned snapshot's count from its observed buckets if
that is the chosen contract. Do not assume this gives a point-in-time view or a
bounded error under arbitrary writer/collector scheduling. Also do not extend
that argument to concurrent resizing, clearing, counter-width changes, or decay
rescaling.

The current compact recorder has both cumulative and decaying state and can
change representation. Its
[rescale and rebase paths](../src/java/org/apache/cassandra/metrics/CompactDecayingEstimatedHistogramReservoir.java#L202)
illustrate why an independently visible bucket array and landmark are not
enough. The collector must not mix two structural or decay generations.

Cache coherence still has a cost. A reader scanning active arrays can cause
traffic on cache lines that writers modify. Volatile publication also costs
instructions and ordering. The target is to eliminate ordinary writer-to-writer
competition on shared buckets, not to claim that all memory traffic disappears.

## Snapshot publication choices

There are several useful controls to compare. None is selected yet.

| Approach | Benefit | Cost or limit |
|---|---|---|
| Read fixed-layout buckets with specified element access modes | Small recording path; no snapshot copy by the writer | Weakly consistent view; structural changes and decay need separate handling |
| Owner publishes a stable snapshot | Collector reads a coherent contribution | Copying or extra buffers; contribution can lag its owner |
| Collector takes an existing owner lock | Simple exclusion; close to Accord's current approach | Collection can delay requests that use the same lock |
| Reader validates a sequence and retries | Can detect overlapping mutation with a correct ordering protocol | Retry starvation under hot writers; wraparound, reclamation, and Java ordering need proof |

For a first worker-owned candidate, owner-assisted publication gives the clearest
ownership argument for mutable histogram representations:

1. A collector requests publication for a contribution generation.
2. At a safe execution boundary, its owner copies a stable cumulative snapshot
   into an available buffer and publishes that buffer and generation.
3. The collector acquires the published snapshot and uses it without mutation.
4. The collector acknowledges completion before the owner reuses the buffer.

The live cumulative recorder remains owned by the worker. Published cumulative
snapshots replace older published snapshots; they are not added to persistent
history on every refresh. A delta-buffer design is a different protocol: it must
consume each delta exactly once and retain all unconsumed deltas. It also needs
event-time handling if observations are applied later. Do not mix cumulative
replacement with delta accumulation.

Two buffers alone do not guarantee safety. A slow reader can still hold a buffer
when the writer wants to reuse it. A bounded implementation can defer another
publication and retain the previous snapshot while the owner continues recording.
That preserves observations but increases reporting delay. It cannot promise a
fresh snapshot under an indefinitely delayed owner or collector.

An idle worker must publish its final updates without waiting for another user
request. Use an explicit owner task or another proven executor mechanism.
Repeated collection requests should coalesce instead of creating an unbounded
queue. A collector running on an owning executor must not wait for work queued
behind itself. Thread exit requires a final handoff; ownership cannot simply
disappear with the thread.

The API does not currently guarantee a single external reader. JMX callers,
virtual-table queries, and internal percentile consumers can overlap. A single
logical collector can serialize refreshes and return immutable cached results
to those callers. This makes single-reader access to mutable contributions a
design property rather than an assumption about operator behavior.

Per-contribution consistency does not imply a simultaneous snapshot of the entire
node. Workers can publish at different moments. Publication age and tolerated
staleness must be explicit, especially for internal control inputs.

## Retirement protocol and accounting invariant

For design B, each contribution needs an identity that distinguishes table ID,
metric family, worker ownership, and recording generation. Names alone are not
sufficient because a dropped table name can be reused. These identities must not
be implemented as unbounded retained maps or arrays sized by historical IDs.

The conceptual states are:

```text
ACTIVE -> CLOSING -> FROZEN -> MERGED -> RECLAIMABLE
```

This is a protocol outline, not proof of a concrete implementation. A candidate
must establish each of the following steps:

1. Prevent new updates from joining the closing generation. Every cached handle
   and every owner that can write it must participate in that rule.
2. Allow admitted operations to finish. An idle-time observation or an atomic
   replacement of the current pointer does not prove that old writers stopped.
3. Obtain a final, stable contribution from each owner. A stalled owner delays
   reclamation; it does not authorize treating its unpublished observations as
   zero.
4. Under coordination shared with collection, merge the final contribution into
   every applicable parent's retired history and remove it from that parent's
   active set as one observable transition.
5. Let existing readers finish before reusing or reclaiming the contribution's
   buffers. Ordinary garbage collection handles reachability, but not unsafe
   reuse of a still-referenced buffer.
6. Start a fresh generation for later observations. Its parent contribution
   begins at zero, even if separate per-table history must remain queryable.

Existing Cassandra operation-ordering mechanisms are worth investigating for
step 2. Their coverage must include metric updates, coordinator completions,
read paths, and cached recorder handles. Memtable flush completion alone does
not establish quiescence of all table metric writers.

A simple prototype can serialize membership changes and parent-history merges
with collection. Ordinary bucket updates stay worker-owned. The collector must
not hold the history lock while waiting for an owner, since that can delay other
queries or create a dependency cycle. If a merge can fail, prepare its result
before publishing the membership/history transition, or keep enough bounded
state to retry without adding the contribution twice.

For each parent, a completed observation must belong to exactly one of:

- Retired history.
- An active contribution, including admitted updates awaiting publication.

A particular snapshot may lag unpublished updates under its freshness contract.
It must not lose them permanently. At quiescence, accounting must be exact.
During retirement, a collector must not include both the final child and its
merged history, or exclude both. Several parents may intentionally receive the
same observation once each; that is not duplication within one parent.

For example, a table records 100 events, sleeps, wakes, then records seven:

```text
before sleep: retired=0, active=100, parent count=100
after sleep:  retired=100, active=0, parent count=100
after wake:   retired=100, active=7, parent count=107
```

Restoring the original 100 into the new parent's active contribution would
produce 207. If table-local reporting needs the earlier 100, its history must be
represented separately or the protocol must track which prefix the parent has
already received. The fresh-generation approach is easier to reason about.

Retirement overlap, pending owner acknowledgments, and readers holding old
snapshots all consume memory. Budget that temporary state. Deferring reclamation
is safe for accounting but cannot provide an unconditional hard heap bound under
arbitrarily stalled participants.

## Decay, rates, counts, and extrema

A complete timer includes more than a percentile distribution. Define the
transfer rule for every exported component:

- Cumulative observation count and total latency must not reset on sleep.
- Compatible histogram buckets can be summed. Merge distributions first, then
  calculate percentiles; averaging per-table p99 values is incorrect.
- Decaying contributions must share a time basis. Preserve the observation age
  when transferring them. Retirement time must not make old observations young.
- One-, five-, and fifteen-minute rates need their own transfer or reconstruction
  rule. Histogram buckets do not contain all of that rate state.
- Mean-rate behavior depends on the chosen lifetime and start time. Summing
  independently started child mean rates differs from computing one parent rate
  over a common lifetime.
- Minimum, maximum, and other derived statistics must retain their specified
  time scope. A lifetime maximum cannot substitute for a recent maximum.

An inactivity threshold of 30 seconds does not mean that histogram weight or
moving-rate contributions have expired. Cumulative values survive indefinitely
within their defined metric lifetime. Rare, inactive tables can also contain
important tail observations. Excluding them creates activity-dependent bias;
the resulting percentile error can go in either direction.

The current snapshot
[merge implementation](../src/java/org/apache/cassandra/metrics/DecayingEstimatedHistogramReservoir.java#L762)
requires matching bucket definitions and aligns decay landmarks. That is useful
precedent. It is not a guarantee of identical results under a different schedule
of merging, decay, and rounding. Start with the existing bucket geometry and
separate ownership tests from histogram-representation experiments.

## Existing release code is a partial precedent

[LatencyMetrics.removeChildren](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L127)
adds a child's count, histogram history, and total latency to its parent before
removing the child reference. The compact implementation's
[release snapshot](../src/java/org/apache/cassandra/metrics/LatencyMetrics.java#L242)
merges into the parent's own history, excluding the other live children.

This supports the feasibility of transfer, but it is not a complete sleep path:

- It does not transfer the child's moving-rate state. Parent rate accessors
  sum rates from remaining children and the parent's own recorder.
- Rebase requires excluding concurrent updates; locking only the parent does
  not freeze a live child.
- Current [TableMetrics.release](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L952)
  removes registered entries and aggregate memberships. It does not explicitly
  call release on each LatencyMetrics field. An implemented sleep protocol must
  wire its ownership cleanup directly rather than assume this method does it.
- Registration-based discovery cannot find every unregistered recorder that
  remains attached to a parent.

Do not use table-drop behavior as an implicit definition of sleep. A sleeping
table remains a database object and can receive subsequent requests.

## Per-table history and internal consumers

Preserving aggregate history does not preserve a table's individual history.
After combining many tables into one parent distribution, the parent cannot
recover one table's observations.

An enabled table metric therefore needs an explicit choice: retain a compact
table summary, persist and restore it, or change the reporting contract to
expose a defined reset/window. The last choice is not authorized by the current
discussion. Even scalar per-table historical summaries scale with tables ever
used; calling them metadata does not remove their cost.

The [dependency inventory](internal_metric_dependencies.md) also identifies
CoordinatorReadLatency and CoordinatorWriteLatency as table-level inputs to
speculation, and TotalDiskSpaceUsed as a repair-ordering input. CompressionRatio
has a conditional helper dependency. Their required state must remain available
or have a tested replacement when a table sleeps. A stale published snapshot can
change control behavior even when an operator dashboard looks representative.

SSTable read meters, the flush-size moving average, and dynamic-snitch sampling
are operational statistics outside ordinary export filtering. This design does
not authorize disabling them. Cold disk usage remains real disk usage; current
state gauges need access to that accounting after metric retirement.

The allowlist describes exposure, while dependencies determine recording and
retention. A disabled table export can still need backing state for an enabled
parent or table gauge. A genuinely unused optional recorder can be a shared
no-op without allocating or registering a contribution. Required state must not
become a no-op through either path.

## Collection cost and practical bounds

Lazy aggregation trades recording work for collection work. For fixed Q buckets
and P active contributions to a parent, combining a fresh parent distribution
requires work proportional to P * Q, plus reading its retired summary. Scanning
only populated buckets may help sparse state, but adds representation complexity.
Worker aggregate design A typically scans far fewer contributions than design B.

Lazy queries can become expensive when many tables stay active. Frequent JMX
scrapes, alias reads, internal percentile queries, and virtual-table queries can
multiply that work. Coalesce refreshes and reuse stable results for an explicit
interval. A fresh node-wide result cannot be promised at negligible cost when
it requires reading every active contribution.

The collector should allocate working buffers proportional to concurrent
collection work, rather than permanently reserving a full snapshot for every
possible table. Owner publication can still require extra storage per active
contribution, so that optimization must be measured rather than assumed.
Scraping an inactive table must not recreate its recording state solely to return
a value. Its reporting path needs the retained or persisted history contract.

The shared parent's synchronization cost moves to collection and retirement.
A burst of tables sleeping together can concentrate that work. Bound retirement
batches and measure backlog, retained frozen bytes, and time to reclaim. A design
can have a cheap steady-state update and still fail under a retirement storm.

The proposed whole system is not formally lock-free: collection, publication
handoff, allocation, and retirement can wait. The narrower objective is a steady
recording path that does not acquire a shared parent lock or perform a shared
parent atomic increment. Ownership checks, publication work, and the first use
of a contribution still have costs that belong in measurements.

## Failure and misrepresentation cases

| Situation | Failure | Required treatment |
|---|---|---|
| Parent reads an optional child replaced with a no-op | Missing counts and biased distributions | Preserve the input or give the parent another complete event source |
| Histogram unregistered but still referenced by parent | No recorder reclamation | Separate export removal from ownership cleanup |
| Every scrape adds the full active snapshot to history | Repeated counting of earlier events | Active snapshots replace a view; only retirement or identified deltas accumulate |
| Parent receives direct events and later merges the child | The same observations enter that parent twice | Select one recording design for that family |
| Several request threads mutate a supposedly single-writer array | Lost updates | Enforce ownership or use a concurrent recorder |
| Reader mixes counts, bucket layout, or decay generations | Invalid or misleading snapshot | Stable publication or a proven validation protocol |
| Collector and retirement inspect different membership/history states | Missing or duplicated retired observations | Coordinate their transition within each parent snapshot |
| Cached handle writes after the final snapshot | Lost late observations | Close admission and establish writer quiescence before freezing |
| Wake restores already-merged history into a new parent contribution | Double counting after wake | New recording generation or explicit transferred-prefix accounting |
| Table name or metric ID is reused | New object inherits old state | Generation-aware identity and lifecycle cleanup |
| Idle worker never publishes | Stale tail data can persist indefinitely | Explicit publication request, freshness accounting, final handoff |
| Writer reuses a buffer held by a reader | Snapshot corruption | Acknowledgment or safe immutable lifetime |
| Table disappears from an aggregate's disk accounting on sleep | Underreported current resource use | Keep or derive operational accounting independently |
| Many workers/tables remain active or retirement stalls | Heap growth despite a timeout | Budget active, frozen, and publication state; expose the unresolved bound |
| Table-local history is discarded while its export stays enabled | Counters reset and history changes silently | Preserve history or obtain agreement on a changed contract |

## Existing implementations to reuse

The first prototype should reuse Java infrastructure already present in the
repository and avoid a new dependency unless an experiment identifies a gap.

- [ThreadLocalMetrics](../src/java/org/apache/cassandra/metrics/ThreadLocalMetrics.java#L64)
  supplies worker-local scalar accounting and a thread-exit history mechanism.
  Its dense metric-ID arrays and reference bookkeeping are residency costs.
  Reusing the ownership idea does not require copying that indexing scheme.
- [ThreadLocalHistogram](../src/java/org/apache/cassandra/metrics/ThreadLocalHistogram.java#L62)
  makes only the observation count thread-local. It still updates a shared
  reservoir and is not an existing single-writer bucket implementation.
- [LogLinearDecayingHistograms](../src/java/org/apache/cassandra/metrics/LogLinearDecayingHistograms.java#L46)
  supplies single-threaded histogram arithmetic and batched work. It has bounded
  histogram-index encoding and a different bucket representation. It is a
  candidate to study, not an automatic replacement for every table metric.
- [ShardedDecayingHistograms](../src/java/org/apache/cassandra/metrics/ShardedDecayingHistograms.java#L35)
  merges shard snapshots using owner-provided locks and caches results. It is a
  useful simpler comparison for a publication protocol, even though it locks.
- The current compact reservoir and its legacy counterpart remain controls.
  The compact path uses synchronization while sparse and atomic bucket updates
  with adaptive striping after contention; it is not contention-free.

The earlier [OpenTelemetry study](metric_threading.md) identified small storage
structures under Apache-2.0 in the reference checkout. Their non-thread-safe
containers would still require this ownership/publication design. Importing the
SDK would not resolve Cassandra's lifetime, decay, internal percentile, and alias
contracts. No native library or Java version upgrade is needed to explore the
ownership design; VarHandle is already available on the branch's Java baseline.

## Proposed experiment sequence

This is a proposed comparison, not authorization to implement all candidates.
Keep the current legacy path selectable. Use the established pre-change,
iteration, and final measurement record for each accepted optimization. A
documentation-only update does not require a new performance run or commit.

1. Finish registration selection and its lifecycle checks first. Repeat the
   existing census with identical workload and scrape behavior. That isolates
   registration savings from recorder changes.
2. Choose one observational histogram family with table, keyspace, and global
   destinations. SSTablesPerReadHistogram is a useful candidate to assess. Keep
   its existing bucket geometry so ownership is the main experimental variable.
3. Compare the existing shared recorder, worker-owned aggregate design A, and
   lazy worker/table design B. Include a simple owner-lock snapshot control.
   Aggregate-specific stripe counts are another bounded shared-recorder control.
4. Establish recording and publication behavior before adding table sleep.
   Then add the retirement transfer, wake, and worker-exit protocols.
5. Extend to LatencyMetrics only after histogram accounting works. Test timer
   count, total latency, each rate, decay, and internal consumers separately.

Use dependency injection for the clock, owner executor, collector, and publication
control. Deterministic tests must control the handoff boundaries without relying
on timing sleeps or production monkey-patching.

Correctness checks should include:

- Known event streams with distinct table distributions and rare outliers.
  Check exact cumulative buckets/counts after quiescence and quantify live
  snapshot delay and percentile error separately.
- A collector paused before and after the retirement membership transition.
  A frozen contribution must appear exactly once in each affected parent.
- A writer paused with an old handle, concurrent wake, repeated retirement, and
  table drop/recreation with the same name but a different identity.
- Idle publication, thread exit, delayed collection, occupied publication
  buffers, duplicate refresh requests, and collection on an owning executor.
- Decay boundaries, clock-controlled time gaps, widening/overflow, empty state,
  and rates after retirement. Aging should follow event time, not scrape time.
- Enabled parent with disabled table export; disabled parent with enabled table;
  both exports disabled but a required internal consumer still active; and the
  all-metrics profile. No disabled recorder may suppress a database operation.
- Independent alias recent-value cursors, simultaneous JMX/virtual-table reads,
  aggregate cleanup, and exact histories after activity stops.

Performance and residency checks should include:

- One writer, many writers on one table, and many writers on distinct tables.
  Use similar event values to stress one bucket and spread values to distinguish
  bucket contention from broader metadata costs.
- Separate keyspace and node-wide contention. Distinct-table writers can still
  converge on the same aggregate recorder.
- Sustained updates with concurrent scrapes and internal percentile reads.
  Report throughput and latency of both recording and collection.
- Never-used tables, sparse observations, hot tables, all-worker/all-table
  touching, shrinking active sets, and repeated sleep/wake cycles.
- Worker churn and synchronized idle expiry. Measure bytes waiting for
  retirement, reclamation delay, and peak memory as well as settled memory.
- Allocation after warmup, first-use allocation, publication copying, buffer
  retention, directory/ID overhead, parent summaries, and compatibility exports.

Keep executed table counts at 100, 500, and 1,000 or smaller. Higher logical counts
can appear in memory-model examples only. Extend the census with real concurrent
recording and lifecycle workloads; its existing sequential worker activation
cannot validate contention claims.

Success needs measured total resident-memory reduction and acceptable recording,
collection, and control-query latency. Moving allocations into a collector or
retaining one historical object per retired table is not evidence of success.
No throughput, freshness, or numerical acceptance threshold has yet been agreed.

## Decisions still open

- Which optional table metrics must preserve their full history across sleep?
- How stale may operator snapshots be, and what tighter rules apply to internal
  percentile consumers?
- Does worker/table contribution memory remain affordable under broad bursts?
- Can existing execution boundaries provide ownership and quiescence without a
  shared reference-count update on every recorded event?
- Are direct worker aggregates, lazy child aggregation, or a mix by metric
  family cheaper under the measured workload?
- How should node-wide Table metrics be selected? The current YAML profiles
  cover table and keyspace families, not the global Table scope.
- What bounds and behavior apply when publication or retirement falls behind?

Registration filtering can proceed while these questions remain open. Choosing
a recorder and proving its ownership/lifecycle behavior is separate work.
