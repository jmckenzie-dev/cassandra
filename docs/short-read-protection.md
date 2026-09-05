<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Cassandra Short Read Protection

> A code-level guide to the coordinator-side continuation mechanism that keeps
> finite, multi-replica reads complete after reconciliation.

This document describes the implementation in the local Cassandra worktree at
`95ba53bcc81666d32b3353b03bf351ec47fda10b` (CASSANDRA-18424). Source citations
are repository-relative and refer to that revision. Historical claims identify
the relevant Git commit.

## Executive summary

Short Read Protection (SRP) is a **coordinator-side continuation mechanism
inside multi-replica read reconciliation**. When replicas individually satisfy
a finite `LIMIT`, reconciliation may remove rows because another replica
supplied newer tombstones or liveness information. SRP lazily asks the affected
replica for more unfiltered data so the final reconciled result can still
satisfy the limit.

SRP is not a consistency level, a separate retry policy, or a form of read
repair. It is an extension of each source replica's iterator inside
`DataResolver`, before reconciliation is complete. That placement lets newly
fetched rows and tombstones pass through the same merge and read-repair
listener as the original response.

The core implementation is mature and has accumulated substantial correctness
hardening since its introduction in 2011. Two current integration risks stand
out in this worktree:

1. CASSANDRA-18424's graceful tombstone paging can deliberately truncate a
   replica response without carrying that fact through `ReadResponse`; SRP can
   therefore mistake truncation for natural exhaustion.
2. Paxos v2's "short read occurred" callback does not cover row-level SRP and
   is checked before the lazy iterator can perform any SRP request.

Both are static-analysis findings. The wiring is clear, but the proposed
runtime failure scenarios still require focused distributed regression tests.

## View the interactive presentation

From the repository root, run:

```bash
bash docs/serve-short-read-protection.sh
```

The launcher selects an available port above `50000`, binds the presentation to
`0.0.0.0`, and prints a clickable URL for every discovered IPv4 interface.
Press `Ctrl-C` to stop it. Both launcher and Python server output are mirrored
to timestamped files under the project-local `./logs/` directory.

To bind, fetch the page once through loopback, and exit instead of serving
forever, use:

```bash
bash docs/serve-short-read-protection.sh --probe
```

## The problem SRP solves

Suppose a query asks for `LIMIT 2`:

```text
Replica A response: row 1, row 2    [stopped at LIMIT; rows 3 and 4 exist]
Replica B response: tombstones deleting rows 1 and 2

After reconciliation: zero live rows
```

Returning zero rows would be incorrect because rows 3 and 4 exist and should
satisfy the limit. The coordinator cannot simply ask an arbitrary replica for
more data: the continuation boundary came from replica A's stream, and the
additional stream may itself contain tombstones required to suppress stale
rows from another replica.

SRP therefore resumes replica A exclusively after row 2 and feeds the
continuation into the existing merge. It repeats this lazily until one of the
following is true:

- the reconciled result reaches the requested limit;
- the source returns fewer rows than requested, proving natural exhaustion
  under the traditional response contract;
- the continuation cannot progress, such as a skinny table's empty clustering;
- or the original request deadline expires.

The class-level description states this purpose directly:
`src/java/org/apache/cassandra/service/reads/ShortReadProtection.java:30-38`.

## Where it fits in the read architecture

```text
CQL SELECT / query pager / SERIAL read
                 |
                 v
      StorageProxy / range executor / Paxos
                 |
        point read: data + digests
                 |
          digest mismatch?
          no  |         | yes
              |         v
              |   full replica responses
              |         |
range reads --+---------+
                        v
                  DataResolver
                        |
        +---------------+---------------+
        |               |               |
   replica A       replica B       replica C
   unfiltered      unfiltered      unfiltered
   response        response        response
        |               |               |
   source counter + row/partition continuation
        \               |              /
         \-------- reconciliation -----/
                        |
              outer read-repair listener
                        |
           remove tombstones/dead rows
                        |
       RFP completion and re-filtering, if needed
                        |
             merged-result limit counter
                        |
                 client page/result
                        ^
                        |
             same-replica CL.ONE retry
       after last clustering or partition key
```

SRP operates between receipt of raw replica responses and completion of the
final reconciled result.

### Point reads

Normal point reads initially send one full-data request and digest requests.
Matching digests return the data directly. A mismatch starts full-data read
repair and `DataResolver`:

- `src/java/org/apache/cassandra/service/reads/AbstractReadExecutor.java:443-459`
- `src/java/org/apache/cassandra/service/reads/repair/AbstractReadRepair.java:128-176`

Matching point-read digests normally bypass SRP because no cross-replica
reconciliation removed rows. Transient-replica data is an exception and still
requires reconciliation:
`src/java/org/apache/cassandra/service/reads/DigestResolver.java:77-102`.

### Range reads

Range reads construct `DataResolver` for every queried token subrange and
resolve full data responses directly:
`src/java/org/apache/cassandra/service/reads/range/RangeCommandIterator.java:205-235`.

Range SRP can continue both within the current partition and after the last
partition returned by a source.

## End-to-end mechanics

### 1. Resolution snapshots its participants

`DataResolver` snapshots the responses present when resolution starts. Later
responses are not added to that merge. It enables SRP when:

- more than one data response is being reconciled;
- the command has a finite limit; and
- the command is not top-K.

See:

- `src/java/org/apache/cassandra/service/reads/DataResolver.java:98-130`
- `src/java/org/apache/cassandra/service/reads/DataResolver.java:183-216`

Classic SRP has no independent configuration switch. These structural
conditions determine whether it is installed.

### 2. It maintains two distinct kinds of counters

Each replica stream gets a non-stopping, pre-reconciliation counter:

- `assumeLiveData=false`, because it counts unfiltered replica data;
- `onlyCount()`, so it observes progress without stopping the source iterator.

The merged result has a separate enforcing counter:

- `assumeLiveData=true`, because tombstones have already been reconciled and
  filtered;
- it enforces the client's total and per-partition limits.

The relevant construction is in:

- `src/java/org/apache/cassandra/service/reads/ShortReadProtection.java:42-61`
- `src/java/org/apache/cassandra/db/filter/DataLimits.java:244-264`

The difference between these counters answers the core question: did a replica
apparently fill its own request while the reconciled result remains underfilled?

### 3. Transformation order is a correctness invariant

`ShortReadProtection.extend()` composes:

1. optional partition continuation;
2. per-partition row continuation;
3. source counting.

The continuation must be installed before transformations. Otherwise, the
first row or partition of a newly appended iterator would bypass its tracker or
counter. See
`src/java/org/apache/cassandra/service/reads/ShortReadProtection.java:63-82`.

`MoreRows` and `MorePartitions` provide the lazy extension points. They invoke
`moreContents()` only after the current iterator is exhausted:

- `src/java/org/apache/cassandra/db/transform/MoreRows.java:31-49`
- `src/java/org/apache/cassandra/db/transform/MorePartitions.java:30-47`

### 4. Row continuation

`ShortReadRowsProtection` records the last observed clustering and requests
more rows only after the current source partition ends.

It stops without retrying when:

- the replica returned fewer live rows than the original limit, where that
  inference is valid;
- the partition supplied zero live regular rows;
- the last clustering is `Clustering.EMPTY`, indicating a skinny table with at
  most one row;
- or a previous retry returned fewer rows than requested.

See
`src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:76-129`.

When continuation is necessary, it:

- requests `min(totalLimit, perPartitionLimit)` instead of only the exact
  deficit;
- resumes exclusively after `lastClustering`;
- preserves columns, row filters, ordering, index query plan, `nowInSec`, and
  limit semantics;
- and uses a new `SinglePartitionReadCommand`, even if the original read was a
  range query.

See
`src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:131-188`.

The overfetch is intentional. A narrowly sized continuation could again be
eliminated by reconciliation and require another network round trip.

### 5. Partition continuation

For a genuine range read, `ShortReadPartitionsProtection` tracks the last
partition key. If the source's partition iterator ends while the merged limit
remains unsatisfied, it constructs a new range beginning exclusively after
that key and retaining the original right bound. Every new partition receives
its own row protector:
`src/java/org/apache/cassandra/service/reads/ShortReadPartitionsProtection.java:87-179`.

Single-partition commands and indexed range commands restricted to one
partition do not install partition continuation:
`src/java/org/apache/cassandra/service/reads/ShortReadProtection.java:75-80`.

### 6. Retry execution

Row and partition continuations both:

- target the same source replica;
- use a synthetic consistency-level `ONE` replica plan;
- permit no speculation or nested read repair;
- retain the original request deadline;
- route through `ReadCoordinator`, allowing classic messaging or Accord
  interop;
- and preserve transient-replica status where applicable.

See:

- `src/java/org/apache/cassandra/locator/ReplicaPlans.java:835-873`
- `src/java/org/apache/cassandra/service/reads/ShortReadPartitionsProtection.java:182-204`

The child resolver takes the raw `ReadResponse` iterator rather than a filtered
result. The outer merge still needs all tombstones in the continuation.

There is no explicit retry-count cap. Progress comes from exclusive cursors,
and the retained request deadline is the ultimate bound:
`src/java/org/apache/cassandra/service/reads/ReadCallback.java:85-106`.

### 7. Final reconciliation order

The normal order is:

1. merge unfiltered source streams;
2. reconcile and filter tombstones and dead rows;
3. perform any Replica Filtering Protection post-filter;
4. enforce the merged limit;
5. discard empty partitions.

See `src/java/org/apache/cassandra/service/reads/DataResolver.java:294-326`.

Moving the merged counter before filtering would let rows that are later
deleted or rejected consume the client's limit. Moving empty-partition
discarding before counting can eagerly consume the iterator before SRP sees the
counter state it needs.

## Principal classes

| Class | Responsibility |
|---|---|
| `DataResolver` | Selects SRP, owns the merged-result counter, merges source streams, coordinates read repair and RFP. |
| `ShortReadProtection` | Composes partition continuation, row continuation, and source counting in the required order. |
| `ShortReadRowsProtection` | Detects underfilled reconciled partitions and resumes after the last clustering. |
| `ShortReadPartitionsProtection` | Extends range reads after the last partition and dispatches all SRP child reads. |
| `DataLimits` | Defines total, per-partition, GROUP BY, and paging accounting and builds retry limits. |
| `MoreRows` / `MorePartitions` | Supply the lazy iterator-extension mechanism. |
| `ReplicaPlans` / `ReadCallback` | Provide the same-replica retry plan and deadline/failure behavior. |
| `NoopReadRepair` | Prevents a child SRP request from starting a nested read repair. |
| `ReplicaFilteringProtection` | Uses an SRP-protected first phase before completion and re-filtering. |
| `RTBoundCloser` / `RowIteratorMergeListener` | Preserve range-tombstone correctness across limits and continuations. |

`RTBoundCloser` is especially important. Stopping at a limit inside an open
range tombstone requires an artificial close marker; otherwise concatenated
continuations can expose shadowed data or lose deletion coverage:
`src/java/org/apache/cassandra/db/transform/RTBoundCloser.java:30-40`.

Read repair also explicitly recognizes the repeated open range-tombstone bound
that an SRP continuation can produce:
`src/java/org/apache/cassandra/service/reads/repair/RowIteratorMergeListener.java:288-311`.

## Features that depend on or specialize SRP

### Replica Filtering Protection

RFP handles stale rows that pass replica-side filtering on one replica while a
newer replica suppresses its version before reconciliation. Its first phase is
lazy and SRP-protected. Potentially incomplete rows are then fetched by primary
key from silent replicas, re-filtered, and only then counted:

- `src/java/org/apache/cassandra/service/reads/ReplicaFilteringProtection.java:81-94`
- `src/java/org/apache/cassandra/service/reads/DataResolver.java:234-276`

This is one of SRP's most tightly coupled consumers. A 2025 fix changed RFP
consumption specifically to avoid unnecessary SRP requests after a satisfied
single-partition limit:
`src/java/org/apache/cassandra/service/reads/ReplicaFilteringProtection.java:384-437`.

### Query paging

Each page creates finite limits based on `min(fetchSize, rowsRemaining)`, so SRP
protects the current page, not the entire logical query. The external pager then
records the last reconciled key and clustering:
`src/java/org/apache/cassandra/service/pager/AbstractQueryPager.java:92-105`.

Paging, SRP, and per-partition accounting therefore share the same exhaustion
contract.

### `PER PARTITION LIMIT`

The normal "source returned fewer rows than requested, therefore it is
exhausted" shortcut is disabled when a per-partition limit exists.
Deserialization may drain an unconsumed partition without executing upstream
transformations, so the source counter can undercount:

- `src/java/org/apache/cassandra/db/partitions/UnfilteredPartitionIterators.java:376-390`
- `src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:89-96`

### GROUP BY and aggregation

GROUP BY limits count both raw rows and completed groups. SRP must reason about
rows fetched rather than groups emitted, and retry limits are converted to
ordinary row limits:
`src/java/org/apache/cassandra/db/filter/DataLimits.java:648-717`.

### Top-K and vector search

Top-K explicitly disables classic SRP and read repair. Appending rows after a
cursor does not preserve global top-K candidate semantics; those plans must
increase the candidate search and post-process instead:
`src/java/org/apache/cassandra/service/reads/DataResolver.java:165-193`.

### Accord interop

Accord interop deliberately reuses Cassandra's read executor and
`ReadCoordinator`, so reconciliation, SRP, and blocking repair execute in the
appropriate Accord command-store context:
`doc/modules/cassandra/pages/architecture/cql-on-accord.adoc:444-454`.

### Transient replication

Transient responses carry data and can force reconciliation even when
full-replica digests match. SRP retains the source's transient status when
constructing its child plan:

- `src/java/org/apache/cassandra/service/reads/DigestResolver.java:70-102`
- `src/java/org/apache/cassandra/service/reads/ShortReadPartitionsProtection.java:189-198`

## History and evolution

### 2011-2013: eager retries in `StorageProxy`

SRP began with CASSANDRA-2643 on 2011-08-02 in commit
`388c690c228b3e93ca870d5f6bdfe3554bd5c6e0`, titled `fix "short reads" in
[multi]get`. The implementation was an eager coordinator loop that reconciled,
detected a deficit, calculated a larger count, retried, and trimmed the result.

The early design quickly exposed the breadth of the problem:

- CASSANDRA-3157 fixed use of the wrong command after a multiget short read.
- CASSANDRA-3303/3395 stopped retries from returning too many columns.
- CASSANDRA-3934 improved retry sizing.
- CASSANDRA-4000 fixed division by zero when no live columns survived.
- CASSANDRA-4882 corrected CQL3 counting.
- CASSANDRA-5113 restored consistency-level guarantees after digest mismatch.

### 2015: the modern lazy architecture

CASSANDRA-8099, commit
`a991b64811f4d6adb6c7b31c0df52288eb06cf19`, moved SRP out of the eager
`StorageProxy` loop and into per-replica lazy iterators in `DataResolver`. It
also introduced `DataLimits.forShortReadRetry`.

CASSANDRA-9975 then moved iterator extension onto the transformation framework
that now exposes `MoreRows`.

### 2016-2017: timeout, ordering, and accounting hardening

- CASSANDRA-12256 charged the initial request and every continuation against
  one coordinator timeout budget.
- CASSANDRA-13747 fixed merge/filter/count order and added the table metric.
- CASSANDRA-12872 fixed extension-versus-transformation order.
- CASSANDRA-13880 handled tables with no clustering columns.
- CASSANDRA-13794 improved batching and retry exhaustion detection.
- CASSANDRA-13911 fixed per-partition-limit accounting.

### 2017: crossing partition boundaries

CASSANDRA-13595, commit
`68a67469f8b25534d086b29b8fe0fa4ec3f9d1ec`, added range continuation after
the last partition. Prior SRP protected only rows inside a partition.

### 2018: current class structure and range tombstones

CASSANDRA-14058, commit
`39807ba48ed2e02223014fbf47dce21d4124b380`, extracted the three current SRP
classes during the read-executor and read-repair refactor.

CASSANDRA-14330 and CASSANDRA-14515 fixed repeated and open range-tombstone
bounds. CASSANDRA-14404 integrated transient replication.

### 2020-2021: filtering and grouping

CASSANDRA-8272 introduced Replica Filtering Protection and its SRP-protected
first phase. CASSANDRA-15907 made that path lazy and hardened response
lifecycle handling. CASSANDRA-15459 and CASSANDRA-16307 fixed GROUP BY row
accounting and pager interaction.

### 2022-2025: transactions, indexes, and deadlines

- CEP-14/CASSANDRA-17164 added the Paxos short-read callback.
- SAI index plans were propagated into continuation commands.
- CASSANDRA-18715 explicitly excluded top-K from classic SRP.
- CASSANDRA-19018 reworked RFP for partially updated SAI rows.
- CASSANDRA-18737 introduced native transport deadlines shared by retries.
- CASSANDRA-19303 and CASSANDRA-20382 integrated Accord routing.
- CASSANDRA-20639 reduced unnecessary RFP-triggered SRP reads.

### Current worktree: graceful tombstone paging

HEAD `95ba53bcc81666d32b3353b03bf351ec47fda10b` adds graceful paging across
tombstones for CASSANDRA-18424. It does not edit the SRP classes, but it changes
a foundational assumption: that an under-limit replica response means natural
exhaustion.

## Current observability and coverage

Every continuation increments the per-table `ShortReadProtectionRequests`
meter:

- `src/java/org/apache/cassandra/metrics/TableMetrics.java:380-384`
- `src/java/org/apache/cassandra/metrics/TableMetrics.java:883-885`

The main distributed test uses three nodes and covers `ALL`, `QUORUM`, and
`SERIAL`; paged and unpaged reads; memtables and SSTables; and Accord interop
modes:
`test/distributed/org/apache/cassandra/distributed/test/ShortReadProtectionTest.java:56-100`.

Its cases include forward and reverse scans, static and skinny rows,
complementary deletions, GROUP BY, multiple row continuations, range partition
continuation, and per-partition limits:
`test/distributed/org/apache/cassandra/distributed/test/ShortReadProtectionTest.java:136-419`.

Important gaps remain:

- The suite creates tables with `read_repair='NONE'`, so this matrix does not
  itself exercise blocking read repair:
  `test/distributed/org/apache/cassandra/distributed/test/ShortReadProtectionTest.java:451-460`.
- SERIAL cases use Paxos v1 unless configuration explicitly selects v2.
- The CASSANDRA-18424 mixed-configuration setup is commented out, and six
  distributed tombstone-trigger scenarios are empty:
  `test/distributed/org/apache/cassandra/distributed/test/storage/paging/MixedConfigPagingAcrossTombstonesTest.java:113-127,224-270`.
- The intended mixed-mode mismatch scenario is also a stub:
  `test/distributed/org/apache/cassandra/distributed/upgrade/MixedModePagingAcrossTombstonesTest.java:70-80`.

## Current integration finding 1: graceful tombstone paging

The new feature defaults to disabled through
`tombstone_paging_threshold = -1`:
`src/java/org/apache/cassandra/config/Config.java:634-640`.

When enabled, a replica stops the current partition after exceeding
`tombstone_failure_threshold`, stores the stopping row only in mutable
`ReadCommand.tombstoneLimitedRow`, and returns the shortened iterator:
`src/java/org/apache/cassandra/db/ReadCommand.java:650-769`.

`ReadResponse` serializes only its normal digest or data bytes and repaired-data
metadata. It carries no tombstone-limited flag or continuation position:
`src/java/org/apache/cassandra/db/ReadResponse.java:359-401`.

For a remote coordinator, the static control flow is therefore:

```text
replica deliberately truncates
        |
        v
ReadResponse loses truncation state
        |
        v
SRP sees counted < requested
        |
        v
treats the replica as naturally exhausted
        |
        v
the pager may emit no continuation state
```

That is the early exit in
`src/java/org/apache/cassandra/service/reads/ShortReadRowsProtection.java:89-115`.
An all-tombstone truncated page is independently rejected because it counted
zero live regular rows.

Additional concerns are visible:

- Only `SinglePartitionPager` caches page command objects for marker
  inspection; `PartitionRangeQueryPager` does not:
  `src/java/org/apache/cassandra/service/pager/SinglePartitionPager.java:88-101`
  and
  `src/java/org/apache/cassandra/service/pager/PartitionRangeQueryPager.java:84-118`.
- The mutable marker stores only a `Row`, not partition key plus clustering, so
  it cannot generally identify a range-query resume position.
- The configured paging threshold is used only as an enabled/disabled sentinel.
  The actual comparison uses `tombstone_failure_threshold`, contrary to the
  numeric description in `conf/cassandra.yaml:2062-2069`.
- Matching truncated digests can bypass `DataResolver` entirely.

The missing wire state and resulting exhaustion ambiguity are high-confidence
static findings. A decisive test should force the coordinator to be outside the
natural replicas, use CL `ALL`, place leading tombstones before live rows, page
with a small fetch size, and assert both complete eventual results and non-null
paging state after an empty or short page.

Keeping `tombstone_paging_threshold` at its default `-1` avoids this path.

## Current integration finding 2: Paxos v2 callback timing

`Paxos.begin()` passes a callback to `DataResolver` because any SRP request can
invalidate an otherwise read-safe prepare:

- `src/java/org/apache/cassandra/service/paxos/Paxos.java:1127-1146`
- `src/java/org/apache/cassandra/service/paxos/PaxosPrepare.java:180-191`

Two independent problems are visible:

1. The callback runs only before **partition** continuation. Single-partition
   reads do not install partition continuation, while
   `ShortReadRowsProtection` has no callback:
   `src/java/org/apache/cassandra/service/reads/ShortReadPartitionsProtection.java:156-163`
   and `src/java/org/apache/cassandra/service/reads/ShortReadProtection.java:75-80`.
2. `DataResolver.resolve()` returns a lazy iterator, but Paxos examines the flag
   immediately. Source advancement and SRP occur later when CQL consumes the
   result:
   `src/java/org/apache/cassandra/utils/MergeIterator.java:170-192` and
   `src/java/org/apache/cassandra/cql3/statements/SelectStatement.java:515-528`.

Simply propagating the callback into row SRP would therefore still be too late
for the current safety decision. RFP also passes `null` into its SRP-protected
first phase, creating a separate callback bypass.

This affects optimized Paxos v2. The local default remains v1:
`src/java/org/apache/cassandra/config/Config.java:1186-1189`.

The callback wiring and timing are confirmed statically. Demonstrating a
linearizability anomaly requires a Paxos-v2 test that pauses immediately before
the row continuation, interleaves a multi-row LWT, and proves the result is one
complete before-or-after state rather than a mixture.

## Risk map for future changes

The following change categories are ranked by likelihood and severity of SRP
interaction.

### 1. Alternative replica termination conditions — critical

Anything that lets a replica stop for a reason other than natural exhaustion
or `DataLimits` directly invalidates SRP's principal inference. Examples include
tombstone short-circuiting, index scan budgets, time or row budgets, partial
response protocols, cancellation, and future memory caps.

**Required invariant:** every non-natural termination must be explicit in the
replica/coordinator response contract and must include a complete resume
position.

### 2. Limit, paging, and grouping accounting — critical

Changes to `DataLimits`, protocol paging, `PER PARTITION LIMIT`, DISTINCT,
aggregation, or GROUP BY can change what source and merged counters mean. Most
historical SRP bugs arose in this category.

### 3. Reconciliation and filtering order — high

Moving tombstone filtering, strict-liveness checks, row filters, index
post-filters, empty-partition removal, or merged counting changes which rows
consume the client's limit.

### 4. Replica Filtering Protection and indexes — high

RFP directly consumes an SRP-protected iterator and may force further
consumption after row completion. Top-K demonstrates that not every distributed
index algorithm is compatible with cursor-based SRP.

### 5. Cursor and range-tombstone semantics — high

Reverse scans, inclusive versus exclusive bounds, partition-boundary paging,
range tombstone markers, static rows, and skinny tables affect safe
continuation. These areas have repeatedly generated historical fixes.

### 6. Paxos, Accord, and transaction boundaries — high

Child reads must execute in the correct transactional context, and lazy
continuation must not invalidate a safety decision already made from the
initial responses.

### 7. Topology, replica plans, and transient replicas — medium to high

SRP assumes it can revisit the same logical source, preserve its replica type,
and retain the original deadline without speculation.

### 8. Wire protocol and mixed versions — high

New incomplete-response modes must be explicitly versioned. Once deliberate
truncation exists, row count alone is not a reliable completeness signal.

### 9. Repaired-data and resource accounting — medium

Repaired-data overreads, response buffers, and per-child read-size counters can
interact with repeated SRP requests even though they are not themselves SRP
mechanisms.

## Review checklist

When changing any adjacent read-path code, ask:

- Can a replica response now stop before natural exhaustion?
- Can the coordinator distinguish every termination reason?
- Is the resume boundary complete for point, range, and reverse reads?
- Are continuations exclusive and guaranteed to progress?
- Does the source counter observe every appended row and partition?
- Does the merged counter run only after final filtering?
- Can a per-partition drain bypass the counter?
- Are range tombstone opens and closes balanced across continuation boundaries?
- Does the child command retain filters, index plans, `nowInSec`, replica type,
  transactional context, and deadline?
- Does read repair see newly fetched content without recursively repairing the
  child request?
- Is a transactional safety decision made only after lazy resolution is fully
  known?
- Are local, remote, mixed-version, and non-replica-coordinator cases tested?
- Does the test verify eventual rows, paging state, retry count, and failure
  behavior rather than only a happy-path first page?

## Conclusion

SRP is best understood as a **lazy source-completion layer embedded inside
reconciliation**. Its correctness rests on three contracts:

1. a short replica response means natural exhaustion unless explicitly marked
   otherwise;
2. continuations preserve the source identity and resume exclusively from the
   last observed key or clustering;
3. all original and continued content passes through one correctly ordered
   reconciliation, filtering, repair, and counting pipeline.

Changes that challenge any of those contracts deserve dedicated SRP analysis
and distributed tests. The current implementation is battle-tested, but its
integration surface continues to grow—and the two current findings demonstrate
that adjacent features can violate its assumptions without changing an SRP
class directly.
