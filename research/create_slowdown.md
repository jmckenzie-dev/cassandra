<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Why creating 5,000 tables takes 10 minutes 38 seconds

## Executive summary

The 638.181-second creation phase is production schema-operation time. Harness sleeps and an explicit `CREATE TABLE` throttle do not explain it.

- **Measured fact:** the 5,000 per-create timing windows sum to 637.317222465 seconds. Only 0.863777535 seconds, or about 0.14%, of the phase is outside those windows.
- **Measured fact:** creation starts near 90 ms per table after first-table startup. Final calls take about 165–198 ms.
- **Source-confirmed mechanism:** `ThreadLocalMeter` expands one shared rate array to the exact required size for each unrecycled meter. Each expansion copies the complete old array.
- **Measured fact:** table creation added exactly 220,000 live meters, or 44 per table. The meter expansion method rose from 9.9% of sampled allocation at N=100 to 60.81% at N=5,000.
- **Conclusion:** meter allocation is O(n) for each new live meter and O(n²) across the run. Schema copies and periodic metadata snapshots add other growing costs.

The timing has two practical parts. A roughly 90 ms/table production base costs about 450 seconds, or 7 minutes 30 seconds, at 5,000 tables. The observed run adds about 188 seconds, or 3 minutes 8 seconds, as work grows with the live meter and schema populations.

Geometric or chunked meter-array growth is the first fix to test. It should remove the dominant accidental allocation and reduce the late-call slope. It cannot make 5,000 serial production schema changes take two or three minutes while the fixed base remains near 90 ms/table.

## Evidence terms

- **Measured fact** comes from a run artifact or a derived view of that artifact.
- **Source-confirmed mechanism** appears directly in the current source.
- **Inference** joins measured evidence to source behavior but lacks an isolating experiment.
- **Unknown** needs another controlled run or profile.

Allocation percentage does not mean CPU percentage. The allocation profile estimates allocated bytes. The CPU view samples execution stacks.

## Reproduction configuration and provenance

The harness runs one Cassandra node in a Java Virtual Machine (JVM) distributed test. It creates one keyspace and 5,000 empty tables. It uses Java Development Kit (JDK) 21 and performs no user writes. It executes one raw `CREATE TABLE` statement at a time through `cluster.schemaChange()` ([harness phases](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L68-L90), [creation loop](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L93-L111)).

The durable N=5,000 evidence for this report is in the ignored local directory [`logs/20260904-083519-many-tables-5000t`](../logs/20260904-083519-many-tables-5000t/). Important artifacts are:

- [`console.txt`](../logs/20260904-083519-many-tables-5000t/console.txt), which records phase boundaries and completion of table `t004999`;
- [`create-times.csv`](../logs/20260904-083519-many-tables-5000t/create-times.csv), which has exactly 5,000 rows;
- the [before](../logs/20260904-083519-many-tables-5000t/histogram-02-before.txt) and [after](../logs/20260904-083519-many-tables-5000t/histogram-02-after.txt) class histograms;
- `02-create-tables.ap.jfr`, 40,690,758 bytes, or 38.8 MiB (shown as about 39 MB);
- `02-create-tables.jdk.jfr`, 66,267,456 bytes, or 63.2 MiB (shown as about 64 MB); and
- `02-create-tables-cpu.html`, derived from the phase recording.

The N=100 evidence comes from the [first run report](../.plans/many-tables-harness-run-1.md#first-findings-n100), the [metrics scope note](../.plans/h1-metrics-scope.md#total-live-object-growth-for-100-tables), and the [record-once harness context](../.plans/many-tables-profiling-harness-context.md#option-a-adopted-2026-09-03-validated). The broader [many-table overhead report](table_overhead.md#superlinear-table-creation-work) records the same N=5,000 artifact set and its earlier analysis.

These ignored logs do not travel with a normal Git checkout. This report therefore records the important values and assumptions.

## Timeline and arithmetic

| Scale | Creation elapsed | Timed create sum | Mean | Early calls | Late calls | Meter allocation share |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | about 8.9 s | about 8.9 s | about 89 ms | about 89 ms | about 89 ms | 9.9% |
| 5,000 | 638.181 s | 637.317222465 s | 127.46 ms timed | about 87–107 ms after startup | about 165–198 ms | 60.81% |

Phase 02 started at 12:35:34.409 and finished at 12:46:12.590. The wall-clock difference and harness result both equal 638.181 seconds. The first row was a 274 ms startup outlier. The next rows were mostly near 87–107 ms. The final rows ranged from 152 to 198 ms, with most near 165–198 ms.

The arithmetic is:

```text
phase elapsed                         638.181000000 s
sum(create-times.csv)                -637.317222465 s
time outside per-create windows         0.863777535 s
outside share = 0.863777535 / 638.181       0.13535%

fixed base = 5,000 * 0.090 s          450.000 s  (7m30s)
observed phase - fixed base            188.181 s  (about 3m08s)
```

The timer starts before query string formatting and stops after `schemaChange()` returns ([creation loop](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L100-L109)). Thus 637.317 seconds is the sum of the call windows, not a bytecode-level measurement of only the method body. CSV writes occur after each timer stops. This small distinction does not change the conclusion.

All 5,000 rows exist. The console records enactment of `t004999` before phase 02 finishes. The run then completes the 300-second hold and starts teardown. The external 30-minute task cap interrupts teardown about 13 minutes later. It does not interrupt creation.

## Harness sleeps and external work

The complete harness sleep list is:

1. Phase 00 sleeps for five seconds, requests garbage collection (GC), then sleeps for two seconds ([baseline](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L72-L77)).
2. Phase 04 sleeps for at most five seconds between hold samples ([hold loop](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L128-L150)).
3. Phase 04 requests GC and then sleeps for two seconds before its post-GC sample ([hold completion](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L153-L155)).

Phase 02 contains no sleep, park, retry delay, or rate limiter. It has one serial loop and one `schemaChange()` call per table.

The driver takes a checkpoint before phase timing. Inside the phase timer it starts the async-profiler session, runs the phase body, stops that session, and dumps the JDK Flight Recording (JFR). It computes elapsed time after those actions ([phase driver](../test/distributed/org/apache/cassandra/distributed/test/ProfiledClusterHarness.java#L173-L207)). Histogram collection runs as an `after` hook after the phase has finished ([after hook](../test/distributed/org/apache/cassandra/distributed/test/ProfiledClusterHarness.java#L209-L214)).

The 0.864-second gap can contain profiler start/stop and dump work, CSV writes, loop work, and other harness bookkeeping. It cannot contain a hidden multi-minute harness wait. Profiling also runs concurrently during every schema call. The gap does not measure that observer overhead. A `--no-profile` control remains necessary before claiming zero profiler effect ([profile switch](../test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java#L277-L282)).

## Production `schemaChange` call flow

Each loop iteration uses the production coordinator path:

1. `AbstractCluster.schemaChange()` creates a `SchemaChangeMonitor`, installs it, executes the CQL at consistency level `ALL`, and waits for completion ([schemaChange](../test/distributed/org/apache/cassandra/distributed/impl/AbstractCluster.java#L896-L936)).
2. The coordinator processes the raw CQL statement. This includes normal CQL parse, prepare, validation, and execution work. The harness does not call a schema fixture helper.
3. `AlterSchemaStatement` first applies the transformation locally as a validation pass. It then commits through `Schema.submit()` and calculates another schema diff ([execution and commit](../src/java/org/apache/cassandra/cql3/statements/schema/AlterSchemaStatement.java#L171-L230)).
4. `CreateTableStatement` builds and validates `TableMetadata`, then returns an updated immutable schema ([table transformation](../src/java/org/apache/cassandra/cql3/statements/schema/CreateTableStatement.java#L147-L198)).
5. The Transformation and Cluster Metadata (TCM) path commits and enacts the schema transformation. The run log confirms one TCM schema-change commit per table.
6. `SchemaListener` applies the schema change before commit completion and sends post-commit notifications ([schema listener](../src/java/org/apache/cassandra/tcm/listeners/SchemaListener.java#L40-L64)).
7. The schema diff creates the table runtime through `Keyspace.initCf()` ([schema initialization](../src/java/org/apache/cassandra/schema/DistributedSchema.java#L205-L250), [CFS creation](../src/java/org/apache/cassandra/db/Keyspace.java#L369-L393)).
8. `ColumnFamilyStore` (CFS) creates the configured memtable, tracker, compaction strategy, index manager, and `TableMetrics` during construction ([CFS initialization](../src/java/org/apache/cassandra/db/ColumnFamilyStore.java#L500-L585)).
9. `TableMetrics` eagerly registers the table's gauges, counters, histograms, timers, and meters ([metrics constructor](../src/java/org/apache/cassandra/metrics/TableMetrics.java#L433-L590)).
10. Schema events and agreement completion let `schemaChange()` return.

The monitor's 120-second value is a failure timeout. It is not a delay added to each call. `waitForCompletion()` returns when agreement is complete and throws only if the timeout expires ([wait logic](../test/distributed/org/apache/cassandra/distributed/impl/AbstractCluster.java#L998-L1012)). Despite its name, `startPolling()` installs a schema event listener in this implementation ([monitor implementation](../test/distributed/org/apache/cassandra/distributed/impl/AbstractCluster.java#L1027-L1067)).

The source confirms the main steps above. The profile does not assign an exact elapsed share to every step. In particular, it does not isolate parsing, TCM persistence, CFS startup, event delivery, and agreement as separate wall-time totals.

## Confirmed quadratic `ThreadLocalMeter` growth

`ThreadLocalMeter` stores three exponentially weighted moving-average rates per meter. `RATES_COUNT` is 3 ([rate layout](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L64-L68)). The static array starts with room for 16 groups, or 48 doubles ([shared array](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L116-L126)).

Allocation is equivalent to this pseudocode ([allocation source](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L128-L154)):

```text
offset = takeRecycledOffset()
if no recycled offset:
    offset = generator.getAndAdd(3)
lock ratesArrayGuard:
    if rates.length < offset + 3:
        next = new double[offset + 3]
        copy every element from rates to next
        rates = next
    initialize the three cells
```

An unrecycled meter therefore leaves no spare capacity after expansion. The next unrecycled meter allocates another array and copies the complete previous array. Meter cleanup can recycle an offset, but the 5,000 new tables keep their meters live ([construction and cleanup](../src/java/org/apache/cassandra/metrics/ThreadLocalMeter.java#L173-L220)). TableMetrics fan-out converts each new table into 44 new live meters in this run.

Let `G` be the total allocated group count, including the first 16 groups that fit without expansion. For group index `g`, an expansion copies about `3g` doubles. Each double uses 8 bytes. Cumulative copied bytes are therefore:

```text
sum(g = 16 to G - 1, 3g * 8)
= 24 * sum(g = 16 to G - 1, g)
= 12 * G * (G - 1) - 2,880 bytes
```

The constant removes the already available first 16 groups. The formula shows O(G) work for one late meter and O(G²) aggregate copying.

The histogram count rises from 4,093 to 224,093 `ThreadLocalMeter` objects. The delta is exactly 220,000, or 44 per table. `MeterCleaner` rises to about 224,135 objects, which independently matches the same population order.

Using only the 220,000 newly live meters as `G` gives 580,797,360,000 copied bytes, or about 581 decimal GB. This is an order estimate, not an exact byte counter. If all 4,093 pre-existing live meters occupied dense unrecycled offsets first, incremental phase copying would be about 602 GB. Live-object counts cannot reveal the generator's full allocation and recycling history. The sampled allocation profile estimates roughly 591 GB below `allocateRateGroupOffset`, between those simple assumptions. Sampling error, array-allocation overhead, prior offsets, and recycling explain why these values need not match exactly.

## Evidence triangle

| Evidence | Observation | Meaning |
|---|---|---|
| Source mechanism | Exact-size expansion copies the old shared array for each unrecycled group | Predicts O(n²) aggregate copying |
| Class histograms | 4,093 before; 224,093 after; delta 220,000 = 44/table | Confirms the table-to-meter fan-out and large live group population |
| JFR allocation | 9.9% at N=100; 60.81% at N=5,000; about 591 GB estimated below the method | Confirms that the predicted cost becomes dominant at scale |

No single side proves elapsed-time causality alone. Together, the code rule, exact object-count slope, and rising sampled allocation share confirm the quadratic growth mechanism and show that it matters in this run.

## Secondary growing costs

### Immutable schema BTree work

Schema `BTreeRemoval`, `BTreeMap.with/without`, and builder paths account for about 12% of sampled N=5,000 allocation. `Keyspaces.withAddedOrUpdated()` removes the old keyspace's table entries and adds the replacement entries ([schema map update](../src/java/org/apache/cassandra/schema/Keyspaces.java#L172-L197)). `BTreeMap.with()` calls `BTree.update`; `without()` calls `BTreeRemoval.remove` ([BTree map](../src/java/org/apache/cassandra/utils/btree/BTreeMap.java#L54-L80)). Removal copies affected nodes ([BTree removal](../src/java/org/apache/cassandra/utils/btree/BTreeRemoval.java#L31-L150)).

The JDK JFR CPU view also shows growing schema work. Hot methods include `TableMetadata.compareColumns` at 4.92%, the `AbstractBTreeMap` comparator at 4.36%, `ImmutableMap.keySet` at 3.76%, the natural comparator at 3.72%, `BTreeRemoval.removeFromLeaf` at 2.67%, and `Keyspaces.withoutKsTablesViews` at 2.37%. These are CPU sample shares, not allocation shares.

### Periodic TCM metadata snapshots

`metadata_snapshot_frequency` defaults to 100 ([configuration](../src/java/org/apache/cassandra/config/Config.java#L244-L247)). The local metadata log submits `TriggerSnapshot` when the epoch is divisible by that value ([schedule rule](../src/java/org/apache/cassandra/tcm/log/LocalLog.java#L955-L967)). The marker causes `MetadataSnapshotListener` to store the current cluster metadata ([snapshot listener](../src/java/org/apache/cassandra/tcm/listeners/MetadataSnapshotListener.java#L38-L53)).

This is source-confirmed periodic serialization and storage of growing metadata. Its scheduling is asynchronous by default ([trigger contract](../src/java/org/apache/cassandra/tcm/transformations/TriggerSnapshot.java#L31-L55)). It can overlap later schema calls and consume shared resources. Its exact share of the slowdown is unknown because this run did not isolate it.

### Separate SSTable flush-path allocation

`StreamingTombstoneHistogramBuilder$Spool` accounts for 7.46% of sampled allocation at N=5,000, down from 27.3% at N=100. This class is not metrics code. `MetadataCollector` creates it for SSTable metadata on writer and flush paths ([collector field](../src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java#L115-L124)). Its default spool size is 100,000 ([SSTable constants](../src/java/org/apache/cassandra/io/sstable/SSTable.java#L69-L73)). Treat this as separate flush and compaction work.

## Why phase reordering and shorter holds do not help creation

Phase 02 completes before the steady hold starts. Reducing the hold saves post-creation wall time only. Moving histogram collection or teardown cannot remove work inside 5,000 serial production schema changes. Reordering profiles can change observation conditions, but it does not remove CQL, TCM, schema update, CFS initialization, metrics registration, or agreement work.

The test goal must decide which setup cost is valid.

- **Production creation-performance mode:** execute 5,000 serial `CREATE TABLE` operations through `cluster.schemaChange()`. Use this mode for claims about user-visible table creation.
- **Residency-only bulk fixture mode:** install many table definitions through a future bulk test fixture. This may reduce repeated CQL and TCM work. It is valid for steady-state heap or residency experiments. It is invalid for production creation-performance claims.

A two-to-three-minute residency setup may require the second mode. A production creation run still has a roughly 450-second base unless deeper schema-path work also improves.

## Candidate fix: geometric or chunked array growth

Change the `rates` expansion policy so one resize adds spare capacity. A geometric policy can grow by a factor. A chunked policy can add a fixed number of rate groups. Either policy changes repeated exact-size copies into infrequent copies.

The implementation must preserve these invariants:

- each meter owns three consecutive cells;
- an allocated or recycled offset remains stable for that meter's lifetime;
- all three cells receive `NON_INITIALIZED` before use;
- `ratesArrayGuard` still coordinates resize publication with ticks;
- volatile publication exposes a complete copied array;
- cleanup can recycle offsets safely; and
- meter counts and one-, five-, and fifteen-minute rates remain correct.

Expected effect: cumulative resize copying becomes linear or amortized linear instead of quadratic. `allocateRateGroupOffset` allocation should collapse, and late create latency should move toward early latency. Schema and TCM costs will then become a larger visible share.

Risks include races among creation, removal, and background ticks; integer overflow near array limits; lost rate cells during publication; and excess spare capacity. Geometric growth can retain up to a bounded fraction of unused array space. Chunked growth bounds unused space by the chunk size but copies more often. At 8 bytes per cell and three cells per group, even tens of thousands of spare groups cost only megabytes. Validation must measure the actual standing-heap change.

## Validation matrix

| Check | N=100 | N=1,000 | N=2,000 | N=5,000 | Pass condition |
|---|---:|---:|---:|---:|---|
| Serial production creation before/after | yes | yes | yes | yes | Same 5,000-operation semantics and no failures |
| Per-table latency slope | compare bins | compare bins | compare bins | compare early and late bins | Late slope falls materially after the fix |
| Total creation time | record | record | record | record | Improvement grows with N; no small-N regression |
| `allocateRateGroupOffset` bytes/share | JFR | JFR | JFR | JFR | Estimated bytes and allocation share collapse |
| No-profile control | optional smoke | yes | optional | yes | Direction and scale remain without profiler sampling |
| Standing heap after GC | record | record | record | record | Unchanged except bounded spare array capacity |

Also run focused correctness tests for mark counts, mean rate, all three moving rates, ticking, and recycled offsets. Add concurrency stress that creates and removes meters while background ticks and reads run. Exercise repeated expansion and recycled-ID reuse. Check that no tick writes to an old array after publication and that no recycled group leaks a prior meter's rate.

Keep node count, JDK, heap, table schema, write mode, snapshot frequency, profiler settings, and phase order fixed within each comparison. Report decimal GB or binary GiB explicitly.

## Expected outcome and non-goals

The first expected result is removal of hundreds of gigabytes of transient meter-array allocation. The per-table latency curve should flatten relative to the current late rise. Total N=5,000 creation time should improve, but the exact gain remains unknown until an A/B run.

This change does not target the roughly 90 ms/table base. It does not remove standing per-table metrics, CFS, memtable, compaction, management, or schema state. It does not optimize schema BTree operations or TCM snapshots. It does not establish a fast bulk fixture. It does not promise a two-to-three-minute production creation result.

## Limitations

- The evidence uses a one-node in-JVM distributed test, not a multi-node deployment.
- The run uses JDK 21.
- Allocation values come from sampling and are estimates.
- The tables are empty.
- The default run has no user writes.
- This report has one N=5,000 run, although N=100 repeat runs were stable.
- The N=5,000 logs are local ignored artifacts.
- Live-object counts do not expose historical meter ID allocation or recycling.
- The run does not isolate profiler observer overhead, schema BTree cost, or snapshot cost.

## Prioritized next steps

1. Implement geometric or chunked growth as a small isolated change.
2. Add rate correctness, recycling, and concurrent create/remove/tick tests.
3. Run the N=100, 1,000, 2,000, and 5,000 before/after matrix, including a no-profile control.
4. Compare total time, latency slope, meter allocation bytes, and post-GC heap.
5. Re-profile the remaining schema BTree and TCM snapshot costs after meter copying falls.
6. Design a separate bulk fixture only for residency experiments that do not claim production creation performance.
