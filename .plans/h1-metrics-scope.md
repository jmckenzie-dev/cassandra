# H1 scope: per-table metrics weight

Measured on record-once N=100 run (`logs/20260903-102943-many-tables-100t`),
class-histogram delta around phase 02 (before → after), divided by 100 tables.
Code reconciliation from TableMetrics/CassandraMetricsRegistry census (HEAD).

## Total live-object growth for 100 tables

- Histogram: 67.6 MB → 110.9 MB = **~432 KB/table** (snapshot pre-GC-gap; the
  used-heap post-GC figure is ~280 KB/table — different moment, same story).

## Attribution by family (per table)

| Family | Measured | Notes |
|---|---|---|
| ObjectName family (ObjectName + Property + Property[]) | **~46 KB** | 242 ObjectNames/table = registry's MBean names |
| JMX wrappers (JmxGauge/Meter/Counter/Timer/Histogram) | **~6.8 KB** | ~272/table; allocated by registry even before MBeanServer decides |
| Metric objects (ThreadLocalMeter +40, MeterCleaner, DecayingEstimatedHistogramReservoir 33/table, buckets, counters, timers) | **~7.7 KB** | reservoir count matches code census exactly (33) |
| Schema objects (TableMetadata, TableParams, ColumnMetadata, refs) | ~0.4 KB | tiny at class level |
| CFS + memtable + compaction machinery | ~1.1 KB | incl. 6 STCS objects/table |
| Identified total | **~62 KB (~14%)** | |
| Unaccounted (diffuse containers) | **~370 KB (~86%)** | registry CHM nodes + name strings (~272 × ~100-200B), MBeanServer repository, general String/byte[] from metadata |

Creation-phase allocation: ThreadLocalMeter.allocateRateGroupOffset ~10%,
ObjectName/wrapper churn part of the rest. **Correction:** the 27.3%
StreamingTombstoneHistogramBuilder$Spool is NOT metrics — it is
MetadataCollector (flush path), 3 MB per instance with the default 100,000
spool size. Separate finding for the flush/compaction track.

## Code facts (child census, cited)

- Eager `new TableMetrics(this)` per table at CFS construction
  (ColumnFamilyStore.java:567-569 via Keyspace.initCf); no traffic or config gate.
- ~272 registry registrations/table → Jmx* wrapper allocated first
  (CassandraMetricsRegistry.java:552-584), fresh ObjectName parse per name.
- 33 DecayingEstimatedHistogramReservoir/table (14 hist + 19 timer).
- No JmxReporter exists; MBeanWrapper decides retention. In-JVM dtest with
  Feature.JMX uses a dedicated MBeanServer (InstanceMBeanWrapper) that retains
  the wrappers; platform MBS stays at ~27 beans (explains inert mbeans counter —
  we measured the wrong server).
- In-tree alternatives: `system_views.*` per-table metric virtual tables read
  `cfs.metric` directly (TableMetricTables.java:191-228); `system_metrics`
  virtual keyspace exposes the whole registry. CQL already replaces JMX reads.

## Levers, priced

- **L1 — MBean registration default-off (registry chokepoint).** Removes
  ObjectName family + wrappers + their registry strings ≈ **60-75 KB/table
  (~14-17%)** plus creation CPU. THE product decision: per-table MBeans are a
  public contract (nodetool/jconsole users). Virtual tables cover reads, but
  upstream sign-off is required; flag-gated (`cassandra.metrics.mbeans_enabled`
  or yaml) to keep compat.
- **L2 — Lazy reservoirs (build on first update).** 33/table; small residency
  but removes construction cost; near-zero behavioral risk (idle tables report
  empty either way).
- **L3 — Meter/cleaner fan-out reduction.** ~4 KB/table; minor.
- **L4 — MetadataCollector spool sizing (separate track, flush path).** 3 MB
  transient per SSTable writer; H3-adjacent, not metrics.

## Honest reframe

H1 (as originally phrased: metrics dominate per-table heap) is confirmed at
~14-17% of live growth. The remaining ~86% is diffuse and unmapped. Before
implementing small levers, map the dominant retained sets at scale.

## Recommended next steps

1. Scale probe at 1k tables with a heap dump diff (dominator analysis) to map
   the unaccounted 86% — uses the existing harness, zero code changes.
2. Take L1's contract question to the community/design discussion early
   (default-off MBeans + virtual-table reads) — it is the biggest single lever
   and the long pole is agreement, not code.
3. mbeans counter fix (measure the instance MBeanServer) rides along with any
   L1 work.
