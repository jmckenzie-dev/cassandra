Required configuration files
============================

cassandra.yaml: main Cassandra configuration file
logback.xml: logback configuration file for Cassandra server


Optional configuration files
============================

cassandra-topology.properties: used by PropertyFileSnitch


Metric profile definitions
==========================

all_metrics.yml: all known Table/IndexTable and Keyspace metrics enabled.
simple_metrics.yml: a smaller selection for the same metric families.
metrics_ref.md: generated descriptions, types, units, aliases, and profile choices
for every name in those two files.

These files define the proposed allowlist configuration. Cassandra does not yet
load them. They do not change the running server or reduce its heap usage.
Runtime loading, profile selection, dependency handling, and enforcement are
separate implementation work.

Both files use mode: allowlist and three lists within each scope:

- required: metrics that must remain recorded and exposed. Both profiles have
  the same required entries. These entries document internal dependencies; an
  operator must not be able to disable required state by editing the file.
- optional: enabled metrics that an operator may move to disabled.
- disabled: metrics excluded from exposure and from optional recording. Internal
  dependencies and enabled aggregate metrics can still need their backing state.

The intended allowlist rule disables unlisted optional names within these scopes.
The runtime implementation must reject unknown names, duplicate entries, entries
in more than one list, and missing or misclassified required names. An explicitly
empty list uses [], not a blank YAML value. This validation is not wired up yet.

Names and coverage
------------------

Names match the case-sensitive registered metric name, not the Java field name.
Each entry selects a metric across every table or keyspace in that family. These
profiles do not select individual table names, individual keyspaces, percentile
attributes, or wildcard patterns. A Latency entry selects the entire timer;
TotalLatency is a separate metric.

The table section covers Table and IndexTable metrics and their ColumnFamily and
IndexColumnFamily aliases. Existing aliases follow their canonical entry; do not
list them separately. Deprecated aliases also follow the canonical entry:

  AllMemtablesOnHeapDataSize -> AllMemtablesHeapSize
  AllMemtablesOffHeapDataSize -> AllMemtablesOffHeapSize
  MemtableOnHeapDataSize -> MemtableOnHeapSize
  MemtableOffHeapDataSize -> MemtableOffHeapSize
  EstimatedPartitionCount -> EstimatedRowCount
  EstimatedPartitionSizeHistogram -> EstimatedRowSizeHistogram
  MinPartitionSize -> MinRowSize
  MaxPartitionSize -> MaxRowSize
  MeanPartitionSize -> MeanRowSize

The catalog contains 126 canonical table names and 101 keyspace names. It includes
the gauges supplied by the built-in SSTable formats, even when a particular
format is not in use. It includes conditional metrics, such as base-table view
timers; selection must not create metrics for an inapplicable runtime object.
The existing table AnticompactionTime and keyspace AntiCompactionTime spellings
are different and remain distinct in these files.

Other metric families are outside this first pass. This includes global Table
aggregates, TrieMemtable, Storage Attached Indexing (SAI), and node/service
metrics. Their current behavior remains in effect. Unregistered operational
statistics, such as the flush-size moving average, are also outside the catalog.

The catalog comes from TableMetrics.java and KeyspaceMetrics.java, with Latency
and TotalLatency names expanded from LatencyMetrics.java. SSTable gauges come
from BloomFilterMetrics.java, IndexSummaryMetrics.java, and KeyCacheMetrics.java
under src/java/org/apache/cassandra/io/sstable/.

Simple profile choices
----------------------

The simple profile enables four required and 15 optional table metrics. The
optional set covers read/write/range latency, disk and SSTable counts, memtable
memory, flush/compaction backlog, repair coverage, read amplification, tombstone
failures/warnings, and speculative retries. Timers already include request counts
and rates, so separate total-latency counters are disabled.

The 31 optional keyspace metrics retain broader resource totals, repair progress,
failed speculation, and read/write limit signals. Keyspace metrics have fewer
instances than table metrics. Detailed repair stages, transaction stages, cache
statistics, and distribution diagnostics remain available in all_metrics.yml.
The simple profile lists every omitted metric under disabled so an operator can
move a name to optional without consulting a separate catalog.

Enabled aggregate metrics must still account for all contributing tables. A
disabled table export can therefore retain recording state used by a keyspace
or global metric. These lists do not predict the eventual heap reduction.

Both profiles preserve CoordinatorReadLatency, CoordinatorWriteLatency, and
TotalDiskSpaceUsed. CompressionRatio is required conservatively because a
conditional size-estimation helper reads it; the normal cleanup path reads
SSTable metadata directly. No registered Keyspace metric has a confirmed database
control dependency. See research/internal_metric_dependencies.md for the audit,
including required node metrics that are outside these profiles.

Generating the metric reference
-------------------------------

From the repository root, with a full JDK (validated with JDK 21):

  .build/sh/ai-generate-metrics-reference
  .build/sh/ai-generate-metrics-reference --check

The first command writes conf/metrics_ref.md. The second checks that the file is
current without changing it. Both log to the console and a timestamped file in
logs/. In the development container, prefix these commands with
"distrobox enter dev --".

The generator uses the JDK Java syntax parser and the existing SnakeYAML jar in
lib/. It does not compile or initialize Cassandra and needs no new dependencies.
Descriptions live in Java field Javadoc. Names, types, and aliases come from
field declarations and registration calls. LatencyMetrics supplies the Latency
and TotalLatency naming convention. Built-in SSTable provider fields supply the
format-specific gauges. Profile columns come directly from the two YAML files.

Update the field Javadoc when a metric's meaning changes, then regenerate. The
generator checks exact catalog coverage for these two shipped profiles, required
selection agreement, duplicate/unknown entries, missing descriptions, and the
registration forms it supports. This is a documentation check, not the future
runtime allowlist validator: operator profiles may omit optional names. Parsing
cannot prove that prose still matches a computation; review the update/read sites
when changing a description. New registration forms need extractor support.

Run the isolated generator tests with ./run_tests.sh --metrics-ref. Run generated
input cases with ./run_property_tests.sh --metrics-ref. These commands use the
project's existing venv and a JDK, and do not start a Cassandra node.
