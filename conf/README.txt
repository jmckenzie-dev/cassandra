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

Select a profile at startup with metrics_config_file in cassandra.yaml:

  metrics_config_file: simple_metrics.yml

The supplied cassandra.yaml selects simple_metrics.yml. Use all_metrics.yml for
the full catalog. Omitting the setting, or setting it to null, preserves all
metrics, including names outside the built-in catalog. A bare filename resolves
on the classpath (normally conf/). An absolute path or file: URI selects a local
file. Missing or invalid files stop startup. Restart to change the selection.

Both files use mode: allowlist and three lists within each scope:

- required: metrics that must remain recorded and exposed. Both profiles have
  the same required entries. These entries document internal dependencies; an
  operator must not be able to disable required state by editing the file.
- optional: enabled metrics that an operator may move to disabled.
- disabled: metrics excluded from exposure and from optional recording. Internal
  dependencies and enabled aggregate metrics can still need their backing state.

The allowlist disables unlisted optional names within these scopes.
Startup rejects unknown names, duplicate entries, entries
in more than one list, and missing or misclassified required names. An explicitly
empty list uses [], not a blank YAML value.

Names and coverage
------------------

Names match the case-sensitive registered metric name, not the Java field name.
Each entry selects a metric across every table or keyspace in that family. These
profiles do not select individual table names, individual keyspaces, percentile
attributes, or wildcard patterns. A Latency entry selects the entire timer;
TotalLatency is a separate metric.

The table section covers Table and IndexTable metrics. The top-level boolean
include_legacy_aliases controls ColumnFamily/IndexColumnFamily and deprecated
metric-name exports. It defaults to true when omitted. all_metrics.yml sets it
to true; simple_metrics.yml sets it to false. This is a startup setting; restart
after changing it. No metrics_config_file still preserves all legacy exports.

When false, Cassandra skips aliases in both JMX and its metrics registry,
including global Table aliases. Modern names, recording, and aggregation remain
unchanged. Monitoring tools that use legacy names must migrate or enable this
setting. When true, aliases follow their canonical entry; do not list them
separately. Deprecated aliases include:

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

The required/optional/disabled lists do not select global Table aggregates;
only include_legacy_aliases changes their compatibility exports. Other metric
families, including TrieMemtable, Storage Attached Indexing (SAI), and node/service
metrics, remain unchanged. Unregistered operational
statistics, such as the flush-size moving average, are also outside the catalog.

The catalog comes from TableMetrics.java and KeyspaceMetrics.java, with Latency
and TotalLatency names expanded from LatencyMetrics.java. SSTable gauges come
from BloomFilterMetrics.java, IndexSummaryMetrics.java, and KeyCacheMetrics.java
under src/java/org/apache/cassandra/io/sstable/.

Simple profile choices
----------------------

The simple profile enables four required and 17 optional table metrics. The
optional set covers read/write/range latency, disk and SSTable counts, memtable
memory, flush/compaction backlog, repair coverage, read amplification, tombstone
failures/warnings, and speculative retries. ReadTotalLatency and WriteTotalLatency
provide cumulative durations for interval averages and aggregation across scopes;
the timers provide the corresponding request counts, rates, and distributions.

The 33 optional keyspace metrics retain broader resource totals, repair progress,
failed speculation, and read/write limit signals. Keyspace metrics have fewer
instances than table metrics. Detailed repair stages, transaction stages, cache
statistics, and distribution diagnostics remain available in all_metrics.yml.
The simple profile lists every omitted metric under disabled so an operator can
move a name to optional without consulting a separate catalog.

Enabled aggregate metrics still account for all contributing tables. A disabled
table export can therefore retain recording state used by a keyspace or global
metric. Independent disabled recorders share no-op instances. Table counters,
computed gauges, and latency children needed by enabled parents remain real.
The two anticompaction byte meters also remain real because the exposed global
ratio reads their table values. Dropping a table releases hidden aggregate
membership and transfers latency history to its parents.

Disabled metrics have no registry or JMX entry. Legacy system_views metric tables
keep their schemas but return no rows for a disabled metric. An omitted metric
does not represent a measured zero. Tooling that requires such a metric must
enable it in the profile.

Both profiles preserve CoordinatorReadLatency, CoordinatorWriteLatency, and
TotalDiskSpaceUsed. CompressionRatio is required conservatively because a
conditional size-estimation helper reads it; the normal cleanup path reads
SSTable metadata directly. No registered Keyspace metric has a confirmed database
control dependency. Required node metrics remain outside these profiles.

Generating the metric reference
-------------------------------

From the repository root, with a full JDK (validated with JDK 21):

  .build/sh/ai-generate-metrics-reference
  .build/sh/ai-generate-metrics-reference --check

The first command writes conf/metrics_ref.md and the metric catalog resource
bundled in the Cassandra jar. The second checks that both files are current
without changing them. Both log to the console and a timestamped file in
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
registration forms it supports. Shipped profiles must cover the whole catalog;
the runtime validator allows operator profiles to omit optional names. Parsing
cannot prove that prose still matches a computation; review the update/read sites
when changing a description. New registration forms need extractor support.

Run isolated generator tests with .build/sh/ai-generate-metrics-reference --test.
Run generated input cases with .build/sh/ai-generate-metrics-reference --property.
These commands need Python 3 and a Java Development Kit (JDK). They do not start
a Cassandra node.
