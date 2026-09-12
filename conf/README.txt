Required configuration files
============================

cassandra.yaml: main Cassandra configuration file
logback.xml: logback configuration file for Cassandra server


Optional configuration files
============================

cassandra-topology.properties: used by PropertyFileSnitch


Optional Java Management Extensions (JMX) history storage

The adaptive_jmx_histogram_history_enabled setting compresses saved JMX scrape
snapshots independently. It defaults to false, preserving long-array history.
Setting it to true preserves exact recent-value deltas and each alias's cursor.

Optional JMX name and registration storage

Uncomment this line in jvm-server.options to keep monitoring property queries
from populating caches on registered metric ObjectNames:

  -Djavax.management.builder.initial=org.apache.cassandra.utils.TransientMBeanServerBuilder

This JVM startup option covers direct platform-server access and connectors
attached to the resulting server. Set it before the platform server is created;
a later YAML or runtime change cannot replace that server. It uses the standard
JDK registration store and preserves property queries, readable values, and
Cassandra authorization checks. Local query results, direct ObjectName results,
and registration notifications use independent name copies. Broad queries thus
allocate more temporary objects. Narrow queries still scan the selected domain.
Third-party MBeans that retain and inspect their own registration names can
still populate their own caches. This option does not copy arbitrary returned
application object graphs. Leave the line commented for the legacy server.

Independently, compact_jmx_registration_enabled: true in cassandra.yaml avoids
one persistent JDK adapter for each metric MBean. It retains
the existing getters and metadata and creates temporary adapters during calls.
Meter and timer wrappers also share the constant rate-unit label. This setting
defaults to false. Neither option changes metric names, aliases, recording, or
histogram history. Restart to change either selection. Historical measurements
and limits remain in the original branch at
research/jmx_query_export.md.


Run focused checks with ./run_tests.sh. Run generated checks with
./run_property_tests.sh. The property selector runs the nested registration suite
and standalone generated name-copy and query checks.
