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

