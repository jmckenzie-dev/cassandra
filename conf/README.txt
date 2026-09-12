Required configuration files
============================

cassandra.yaml: main Cassandra configuration file
logback.xml: logback configuration file for Cassandra server


Optional configuration files
============================

cassandra-topology.properties: used by PropertyFileSnitch


Runtime metric storage
======================

The optimized_metrics_enabled setting controls histogram storage. It defaults
to true. Set it to false and restart to restore legacy reservoir storage.

Set -Dcassandra.geometric_meter_arrays=true in the Java Virtual Machine (JVM)
startup options to store meter rates in shared geometric arrays. The default is
false. Remove the option or set it to false and restart to restore the legacy
thread-local meter arrays.


Experimental lazy metric IDs
============================

Set -Dcassandra.lazy_metric_ids=true in the JVM startup options to allocate
thread-local counter IDs on the first update. The default is false. Remove the
option or set it to false and restart to restore eager allocation. Cassandra's
metric factories select the implementation before constructing each metric.
Changing the property later does not change existing metrics. Direct constructors
retain the eager implementation; create factories select the optional behavior.

This uses a JVM property because counters also initialize in utility classes
before cassandra.yaml loads. It covers plain counters, histogram counts, and
both counter IDs inside either thread-local meter implementation. Empty reads,
resets, snapshots, and meter ticks do not allocate IDs. A zero-valued update
does allocate an ID; a live zero count never means an unused ID.

Worker arrays remain dense. This experimental option reduces retained storage
for untouched metrics but adds first-use work and can change recording costs.

