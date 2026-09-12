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

# Metric selection source manifest

Source commit: `319995ce5c178f5eb0dbaf135b98d395c40a045c`

Corrected frozen base: `f7d5a7e2c0fcf769293ee27073630a3574584b06`

## Complete SELECT-owned diffs

- `.build/metrics/GenerateMetricsReference.java`
- `.build/metrics/test_metrics_reference.py`
- `.build/sh/ai-generate-metrics-reference`
- `conf/all_metrics.yml`
- `conf/metrics_ref.md`
- `conf/simple_metrics.yml`
- `debian/cassandra.install`
- `src/java/org/apache/cassandra/db/virtual/TableMetricTables.java`
- `src/java/org/apache/cassandra/io/sstable/filter/BloomFilterMetrics.java`
- `src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummaryMetrics.java`
- `src/java/org/apache/cassandra/io/sstable/keycache/KeyCacheMetrics.java`
- `src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java`
- `src/java/org/apache/cassandra/metrics/MetricProfile.java`
- `src/java/org/apache/cassandra/metrics/NoOpMetrics.java`
- `src/java/org/apache/cassandra/metrics/TableMetrics.java`
- `src/resources/org/apache/cassandra/metrics/metrics-catalog.properties`
- `test/resources/org/apache/cassandra/metrics/test-metrics-profile.yml`
- `test/unit/org/apache/cassandra/metrics/LatencyMetricsTest.java`
- `test/unit/org/apache/cassandra/metrics/MetricProfilePropertyTest.java`
- `test/unit/org/apache/cassandra/metrics/MetricProfileTest.java`
- `test/unit/org/apache/cassandra/metrics/NoOpMetricsTest.java`
- `test/unit/org/apache/cassandra/metrics/TableMetricBookkeepingLifecycleTest.java`
- `test/unit/org/apache/cassandra/metrics/TableMetricBookkeepingTest.java`

The generator wrapper started from its complete source diff. Its Python runner
has the portable adapter below. `conf/README.txt` omits a link to the archived,
unshipped dependency research. The two distributed test files also started from
their complete source diffs and have the standalone adaptations below.

## Shared-file hunk allocation

| Path | SELECT-owned content |
| --- | --- |
| `.build/sh/ai-profile-many-tables` | `PROFILE_COMPACT_BOOKKEEPING` JVM property only. |
| `conf/README.txt` | Metric profile, alias, catalog, generator, and compact bookkeeping sections. |
| `conf/cassandra.yaml` | `metrics_config_file: simple_metrics.yml` and its comments. |
| `CassandraRelevantProperties.java` | `COMPACT_TABLE_METRIC_BOOKKEEPING` only. |
| `Config.java` | `metrics_config_file` only. |
| `DatabaseDescriptor.java` | MetricProfile import/state, client `setConfig`, profile load, and getter. |
| `CassandraMetricsRegistry.java` | Profile guards in counter, atomic counter, meter, and registration. Legacy constructors remain. |
| `LatencyMetrics.java` | Empty/concurrent children, shared no-op instance, recording state, idempotent release, release snapshot, child aggregation, and no-op timer constructor. |

No shared file includes optimized recording storage, lazy metric identifiers,
adaptive Java Management Extensions (JMX) history, or compact JMX registration.

## Standalone test adaptations

The generator wrapper prefers `./venv/bin/python`. It uses host `python3` for
its standard-library-only tests when no project environment exists, and fails
clearly when neither interpreter exists. `conf/README.txt` names the extracted
generator entrypoints instead of integration-owned root selector scripts.

| Source location | Standalone change | Preserved behavior | Integration assertion |
| --- | --- | --- | --- |
| `MetricProfileRegistrationTest.exerciseProfile` | Removed `optimized_metrics_enabled` and `compact_jmx_registration_enabled`. Configured TrieMemtable without `lazy_initialization`. | Registration filtering, aliases, table create/drop/recreate, writes, reads, flush, history transfer, child release, and JMX checks. | Run the same profile test with STORE and EXPORT settings after branch merge. |
| `MetricProfileRegistrationTest.exerciseProfile`, after create | Removed the first `TrieMemtable.isInitialized()` false assertion. | Initial ownership, latency children, and export assertions remain. | Restore the false assertion for combined lazy TrieMemtable coverage. |
| `MetricProfileRegistrationTest.exerciseProfile`, after flush | Removed the second `TrieMemtable.isInitialized()` false assertion. | SSTable creation, metric counts, aggregate input, and release history assertions remain. | Restore the false assertion after flush for combined lazy TrieMemtable coverage. |
| `MetricProfileRecordingTest.newCluster` | Removed `optimized_metrics_enabled` and `compact_jmx_registration_enabled`. Configured TrieMemtable without `lazy_initialization`. | No-op sharing, enabled aggregate inputs, hidden latency children, virtual views, and table writes/reads remain. | Run the same recording test with MEM, STORE, and EXPORT settings after merge. |

## Validation state

Extraction and the Java Development Kit (JDK) 21 build passed. The stale-output
check found 126 table and 101 keyspace metrics. Generator unit tests passed 11
tests, and the property entrypoint passed one test. The runtime catalog exists
in the jar. The artifact target copies all `conf/` files, and Debian packaging
names both profile YAML files.

Eight targeted Cassandra classes passed 37 top-level tests. The two nested
property launches passed five executions, for 40 unique Cassandra tests. The
final production and test files match the tree that the build and tests covered.
The all-versus-simple measurement moved to the integration harness because the
generic harness does not apply `PROFILE_CONFIG_DIR` to its in-JVM node config.
