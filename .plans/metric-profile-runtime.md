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

# Runtime metric profiles

Implement registration selection first, then eliminate unused recording state.
Keep the current path selectable. All workloads remain at or below 1,000 tables.

1. Capture a fresh 100-table census before production edits. Preserve all six
   checkpoints, scraper behavior, worker count, JVM settings, and raw artifacts.
2. Add startup-only metrics_config_file to cassandra.yaml. A bare resource name
   resolves on the classpath; absolute paths and file URIs select local files.
   Omitted/null preserves current behavior. The supplied yaml selects simple.
3. Parse an immutable policy with existing SnakeYAML. Enforce mandatory required
   names in code; reject unknown/duplicate/misclassified names and malformed
   sections. Unlisted optional names are disabled. Generate the runtime catalog
   from the same Java syntax model as the metric reference, not a second list.
4. Suppress Table/IndexTable and Keyspace registry/JMX entries, including aliases.
   Leave global Table and other metric families enabled. Preserve actual state.
5. Release aggregate membership and latency children independently of registry
   exposure. Preserve format-gauge deduplication and idempotent table release.
6. Test policy input and generated cases, registration names/aliases, aggregate
   equivalence, and drop/recreate lifecycle. Measure the registration-only state.
7. Replace independent disabled recorder destinations with shared no-ops. Keep
   counters/gauges read by global aggregates, required control timers, SAI disk
   accounting, and the anticompaction ratio inputs. Preserve children whenever
   an enabled latency parent needs their values. Keep valid empty snapshots and
   timer callback/exception semantics.
8. Exercise real writes, reads, flushes, metric fan-out, control dependencies,
   and lifecycle with targeted tests. Specify disabled legacy virtual-view
   behavior. Run the build and main/test Checkstyle.
9. Capture two matched final runs at 1,000 tables: explicit all_metrics.yml and
   simple_metrics.yml. Compare rested creation and post-activity heap, registry
   and MBean counts, and retained metric state. Do not extrapolate capacity from
   an empty-table measurement.
10. Record pre/intermediate/final measurements, validation, limitations, and the
    next residency targets in research/metric_profile_runtime.md and TODO.md.
