<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Monitoring name-cache investigation

Determine which Java Management Extensions (JMX) monitoring operations add
server-retained name caches, how much of the measured 65.1 MiB this explains,
and which remedies preserve metric discovery and values.

## Scope and baseline

- Branch `moar_tables`, HEAD `4192a00e1f3f26bc92a7a56da7cb40debe79ff9b`.
- Preserve the existing dirty tree. No production changes or commits.
- Inspect `CassandraMetricsRegistry`, `MBeanWrapper`, the census scraper, and
  Cassandra monitoring clients. Exercise actual Java 21 JMX operations.
- Baseline: the 1000-table simple/adaptive full scrape in
  `research/optimized_heap_next_steps.md`. Repeat full and attribute-only runs
  on the current runtime, sequentially, with identical workload and settings.
- All table counts stay at or below 1000. This is an empty-table metric census,
  not a populated-node capacity or throughput test.

## Measurements

1. Use fresh registered names for each isolated operation. Observe property
   caches without invoking property accessors in the observer. Measure the
   reachable ObjectName graph with the existing Jamm dependency. Verify name
   sets, counts, and attribute values.
2. Cover broad discovery, property-filtered queries, canonical and natural
   sorting, individual property access, copying, and real loopback remote JMX.
   Separate remote client parsing from work executed inside the server.
3. Check quoting and canonical-name copy behavior with generated names. Do not
   replace a property parser with substring matching as a proposed general fix.
4. Compare Cassandra live heaps before/after monitoring using MAT dominators
   and the existing property-map ownership analyzer. Keep disjoint retained
   sizes separate from overlapping reachable graphs.
5. Record the findings, commands, limits, and next implementation decision in
   `research/jmx_monitoring_name_retention.md`. Update TODO and the checkpoint
   to prioritize the largest supported registration target.

## Success criteria

Identify triggers and a measured avoidable byte count. Distinguish a collector
change from a server-wide guarantee. Preserve discovery and returned values in
the experiments. Keep fixed registration residency visible after cache costs
are removed. Do not describe investigation results as implemented savings.
