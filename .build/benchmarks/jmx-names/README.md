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

# JMX monitoring name-retention probe

This diagnostic uses real Java Management Extensions (JMX) operations to
measure cache growth in registered `ObjectName` graphs. Each case starts with
fresh names. Remote cases use a real loopback Remote Method Invocation (RMI)
connection. The launcher uses the existing Jamm jar and writes console output
and a dated log under `logs/`.

```sh
./run_tests.sh --jmx-names
.build/sh/ai-probe-jmx-names --property-only
.build/sh/ai-probe-jmx-names 1000
.build/sh/ai-probe-jmx-names --bytecode
```

Supported population: 2..1000 names, default 1000. Use 10 for a smoke test.
The property-only entry point checks 16000 generated canonical-name copies.
Cache assertions target the measured Java 21 implementation. Read-only
reflection observes the JDK cache without populating it. No private state is
modified. The launcher also compiles the current optional query builder for
the boundary tests below. The original retention cases use the legacy server.

Graph sizes exclude connector state, temporary client objects, and metric
recording structures. These are residency observations, not throughput or
allocation benchmarks. Historical results remain in the original branch at
research/jmx_monitoring_name_retention.md.

## Optional query and registration checks

```sh
.build/sh/ai-probe-jmx-names --boundaries 10
.build/sh/ai-probe-jmx-names --boundaries --protected 10
.build/sh/ai-probe-jmx-names --boundaries --late
.build/sh/ai-probe-jmx-names --boundaries --properties
.build/sh/ai-probe-jmx-names --boundaries --benchmark
./run_tests.sh --jmx-registration
./run_property_tests.sh
env PROFILE_SKIP_BUILD=true PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.CompactJmxRegistrationTest .build/sh/ai-profile-many-tables
```

The query suite covers the actual optional builder, direct platform access,
early/late installation, local and RMI queries, custom expressions, notification
and operation-name copies, and Cassandra authorization. Its generated suite
compares queries with the legacy server and exercises lifecycle changes.
The intentionally incomplete diagnostic builder remains a regression control.

The query benchmark reports registered-name graph bytes, calling-thread
allocation, elapsed time, and serialized result bytes separately. Serialized
result bytes exclude RMI protocol traffic. The registration benchmark compares
counter reads with persistent and temporary JDK adapters. These short timings
have no confidence intervals. Historical results remain in the original branch
at research/jmx_query_export.md. Build first with Java 21. Run timing measurements
without competing builds or test workloads.
