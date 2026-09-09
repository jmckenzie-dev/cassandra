<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0 . Unless required by applicable law
or agreed to in writing, software distributed under the License is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
-->

# Transient JMX queries and registration cost

Measured on September 7, 2026. All actual table populations remain at
or below 1000. The isolated query experiment registers 1000 names, not 1000
Cassandra tables. No million-table capacity claim follows from these results.

## Design

The optional `javax.management.builder.initial` startup property selects
[`TransientMBeanServerBuilder`](../src/java/org/apache/cassandra/utils/TransientMBeanServerBuilder.java).
The public Java Management Extensions (JMX) builder hook covers the server
returned directly by `ManagementFactory.getPlatformMBeanServer()`, ordinary
server factories, and remote connectors attached to those servers. Install it
at JVM startup. Setting it after platform-server creation cannot replace that
server. Cassandra configuration loaded later is unsuitable for this switch.

The adapter retains the JDK registration repository. It sends domain selection
to that repository, then matches property filters against validated canonical
strings with [`ObjectNamePropertyPattern`](../src/java/org/apache/cassandra/utils/ObjectNamePropertyPattern.java).
No property cache or second persistent name catalog is created. The matcher
keeps JMX's encoded-value, quoting, wildcard, and exact-property-count behavior.
Only returned names and names passed to caller query expressions need copies.
Remote narrow queries therefore return only matching names.

Query results, registration results, `getObjectInstance`, direct ObjectName
operation results, and registration notifications contain independent names.
Copies preserve the original property-list ordering. The builder supplies the
outer server to JDK registration and query callbacks. It forwards the original
delegate's metadata and lifecycle checks, including its deregistration rules.
Cassandra authorization remains outside this adapter and makes its normal
target/grant queries through it.

This protects Cassandra metric names against these monitoring paths. Arbitrary
MBeans can retain a name in `preRegister`, inspect their own name, or expose it
inside arbitrary application values. The adapter does not deep-copy arbitrary
object graphs or prohibit that behavior. It is not a universal prohibition on
all ObjectName caches in arbitrary in-process code. No private JDK state is
modified, and no special JDK or dependency is required by production code.

## Boundary evidence

The deliberately incomplete diagnostic query adapter demonstrates why copying
query results alone is insufficient. At 10 names, its registered-name graph
stays at 3280 B after local and real loopback Remote Method Invocation (RMI)
queries. Inspecting registration-notification names increases it to 10000 B.
An `objectName()` operation exposes the same repository identity. The production
adapter protects both cases; the graph stays at 3280 B with zero populated
registered-name caches.

The first startup probe failed because constructing Jamm's MemoryMeter before
setting the builder had already initialized the platform server. The corrected
probe sets the builder first. A separate late-install test proves that the old
platform server remains unchanged while newly created servers use the builder.

Generated tests exposed a real initial defect: replacing the delegate changed
the class name reported by `queryMBeans`. Forwarding its original DynamicMBean
metadata and registration callbacks fixed the mismatch. The failing test
remains in the generated differential suite.

## Isolated query measurements

Java 21.0.12, G1, eight available processors, 1000 fresh registered names per
case, 100 warmups followed by 500 measured queries. These are simple in-process
timings, without confidence intervals; use them as implementation evidence,
not a general throughput benchmark. Allocation counters cover the calling
thread. Serialized bytes cover the result set and exclude RMI protocol traffic.

| Mode and query | Allocated B/query | Approximate microseconds/query | Result count | Serialized B |
| --- | ---: | ---: | ---: | ---: |
| Legacy, one matching scope | 96,543 | 91 | 1 | 187 |
| Initial candidate-copy adapter, one scope | 2,611,704 | 723 | 1 | 187 |
| Canonical matcher, one scope | 52,149 | 104 | 1 | 187 |
| Legacy, no matching type | 104,304 | 78 | 0 | 53 |
| Canonical matcher, no matching type | 50,144 | 83 | 0 | 53 |
| Legacy, matching keyspace | 201,176 | 90 | 1000 | 97,981 |
| Canonical matcher, matching keyspace | 1,955,008 | 629 | 1000 | 97,981 |
| Legacy, broad domain | 97,120 | 36 | 1000 | 97,981 |
| Canonical matcher, broad domain | 1,953,769 | 515 | 1000 | 97,981 |

All cases start with a 324,976 B reachable registered-name graph. Legacy
property queries increase it to 980,976 B. Both adapter versions leave it at
324,976 B. Broad legacy discovery also leaves that graph cold until a local
caller parses returned identities. These are overlapping reachable graphs,
not disjoint JVM retained-heap measurements.

The canonical matcher avoids the initial adapter's parsing allocation for
rejected candidates. Independent result copies still make broad discovery more
expensive. Narrow query scans remain linear in the selected domain, as with
the existing JDK repository; this change does not introduce an index or make
million-table enumeration inexpensive. RMI calls were exercised, but total
wire traffic was not measured. Cassandra scrape timings below cover local
platform-server calls, not a remote monitoring deployment.

## Validation

- 16,000 generated query comparisons against the legacy server, covering
  quoted delimiters, escaped wildcard literals mixed with active wildcards,
  backslashes, Unicode, default/wildcard domains, exact property sets, no
  matches, exact lookups, queryNames, queryMBeans, and attribute expressions.
- 1600 drop/recreate names and 1000 concurrent registration/removal cycles.
- Local and real loopback RMI query, expression, attribute, operation, and
  notification checks; direct platform-server access and early/late startup.
- Cassandra AuthorizationProxy differential checks through local Subject
  calls and real authenticated loopback connectors. Cases include root,
  exact, combined, wildcard, and missing grants; superusers; disabled
  authorization; allowed/denied patterns; queryNames/queryMBeans; and reads.
- Full build and checkstyle passed with the final implementation.
- Compact gauge/counter registration preserves exact MBeanInfo, ObjectInstance,
  queryMBeans results, values, read-only behavior, operation signatures, and
  exception classes/messages. It covers null values, failing gauges, and the
  public configuration-selected registration path.
- 16 generated seeds each execute 1000 updates to both counter and gauge,
  including long overflow and changing gauge values, against legacy exports.

A custom QueryExp with a no-op setMBeanServer exposed another initial defect:
extending QueryEval changed the thread's binding even when the caller did not.
CopyingQuery now implements QueryExp directly and forwards only the caller's
binding call. The regression test passed after the fix. This matters for
in-process callers as well as standard query expressions.

The authorization fixture first failed under a launcher without Cassandra's
storage-directory test configuration. Running it with `ai-ci-test` supplied
the correct environment. Its loopback listener also initially advertised the
host address through RMI. An injected loopback client socket factory fixed
that fixture without changing global network properties or production code.

## Reproduction and artifacts

```sh
distrobox enter dev -- ./run_tests.sh --jmx-query
distrobox enter dev -- ./run_property_tests.sh --jmx-query
distrobox enter dev -- ./run_tests.sh --jmx-registration
distrobox enter dev -- ./run_property_tests.sh --jmx-registration
distrobox enter dev -- bash .build/sh/ai-probe-jmx-names --boundaries --benchmark
distrobox enter dev -- .build/sh/ai-ci-test org.apache.cassandra.auth.jmx.TransientMBeanServerAuthorizationTest
distrobox enter dev -- .build/sh/ai-build
```

- First startup failure: `logs/20260907-173929-832016806-jmx-names.log`.
- Incomplete boundary adapter: `logs/20260907-173948-177855009-jmx-names.log`.
- Late startup: `logs/20260907-174044-584176151-jmx-names.log`.
- Protected boundary: `logs/20260907-174316-930318208-jmx-names.log`.
- Delegate metadata regression: `logs/20260907-174536-445458738-jmx-names.log`.
- Initial candidate-copy cost: `logs/20260907-175622-303876942-jmx-names.log`.
- Canonical matcher generated checks: `logs/20260907-180025-326937242-jmx-names.log`.
- Canonical matcher cost: `logs/20260907-180049-874804341-jmx-names.log`.
- QueryEval regression: `logs/20260907-181722-171605270-jmx-names.log`;
  corrected generated run: `logs/20260907-181733-544802358-jmx-names.log`.
- Final build/checkstyle: `logs/20260907-182835-ai-build.log`.

## Optional registration reduction

`compact_jmx_registration_enabled: true` selects field-free DynamicMBean
subclasses for gauges and counters. The default remains false. The subclasses
inherit the existing metric getters and share their legacy metadata. They
create temporary StandardMBean adapters for attribute and operation calls.
This preserves JDK dispatch and error behavior while removing the persistent
StandardMBeanSupport adapter for these two metric types. Timers, histograms,
recording algorithms, aliases, enabled profile membership, and saved recent
history remain unchanged. The setting works independently of the builder.

The final 1000-table heap contains 22,644 dynamic gauges and 16,055 dynamic
counters. StandardMBeanSupport falls from 55,061 instances / 1,321,464 B to
16,362 instances / 392,688 B. The 38,699 removed adapters save 928,776 B,
exactly matching the reduction in the disjoint JMX retained subtrees between
the query-only and combined profiles.

A calling-thread allocation benchmark performs 100,000 warmups and 200,000
counter reads per case, over three alternating rounds. Legacy reads allocate
240 B/read; compact reads allocate 288 B/read, an increase of 48 B/read.
Legacy timings are 179, 215, and 168 ns/read; compact timings are 254, 251, and
146 ns/read. These short timings are noisy and do not establish a throughput
benefit. The purpose of this option is lower residency. Reproduce with:

```sh
distrobox enter dev -- env PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.CompactJmxRegistrationTest .build/sh/ai-profile-many-tables
```

Artifact: `logs/20260907-183113-many-tables-launch.log`.

## Cassandra heap measurements

All profiles use 1000 empty user tables, `simple_metrics.yml`, compact recording,
adaptive recent history, and the existing synthetic metric activity on eight
workers. No user TrieMemtables or user SSTables are initialized. Each full
scrape reads 259,233 attributes across 52,914 metric MBeans, including 48,031
user-keyspace MBeans and 13,443 recent-value attributes, with zero failures.

| Profile | Final whole heap B | Disjoint JMX retained B |
| --- | ---: | ---: |
| Prior full-scrape legacy baseline | 161,310,608 | 68,274,120 |
| Prior attribute-only legacy control | 125,852,920 | 33,180,968 |
| Query adapter, full scrape | 126,094,096 | 33,181,264 |
| Matched legacy, full scrape + property queries | 161,206,488 | 68,277,768 |
| Query adapter + compact registration + property queries | 125,428,392 | 32,252,488 |

The query-only run adds 296 B of persistent server/delegate overhead to the
previous cold-cache JMX control. The combined run removes another 928,776 B.
Both new profiles have zero populated metric-name property maps. The analyzer
reports only one shared empty map, 24 B; all 1000 user tables have zero property
maps. The combined run includes narrow first-table queries, the real ThreadPools
query shape, no-match queries, queryMBeans equality checks, and objectName()
result inspection after each full scrape. Thus the saving survives actual
server-side property queries and local operation results.

JMX retention sums both separate server subtrees. Intermediate proxy roots
retain 33,166,808 B and 14,456 B; final roots retain 32,238,032 B and 14,456 B.
The proxy handler type was checked through a separate Memory Analyzer Tool
(MAT) object query. These subtrees are disjoint; reachable cache graphs are
not added to their totals. The adaptive saved-history graph remains 544,080 B.

The added property workload makes the final whole heap and scrape time
unsuitable for attributing the adapter-only delta against the intermediate
run. Final full rescrape time is 1.131 seconds, compared with 0.895 seconds in
the query-only run without those extra checks. The matched legacy control
with the same property workload takes 0.851 seconds. These are single-run
local scrape measurements, not a latency distribution or remote service-level
claim.

Against the matched legacy control, the combined path saves 35,778,096 B
(34.12 MiB) of whole heap and 36,025,280 B (34.36 MiB, 52.76%) of disjoint JMX
retention. The JMX saving has an exact structural explanation:

```text
35,096,800 B removed property-cache graph
   928,776 B removed persistent registration adapters
      -296 B added server/delegate overhead
36,025,280 B net JMX retained saving
```

The matched legacy cache graph includes 228 HashMap.EntrySet objects, 3648 B,
created by inspecting objectName() results for 48 table and 180 ThreadPools
MBeans. That explains its increase over the earlier full-scrape baseline. The
control has 52,914 populated metric-name maps; the final path has none.

The following samples time queryNames only during the final rescrape. Each
query scans the actual 52,914-name metrics domain. Allocation covers the
calling thread. Follow-up queryMBeans and objectName() checks establish result
equivalence but are outside these queryNames samples.

| Property query | Matches | Legacy allocated B | Combined allocated B | Legacy ms | Combined ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| First user table | 48 | 5,049,896 | 2,844,288 | 21.79 | 28.37 |
| ThreadPools | 180 | 5,519,008 | 3,121,352 | 15.29 | 19.58 |
| Missing keyspace | 0 | 4,987,408 | 2,743,984 | 17.50 | 17.78 |

These samples show reduced narrow-query allocation and a remaining linear
scan cost. They do not establish a million-table query budget. Broad result
copies and temporary registration adapters explain why less resident memory
does not imply less allocation for every monitoring operation.

Reproduction:

```sh
distrobox enter dev -- env PROFILE_TRANSIENT_JMX=true .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 125 --metrics-config simple_metrics.yml --adaptive-jmx-history --out logs/jmx-query-intermediate-1000
distrobox enter dev -- env PROFILE_TRANSIENT_JMX=true .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 126 --metrics-config simple_metrics.yml --adaptive-jmx-history --compact-jmx-registration --property-queries --out logs/jmx-query-final-1000
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 127 --metrics-config simple_metrics.yml --adaptive-jmx-history --property-queries --out logs/jmx-query-control-1000
```

Profile artifacts contain summary.json, phase heap dumps/histograms, and scrape
JSON. Use `bash .build/sh/ai-analyze-heap-dominators HEAP` in the dev distrobox
for retained sizes. Use the following logged analyzer for bounded ownership:

```sh
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/analyze-heap-ownership.py HEAP --expected-tables 1000
```

- Intermediate directory: `logs/jmx-query-intermediate-1000/20260907-180341-heap-ownership-1000t/`.
- Intermediate MAT: `logs/20260907-180613-rescraped-dominators.zip`;
  handler check: `logs/20260907-181042-rescraped-dominators.zip`.
- Intermediate census: `logs/20260907-180727-081118-rescraped-heap-ownership.json`.
- Final directory: `logs/jmx-query-final-1000/20260907-183319-heap-ownership-1000t/`.
- Final MAT: `logs/20260907-183619-rescraped-dominators.zip`;
  handler check: `logs/20260907-183802-rescraped-dominators.zip`.
- Final census: `logs/20260907-183809-251555-rescraped-heap-ownership.json`.
- Matched control: `logs/jmx-query-control-1000/20260907-184436-heap-ownership-1000t/`.
- Control MAT: `logs/20260907-184642-rescraped-dominators.zip`.
- Control census: `logs/20260907-184749-515210-rescraped-heap-ownership.json`.

## Remaining cost and compatibility boundary

The combined path still retains 30.76 MiB in the JMX server subtrees. Registered
ObjectNames, their canonical strings and property offsets, NamedObject entries,
repository maps, metric wrappers, and the other StandardMBeanSupport objects
remain resident. This implementation reduces registration objects; it does
not eliminate registration residency. Reducing these remaining structures
requires another bounded design and measurement pass.

The builder must load before the platform server initializes and does not
compose with a third-party custom MBeanServerBuilder. Local users that rely
on object identity or concrete JDK server classes cannot assume those details
under this optional path. JMX names remain equal and retain their text order.
Arbitrary MBeans can still inspect retained names themselves or expose them
inside application values. Java 21 and the tested Cassandra authorization/RMI
paths are the measured compatibility scope; no other JDK or security-provider
deployment is claimed. The default legacy path remains available.
