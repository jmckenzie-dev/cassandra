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

# Name-cache retention during monitoring

Measured September 7, 2026, on branch `moar_tables`. No production behavior or
configuration changed during this investigation. The added probe and runners
exercise the existing Java Management Extensions (JMX) implementation.

Subsequent implementation: [transient JMX queries and registration cost](jmx_query_export.md)
records the optional server builder, local/remote authorization checks, compact
gauge/counter registration, and fresh 1000-table measurements. The results below
remain the original diagnostic control; they do not include those options.

## Result

**33.47 MiB of the measured 65.11 MiB JMX-owned heap is avoidable property-cache
storage.** A matched 1000-table run that read the same attributes without
inspecting persistent names retained 31.64 MiB under JMX. Both runs exposed
the same metrics and passed the same metric-count checks.

The cache is Java 21's `ObjectName._propertyList`. Each registered name already
stores a canonical string and property offsets. Reading a property also creates
a `HashMap`, entries, and separate key/value strings with backing arrays.
That map stays reachable through the registered name after monitoring ends.

**Remote monitoring can also cause this retention.** A remote client that
receives names from broad discovery parses independent copies. But a query
with property filters makes the server inspect its registered names first.
Even a query that returns no matches can populate every candidate's cache.
The earlier distinction between local and remote scrapes was incomplete.

This investigation measured the opportunity before implementation. Preventing
it for one collector does not prevent another collector or administrative
command from populating the same caches. The linked implementation addresses
server-side query matching and name escape paths within its documented scope.

## Matched Cassandra measurements

Both runs used 1000 empty tables, `simple_metrics.yml`, optimized recording
histograms, adaptive JMX history, lazy TrieMemtables, and BTI SSTable format.
The Java 21 process used G1, eight available processors, and an 8 GiB heap
ceiling. Eight workers recorded the same synthetic metric observations.
No user-table writes, SSTables, or initialized TrieMemtables were present.

The existing full scraper sorts names naturally and reads their keyspace
property. The attribute-only scraper sorts canonical strings and identifies
the fixed harness keyspace from those strings. Both enumerate the same metric
domain, inspect bean metadata, and read every readable attribute. The fixed
keyspace filter in this harness is not proposed as a general ObjectName parser.

Whole-process heap, in bytes, from settled checkpoint measurements:

| Checkpoint | Full name inspection | Attribute-only | Difference |
| --- | ---: | ---: | ---: |
| Startup | 53,603,968 | 49,351,208 | 4,252,760 |
| Created | 119,539,768 | 119,396,448 | 143,320 |
| First scrape | 154,962,664 | 119,496,416 | 35,466,248 |
| One worker | 156,540,968 | 121,074,680 | 35,466,288 |
| Eight workers | 160,794,776 | 125,333,280 | 35,461,496 |
| Repeat scrape | 161,310,608 | 125,852,920 | 35,457,688 |

The final difference is 33.815 MiB. Startup differs by about 4 MiB, but that
difference does not persist through table creation. Use the heap ownership
results below to attribute the saving; do not subtract startup readings and
treat the result as exact per-table storage.

Eclipse Memory Analyzer (MAT) found these **disjoint JMX-server retained
subtrees**, including system and harness beans:

| State | Full name inspection | Attribute-only |
| --- | ---: | ---: |
| Created | 32,636,888 B | 32,636,888 B |
| Repeat scrape | 68,274,120 B | 33,180,968 B |
| Growth after creation | 35,637,232 B | 544,080 B |

The final JMX difference is **35,093,152 B = 33.47 MiB = 51.4%** of full JMX
residency. The remaining 544,080 B of post-creation JMX growth is the saved
adaptive histogram-history arrays. It is present in both runs.

Both scrapes in both modes reported:

- 52,914 Cassandra metric MBeans, including 48,031 for the user keyspace.
- 259,233 successful attribute reads, including 13,443 recent-value reads.
- Zero failed attribute reads.

The census also verified selected table counters and keyspace aggregates,
48031 user registry entries, and all 1000 clean, uninitialized user memtables.
It does not assert byte equality for every time-dependent metric attribute.

## What the cache contains

The bounded heap analyzer found 52,914 populated property maps in the full
run. The attribute-only run had none. Both heaps also contain a shared empty
map associated with a wildcard name; its 24 B are excluded from this difference.

| Object family | Objects added to property-cache graphs | Bytes |
| --- | ---: | ---: |
| Key/value byte arrays | 418,176 | 11,593,120 |
| Key/value strings | 418,176 | 10,036,224 |
| Hash-map entry nodes | 209,088 | 6,690,816 |
| Hash-map backing arrays | 52,914 | 4,233,120 |
| Hash-map objects | 52,914 | 2,539,872 |
| Total | 1,151,268 | 35,093,152 |

The reachable cache-graph difference exactly equals the independent MAT
JMX-retained difference. The graph is inside the JMX subtree; do not add
these measurements together.

Every user table has 48 metric wrappers in both modes. Full inspection adds
48 property maps, 1056 objects, and **32,120 B per table**. Attribute-only
inspection adds zero. Both modes retain 512 B of saved histogram history per
table after worker activity.

At one million tables, 32,120 B per table alone is approximately **29.9 GiB**.
This is a linear projection of a specific object graph with these names and
compressed references. It is not a node-capacity prediction. Name lengths,
enabled metrics, object alignment, and reference width can change the cost.

## Which monitoring calls populate the cache

The isolated probe creates a fresh standard JMX server and fresh names for
each case. Each name has the same four properties as a table latency metric.
The probe uses real JDK operations, Jamm graph measurements, and read-only
reflection to count the private cache fields. It does not modify JDK fields.

Each operation runs twice. The graph remains stable on the second call.
At both 10 and 1000 names, the results were:

| Operation | Persistent registered-name caches |
| --- | --- |
| `queryNames("org.apache.cassandra.metrics:*", null)` | None |
| Broad `queryMBeans`, followed by attribute reads | None |
| Canonical-string sorting, metadata, and `getAttribute` | None |
| Query for one complete, non-pattern ObjectName | None |
| `getKeyPropertyListString()` | None |
| Natural `ObjectName` sorting | Every name compared within the domain |
| `getKeyProperty("keyspace")`, including a missing-property lookup | Every inspected name |
| `getKeyPropertyList()` | Every inspected name |
| Query with `keyspace=heap_census,*` or `name=Read*,*` | Every candidate in this domain |
| Query with `scope=t000000,*`, returning only one bean | All 1000 candidates |
| Query with `keyspace=missing,*`, returning no beans | All 1000 candidates |
| Query with `type=ThreadPools,*` against this table-only domain | All 1000 candidates |
| Broad query with an attribute-value expression on `Count` | None |
| `ObjectName.getInstance(existing)`, then property inspection | Original name is reused; caches populate |
| `new ObjectName(existing.getCanonicalName())`, then inspection and attribute reads | None on original names |
| Remote broad discovery, then client-side sorting, inspection, and attribute reads | None on server names |
| Remote property-filtered discovery | Server caches populate |

For these fixed names, a populated cache adds 656 B per registered name.
`getKeyPropertyList()` adds 672 B because copying that map also initializes
its entry-set view. These are reachable name-graph increases, not total JVM
heap or transient allocation measurements.

Remote cases used a real Remote Method Invocation (RMI) JMX connector bound
to loopback. The probe checked that received names were equal to, but distinct
from, the registered objects. It read real bean metadata and `Count = 7`.
Client and server lived in one process, but the calls crossed the actual RMI
serialization boundary. The probe measured only the registered-name graph.

The remote probe does not include Cassandra's authentication and authorization
interceptor. That interceptor can make additional property queries when it
checks grants, so broad client discovery alone is not an unconditional promise
of zero server caches under every authorization configuration.

## Evidence in the current code

- [The census scraper](../test/distributed/org/apache/cassandra/distributed/test/HeapOwnershipCensusHarness.java)
  calls natural sorting and `getKeyProperty("keyspace")` in full mode.
- [MBeanWrapper](../src/java/org/apache/cassandra/utils/MBeanWrapper.java)
  forwards registration and queries to the JDK server and exposes that server
  through `getMBeanServer()`. The registry passes the same name into the metric
  wrapper and JMX registration. There is no defensive-copy boundary here.
- [NodeProbe.getJmxThreadPools](../src/java/org/apache/cassandra/tools/NodeProbe.java)
  queries `org.apache.cassandra.metrics:type=ThreadPools,*`.
  [TpStatsHolder](../src/java/org/apache/cassandra/tools/nodetool/stats/TpStatsHolder.java)
  uses it for `nodetool tpstats`. The probe reproduces this query's effect on
  table names. A complete `nodetool tpstats` command was not run in this census.
- `NodeProbe.getCFSMBeans` also uses property-filtered queries, in the separate
  `org.apache.cassandra.db` domain. That is another candidate for avoiding
  cache growth, but it does not explain the metric-domain bytes above.
- [JMXTool](../src/java/org/apache/cassandra/tools/JMXTool.java) uses a remote
  connection and broad domain queries before natural sorting. That client-side
  sort does not itself warm persistent server names.
- [AuthorizationProxy.checkPattern](../src/java/org/apache/cassandra/auth/jmx/AuthorizationProxy.java)
  expands target and grant patterns through server queries. Include this
  path in any future query-adapter compatibility tests.
- [CassandraMetricsRegistry.AbstractBean](../src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java)
  also returns its stored name through the `objectName()` operation. A server
  design must consider returned names beyond `queryNames` alone.

The installed Java 21 source archive symlink was broken, and upstream source
fetches were unavailable. The probe launcher also records `javap -c -p` output
from the installed runtime. It confirms that `getKeyProperty` enters the
synchronized `_getKeyPropertyList`, builds the whole map when absent, and
returns the requested entry. `compareTo` calls `getKeyProperty("type")` after
comparing domains. `getInstance(ObjectName)` returns an exact-class input
unchanged. This investigation relies on the installed runtime and measured
behavior, not an assumed source version.

## Remedies and the next implementation decision

A collector controlled by Cassandra can avoid this retention using broad
domain discovery and exact attribute lookups. When local code needs property
parsing, construct temporary names from canonical strings first. Remote
clients already receive copies from broad discovery. Canonical-string sorting
also avoids caches where that ordering satisfies the caller's needs.

Copying trades retained memory for transient parsing and allocation. Broad
remote discovery can return far more names than a filtered query. Do not make
every collector fetch a million-table domain without measuring the resulting
allocation, network traffic, and scrape latency.

These changes prevent future cache creation on untouched names. They do not
evict maps that an earlier caller already populated. Keeping permanent parsed
copies in another registry would move much of the memory cost elsewhere.

**Prioritize a server-side query/export design before worker counter-array
improvements.** It must avoid property matching against persistent JDK names
and avoid exposing those same names to local callers. Copying only query
results is too late when the underlying query has already populated caches.

The next bounded prototype should establish where all relevant calls can be
intercepted, preserve normal queries and returned values, and measure a
cache-free alternative to property matching. Include local exporters that
obtain the platform server directly, remote connectors, authorization checks,
aliases, `queryMBeans`, and other operations that return names. The existing
`MBeanWrapper` alone does not cover every access route. Keep the old path for
comparison. No particular replacement server or facade is selected yet.

Separately, 31.64 MiB remains under JMX even without property maps. Registration
wrappers, canonical names, property-offset arrays, and repository entries
still scale with enabled metrics and tables. A shared metric catalog with
on-demand export remains a candidate for that standing cost. Avoiding caches
does not remove registration, schema, or table runtime objects.

## Reproduction and artifacts

All completed commands below exited 0. Run the Cassandra profiles sequentially.
The existing launchers compile test support; no profile JVM ran concurrently
with another build or heap analysis.

```sh
distrobox enter dev -- ./run_tests.sh --jmx-names
distrobox enter dev -- ./run_property_tests.sh --jmx-names
distrobox enter dev -- bash .build/sh/ai-probe-jmx-names 1000
distrobox enter dev -- bash .build/sh/ai-probe-jmx-names --bytecode
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 123 --metrics-config simple_metrics.yml --adaptive-jmx-history --attributes-only --out logs/jmx-name-cache-attributes-1000
distrobox enter dev -- .build/sh/ai-profile-heap-ownership --tables 1000 --subnet 124 --metrics-config simple_metrics.yml --adaptive-jmx-history --out logs/jmx-name-cache-full-1000
```

Generated checks exercised 16000 canonical-name copies across 16 seeds,
including quoting, delimiters, wildcard literals, backslashes, newlines,
Unicode, and empty fragments. They checked equality, property values, and
absence of caches on the originals. All passed.
The isolated probe is outside the Cassandra build source tree; its launcher
compiles it directly against the existing Jamm jar. No dependency was added.

Run directories:

- Attributes: `logs/jmx-name-cache-attributes-1000/20260907-165318-heap-ownership-1000t`.
- Full: `logs/jmx-name-cache-full-1000/20260907-165720-heap-ownership-1000t`.

For either `created.hprof` or `rescraped.hprof` under these directories:

```sh
distrobox enter dev -- bash .build/sh/ai-analyze-heap-dominators PATH_TO_HEAP
uv --cache-dir tmp/uv-cache run --no-project --offline --python venv/bin/python .build/sh/analyze-heap-ownership.py PATH_TO_HEAP --expected-tables 1000
```

The bounded analyzer ran on both final heaps. Layout/calibrated bytes cover
99.22% of the attribute-only heap and 99.39% of the full heap; the remainder
uses estimated field layout. MAT removes unreachable objects and supplies the
independent retained-size result. These accounting methods are not additive.

Evidence files:

- Final 10-name checks: `logs/20260907-170059-901410106-jmx-names.log`.
- Generated checks: `logs/20260907-170808-798817381-jmx-names.log`.
- 1000-name probe: `logs/20260907-165915-080590967-jmx-names.log`.
- Installed JDK bytecode: `logs/20260907-165802-968747574-jmx-names.log`.
- MAT attributes created/final: `logs/20260907-170058-created-dominators.zip`,
  `logs/20260907-165952-rescraped-dominators.zip`.
- MAT full created/final: `logs/20260907-170127-created-dominators.zip`,
  `logs/20260907-170034-rescraped-dominators.zip`.
- Bounded attributes/full: `logs/20260907-170042-856456-rescraped-heap-ownership.json`,
  `logs/20260907-170113-985667-rescraped-heap-ownership.json`.
- Checked combined summary: `logs/20260907-170303-480118-jmx-name-summary.json`.

The first direct MAT invocation returned 126 because its script is not
executable. Running that existing script through `bash` succeeded. The first
source-archive read returned 9 because the JDK source archive was absent;
installed bytecode and executable checks supplied the implementation evidence.
No performance result depends on either failed command.
