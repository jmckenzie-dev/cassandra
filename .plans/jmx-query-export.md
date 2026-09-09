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

# JMX query/export implementation boundary

Preserve the current dirty working tree and all metric behavior. Keep every
table-count experiment at or below 1000. Do not stage or commit changes.

## Baseline

The matched current-tree 1000-table profiles in
`research/jmx_monitoring_name_retention.md` establish 35,093,152 bytes of
property-cache graphs and 33,180,968 bytes of remaining JMX retention after
scraping with adaptive history. No production changes precede this plan.

## Candidate

The public `javax.management.builder.initial` startup hook can return a server
that `ManagementFactory.getPlatformMBeanServer()` and remote connectors share.
A query adapter must apply property patterns to temporary independent names,
not delegate those patterns to the underlying repository. Domain selection can
remain in the repository. Return only matching names over remote connections.
Do not keep a second name catalog merely to move caches elsewhere.

Before adopting that approach, use the actual Java 21 runtime to check:

1. Builder startup timing, including an already initialized platform server.
2. Names passed to query expressions and the server passed to their callbacks.
3. Registration return values, `getObjectInstance`, delegate notifications,
   registration callbacks, and metric `objectName()` operations.
4. Authorization target/grant expansion and existing query semantics, including
   default domains, quoting, expressions, and exact lookup.

The implemented boundary uses a builder under `utils`, a commented startup JVM
option, and copies of direct ObjectName results and delegate notifications.
The delegate forwards original metadata and lifecycle checks. The property
matcher scans validated canonical strings; copying every rejected candidate
proved too expensive. Caller expressions receive copies and retain control of
their own QueryEval binding. A YAML switch loaded after platform-server
creation cannot retrofit that server.

## Validation and next step

Use isolated unit/property entry points and real loopback remote calls. Measure
name residency, transient allocation, query latency, and serialized results
separately. Then run matched Cassandra profiles if production integration is
viable. No build or heap analysis may overlap a measurement JVM.

The query-only 1000-table intermediate profile completed successfully. Final
settled heap was 126,094,096 B, with zero populated metric-name property maps,
unchanged MBean/attribute counts, and zero attribute failures. The two server
proxy subtrees retained 33,181,264 B, including 296 B of new adapter overhead
versus the prior cold-cache control.

Registration reduction now targets the JDK's per-registration adapter around
gauges and counters. Optional field-free DynamicMBean subclasses inherit the
current getters and share legacy metadata. They use temporary StandardMBean
adapters for calls, preserving JDK dispatch/error behavior at an allocation
cost. Keep the setting off by default. Leave timers and histograms unchanged.
The final matched 1000-table runs passed. Legacy JMX retains 68,277,768 B;
both options retain 32,252,488 B. The 36,025,280 B saving is exactly removed
property caches plus 928,776 B of removed adapters minus 296 B of boundary
overhead. Counter reads add 48 B of transient allocation. Local/RMI authorization,
generated query/lifecycle checks, compact export equivalence, harness parsing,
build, and checkstyle passed. See research/jmx_query_export.md for artifacts.

This completes the cache path and a measured registration reduction. The next
TODO keeps the remaining 30.76 MiB registration/export floor explicit; it is
not a claim that JMX registration residency or million-table capacity is solved.
