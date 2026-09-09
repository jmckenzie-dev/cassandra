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

# JMX query/export delegation

On September 7, 2026, the user requested a subagent to execute the first TODO
with focused context. Agent `/root/jmx_query_export` received a standalone
brief with no inherited conversation history.

## Assigned work

Design, implement where viable, and measure an optional Java Management
Extensions (JMX) query/export path that avoids persistent ObjectName caches.
Preserve property queries, local and remote clients, authorization, metric
names, aliases, values, and lifecycle behavior. Address the remaining
registration residency as a distinct measured part of the TODO.

The worker owns relevant production/configuration changes, focused tests,
profiling fixtures, its implementation plan, results, and TODO updates.
The parent owns this handoff note and coordinates review. The worker must
preserve all existing changes and report intended production paths first.
No staging or commits are authorized in this task.

## Evidence supplied

- [Monitoring investigation](../research/jmx_monitoring_name_retention.md):
  33.47 MiB of 65.11 MiB JMX residency is name-property cache storage.
  Matched 1000-table runs preserve metric and attribute counts.
- [Million-table checkpoint](../research/9_7_checkpoint.md): resident memory
  takes priority over allocation; actual table-count runs remain capped at 1000.
- [Probe](../.build/benchmarks/jmx-names/README.md): actual local/remote JMX
  operations and generated canonical-name copy checks.
- Existing runtime metric profiles and adaptive-history reports for affected
  configuration and compatibility details.

The brief highlights that property-filtered queries populate server caches
before returning results, even with zero matches. Result copying alone does
not fix this. Raw platform-server access, authorization pattern expansion,
and other operations returning names require explicit treatment.

## Acceptance and coordination

Keep the legacy path available. Record baseline, intermediate, and final
measurements with reproducible commands. Separate retained memory from query
cost, allocation, and remote enumeration volume. Use focused equivalence and
lifecycle tests. Do not describe a collector-only mitigation as a server-wide
guarantee or count persistent copies elsewhere as a saving.

The worker received the project environment and safety constraints: Java 21
through distrobox, existing ai-* wrappers, no dependencies or full suite,
project-local temporary files, dated console/file logs, and no concurrent
builds during measurement JVM runs. Initial HEAD is `4192a00e1f`; the existing
dirty tree is the baseline.

The worker will send an initial design/ownership update, progress evidence,
and a final implementation/measurement report. Genuine architectural blockers
must be reported with evidence. Partial cache work must not mark the entire
registration TODO complete.
