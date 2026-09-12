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

# Isolated export measurements

Revision: b8efde8316. Java 21.0.12.1, G1, eight active processors.
No competing build, test, or benchmark ran. Jars and test classes stayed frozen.
These short local measurements are diagnostics, not production capacity claims.

## Adaptive history

Command: PROFILE_SKIP_BUILD=true MANY_TABLES_XMX=1g
.build/sh/ai-benchmark-jmx-history --iterations 20000 --warmup-rounds 5 --rounds 9

Exit 0. Log: logs/20260911-224728-873-adaptive-jmx-history-benchmark.log.
All 21 path/case pairs ran (helper/histogram/timer, seven histories).
The program checks full deltas and array independence outside timing.
It alternates implementation order and retains all samples.

For byte-width history, histogram median latency was 202.879 ns legacy versus
256.056 ns adaptive; timer median was 202.717 versus 270.906 ns.
Both allocated 2816 bytes per scrape. Widen/reset allocated 2816 versus 2962.
Thus this optimization does not reduce steady scrape allocation in this fixture.
It trades history retention for packing work. Whole-node retained history remains
part of the integration census; array-width correctness is covered by unit tests.

## Registration reads

Command: PROFILE_SKIP_BUILD=true MANY_TABLES_XMX=1g
PROFILE_MAIN_CLASS=org.apache.cassandra.metrics.CompactJmxRegistrationTest
.build/sh/ai-profile-many-tables

Exit 0. Log: logs/20260911-224826-many-tables-launch.log.
Three rounds of 200000 reads, alternating order:
legacy 138/136/144 ns per read, compact 147/150/137.
Calling-thread allocation: legacy 240, compact 288 bytes per read.
No confidence interval is claimed.

## Retained names and query cost

Commands: .build/sh/ai-probe-jmx-names 1000
and .build/sh/ai-probe-jmx-names --boundaries --benchmark

Both exited 0. Logs:
logs/20260911-224753-155409000-jmx-names.log
logs/20260911-224806-957034000-jmx-names.log

The legacy 1000-name retention probe passed all local/remote cases.
Property inspection added 656000 bytes to the observed registered-name graph;
copy-based and canonical-only inspection left the graph unchanged.

The paired query benchmark kept protected registered-name graphs at 324976 bytes.
Legacy property queries warmed the graph to 980976 bytes. Result counts and
serialized sizes matched. Broad protected queries allocated roughly 1.95 MB per
query versus 0.10-0.20 MB legacy, and took about 0.43-0.50 ms versus 0.03-0.06 ms.
Narrow protected queries allocated less but still took longer in this sample.
This cost is why the builder stays opt-in. Serialized sizes exclude RMI traffic.

## Independent extraction audit

The MEM worker verified all owned source blobs, shared export hunks, default-off
controls, and legacy-storage fixture adapters. No missing production change or
sibling feature dependency was found. Branch-local selectors are intentional
subsets; integration will restore the combined selectors and moved assertions.
