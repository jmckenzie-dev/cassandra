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

# Stock versus optimized Cassandra at 5000 tables

Compare upstream 4c79cf739161985b6e066f40303154b9cd2783c1 with the current
working branch and its optimized simple-metrics configuration. The user's
request for 5000 tables supersedes the previous 1000-table limit for these runs.

1. Export stock source into tmp/stock-5000 and build through ai-build. Reuse
   already available dependencies, including the unchanged Accord revision.
   Preserve the working tree and avoid changes to stock production sources.
2. Add a stock mode to the existing heap census harness. Stock mode omits
   branch-only configuration and lazy-memtable assertions. Keep the same
   CREATE TABLE statements, worker updates, scrapes, and heap checkpoints.
   Raise the census limit to 5000 and validate its argument boundaries.
3. Use a production JAR first on the classpath, with test classes and dependency
   JARs but no fallback production classes directory. Record the loaded
   ColumnFamilyStore code source. Run a small stock smoke check before scaling.
4. Run stock then optimized sequentially at 5000 user tables. Hold Java 21,
   G1, processor count, heap ceiling, BTI, TrieMemtable selection, and workload
   constant. Do not build or analyze heaps while measurement JVMs are active.
5. Compare settled heap before scraping and after eight workers and a second
   scrape. Report exact table counts, registration counts, aggregate checks,
   failures, elapsed CREATE time, and limitations. Attribute large changes
   using class histograms and retain heap dumps for follow-up work.

This is a comparison of complete configurations, including reduced metric
exposure and eager versus lazy TrieMemtables. It does not isolate the alias
flag or prove query throughput, sustained data residency, or million-table
capacity. Do not commit unless requested.
