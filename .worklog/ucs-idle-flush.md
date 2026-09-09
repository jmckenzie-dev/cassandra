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

# Idle-flush investigation lessons

- Compile changed tests explicitly before running `ai-ci-test`. An existing test
  JAR can cause the wrapper to run stale classes. Confirm the case count in XML.
- Do not run any build wrapper while an in-JVM benchmark uses the shared JAR.
  Rewriting it caused ClassNotFoundException in the first 30-second smoke run
  (`logs/20260909-162349-ucs-idle-smoke/`). Discard that run and rerun sequentially.
- A replacement memtable can initialize before its metadata reference publishes
  a schema update. The active compaction manager already has the new parameters.
- JMX compaction overrides bypass ColumnFamilyStore's schema reload path. Test
  both paths when a lifecycle policy depends on the compaction strategy.
- Flush completion does not establish reader reclamation. Hold admission permits
  until both have completed, and wake admission on either event.
- Queue emptiness before the idle deadline does not prove retirement. Benchmark
  checkpoints must also wait for dirty memtables to leave memory.
- The in-JVM cluster sets a 10 MiB memtable pool independently of `-Xmx`.
  The first 1,000-table disabled control performed ordinary pressure flushes.
  Set `--memtable-heap-mib 256` on both sides of the larger comparison and check
  the effective pool limit and flush reasons before interpreting the result.
