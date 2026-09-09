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

# Idle flushing for TrieMemtable and UCS

Implement a startup-configured, disabled-by-default idle flush policy for user
tables using lazy TrieMemtables and UnifiedCompactionStrategy (UCS). A timeout
marks eligibility; bounded admission may delay a flush. Preserve existing
write ordering, index coordination, failure handling, and reader reclamation.
Reads do not refresh the timeout. Other memtables and compaction strategies
retain their existing behavior.

## Sequence

1. Extend the existing residency harness for UCS options, append/overwrite
   traces, per-cycle settled checkpoints, and compaction history. Capture
   baseline measurements before production changes. Use the same metric
   profile and JVM settings across each comparison; keep tables <= 1000.
2. Add an idle timeout (zero disables) and maximum outstanding idle flushes
   to cassandra.yaml. Track last write per allocated trie shard under its
   existing lock. Register an initialized memtable once with a shared
   controller; do not create per-table timers or update queues on each write.
3. Recheck eligibility and generation under the normal table lifecycle lock,
   submit asynchronous flushes, and unregister switched/dropped generations.
   Scan active candidates in bounded batches. Stop admission before flush
   executor shutdown. Keep the disabled path free of scheduling and clock reads.
4. Test timeout boundaries, read-only access, renewed writes, schema/scope
   exclusions, generation races, bounded admission, flush failure, drop,
   reclamation, and reactivation. Use injectable clocks/flush submission in
   controller tests and a real scheduled distributed test for startup wiring.
5. Compare idle flushing off/on with UCS, then compare existing UCS settings
   for bottom-level tiering and reduced small-file sharding. Measure appends
   and overwrites over repeated cycles. Record heap, file counts/sizes,
   compaction bytes/jobs, and read/write latency. Run a real 30-second timeout
   smoke test; identify shorter experimental timeouts explicitly.
6. Add a constrained small-file compaction policy only if measured UCS behavior
   demonstrates a gap that configuration cannot reasonably address. Do not
   introduce another strategy or a competing compactor speculatively.
7. Run targeted lifecycle/property/integration tests and Checkstyle. Report
   pre/intermediate/final measurements, limitations, and any remaining work
   in research/ucs_idle_flush.md and summarize findings in the conversation.

## Acceptance

Idle dirty eligible tables eventually flush and reclaim their old trie storage;
reads retain dormancy and later writes restore storage without data loss.
Empty tables have no controller entries. Stale candidates cannot switch newer
memtables. Admission is bounded across tables and failures do not create tight
retry loops. The default configuration performs no idle flushing. Comparisons
account for remaining SSTable and compaction-history residency rather than
claiming all flushed memory disappears. No Accord changes or remote pushes.

## Outcome

Implemented the disabled-by-default policy and reusable comparisons. All 58
targeted tests pass, including scheduled index flushing and CQL/JMX transitions.
The real 30-second smoke test passes. The corrected 1,000-table pair reduces
settled heap by 87.35%, but draining takes another 32–42 seconds after eligibility.
The report records initial failures, fixes, controls, and final measurements.

Existing UCS submits compaction promptly. T8 lowers rewrite work while retaining
more files; T4,L10 does not change tiny-file behavior. No additional compactor was
justified by these results. A smaller optional hierarchy base is the next bounded
experiment, alongside a census of post-retirement heap. See
[the report](../research/ucs_idle_flush.md) and the remaining TODO entries.
