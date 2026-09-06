<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Compare tombstone page continuations

## Goal

For a finite single-partition read, accept the data replica's result when the
participating full replicas agree on both digest and continuation. An empty page
with a continuation is valid. Reconcile full data when either differs. Preserve
ordinary failures and safe rejection where no continuation consumer exists.

## Implementation

1. Add a schema-independent clustering continuation message parameter with a new
   ID. Keep the existing integer checkpoint count and failure parameter for
   compatibility. Presence means the replica stopped early; absence means the
   ordinary command boundary. The partition and direction come from the request.
2. Let eligible initial callbacks accept checkpoint responses for comparison or
   full-data reconciliation. Do not suppress a failure without checkpoint metadata.
3. Compare continuation status and clustering as well as digests. Do not compare
   tombstone counts as positions. Missing cursor metadata from older checkpoint
   responses requires full-data reconciliation when supported.
4. Adopt the agreed data response's continuation into the outer pager. Dispatch
   local reads on copies so a late response cannot overwrite the chosen cursor.
5. Use existing full-data reconciliation for mismatches and for coordinators whose
   pager cannot adopt a continuation, including a disabled coordinator in a mixed
   configuration. Preserve the local ONE path and transient reconciliation.
6. Suppress static-only rows on incomplete empty pages. Only suppress static data
   at a later exhausted page if a prior page actually returned data from that
   partition. The new static regression exposed both sides of this distinction.

## Tests

- Preserve the five-row memtable and flushed reproductions; assert empty initial
  pages and no full-data fallback when digest and continuation agree.
- Exercise reverse order, remote data responses, repeated checkpoints, static
  data, range tombstones, and QUORUM as well as ALL where applicable.
- Test equal cursor/different digest, different cursor/equal digest, checkpoint
  versus normal completion, missing cursor metadata, and genuine failure markers.
- Verify mixed thresholds/configurations, stale rows, read repair writes, and
  checkpoint ownership through existing regressions and the distributed fuzzer.
- Round-trip continuation serialization with empty/null clustering components.

## Validation

Use Java 17 through the existing dev wrapper. Build with Ant, run the focused
distributed regression first, then the corrected fuzzer and relevant read/pager
unit and distributed tests. Run targeted JaCoCo and Checkstyle. Record commands,
exit status, results, and limits in the debug notes. Do not commit.

## Scope

Compare current changes with HEAD 4e454fb560. Production paths: ReadCommand,
ReadCommandVerbHandler, ParamType, the continuation serializer, ReadCallback,
DigestResolver, and AbstractReadExecutor. AbstractQueryPager also changes to
distinguish cursor progress from previously returned data for static-only
partitions. Test authoring follows the integration skill for these interactions.
Existing unrelated worktree edits remain intact.
