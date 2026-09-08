<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Overarching plan for CASSANDRA-18424 tombstone-paging branch (inferred)

Synthesized from the 12 `.plans/` artifacts and the six branch commit messages
(`1ff90eee47^..HEAD`). This is the source-of-truth intent statement for the
review of the full branch diff.

## Feature objective

Replace `TombstoneOverwhelmingException` failure on tombstone-heavy paged reads
with graceful paging. Commit `1ff90eee47` implements the core: a replica that
hits the tombstone paging threshold mid-partition stops at a *tombstone page
checkpoint* and emits continuation metadata instead of throwing. The coordinator
adopts the checkpoint as a paging cursor and resumes the read, so a LIMIT-bounded
paged read completes rather than failing. A separate paging threshold (default
100000) is independent of the tombstone failure cutoff; the failure cutoff still
applies to unpaged reads (`4decacfc0d`).

## Architecture of the change

- **Replica side** (`ReadCommand`, `ReadQuery`, `SinglePartitionReadCommand`,
  `StoppingTransformation`, `ReadCommandVerbHandler`): execution stops at the
  paging threshold and records the checkpoint (last clustering row + tombstone
  count). Local execution state (`tombstoneLimitedRow`/`tombstoneLimitedCount`)
  is distinct from transmitted page-checkpoint metadata.
- **Wire** (`ParamType`, `TombstonePageContinuation`): new schema-independent
  clustering-continuation message parameter plus the compatibility
  `TOMBSTONE_FAIL` parameter. Every remote checkpoint also carries
  `TOMBSTONE_FAIL` so old coordinators, which skip the unknown parameter, fail
  the partial response safely. Unknown parameters remain skippable.
- **Coordinator side** (`ReadCallback`, `DigestResolver`, `DataResolver`,
  `AbstractReadExecutor`, `ShortReadRowsProtection`,
  `ShortReadPartitionsProtection`, `AbstractReadRepair`): supported checkpoint
  reconciliation is limited to a finite, non-top-k, single-partition read with
  read quorum > 1. Initial digest reads compare digest *and* continuation
  (`7ec7490bc`); agreement accepts the data replica's page, disagreement
  reconciles full data, genuine failures are preserved. An
  `ACCEPT`/`REJECT` policy replaces the ambiguous `USE_LOCAL_COMMAND`
  (`4decacfc0d`): `ACCEPT` strips only the local `TOMBSTONE_FAIL` copy from
  non-digest checkpointed data responses.
- **Pager side** (`AbstractQueryPager`, `SinglePartitionPager`,
  `MultiPartitionPager`, `AggregationQueryPager`): checkpoint adoption with
  explicit ownership. The pager marks the command it created; ownership must
  transfer before full-data read repair, and repair uses copied commands so a
  late local response cannot restore a stale cursor (`08e09677b`). Tracked
  `pageQueries` are reset per fetch so a prior fetch cannot overwrite a newer
  live-row cursor (`0b3a07c55`).
- **Config** (`Config`, `DatabaseDescriptor`, `cassandra.yaml`,
  `StorageService` accessors): separate paging vs failure thresholds,
  documented as independent.

## Core invariants the patch must preserve

1. A paged read with a checkpoint is never reported as complete; a failed read
   keeps its typed failure (`ReadFailureException`, reason
   `READ_TOO_MANY_TOMBSTONES`) and never returns partial results as complete.
2. Only the current fetch may supply or keep its tombstone continuation alive;
   a stale checkpoint cannot rewind the cursor or block exhaustion detection.
3. Empty pages with a continuation are valid; tests and production must not
   treat an empty page as completion. Empty checkpoints consume no live-row
   LIMIT allowance in filtered reads.
4. Static-only partitions: suppress repeated static output only when a prior
   page actually returned data from that partition; do not complete an
   unfinished group at a checkpoint; do not transfer another partition's
   grouping state to an IN child after an empty checkpoint.
5. Checkpoint-driven retries must make clustering progress; without an anchor,
   fail safely (`STATIC_CLUSTERING` exception) rather than invent a cursor.
6. Unsupported paths (digest response, CL ONE, unlimited, top-k, non-owned
   command, mixed-version peer) fail safe — never accept an incomplete response.
7. Old coordinators stay safe across mixed versions: keep `TOMBSTONE_FAIL` on
   every remote checkpoint; do not unify checkpoint eligibility predicates —
   `DataResolver`, `ShortReadPartitionsProtection`, and
   `AbstractReadExecutor` have deliberately different contracts.

## Delivery history (each commit's contribution)

1. `1ff90eee47` — core graceful paging across tombstones.
2. `08e09677bf` — short-read continuation across checkpoints; checkpoint
   metadata through message parameters; pager ownership before read repair.
3. `5ec2c3bfb` — LLM supporting infra (build/test wrappers, docs, plans).
4. `7ec7490bc` — replica digest+continuation comparison; continuation message
   parameter; static-only empty-page vs exhaustion distinction.
5. `0b3a07c55` — stale-checkpoint reuse fixes (per-fetch `pageQueries` reset,
   group state at page boundaries, IN-child isolation, short-read refill
   exhaustion, typed read failure without resumable cursor, read-only heap
   buffer copy fix in `FastByteOperations`).
6. `4decacfc0d` — honor the independent paging threshold (default 100000),
   safe rejection of unsupported initial/speculative checkpoints, explicit
   checkpoint opt-in for local reads and replica handlers, typed read-failure
   reporting, bounded fuzzing and benchmark.

## Testing and validation claims to verify

- Bounded deterministic regressions plus model-checked fuzzing (seeds 18424, 1)
  across configurations and one-/two-upgraded-node mixed-version stages;
  disabled-paging controls validated against an independent live-row model.
- Claimed: 120 executions post-audit, then 78 unit + 35 distributed passing;
  13 distributed regressions + 372 existing cases + 2,160 fuzz queries at
  `7ec7490bc`; cumulative pager coverage 96.37% lines / 93.12% branches at
  `0b3a07c55`. Six unit cases intentionally skipped.
- Test rules: finite traversal, no sleeps, no bytecode injection, no pager
  mocks, no fabricated internal state to reach branches; `0 < fetchSize <
  effective LIMIT` for paged CQL cases because `SelectStatement.execute`
  bypasses the pager at or below that boundary.
- Performance: bounded read-cost benchmark (enabled/disabled, matched
  checkpoints, full-data reconciliation) with in-JVM timing caveats stated.

## Deliberate non-goals / declined remediations

No CQL grammar change, no wire cursor beyond the continuation parameter, no
version negotiation, no new dependencies, no shared checkpoint-eligibility
helper, no removal of `TOMBSTONE_FAIL`, no guessed resumption for the
missing-anchor case, no duplicate CHANGES.txt entry, no full-suite runs,
pager snapshot semantics not promised against concurrent writes.
