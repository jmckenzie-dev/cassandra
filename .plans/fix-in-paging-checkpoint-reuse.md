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

# Fix stale checkpoint reuse in partition-key IN paging

Status: implemented and validated on 2026-09-06. See the
[execution report](../.debug/fix-in-paging-checkpoint-reuse.md).
Production baseline: `168966f1c9`.
The failing regression exists in the current, uncommitted test batch.
The implementation and tests remain uncommitted.

## Failure and cause

The deterministic three-replica test
[`partitionKeyInContinuesEachPartition`](../test/distributed/org/apache/cassandra/distributed/test/PagingAcrossTombstonesInteractionTest.java)
inserts clustering keys 0–4 in each of partitions 0, 1, and 2, then deletes
clustering keys 0–2 on every replica. Both tombstone thresholds are 2.
The query uses consistency ALL and fetch size 1:

```cql
SELECT pk, ck FROM t WHERE pk IN (0, 1, 2) LIMIT 7;
```

Expected rows, sorted for comparison:
`(0,3), (0,4), (1,3), (1,4), (2,3), (2,4)`.
Actual rows:
`(0,3), (0,3), (0,4), (1,3), (1,3), (1,4), (2,3)`.
The disabled-paging control passes on the same data. The failure is recorded
in `logs/run_tests_20260906T045123Z-1083278.log`, starting at line 213.

The source trace explains how the cursor moves backward:

1. [`SinglePartitionPager.nextPageReadQuery`](../src/java/org/apache/cassandra/service/pager/SinglePartitionPager.java)
   appends every fetch's command to `pageQueries`. The list survives later fetches.
2. [`MultiPartitionPager.PagersIterator.computeNext`](../src/java/org/apache/cassandra/service/pager/MultiPartitionPager.java)
   closes an empty child page, then fetches again through the same child pager
   while that pager remains unexhausted.
3. The first child fetch stops at deleted clustering key 2. The next fetch
   returns live clustering key 3.
4. [`AbstractQueryPager.Pager.onClose`](../src/java/org/apache/cassandra/service/pager/AbstractQueryPager.java)
   first records live key 3, then scans the retained commands. It finds the
   earlier checkpoint at key 2 and overwrites the continuation with key 2.
5. A subsequent fetch reads key 3 again. Each duplicate consumes the ordinary
   live-row limit. Old checkpoints can also prevent detection of exhaustion.

This is a source-backed diagnosis. A direct pager regression followed by the
proposed fix will verify the precise cursor transitions.

## Proposed production change

Reset `pageQueries` at the start of each
`SinglePartitionPager.nextPageReadQuery` call. Retain the existing lazy
allocation; clear the existing list before adding the new command:

```java
if (pageQueries == null)
    pageQueries = new ArrayList<>();
else
    pageQueries.clear();
```

Keep `setTombstonePageCheckpointOwner()` and registration of the new command.
Update the field and consumption comments in `AbstractQueryPager` to state
that tracked commands belong to the current fetch. Checkpoint information
must remain readable until that fetch's iterator closes, because execution
and iteration produce the final checkpoint.

This method is the common query-creation path for `fetchPage`,
`fetchPageInternal`, and `fetchPageUnfiltered`. The only writer of
`pageQueries` is `SinglePartitionPager`; `PartitionRangeQueryPager` does not
register commands. `MultiPartitionPager` closes the prior child iterator
before making the next fetch, so clearing the list here preserves the prior
fetch's close-time accounting. Keep this sequential lifecycle in the tests:
fully consume and close the row and partition iterators before fetching or
serializing state. This change does not establish support for overlapping
fetches through the same pager.

The production behavior change is two lines in `SinglePartitionPager`.
The existing LIMIT counters then count each returned row once because the
cursor no longer rewinds. No separate LIMIT-counter adjustment is indicated
by this trace. If the regression remains after this change, investigate the
remaining failure before expanding the patch.

## Required invariants

- Only the current fetch can supply its tombstone continuation or keep it
  unexhausted. A prior fetch cannot override a later live-row cursor.
- Within a partition, a continuing read advances in the query's clustering
  direction. It may advance through an empty page. A terminal empty read may
  instead mark the pager exhausted; its cursor need not advance.
- On a fixed dataset, the concatenated results contain each selected row once
  and include the tail when the query's limits permit it.
- Empty checkpoints in filtered CQL reads consume no live-row LIMIT allowance.
  A completed read without a current checkpoint can exhaust the partition.
  Unfiltered reads expose tombstoned rows and use different accounting, as
  specified below.
- Short and empty pages remain valid. Tests must not require a page to fill
  its fetch size or treat an empty page as proof of completion.

## Implementation and tests

1. Add a direct filtered pager regression in
   [`QueryPagerTest`](../test/unit/org/apache/cassandra/service/QueryPagerTest.java).
   Use real rows and deletions. Set the thresholds before constructing the
   pager, which snapshots its paging threshold, and restore settings in
   `finally`. Call `fetchPageInternal` with size 1 on the same single-partition
   pager across an empty checkpoint, live key 3, live key 4, and exhaustion.
   Use a query limit above the two live rows so completion must come from
   storage exhaustion. After each closed iterator, assert the remaining
   live-row allowance; inspect or serialize the saved clustering only while
   the pager is not exhausted. `QueryPager.state()` is undefined after
   exhaustion. Repeat with consecutive checkpoints and reverse traversal.
   Compare continued use with reconstruction from serialized paging state at
   nonterminal boundaries. Confirm the focused regression fails before
   changing production code.
2. Apply the tracking reset and comment changes. Run the direct regression
   and the retained three-node IN regression. Require the exact six expected
   rows with `LIMIT 7`.
3. Extend IN coverage in
   [`PagingAcrossTombstonesInteractionTest`](../test/distributed/org/apache/cassandra/distributed/test/PagingAcrossTombstonesInteractionTest.java).
   Use a bounded set of cases: fetch sizes 1 and 2; LIMIT below, equal to,
   and above the number of live rows (for the six-row fixture, 3, 6, and 7);
   no LIMIT with a bounded completion
   check; multiple checkpoint stretches; an all-deleted partition followed
   by a live partition; and `PER PARTITION LIMIT` combined with LIMIT.
   For every paged CQL case, require `0 < fetchSize < effective query LIMIT`,
   or use no LIMIT. `SelectStatement.execute` bypasses the pager when
   `query.limits().count() <= pageSize`. Cases at or above that boundary are
   separate non-paged controls and must not require resumable checkpoints.
   The existing distributed fuzzer already applies this restriction.
   Run representative cases in memory and after flush. Use a descending
   clustering-order table for reverse IN coverage, without introducing a
   separate CQL `IN` plus `ORDER BY` restriction. Include a static-column case
   and one divergent-replica case that exercises full-data reconciliation.
   Verify checkpoint activity and, for the divergent case, reconciliation.
4. Add a bounded multi-partition property to
   [`PagingAcrossTombstonesFuzzTest`](../test/distributed/org/apache/cassandra/distributed/test/PagingAcrossTombstonesFuzzTest.java).
   Use at most 12 fixtures per seed, 2–4 partition keys, and at most 12
   clustering keys per partition. Generate deleted prefixes and gaps,
   all-deleted partitions, fetch sizes, and limits. Include fixed checkpoint
   cases in each seed rather than relying on random threshold crossings.
   Apply the paged-query eligibility rule from step 3 before running a query
   or claiming paging coverage. Initially use identical replica data and
   explicit increasing mutation timestamps to isolate pager lifecycle behavior.
   Compute live membership independently from inserts and row deletions.
   Validate the unlimited disabled-paging control against that model before
   testing enabled paging. For LIMIT truncation, use the disabled control
   for partition traversal order; do not assume IN arguments define order.
   Preserve duplicates when comparing results. Use a maximum row/page budget
   based on the fixture and finite client request timeouts, including for
   unlimited and all-deleted queries. Record seed, mutations, query, and
   page states on failure. Use the existing property-test entrypoint and run
   seeds 18424 and 1.
5. Add a separate unfiltered pager regression in `QueryPagerTest`.
   `AbstractQueryPager.Pager` creates its counter with `assumeLiveData=true`,
   and `UnfilteredPager` sends tombstoned rows through that counter.
   `DataLimits.CQLCounter.applyToRow` therefore counts those rows. The real
   caller, `SecondaryIndexManager.indexPartition`, consumes unfiltered data
   for index construction. Do not apply the filtered empty-page or zero
   live-row-decrement assertions to this API.
   With the same deleted 0–2/live 3–4 fixture, use `DataLimits.NONE` and an
   unfiltered page size of 8 so the outer counter does not stop iteration
   before the third tombstone. Expect the first page to contain deleted
   rows 0–2 and remain unexhausted with continuation 2. Expect the next page
   to contain live rows 3–4 and exhaust the pager. Assert exact unfiltered
   row identities and deletion state, then completion, without reading
   undefined terminal paging state. Keep existing unfiltered accounting;
   any separate accounting defect needs its own reproduction and scope.

The direct cursor test and retained three-node test must detect removal of
the tracking reset. Test output must distinguish a failed model control from
an enabled-paging failure. No mocks of the pager or injected checkpoints are
needed.

## Validation and completion

Build with Java 17 and the existing Ant helpers. Finish the build before
starting test JVMs. Run the focused new tests, `QueryPagerTest`,
`PagingAcrossTombstonesTest`, `PagingAcrossTombstonesDistributedTest`, and the
existing plus extended distributed fuzzer. Run the interaction methods
separately from the known failing GROUP BY case so that its asynchronous
error does not obscure the IN result. Preserve that GROUP BY regression and
report it separately.

Collect isolated JaCoCo profiles through `run_tests.sh coverage` and the
existing selected-method Ant harness. Confirm execution of the reused-list
path and checkpoint, live-row, and exhaustion paths in `Pager.onClose`.
Report changed-line and branch coverage with test outcomes; coverage alone
does not establish cursor correctness. Run targeted Checkstyle and
`git diff --check`. Save dated logs under `logs/` with exit codes preserved.

After successful implementation and validation, update the coverage review
and `.build/README.md`, then move the IN item in `TODO.md` to DONE. Keep this
fix separate from the GROUP BY grouping-state defect and mixed-version
artifact work. Replica digest comparison and wire continuations retain
their current behavior.
