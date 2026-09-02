# Collection compaction gap tests

Add four differential scenarios that close the merge holes JaCoCo showed in `CursorCompactor.mergeCells` / `anyMergedCellDeadAtNow`. No new test classes. No production changes.

## Placement

| Test | Class | Why |
|---|---|---|
| List cells across sstables | `EdgeCaseDifferentialCompactionTest` | Supported-shape merge catalog; already holds `multiCellColumnsAcrossSSTables` |
| Same-timestamp map value `COMPARE` | same | Same class already pins simple-cell timestamp ties (`expiringVsLiveTies`, `rowAndCellTtlMix`); this is the collection arm of that table |
| Map-element TTL across sstables | same | Pin `now` with `taskWithFixedNow`; do not sleep |
| Strict liveness, two sstables, one map path | `MaterializedViewDifferentialCompactionTest` | View-only `enforceStrictLiveness`; reuse `livenessFreeViewRow` / `applyViewRow` |

Do not put 1–3 on `ComplexColumnCursorReadTest` (reader only) or `CursorCellPathOrderingTest` (no compaction). Do not put 4 on EdgeCase (no views, no `requireNetwork()`).

Put the three EdgeCase methods immediately after `complexDeletionsWithRangeTombstones` so the collection merge block stays together.

## Shared conventions

- `disableAutoCompaction()` after `createTable`.
- `gc_grace_seconds = 864000` on EdgeCase tables so a converted tombstone is not purged before the merge decision.
- Assert the cursor path actually ran (`assertCursorMatchesIterator` already does this via `assertCursorPathWillRun`).
- Follow existing collection tests: first compaction uses `assertCursorMatchesIterator(cfs)` unless TTL pinning is required.
- For TTL, use `assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(pinnedNow), gcBefore)` plus a non-vacuity check on JSON. See `strictLivenessDropsARowThePurgerEmptied` and `rowAndCellTtlMix`.
- Each scenario must have a control row that would fail the opposite way if the merge skipped the branch.
- 4-space indent, existing license headers, no new files.

## Test 1 — `listCellsAcrossSSTables`

**Hole:** list paths compare as timeuuid, not bytes. `CursorCellPathOrderingTest.listPathsUseTimeUuidOrder` never sorts real cursors. Two sstables, one cell each, inverted paths: if `compareByColumnAndPath` used unsigned bytes, output cell order would disagree with the iterator and the harness byte comparison would fail.

**Why CQL prepend/append is not enough:** CQL timeuuids almost never invert time_low vs time_hi the way the unit test constructs. Prepend vs append is still worth one row as a realistic merge, but the trap needs crafted `CellPath`s.

**Setup:**

1. Table `(pk bigint, ck bigint, l list<text>, v text, PRIMARY KEY (pk, ck))`.
2. Ordinary rows via CQL: flush 1 writes `UPDATE … SET l = l + ['a','b']`; flush 2 writes `UPDATE … SET l = ['x'] + l` on overlapping `ck`. Also write `v` so the row is not collection-only.
3. One extra `ck` that CQL never touches for `l`. After a dummy INSERT of `v` (so the row exists, and so an INSERT of a list does not add a complex deletion that would shadow the crafted cells), apply two `BufferCell.live` cells on `l` via `Mutation`/`PartitionUpdate.singleRowUpdate`, one per sstable:
   - Path A / path B copied from `CursorCellPathOrderingTest.timeUuid` (`0xFFFFFFFF00001001L` vs `0x0000000000001002L`).
   - Values `"byte-high"` and `"time-high"` so JSON can say which cell is first if needed.
4. Flush after each apply so they are separate sources.

**Assertions:** `assertCursorMatchesIterator(cfs)`. Optional: `allJson` contains both crafted values (output must not drop the column). Do not assert CQL SELECT order as the primary oracle; the harness byte comparison is the oracle for path order.

**Private helper:** a small `applyListCell(metadata, pk, ck, path, value, timestamp)` next to the test, same pattern as `FarFutureDeletionDifferentialCompactionTest.applyComplexDeletion`. Do not share a helper across classes.

## Test 2 — `mapValueTiesAtSameTimestamp`

**Hole:** `mergeCells` `COMPARE` arm (equal timestamp, both live, value bytes decide). Existing EdgeCase ties are simple columns (`expiringVsLiveTies` is CASSANDRA-14592, not value compare). Collection cells copy through `tempCellBuffer` before `Arrays.compareUnsigned`.

**Setup:**

1. Table `(pk bigint, ck bigint, m map<text, text>, v text, PRIMARY KEY (pk, ck))`.
2. Flush 1: `UPDATE … USING TIMESTAMP 1000 SET m['k'] = 'aaa', v = 'keep'` for several `ck`.
3. Flush 2: `UPDATE … USING TIMESTAMP 1000 SET m['k'] = 'zzz'` on the same keys (unsigned compare: `zzz` > `aaa`).
4. Control partition: same map key with **different** timestamps (`1000` then `2000`) so a regression that ignored timestamps still has a row that can fail.
5. One row with equal values at the same timestamp (tie keeps the first source, matching `Cells.resolveRegular`).

**Assertions:** `assertCursorMatchesIterator(cfs)`. Absolute JSON: surviving value is `zzz` on the equal-timestamp rows (`cellValue("zzz")` present, `cellValue("aaa")` absent for those clustering keys). Mirror `rowAndCellTtlMix` so a both-wrong merge cannot hide behind byte equality.

Use `text` map values so the variable-length vint skip in `COMPARE` runs (`valueLengthIfFixed() < 0`). Do not use `bigint` values for the tie rows.

## Test 3 — `mapElementTtlAcrossSSTables`

**Hole:** expire-to-tombstone in `mergeCells` when a collection cell has TTL and another source holds the same path. Pathological TTLs whole INSERT windows, not one map entry vs a second sstable.

**Setup:**

1. Same table shape as test 2. `gc_grace_seconds = 864000`.
2. Flush 1: live map entry `m['k'] = 'live'` at `TIMESTAMP 1000`, plus `v` keep-column.
3. Flush 2: `UPDATE … USING TIMESTAMP 2000 AND TTL 1 SET m['k'] = 'expiring'` on the same path.
4. Control: a second `ck` where the TTL cell is **older** than a live cell (`TTL` at 1000, live at 2000) so the live cell must survive.
5. After the last flush, pin `nowInSec = FBUtilities.nowInSeconds() + 60` (past TTL 1, not past a 86400 tombstone). `gcBefore = cfs.getDefaultGcBefore(nowInSec)` or `nowInSec - gc_grace` as the other TTL tests do — pick the same formula as `strictLivenessDropsARowThePurgerEmptied` / EdgeCase TTL tests so the tombstone is **not** purged (`gc_grace` 864000 keeps it).
6. Call `assertAtLeastOneCellExpired` (or the existing precondition helper on `DifferentialCompactionTester`) after flush if the TTL tests use it.

**Assertions:** harness match with `taskWithFixedNow(pinnedNow)`. JSON: the first `ck` must not contain `live` or `expiring` as a live value (expired winner becomes a tombstone and shadows the older live cell). The control `ck` must contain the later live value.

Do not `Thread.sleep`. Do not use `TTL 1` without pinning now.

## Test 4 — `strictLivenessAccountsForMergedComplexCell`

**Hole:** `anyMergedCellDeadAtNow` never entered `for (i = 1; i < cellMergeLimit; i++)`. `strictLivenessAccountsForComplexColumnDeletion` is one sstable and short-circuits on a non-live **complex deletion**, not on a merged cell.

**Setup:** copy the view DDL from `strictLivenessAccountsForComplexColumnDeletion` (`v1` in the view PK, map `m` selected). `assertTrue(viewCfs.metadata().enforceStrictLiveness())`.

1. `execute` + `flush` one normal base insert so the view has a control row (same as the purger test).
2. **Sstable A:** `livenessFreeViewRow(2, 2)` + live `BufferCell` on `m` path `k` at timestamp 100. `applyViewRow(viewCfs, 7L, …)` + `flush(KEYSPACE, view)`.
3. **Sstable B:** same view clustering / `v1`, live complex deletion must stay **LIVE** (do not call `addComplexDeletion`). Add a **tombstone** cell on the same path `k` at timestamp 200 with `localDeletionTime = nowInSec` (reuse `BufferCell.tombstone` like `addCellTombstone`, but on the map column with `CellPath`). `applyViewRow` + flush.
4. Assert `getLiveSSTables().size() == 2` (or 3 with the control flush) before compact.
5. Control clustering `(3,3)`: two sstables, **both live** cells on `k` (timestamps 100 and 200). Row must be kept.

**Why a tombstone, not COMPARE:** `cellMergeLimit > 1` is the requirement. Timestamp win of a dead cell is enough to enter the resolve loop. Do not add a complex deletion; that takes the `complexDeletionDead` branch and skips the loop again.

**Assertions:** `assertCursorMatchesIterator(viewCfs)`. JSON: dropped row’s map value absent; control row’s value present; base `normal` row present. Pin `now`/`gcBefore` only if the tombstone would otherwise purge; with default gc grace and `ldt = nowInSec` it should survive like `addCellTombstone`.

## What not to add in this change

- All-types map/list/set matrix
- BTI subclass of these scenarios (cell body is the same; Pathological already has BTI)
- 3+ sources as a fifth test
- Tests for `IllegalStateException` arms / `pathType == null` / counter throw
- Un-`@Ignore` of `droppedComplexColumnSurvivingCells`
- Production code, including ADV-001

## Verification

JDK 17, same as this worktree:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ant test -Dtest.name=EdgeCaseDifferentialCompactionTest
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ant test -Dtest.name=MaterializedViewDifferentialCompactionTest
```

Expect: EdgeCase 40 tests (37 existing + 3), MV 8 tests (7 existing + 1), 0 failures. If a scenario is vacuous (both paths drop the row, JSON assertions still pass), add the control-row counts before merging.

## Implementation order

1. Test 2 (CQL only, smallest).
2. Test 3 (CQL + `taskWithFixedNow`).
3. Test 1 (CQL rows, then Mutation crafted paths).
4. Test 4 (copy MV helpers; add map-path tombstone helper).
