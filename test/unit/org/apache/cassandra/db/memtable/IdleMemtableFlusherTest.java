/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class IdleMemtableFlusherTest extends CQLTester
{
    private DurationSpec.IntMillisecondsBound previous;
    private final List<IdleMemtableFlusher> controllers = new ArrayList<>();
    private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(30);

    @Before
    public void enableTracking()
    {
        previous = DatabaseDescriptor.getRawConfig().memtable_idle_timeout;
        DatabaseDescriptor.getRawConfig().memtable_idle_timeout = new DurationSpec.IntMillisecondsBound("1d");
    }

    @After
    public void restore()
    {
        controllers.forEach(IdleMemtableFlusher::close);
        DatabaseDescriptor.getRawConfig().memtable_idle_timeout = previous;
    }

    @Test
    public void timeoutReadsAndReactivation() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        TrieMemtable empty = current(cfs);
        assertFalse(empty.isInitialized());
        assertFalse(empty.isIdle(Long.MAX_VALUE, TIMEOUT));
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        long written = empty.lastWriteNanos();
        assertFalse(empty.isIdle(written + TIMEOUT - 1, TIMEOUT));
        assertTrue(empty.isIdle(written + TIMEOUT, TIMEOUT));
        assertRows(execute("SELECT * FROM %s"), row(1, 10));
        assertEquals(written, empty.lastWriteNanos());

        AtomicLong now = new AtomicLong(written + TIMEOUT - 1);
        IdleMemtableFlusher flusher = controller(now, 1);
        flusher.add(empty);
        flusher.scan();
        assertSame(empty, current(cfs));
        now.incrementAndGet();
        flusher.scan();
        awaitReclaimed(empty);
        flusher.scan();
        assertEquals(0, flusher.flushingCount());
        TrieMemtable replacement = current(cfs);
        assertNotSame(empty, replacement);
        assertFalse(replacement.isInitialized());
        assertRows(execute("SELECT * FROM %s"), row(1, 10));
        assertFalse(replacement.isInitialized());
        execute("INSERT INTO %s (pk, v) VALUES (2, 20)");
        assertTrue(replacement.isInitialized());
        assertRows(execute("SELECT * FROM %s WHERE pk = 2"), row(2, 20));
    }

    @Test
    public void newWriteAndOldGenerationPreventStaleFlush() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable old = current(cfs);
        long staleNow = old.lastWriteNanos() + TIMEOUT;
        execute("UPDATE %s SET v = 20 WHERE pk = 1");
        assertNull(cfs.flushIdleMemtable(old, staleNow, TIMEOUT));
        assertSame(old, current(cfs));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        TrieMemtable replacement = current(cfs);
        execute("INSERT INTO %s (pk, v) VALUES (2, 30)");
        assertNull(cfs.flushIdleMemtable(old, old.lastWriteNanos() + TIMEOUT, TIMEOUT));
        assertSame(replacement, current(cfs));
        assertRows(execute("SELECT * FROM %s WHERE pk = 1"), row(1, 20));
    }

    @Test
    public void admissionWaitsForPinnedReaderReclamation() throws Throwable
    {
        ColumnFamilyStore first = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable old = current(first);
        AtomicLong now = new AtomicLong(old.lastWriteNanos() + TIMEOUT);
        IdleMemtableFlusher flusher = controller(now, 1);
        flusher.add(old);
        try (OpOrder.Group read = first.readOrdering.start())
        {
            flusher.scan();
            first.forceFlush(ColumnFamilyStore.FlushReason.USER_FORCED).get(30, TimeUnit.SECONDS);
            assertFalse(old.idleFlushReclaimed());
            ColumnFamilyStore second = createEligibleTable();
            execute("INSERT INTO %s (pk, v) VALUES (2, 20)");
            TrieMemtable another = current(second);
            now.set(another.lastWriteNanos() + TIMEOUT);
            flusher.add(another);
            flusher.scan();
            assertEquals(1, flusher.flushingCount());
            assertSame(another, current(second));
        }
        awaitReclaimed(old);
        flusher.scan();
        assertEquals(0, flusher.candidateCount());
        assertFalse(current(getCurrentColumnFamilyStore()).isInitialized());
    }

    @Test
    public void exclusionsAndStrategyChanges() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().memtable_idle_timeout = new DurationSpec.IntMillisecondsBound("0s");
        ColumnFamilyStore disabled = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        assertFalse(current(disabled).idleFlushEligible());
        assertEquals(0, current(disabled).lastWriteNanos());
        DatabaseDescriptor.getRawConfig().memtable_idle_timeout = new DurationSpec.IntMillisecondsBound("1d");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH memtable = 'trie' " +
                    "AND compaction = {'class':'SizeTieredCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable stcs = current(cfs);
        assertFalse(stcs.idleFlushEligible());
        assertEquals(0, stcs.lastWriteNanos());
        execute("ALTER TABLE %s WITH compaction = {'class':'UnifiedCompactionStrategy'}");
        assertNotSame(stcs, current(cfs));
        execute("INSERT INTO %s (pk, v) VALUES (2, 20)");
        TrieMemtable ucs = current(cfs);
        assertTrue(ucs.idleFlushEligible());
        execute("ALTER TABLE %s WITH compaction = {'class':'SizeTieredCompactionStrategy'}");
        assertFalse(ucs.idleFlushEligible());
        assertFalse(current(cfs).idleFlushEligible());
    }

    @Test
    public void failedSubmissionStopsAdmissionWithoutDiscardingData() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable old = current(cfs);
        AtomicLong calls = new AtomicLong();
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, 1, 100, 16 * 1024 * 1024, () -> old.lastWriteNanos() + TIMEOUT, m -> {
            calls.incrementAndGet();
            return ImmediateFuture.failure(new IllegalStateException("test flush failure"));
        });
        controllers.add(flusher);
        flusher.add(old);
        flusher.scan();
        flusher.scan();
        flusher.add(old);
        flusher.scan();
        assertEquals(1, calls.get());
        assertEquals(0, flusher.candidateCount());
        assertSame(old, current(cfs));
        assertFalse(old.idleFlushReclaimed());
        assertRows(execute("SELECT * FROM %s"), row(1, 10));
    }

    @Test
    public void thrownSubmissionStopsAdmissionWithoutDiscardingData() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable old = current(cfs);
        AtomicLong calls = new AtomicLong();
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, 1, 100, 16 * 1024 * 1024,
                                                            () -> old.lastWriteNanos() + TIMEOUT, m -> {
            calls.incrementAndGet();
            throw new IllegalStateException("test submission exception");
        });
        controllers.add(flusher);
        flusher.add(old);
        flusher.scan();
        flusher.add(old);
        flusher.scan();
        assertEquals(1, calls.get());
        assertEquals(0, flusher.candidateCount());
        assertEquals(0, flusher.flushingCount());
        assertSame(old, current(cfs));
        assertFalse(old.idleFlushReclaimed());
        assertRows(execute("SELECT * FROM %s"), row(1, 10));
    }

    @Test
    public void localStrategyOverridesRefreshTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH memtable = 'trie' " +
                    "AND compaction = {'class':'SizeTieredCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk,v) VALUES (1,10)");
        TrieMemtable stcs = current(cfs);
        cfs.setCompactionParameters(Map.of("class", "UnifiedCompactionStrategy"));
        assertNotSame(stcs, current(cfs));
        execute("INSERT INTO %s (pk,v) VALUES (2,20)");
        TrieMemtable ucs = current(cfs);
        assertTrue(ucs.idleFlushEligible());
        cfs.setCompactionParameters(Map.of("class", "UnifiedCompactionStrategy", "scaling_parameters", "T8"));
        assertSame(ucs, current(cfs));
        execute("ALTER TABLE %s WITH comment = 'preserve local strategy override'");
        execute("UPDATE %s SET v = 21 WHERE pk = 2");
        ucs = current(cfs);
        assertTrue(ucs.idleFlushEligible());
        cfs.setCompactionParameters(Map.of("class", "SizeTieredCompactionStrategy"));
        assertNotSame(ucs, current(cfs));
        assertFalse(ucs.idleFlushEligible());
        assertRowsIgnoringOrder(execute("SELECT * FROM %s"), row(1, 10), row(2, 21));
    }

    @Test
    public void droppedCandidatesAreRemoved() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk, v) VALUES (1, 10)");
        TrieMemtable old = current(cfs);
        IdleMemtableFlusher flusher = controller(new AtomicLong(old.lastWriteNanos() + TIMEOUT), 1);
        flusher.add(old);
        dropTable("DROP TABLE %s");
        flusher.scan();
        assertEquals(0, flusher.candidateCount());
        assertEquals(0, flusher.flushingCount());
        assertFalse(old.idleFlushEligible());
    }

    private IdleMemtableFlusher controller(AtomicLong now, int maxConcurrent)
    {
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, maxConcurrent, 100, 16 * 1024 * 1024, now::get,
                                                            m -> ((ColumnFamilyStore) m.owner).flushIdleMemtable(m, now.get(), TIMEOUT));
        controllers.add(flusher);
        return flusher;
    }

    @Test
    public void rateBudgetSurvivesCompletionAndDoesNotBlockForcedFlush() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk,v) VALUES (1,10)");
        TrieMemtable first = current(cfs);
        AtomicLong now = new AtomicLong(first.lastWriteNanos() + TimeUnit.HOURS.toNanos(1));
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, 2, 1, 1_000_000, now::get,
                                                            m -> ((ColumnFamilyStore) m.owner).flushIdleMemtable(m, now.get(), TIMEOUT));
        controllers.add(flusher);
        flusher.add(first);
        flusher.scan();
        awaitReclaimed(first);
        execute("INSERT INTO %s (pk,v) VALUES (2,20)");
        TrieMemtable second = current(cfs);
        flusher.add(second);
        flusher.scan();
        assertEquals(0, flusher.flushingCount());
        assertSame(second, current(cfs));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        flusher.scan();
        assertRowsIgnoringOrder(execute("SELECT * FROM %s"), row(1, 10), row(2, 20));
        now.addAndGet(TimeUnit.SECONDS.toNanos(1));
        flusher.scan();
        assertEquals(0, flusher.candidateCount());
    }

    @Test
    public void byteDebtDelaysAnotherTableUntilRepaid() throws Throwable
    {
        ColumnFamilyStore first = createEligibleTable();
        execute("INSERT INTO %s (pk,v) VALUES (1,10)");
        TrieMemtable old = current(first);
        assertTrue(old.getLiveDataSize() > 1);
        long estimate = old.getLiveDataSize();
        AtomicLong now = new AtomicLong(old.lastWriteNanos() + TimeUnit.HOURS.toNanos(1));
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, 2, 100, 1, now::get,
                                                            m -> ((ColumnFamilyStore) m.owner).flushIdleMemtable(m, now.get(), TIMEOUT));
        controllers.add(flusher);
        flusher.add(old);
        flusher.scan();
        awaitReclaimed(old);
        ColumnFamilyStore second = createEligibleTable();
        execute("INSERT INTO %s (pk,v) VALUES (2,20)");
        TrieMemtable another = current(second);
        flusher.add(another);
        flusher.scan();
        assertSame(another, current(second));
        assertEquals(0, flusher.flushingCount());
        now.addAndGet(TimeUnit.SECONDS.toNanos(estimate + 1));
        flusher.scan();
        awaitReclaimed(another);
        assertRows(execute("SELECT * FROM %s"), row(2, 20));
        flusher.scan();
        assertEquals(0, flusher.candidateCount());
    }

    @Test
    public void staleSubmissionDoesNotSpendBudget() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk,v) VALUES (1,10)");
        TrieMemtable old = current(cfs);
        AtomicLong now = new AtomicLong(old.lastWriteNanos() + TIMEOUT);
        AtomicLong calls = new AtomicLong();
        IdleMemtableFlusher flusher = new IdleMemtableFlusher(TIMEOUT, 1, 1, 1, now::get, m -> {
            if (calls.getAndIncrement() == 0)
                return null;
            return cfs.flushIdleMemtable(m, now.get(), TIMEOUT);
        });
        controllers.add(flusher);
        flusher.add(old);
        flusher.scan();
        assertSame(old, current(cfs));
        flusher.scan();
        awaitReclaimed(old);
        assertEquals(2, calls.get());
        assertRows(execute("SELECT * FROM %s"), row(1, 10));
    }

    @Test
    public void queuedWriteIsRecheckedBeforeAdmission() throws Throwable
    {
        ColumnFamilyStore cfs = createEligibleTable();
        execute("INSERT INTO %s (pk,v) VALUES (1,10)");
        TrieMemtable old = current(cfs);
        AtomicLong now = new AtomicLong(old.lastWriteNanos() + TIMEOUT);
        long staleNow = now.get();
        IdleMemtableFlusher flusher = controller(now, 1);
        flusher.add(old);
        execute("UPDATE %s SET v = 20 WHERE pk = 1");
        now.set(staleNow);
        flusher.scan();
        assertSame(old, current(cfs));
        now.set(old.lastWriteNanos() + TIMEOUT);
        flusher.scan();
        awaitReclaimed(old);
        assertRows(execute("SELECT * FROM %s"), row(1, 20));
    }

    @Test
    public void generatedLifecycleTracesPreserveRows() throws Throwable
    {
        for (int seed = 0; seed < 8; seed++)
        {
            ColumnFamilyStore cfs = createEligibleTable();
            Random random = new Random(seed);
            TreeMap<Integer, Integer> expected = new TreeMap<>();
            AtomicLong now = new AtomicLong();
            IdleMemtableFlusher flusher = controller(now, 2);
            for (int step = 0; step < 40; step++)
            {
                int key = random.nextInt(8);
                switch (random.nextInt(4))
                {
                    case 0:
                        execute("INSERT INTO %s (pk,v) VALUES (?,?)", key, step);
                        expected.put(key, step);
                        break;
                    case 1:
                        execute("DELETE FROM %s WHERE pk = ?", key);
                        expected.remove(key);
                        break;
                    case 2:
                        TrieMemtable old = current(cfs);
                        if (!old.isClean())
                        {
                            now.set(old.lastWriteNanos() + TIMEOUT);
                            flusher.add(old);
                            flusher.scan();
                            awaitReclaimed(old);
                            flusher.scan();
                            assertFalse(current(cfs).isInitialized());
                        }
                        break;
                    default:
                        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
                }
                assertRowsIgnoringOrder(execute("SELECT * FROM %s"),
                                        expected.entrySet().stream().map(e -> row(e.getKey(), e.getValue())).toArray(Object[][]::new));
            }
        }
    }

    private ColumnFamilyStore createEligibleTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH memtable = 'trie' " +
                    "AND compaction = {'class':'UnifiedCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static TrieMemtable current(ColumnFamilyStore cfs)
    {
        return (TrieMemtable) cfs.getCurrentMemtable();
    }

    private static void awaitReclaimed(TrieMemtable memtable)
    {
        await().atMost(30, TimeUnit.SECONDS).until(memtable::idleFlushReclaimed);
    }
}
