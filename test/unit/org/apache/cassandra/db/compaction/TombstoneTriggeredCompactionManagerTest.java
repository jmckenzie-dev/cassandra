/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import com.google.common.base.Ticker;

import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.ACCEPTED;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.COOLDOWN;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.DISABLED;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.DUPLICATE;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.FULL;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.ExecutionResult.BUSY;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.ExecutionResult.COMPLETED;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TombstoneTriggeredCompactionManagerTest implements WithQuickTheories
{
    private static final TableId TABLE_ID = TableId.generate();

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testFifoAndActiveDeduplication() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<DecoratedKey> executed = Collections.synchronizedList(new ArrayList<>());
        DecoratedKey first = key("first");
        DecoratedKey second = key("second");
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            executed.add(key);
            if (key.equals(first))
            {
                firstStarted.countDown();
                assertTrue(releaseFirst.await(5, SECONDS));
            }
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, first));
            assertTrue(firstStarted.await(5, SECONDS));
            assertEquals(DUPLICATE, manager.enqueue(TABLE_ID, first));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, second));
            releaseFirst.countDown();

            await().atMost(5, SECONDS).until(() -> manager.outstandingTasks() == 0);
            assertEquals(List.of(first, second), executed);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void testLiveCapacityChangesDoNotCancelAcceptedWork() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger capacity = new AtomicInteger(1);
        CountDownLatch active = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TombstoneTriggeredCompactionManager manager = newManager(capacity::get, (table, key) -> {
            active.countDown();
            assertTrue(release.await(5, SECONDS));
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("one")));
            assertTrue(active.await(5, SECONDS));
            assertEquals(FULL, manager.enqueue(TABLE_ID, key("two")));

            capacity.set(2);
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("two")));
            capacity.set(1);
            assertEquals(FULL, manager.enqueue(TABLE_ID, key("three")));
            capacity.set(0);
            assertEquals(DISABLED, manager.enqueue(TABLE_ID, key("three")));

            release.countDown();
            await().atMost(5, SECONDS).until(() -> manager.outstandingTasks() == 0);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void testBusyRequestMovesBehindOtherWork() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DecoratedKey first = key("first");
        DecoratedKey second = key("second");
        CountDownLatch firstAttempt = new CountDownLatch(1);
        CountDownLatch secondQueued = new CountDownLatch(1);
        AtomicInteger firstRuns = new AtomicInteger();
        List<DecoratedKey> attempts = Collections.synchronizedList(new ArrayList<>());
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            attempts.add(key);
            if (key.equals(first) && firstRuns.getAndIncrement() == 0)
            {
                firstAttempt.countDown();
                assertTrue(secondQueued.await(5, SECONDS));
                return BUSY;
            }
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, first));
            assertTrue(firstAttempt.await(5, SECONDS));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, second));
            secondQueued.countDown();

            await().atMost(5, SECONDS).until(() -> manager.outstandingTasks() == 0);
            assertEquals(List.of(first, second, first), attempts);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void testFailureDoesNotStopQueue()
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DecoratedKey failing = key("failing");
        AtomicInteger completed = new AtomicInteger();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            if (key.equals(failing))
                throw new IllegalStateException("expected");
            completed.incrementAndGet();
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, failing));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("next")));
            await().atMost(5, SECONDS).until(() -> manager.outstandingTasks() == 0);
            assertEquals(1, completed.get());
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void errorDoesNotStopQueue()
    {
        ManualExecutor executor = new ManualExecutor();
        List<DecoratedKey> completed = new ArrayList<>();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            if (key.equals(key("error")))
                throw new AssertionError("expected task failure");
            completed.add(key);
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("error")));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("queued")));
            executor.runAll();
            assertEquals(List.of(key("queued")), completed);
            assertEquals(0, manager.outstandingTasks());
            assertTrue(!manager.hasTasks());
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("error")));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("later")));
            executor.runAll();
            assertEquals(List.of(key("queued"), key("later")), completed);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void interruptedAttemptsReleaseWorkWithoutCooldown()
    {
        for (int failureMode = 0; failureMode < 5; failureMode++)
        {
            int mode = failureMode;
            ManualExecutor executor = new ManualExecutor();
            TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
                switch (mode)
                {
                    case 0:
                        throw new InterruptedException("expected task interruption");
                    case 1:
                        throw new UncheckedInterruptedException(new InterruptedException("expected scanner interruption"));
                    case 2:
                        throw new CompactionInterruptedException("expected cancellation");
                    case 3:
                        throw new IllegalStateException(new InterruptedException("expected wrapped interruption"));
                    default:
                        Thread.currentThread().interrupt();
                        return BUSY;
                }
            }, executor);
            try
            {
                assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("interrupted")));
                assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("queued")));
                executor.runAll();
                assertTrue(Thread.interrupted());
                assertEquals(0, manager.outstandingTasks());
                assertTrue(!manager.hasTasks());
                assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("interrupted")));
                assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("queued")));
            }
            finally
            {
                Thread.interrupted();
                manager.shutdown(true);
            }
        }
    }

    @Test
    public void testConcurrentDuplicateAdmission() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(16);
        CountDownLatch active = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        DecoratedKey key = key("same");
        TombstoneTriggeredCompactionManager manager = newManager(() -> 10, (table, ignored) -> {
            active.countDown();
            assertTrue(release.await(10, SECONDS));
            return COMPLETED;
        }, executor);
        try
        {
            for (int i = 0; i < 10000; i++)
            {
                callers.execute(() -> {
                    TombstoneTriggeredCompactionManager.AdmissionResult result = manager.enqueue(TABLE_ID, key);
                    if (result == ACCEPTED)
                        accepted.incrementAndGet();
                    else if (result == DUPLICATE)
                        duplicates.incrementAndGet();
                });
            }
            callers.shutdown();
            assertTrue(callers.awaitTermination(10, SECONDS));
            assertTrue(active.await(5, SECONDS));
            assertEquals(1, accepted.get());
            assertEquals(9999, duplicates.get());
            release.countDown();
        }
        finally
        {
            release.countDown();
            callers.shutdownNow();
            manager.shutdown(true);
        }
    }

    @Test
    public void queuedAdmissionsMatchCapacityDeduplicationAndFifoContract()
    {
        qt().withFixedSeed(0x5EEDC0DEL)
            .withExamples(1000)
            .withShrinkCycles(100)
            .forAll(lists().of(queueOperationGenerator()).ofSizeBetween(1, 64))
            .checkAssert(this::assertQueuedAdmissions);
    }

    @Test
    public void cooldownStartsAtCompletionAndRejectedReadsDoNotExtendIt()
    {
        ManualExecutor executor = new ManualExecutor();
        TestTicker ticker = new TestTicker();
        TombstoneTriggeredCompactionManager manager = new TombstoneTriggeredCompactionManager(() -> 1, (table, key) -> {
            ticker.advance(SECONDS.toNanos(120));
            return COMPLETED;
        }, executor, 1, ticker, 4);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("hot")));
            ticker.advance(SECONDS.toNanos(120));
            assertEquals(DUPLICATE, manager.enqueue(TABLE_ID, key("hot")));
            executor.runAll();
            assertEquals(0, manager.outstandingTasks());
            assertTrue(!manager.hasTasks());

            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("hot")));
            ticker.advance(SECONDS.toNanos(59));
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("hot")));
            ticker.advance(SECONDS.toNanos(1) - 1);
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("hot")));
            ticker.advance(1);
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("hot")));
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void failedExecutionCoolsDownWithoutUsingQueueCapacity()
    {
        ManualExecutor executor = new ManualExecutor();
        TestTicker ticker = new TestTicker();
        AtomicInteger completed = new AtomicInteger();
        TombstoneTriggeredCompactionManager manager = new TombstoneTriggeredCompactionManager(() -> 1, (table, key) -> {
            if (key.equals(key("failed")))
                throw new IllegalStateException("expected");
            completed.incrementAndGet();
            return COMPLETED;
        }, executor, 1, ticker, 4);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("failed")));
            executor.runAll();
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("failed")));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("other")));
            executor.runAll();
            assertEquals(1, completed.get());
            assertEquals(0, manager.outstandingTasks());
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("failed")));
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("other")));
            ticker.advance(SECONDS.toNanos(60));
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("failed")));
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void cooldownIsScopedToTableAndCopiedKey()
    {
        ManualExecutor executor = new ManualExecutor();
        TestTicker ticker = new TestTicker();
        AtomicInteger capacity = new AtomicInteger(1);
        TombstoneTriggeredCompactionManager manager = new TombstoneTriggeredCompactionManager(capacity::get,
                                                                                              (table, key) -> COMPLETED,
                                                                                              executor, 1, ticker, 4);
        try
        {
            DecoratedKey submitted = key("owned");
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, submitted));
            submitted.getKey().put(0, (byte) 'X');
            executor.runAll();
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("owned")));
            capacity.set(0);
            assertEquals(DISABLED, manager.enqueue(TABLE_ID, key("owned")));
            capacity.set(1);
            TableId otherTable = TableId.generate();
            assertEquals(ACCEPTED, manager.enqueue(otherTable, key("owned")));
            assertEquals(DUPLICATE, manager.enqueue(otherTable, key("owned")));
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("owned")));
            executor.runAll();
            assertEquals(COOLDOWN, manager.enqueue(otherTable, key("owned")));
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void cooldownEvictionDoesNotEvictOutstandingWork()
    {
        ManualExecutor executor = new ManualExecutor();
        TestTicker ticker = new TestTicker();
        List<DecoratedKey> executed = new ArrayList<>();
        TombstoneTriggeredCompactionManager manager = new TombstoneTriggeredCompactionManager(() -> 1, (table, key) -> {
            executed.add(key);
            return COMPLETED;
        }, executor, 1, ticker, 1);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("first")));
            executor.runAll();
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("second")));
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("first")));
            assertEquals(DUPLICATE, manager.enqueue(TABLE_ID, key("second")));
            assertEquals(FULL, manager.enqueue(TABLE_ID, key("third")));
            executor.runAll();
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("first")));
            assertEquals(COOLDOWN, manager.enqueue(TABLE_ID, key("second")));
            assertEquals(1, manager.outstandingTasks());
            executor.runAll();
            assertEquals(List.of(key("first"), key("second"), key("first")), executed);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void acceptedRequestOwnsKeyCopy()
    {
        ManualExecutor executor = new ManualExecutor();
        List<DecoratedKey> executed = new ArrayList<>();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 1, (table, key) -> {
            executed.add(key);
            return COMPLETED;
        }, executor);
        DecoratedKey submitted = key("owned");
        DecoratedKey expected = key("owned");
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, submitted));
            submitted.getKey().put(0, (byte) 'X');
            executor.runAll();

            assertEquals(Collections.singletonList(expected), executed);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void sameKeyInDifferentTablesUsesSeparateCapacity()
    {
        ManualExecutor executor = new ManualExecutor();
        List<TableId> executed = new ArrayList<>();
        TableId otherTable = TableId.generate();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            executed.add(table);
            return COMPLETED;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("same")));
            assertEquals(ACCEPTED, manager.enqueue(otherTable, key("same")));
            assertEquals(DUPLICATE, manager.enqueue(otherTable, key("same")));
            assertEquals(FULL, manager.enqueue(TABLE_ID, key("different")));
            executor.runAll();
            assertEquals(List.of(TABLE_ID, otherTable), executed);
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    @Test
    public void executorRejectionReleasesRequest()
    {
        ManualExecutor executor = new ManualExecutor();
        executor.shutdown();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 1, (table, key) -> COMPLETED, executor);
        assertEquals(TombstoneTriggeredCompactionManager.AdmissionResult.SHUTDOWN,
                     manager.enqueue(TABLE_ID, key("rejected")));
        assertEquals(0, manager.outstandingTasks());
        assertTrue(!manager.hasTasks());
    }

    @Test
    public void forcedShutdownBeforeWorkerStartsClearsTaskState()
    {
        ManualExecutor executor = new ManualExecutor();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 1, (table, key) -> COMPLETED, executor);
        assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("queued")));
        manager.shutdown(true);
        assertEquals(0, manager.outstandingTasks());
        assertTrue(!manager.hasTasks());
    }

    @Test
    public void testGracefulShutdownDrainsAcceptedWork() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger completed = new AtomicInteger();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            completed.incrementAndGet();
            return COMPLETED;
        }, executor);
        assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("one")));
        assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("two")));
        manager.shutdown(false);
        assertTrue(manager.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(2, completed.get());
        assertEquals(TombstoneTriggeredCompactionManager.AdmissionResult.SHUTDOWN,
                     manager.enqueue(TABLE_ID, key("three")));
    }

    @Test
    public void testGracefulShutdownDropsBusyRetry() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 1, (table, key) -> {
            attempts.incrementAndGet();
            started.countDown();
            assertTrue(release.await(5, SECONDS));
            return BUSY;
        }, executor);
        try
        {
            assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("busy")));
            assertTrue(started.await(5, SECONDS));
            manager.shutdown(false);
            release.countDown();

            assertTrue(manager.awaitTermination(5, SECONDS));
            assertEquals(1, attempts.get());
            assertEquals(0, manager.outstandingTasks());
            assertEquals(TombstoneTriggeredCompactionManager.AdmissionResult.SHUTDOWN,
                         manager.enqueue(TABLE_ID, key("later")));
        }
        finally
        {
            release.countDown();
            manager.shutdown(true);
        }
    }

    @Test
    public void testForcedShutdownClearsPendingWork() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch active = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();
        TombstoneTriggeredCompactionManager manager = newManager(() -> 2, (table, key) -> {
            active.countDown();
            try
            {
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                completed.incrementAndGet();
            }
            catch (InterruptedException e)
            {
                interrupted.countDown();
                throw e;
            }
            return COMPLETED;
        }, executor);
        assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("active")));
        assertTrue(active.await(5, SECONDS));
        assertEquals(ACCEPTED, manager.enqueue(TABLE_ID, key("pending")));

        manager.shutdown(true);
        assertTrue(interrupted.await(5, SECONDS));
        assertTrue(manager.awaitTermination(5, SECONDS));
        assertEquals(0, completed.get());
        assertEquals(TombstoneTriggeredCompactionManager.AdmissionResult.SHUTDOWN,
                     manager.enqueue(TABLE_ID, key("new")));
    }

    private static DecoratedKey key(String value)
    {
        return Murmur3Partitioner.instance.decorateKey(bytes(value));
    }

    private void assertQueuedAdmissions(List<QueueOperation> operations)
    {
        AtomicInteger capacity = new AtomicInteger(2);
        ManualExecutor executor = new ManualExecutor();
        TestTicker ticker = new TestTicker();
        Map<DecoratedKey, Long> cooldownUntil = new HashMap<>();
        List<DecoratedKey> executed = new ArrayList<>();
        TombstoneTriggeredCompactionManager manager = new TombstoneTriggeredCompactionManager(capacity::get, (table, key) -> {
            executed.add(key);
            return COMPLETED;
        }, executor, 1, ticker, 16);
        Set<DecoratedKey> accepted = new LinkedHashSet<>();
        List<DecoratedKey> expectedExecuted = new ArrayList<>();

        try
        {
            for (QueueOperation operation : operations)
            {
                if (!operation.isCapacityUpdate() && operation.value == -1)
                {
                    expectedExecuted.addAll(accepted);
                    for (DecoratedKey key : accepted)
                        cooldownUntil.put(key, ticker.read() + SECONDS.toNanos(60));
                    accepted.clear();
                    executor.runAll();
                    assertEquals(expectedExecuted, executed);
                    assertEquals(0, manager.outstandingTasks());
                    continue;
                }
                if (!operation.isCapacityUpdate() && operation.value == -2)
                {
                    ticker.advance(SECONDS.toNanos(30));
                    continue;
                }
                if (operation.isCapacityUpdate())
                {
                    capacity.set(operation.value);
                    continue;
                }

                DecoratedKey key = key("property-" + operation.value);
                TombstoneTriggeredCompactionManager.AdmissionResult expected = expectedAdmission(capacity.get(), accepted, key,
                                                                                                  ticker.read() < cooldownUntil.getOrDefault(key, 0L));
                assertEquals(expected, manager.enqueue(TABLE_ID, key));
                if (expected == ACCEPTED)
                    accepted.add(key);
                assertEquals(accepted.size(), manager.outstandingTasks());
            }

            expectedExecuted.addAll(accepted);
            executor.runAll();
            assertEquals(expectedExecuted, executed);
            assertEquals(0, manager.outstandingTasks());
            assertTrue(!manager.hasTasks());
        }
        finally
        {
            manager.shutdown(true);
        }
    }

    private static TombstoneTriggeredCompactionManager.AdmissionResult expectedAdmission(int capacity,
                                                                                          Set<DecoratedKey> accepted,
                                                                                          DecoratedKey key,
                                                                                          boolean coolingDown)
    {
        if (capacity == 0)
            return DISABLED;
        if (accepted.contains(key))
            return DUPLICATE;
        if (coolingDown)
            return COOLDOWN;
        if (accepted.size() >= capacity)
            return FULL;
        return ACCEPTED;
    }

    private Gen<QueueOperation> queueOperationGenerator()
    {
        return integers().between(0, 21)
                         .map(value -> value == 21 ? QueueOperation.enqueue(-2)
                                                  : value == 20 ? QueueOperation.enqueue(-1)
                                                  : value < 5 ? QueueOperation.capacity(value)
                                                 : QueueOperation.enqueue((value - 5) % 8))
                         .describedAs(QueueOperation::toString);
    }

    private static final class QueueOperation
    {
        private final boolean capacityUpdate;
        private final int value;

        private QueueOperation(boolean capacityUpdate, int value)
        {
            this.capacityUpdate = capacityUpdate;
            this.value = value;
        }

        private static QueueOperation capacity(int value)
        {
            return new QueueOperation(true, value);
        }

        private static QueueOperation enqueue(int value)
        {
            return new QueueOperation(false, value);
        }

        private boolean isCapacityUpdate()
        {
            return capacityUpdate;
        }

        @Override
        public String toString()
        {
            if (!capacityUpdate && value == -2)
                return "advance(30s)";
            return capacityUpdate ? "capacity(" + value + ')' : value == -1 ? "drain" : "enqueue(" + value + ')';
        }
    }

    private static final class TestTicker extends Ticker
    {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read()
        {
            return nanos.get();
        }

        private void advance(long elapsedNanos)
        {
            nanos.addAndGet(elapsedNanos);
        }
    }

    private static final class ManualExecutor extends AbstractExecutorService
    {
        private final List<Runnable> queued = new ArrayList<>();
        private boolean shutdown;

        @Override
        public void execute(Runnable command)
        {
            if (shutdown)
                throw new RejectedExecutionException();
            queued.add(command);
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            shutdown = true;
            List<Runnable> notStarted = new ArrayList<>(queued);
            queued.clear();
            return notStarted;
        }

        @Override
        public boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        public boolean isTerminated()
        {
            return shutdown && queued.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
        {
            return isTerminated();
        }

        private void runAll()
        {
            while (!queued.isEmpty())
                queued.remove(0).run();
        }
    }

    private static TombstoneTriggeredCompactionManager newManager(IntSupplier capacity,
                                                                  TombstoneTriggeredCompactionManager.TaskRunner taskRunner,
                                                                  ExecutorService executor)
    {
        return new TombstoneTriggeredCompactionManager(capacity, taskRunner, executor, 1);
    }
}
