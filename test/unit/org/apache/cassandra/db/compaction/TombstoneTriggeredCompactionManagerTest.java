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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.ACCEPTED;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.DISABLED;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.DUPLICATE;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.AdmissionResult.FULL;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.ExecutionResult.BUSY;
import static org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManager.ExecutionResult.COMPLETED;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TombstoneTriggeredCompactionManagerTest
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

    private static TombstoneTriggeredCompactionManager newManager(IntSupplier capacity,
                                                                  TombstoneTriggeredCompactionManager.TaskRunner taskRunner,
                                                                  ExecutorService executor)
    {
        return new TombstoneTriggeredCompactionManager(capacity, taskRunner, executor, 1);
    }
}
