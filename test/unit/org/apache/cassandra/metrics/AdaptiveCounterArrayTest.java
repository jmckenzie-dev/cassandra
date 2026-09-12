/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class AdaptiveCounterArrayTest
{
    @Test
    public void nonnegativeIntValuesStayNarrowUntilOverflow()
    {
        AdaptiveCounterArray values = new AdaptiveCounterArray(17);
        assertEquals(17, values.length());
        for (int i = 0; i < values.length(); i++)
        {
            assertEquals(0, values.get(i));
            values.set(i, i);
        }
        values.set(0, Integer.MAX_VALUE - 1L);
        assertEquals(Integer.MAX_VALUE, values.addAndGet(0, 1));
        assertFalse(values.isWide());
        assertEquals(Integer.MAX_VALUE + 1L, values.addAndGet(0, 1));
        assertTrue(values.isWide());
        assertEquals(17, values.length());
        for (int i = 1; i < values.length(); i++)
            assertEquals(i, values.get(i));
    }

    @Test
    public void negativeValuesAndLongOverflowRetainSignedArithmetic()
    {
        for (long initial : new long[]{ -1, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE })
        {
            AdaptiveCounterArray values = new AdaptiveCounterArray(1);
            values.set(0, initial);
            assertTrue(values.isWide());
            assertEquals(initial, values.get(0));
            assertEquals(initial + 1, values.addAndGet(0, 1));
            values.set(0, 0);
            assertTrue(values.isWide());
        }
        AdaptiveCounterArray values = new AdaptiveCounterArray(1);
        values.set(0, 1);
        assertEquals(0, values.addAndGet(0, -1));
        assertTrue(values.isWide());
        assertEquals(Long.MIN_VALUE, values.addAndGet(0, Long.MIN_VALUE));
        assertEquals(0, values.addAndGet(0, Long.MIN_VALUE));

        values = new AdaptiveCounterArray(1);
        values.set(0, 7);
        assertEquals(Long.MIN_VALUE + 6, values.addAndGet(0, Long.MAX_VALUE));
        assertTrue(values.isWide());
    }

    @Test
    public void failedCompareAndSetDoesNotWiden()
    {
        AdaptiveCounterArray values = new AdaptiveCounterArray(1);
        values.set(0, 7);
        for (long expected : new long[]{ 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE + 1L, Long.MAX_VALUE })
        {
            assertFalse(values.compareAndSet(0, expected, Long.MIN_VALUE));
            assertEquals(7, values.get(0));
            assertFalse(values.isWide());
        }
        assertTrue(values.compareAndSet(0, 7, Integer.MAX_VALUE));
        assertFalse(values.isWide());
        assertTrue(values.compareAndSet(0, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L));
        assertTrue(values.isWide());
        assertTrue(values.compareAndSet(0, Integer.MAX_VALUE + 1L, Integer.MIN_VALUE));
        assertEquals(Integer.MIN_VALUE, values.get(0));
        assertTrue(values.compareAndSet(0, Integer.MIN_VALUE, Long.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, values.addAndGet(0, 1));
    }

    @Test
    public void invalidIndicesFailBeforePromotion()
    {
        AdaptiveCounterArray values = new AdaptiveCounterArray(1);
        for (int index : new int[]{ -1, 1, Integer.MAX_VALUE })
        {
            expectIndexFailure(() -> values.get(index));
            expectIndexFailure(() -> values.set(index, 1));
            expectIndexFailure(() -> values.set(index, -1));
            expectIndexFailure(() -> values.addAndGet(index, 1));
            expectIndexFailure(() -> values.addAndGet(index, Long.MAX_VALUE));
            expectIndexFailure(() -> values.compareAndSet(index, 0, 1));
            expectIndexFailure(() -> values.compareAndSet(index, -1, Long.MIN_VALUE));
            expectIndexFailure(() -> values.compareAndSet(index, 0, Long.MIN_VALUE));
            assertFalse(values.isWide());
        }
        values.set(0, Long.MAX_VALUE);
        expectIndexFailure(() -> values.get(1));
        expectIndexFailure(() -> values.set(1, 0));
        expectIndexFailure(() -> values.addAndGet(1, 1));
        expectIndexFailure(() -> values.compareAndSet(1, 0, 1));
        assertEquals(0, new AdaptiveCounterArray(0).length());
        expectIndexFailure(() -> new AdaptiveCounterArray(0).compareAndSet(0, -1, 0));
        try
        {
            new AdaptiveCounterArray(-1);
            fail("Negative length must fail");
        }
        catch (NegativeArraySizeException expected)
        {
            // Same constructor contract as the JDK atomic arrays.
        }
    }

    @Test
    public void writeContentionWidensBeforeIntegerOverflow() throws Exception
    {
        assumeTrue("Contention stress requires concurrent execution", Runtime.getRuntime().availableProcessors() > 1);
        AdaptiveCounterArray values = new AdaptiveCounterArray(17);
        for (int i = 0; i < 100000; i++)
            values.addAndGet(0, 1);
        assertEquals(100000, values.get(0));
        assertFalse(values.isWide());

        Runnable[] actors = new Runnable[8];
        for (int actor = 0; actor < actors.length; actor++)
            actors[actor] = () -> {
                for (int i = 0; i < 100000; i++)
                    values.addAndGet(0, 1);
            };
        concurrently(actors);
        assertEquals(900000, values.get(0));
        assertTrue("Concurrent-add stress should widen without integer overflow", values.isWide());
    }

    @Test
    public void concurrentIncrementsAndReadsSurvivePromotion() throws Exception
    {
        AdaptiveCounterArray values = new AdaptiveCounterArray(17);
        long initial = Integer.MAX_VALUE - 100L;
        values.set(0, initial);
        Runnable[] actors = new Runnable[5];
        for (int worker = 0; worker < 4; worker++)
        {
            final int ownIndex = worker + 1;
            actors[worker] = () -> {
                for (int i = 0; i < 10000; i++)
                {
                    values.addAndGet(0, 1);
                    values.addAndGet(ownIndex, 1);
                }
            };
        }
        actors[4] = () -> {
            long previous = initial;
            for (int i = 0; i < 10000; i++)
            {
                long current = values.get(0);
                assertTrue(current >= previous);
                previous = current;
            }
        };
        concurrently(actors);
        assertTrue(values.isWide());
        assertEquals(initial + 40000, values.get(0));
        for (int i = 1; i <= 4; i++)
            assertEquals(10000, values.get(i));
    }

    @Test
    public void migrationCannotFailAnUnchangedCompareAndSetOrLoseSet() throws Exception
    {
        for (int round = 0; round < 32; round++)
        {
            AdaptiveCounterArray values = new AdaptiveCounterArray(4096);
            values.set(0, 7);
            concurrently(() -> {
                for (int i = 0; i < 10000; i++)
                    assertTrue("Migration must not fail a strong CAS", values.compareAndSet(0, 7, 7));
            }, () -> values.set(1, 9), () -> values.set(2, Long.MAX_VALUE));
            assertTrue(values.isWide());
            assertEquals(7, values.get(0));
            assertEquals(9, values.get(1));
            assertEquals(Long.MAX_VALUE, values.get(2));
        }
    }

    @Test
    public void concurrentCompareAndSetUpdatesSurviveMultiplePromotionRequests() throws Exception
    {
        AdaptiveCounterArray values = new AdaptiveCounterArray(128);
        Runnable increment = () -> {
            for (int i = 0; i < 10000; i++)
            {
                long previous;
                do
                {
                    previous = values.get(0);
                }
                while (!values.compareAndSet(0, previous, previous + 1));
            }
        };
        concurrently(increment, increment, () -> values.set(1, -1),
                     () -> values.addAndGet(2, Long.MAX_VALUE),
                     () -> assertTrue(values.compareAndSet(3, 0, Integer.MAX_VALUE + 1L)));
        assertEquals(20000, values.get(0));
        assertEquals(-1, values.get(1));
        assertEquals(Long.MAX_VALUE, values.get(2));
        assertEquals(Integer.MAX_VALUE + 1L, values.get(3));
    }

    private static void expectIndexFailure(Runnable operation)
    {
        try
        {
            operation.run();
            fail("Invalid index must fail");
        }
        catch (IndexOutOfBoundsException expected)
        {
            // The exception must precede any representation change.
        }
    }

    private static void concurrently(Runnable... actors) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(actors.length);
        CountDownLatch ready = new CountDownLatch(actors.length);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try
        {
            for (Runnable actor : actors)
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(30, TimeUnit.SECONDS));
                    actor.run();
                    return null;
                }));
            assertTrue(ready.await(30, TimeUnit.SECONDS));
            start.countDown();
            for (Future<?> future : futures)
                future.get(30, TimeUnit.SECONDS);
        }
        finally
        {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }
}
