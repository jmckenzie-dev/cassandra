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

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import com.google.common.annotations.VisibleForTesting;

/** Nonnegative int counters that widen once to preserve AtomicLongArray arithmetic. */
final class AdaptiveCounterArray
{
    private static final int FROZEN = Integer.MIN_VALUE;

    private volatile Object values;

    AdaptiveCounterArray(int length)
    {
        values = new AtomicIntegerArray(length);
    }

    int length()
    {
        Object current = values;
        return current instanceof AtomicIntegerArray ? ((AtomicIntegerArray) current).length()
                                                     : ((AtomicLongArray) current).length();
    }

    long get(int index)
    {
        Object current = values;
        if (current instanceof AtomicLongArray)
            return ((AtomicLongArray) current).get(index);
        int value = ((AtomicIntegerArray) current).get(index);
        return value >= 0 ? value : awaitWide().get(index);
    }

    void set(int index, long value)
    {
        Object current = values;
        if (current instanceof AtomicLongArray)
        {
            ((AtomicLongArray) current).set(index, value);
            return;
        }
        AtomicIntegerArray narrow = (AtomicIntegerArray) current;
        if (value < 0 || value > Integer.MAX_VALUE)
        {
            narrow.get(index);
            promote().set(index, value);
            return;
        }
        while (true)
        {
            int previous = narrow.get(index);
            if (previous < 0)
            {
                awaitWide().set(index, value);
                return;
            }
            if (narrow.compareAndSet(index, previous, (int) value))
                return;
        }
    }

    long addAndGet(int index, long delta)
    {
        Object current = values;
        if (current instanceof AtomicLongArray)
            return ((AtomicLongArray) current).addAndGet(index, delta);
        AtomicIntegerArray narrow = (AtomicIntegerArray) current;
        if (delta < 0 || delta > Integer.MAX_VALUE)
        {
            narrow.get(index);
            return promote().addAndGet(index, delta);
        }
        int previous = narrow.get(index);
        if (previous < 0)
            return awaitWide().addAndGet(index, delta);
        long next = previous + delta;
        if (next <= Integer.MAX_VALUE && narrow.compareAndSet(index, previous, (int) next))
            return next;
        // Contended counters use the wide array's direct atomic addition.
        return promote().addAndGet(index, delta);
    }

    boolean compareAndSet(int index, long expected, long update)
    {
        Object current = values;
        if (current instanceof AtomicLongArray)
            return ((AtomicLongArray) current).compareAndSet(index, expected, update);
        AtomicIntegerArray narrow = (AtomicIntegerArray) current;
        if (expected < 0 || expected > Integer.MAX_VALUE)
        {
            narrow.get(index);
            return false;
        }
        if (update < 0 || update > Integer.MAX_VALUE)
            return get(index) == expected && promote().compareAndSet(index, expected, update);
        if (narrow.compareAndSet(index, (int) expected, (int) update))
            return true;
        // Freezing is not a logical value change and must not cause a failed strong CAS.
        return narrow.get(index) < 0 && awaitWide().compareAndSet(index, expected, update);
    }

    private synchronized AtomicLongArray promote()
    {
        Object current = values;
        if (current instanceof AtomicLongArray)
            return (AtomicLongArray) current;
        AtomicIntegerArray narrow = (AtomicIntegerArray) current;
        // Complete allocation before freezing so an allocation failure leaves all cells usable.
        AtomicLongArray wide = new AtomicLongArray(narrow.length());
        for (int i = 0; i < narrow.length(); i++)
            wide.set(i, narrow.getAndSet(i, FROZEN));
        values = wide;
        return wide;
    }

    private AtomicLongArray awaitWide()
    {
        Object current;
        while ((current = values) instanceof AtomicIntegerArray)
            Thread.onSpinWait();
        return (AtomicLongArray) current;
    }

    @VisibleForTesting
    boolean isWide()
    {
        return values instanceof AtomicLongArray;
    }
}
