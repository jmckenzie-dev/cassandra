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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;

/** Bounded, node-wide admission of idle TrieMemtable flushes. Entries exist only for initialized memtables. */
public final class IdleMemtableFlusher
{
    private static final Logger logger = LoggerFactory.getLogger(IdleMemtableFlusher.class);
    private static final int SCAN_BATCH_SIZE = 16384;
    private static volatile IdleMemtableFlusher instance;
    private static volatile boolean stopped;

    private final Set<TrieMemtable> candidates = ConcurrentHashMap.newKeySet();
    private final Map<TrieMemtable, Future<?>> flushing = new LinkedHashMap<>();
    private final long timeoutNanos;
    private final int maxConcurrent;
    private final AdmissionBudget budget;
    private final LongSupplier clock;
    private final Function<TrieMemtable, Future<?>> submit;
    private final AtomicBoolean scanQueued = new AtomicBoolean();
    private Iterator<TrieMemtable> iterator;
    private volatile ScheduledFuture<?> task;
    private volatile boolean closed;

    IdleMemtableFlusher(long timeoutNanos, int maxConcurrent, int maxPerSecond, double bytesPerSecond,
                       LongSupplier clock, Function<TrieMemtable, Future<?>> submit)
    {
        if (timeoutNanos <= 0 || maxConcurrent <= 0)
            throw new IllegalArgumentException("Idle timeout and concurrency must be positive");
        this.timeoutNanos = timeoutNanos;
        this.maxConcurrent = maxConcurrent;
        this.clock = clock;
        this.submit = submit;
        this.budget = new AdmissionBudget(maxPerSecond, bytesPerSecond, clock.getAsLong());
    }

    private static final class Holder
    {
        static final IdleMemtableFlusher INSTANCE = start();

        private static IdleMemtableFlusher start()
        {
            long timeout = DatabaseDescriptor.getMemtableIdleTimeoutNanos();
            IdleMemtableFlusher flusher = new IdleMemtableFlusher(timeout, DatabaseDescriptor.getMemtableIdleFlushMaxConcurrent(),
                                                                DatabaseDescriptor.getMemtableIdleFlushMaxPerSecond(),
                                                                DatabaseDescriptor.getMemtableIdleFlushThroughputBytesPerSecond(),
                                                                Clock.Global::nanoTime,
                                                                memtable -> ((ColumnFamilyStore) memtable.owner)
                                                                            .flushIdleMemtable(memtable, Clock.Global.nanoTime(), timeout));
            instance = flusher;
            flusher.task = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(flusher::scan, 100, 100, TimeUnit.MILLISECONDS);
            if (stopped)
                flusher.close();
            return flusher;
        }
    }

    static void register(TrieMemtable memtable)
    {
        if (!stopped)
            Holder.INSTANCE.add(memtable);
    }

    static void unregister(TrieMemtable memtable)
    {
        IdleMemtableFlusher flusher = instance;
        if (flusher != null)
            flusher.candidates.remove(memtable);
    }

    static void reclaimed()
    {
        IdleMemtableFlusher flusher = instance;
        if (flusher != null)
            flusher.requestScan();
    }

    // Completion can precede reader reclamation or follow it. Both events wake admission.
    private void requestScan()
    {
        if (closed || task == null || !scanQueued.compareAndSet(false, true))
            return;
        try
        {
            ScheduledExecutors.optionalTasks.execute(() -> {
                scanQueued.set(false);
                scan();
            });
        }
        catch (RejectedExecutionException e)
        {
            scanQueued.set(false);
            logger.debug("Idle flush wakeup rejected during executor shutdown", e);
        }
    }

    public static void shutdown()
    {
        stopped = true;
        IdleMemtableFlusher flusher = instance;
        if (flusher != null)
            flusher.close();
    }

    void add(TrieMemtable memtable)
    {
        if (!closed)
        {
            candidates.add(memtable);
            if (closed)
                candidates.remove(memtable);
        }
    }

    synchronized void scan()
    {
        if (closed)
            return;
        try
        {
            Iterator<Map.Entry<TrieMemtable, Future<?>>> pending = flushing.entrySet().iterator();
            while (pending.hasNext())
            {
                Map.Entry<TrieMemtable, Future<?>> entry = pending.next();
                Future<?> future = entry.getValue();
                if (future.isDone() && !future.isSuccess())
                {
                    logger.error("Idle memtable flush failed; stopping idle flush admission until restart", future.cause());
                    close();
                    return;
                }
                if (future.isSuccess() && entry.getKey().idleFlushReclaimed())
                    pending.remove();
            }

            if (candidates.isEmpty())
            {
                iterator = null;
                return;
            }
            if (iterator == null || !iterator.hasNext())
                iterator = candidates.iterator();
            long now = clock.getAsLong();
            for (int scanned = 0; scanned < SCAN_BATCH_SIZE && iterator.hasNext() && flushing.size() < maxConcurrent
                                  && budget.available(now); scanned++)
            {
                TrieMemtable memtable = iterator.next();
                if (!memtable.idleFlushEligible())
                    candidates.remove(memtable);
                else if (memtable.isIdle(now, timeoutNanos))
                {
                    long estimatedBytes = Math.max(0, memtable.getLiveDataSize());
                    Future<?> future = submit.apply(memtable);
                    if (future != null)
                    {
                        budget.charge(estimatedBytes);
                        candidates.remove(memtable);
                        flushing.put(memtable, future);
                        if (task != null)
                            future.addListener(this::requestScan);
                    }
                }
            }
        }
        catch (RuntimeException e)
        {
            logger.error("Idle memtable flush admission failed; stopping until restart", e);
            close();
        }
    }

    synchronized void close()
    {
        closed = true;
        if (task != null)
            task.cancel(false);
        candidates.clear();
        flushing.clear();
        iterator = null;
    }

    /** One second of credit; an oversized flush borrows bytes from future admission. Guarded by scan's lock. */
    static final class AdmissionBudget
    {
        private final int maxPerSecond;
        private final double bytesPerSecond;
        private double operations;
        private double bytes;
        private long lastUpdate;

        AdmissionBudget(int maxPerSecond, double bytesPerSecond, long now)
        {
            if (maxPerSecond < 1 || !Double.isFinite(bytesPerSecond) || bytesPerSecond <= 0)
                throw new IllegalArgumentException("Idle flush rates must be finite and positive");
            this.maxPerSecond = maxPerSecond;
            this.bytesPerSecond = bytesPerSecond;
            operations = maxPerSecond;
            bytes = bytesPerSecond;
            lastUpdate = now;
        }

        boolean available(long now)
        {
            long elapsed = now - lastUpdate;
            if (elapsed > 0)
            {
                double seconds = elapsed / 1_000_000_000.0;
                operations = Math.min(maxPerSecond, operations + seconds * maxPerSecond);
                bytes = Math.min(bytesPerSecond, bytes + seconds * bytesPerSecond);
                lastUpdate = now;
            }
            return operations >= 1 && bytes > 0;
        }

        void charge(long estimatedBytes)
        {
            operations--;
            bytes -= estimatedBytes;
        }
    }

    synchronized int flushingCount()
    {
        return flushing.size();
    }

    int candidateCount()
    {
        return candidates.size();
    }
}
