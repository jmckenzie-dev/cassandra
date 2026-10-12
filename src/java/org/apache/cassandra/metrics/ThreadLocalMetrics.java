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

package org.apache.cassandra.metrics;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownAndWait;

/**
 * A thread-local counter implementation designed to use in metrics as an alternative to LongAdder used by Dropwizard metrics.
 * This implementation has reduced write (increment) CPU usage costs in exchange for a higher read cost.
 * We keep and increment parts of a counter locally for each thread.
 * To reduce memory footprint per counter they are grouped together in fixed-size pages for each thread.
 * A position of a counter value is the same for every thread for the same counter id.
 * A thread allocates only the pages that hold ids it updates. Ids are given out in order, so a single array
 * sized to the highest id a thread touched held a slot for every metric on every such thread: with 10M metrics
 * (80k tables) that was 85 MB per thread.
 * Piggyback volatile visibility is expected for readers who execute getCount method to see recent writes to thread local arrays.
 * If a metric is not used anymore the position in the array is reused. Phantom references are used to track aliveness of metric users.
 * When a thread died the counter values accumulated by it are transfered to a shared summaryValues collection.
 * Threads death is tracked using 2 approaches: FastThreadLocal.onRemoval callback and phantom references to Thread objects.
 */
public class ThreadLocalMetrics
{
    private static final int INITIAL_COUNTERS_CAPACITY = 16;
    @VisibleForTesting
    static final int PAGE_SHIFT = 8;
    @VisibleForTesting
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long[][] NO_PAGES = new long[0][];

    static final AtomicInteger idGenerator = new AtomicInteger();

    @VisibleForTesting
    static final FreeMetricIdSetTracker freeMetricIdSetTracker = new FreeMetricIdSetTracker();

    private static final List<ThreadLocalMetrics> allThreadLocalMetrics = new CopyOnWriteArrayList<>();

    /* the lock is used to coordinate the threads which:
     * 1) transfer values from a dead thread to summaryValues
     * 2) calculate a getCount value.
     * Using this lock we want to avoid
     *  a value lost while moving it in getCount
     *  as well as a double-counting
     */
    private static final ReadWriteLock summaryLock = new ReentrantReadWriteLock();

    private static final FastThreadLocal<ThreadLocalMetrics> threadLocalMetricsCurrent = new FastThreadLocal<>()
    {
        @Override
        protected ThreadLocalMetrics initialValue()
        {
            return create();
        }

        // this method is invoked when a thread is going to finish, but it works only for FastThreadLocalThread
        // so, we use phantom references for other cases
        @Override
        protected void onRemoval(ThreadLocalMetrics value)
        {
            value.release();
        }
    };

    public static ThreadLocalMetrics create()
    {
        ThreadLocalMetrics result = new ThreadLocalMetrics();
        allThreadLocalMetrics.add(result);

        Thread thread = Thread.currentThread();
        // use phantom references ony if needed
        // CassandraThread is FastThreadLocalThread too
        if (!(thread instanceof FastThreadLocalThread))
            destroyWhenUnreachable(thread, result::release);

        return result;
    }

    private static volatile AtomicLongArray summaryValues = new AtomicLongArray(INITIAL_COUNTERS_CAPACITY);

    private static final Shutdownable cleaner;
    private static final Set<PhantomReference<Object>> phantomReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();

    static
    {
        cleaner = executorFactory().infiniteLoop("ThreadLocalMetrics-Cleaner", ThreadLocalMetrics::cleanupOneReference, UNSAFE);
    }

    // Page p holds ids [p * PAGE_SIZE, (p + 1) * PAGE_SIZE); a null page has only zero values.
    // We assume that pages can only be added, never removed or replaced.
    // piggyback volatile visibility is expected for readers who execute getCount method to see recent writes
    private long[][] pages = NO_PAGES;

    // The value of metricId in one thread's pages, 0 when its page does not exist
    private static long valueOf(long[][] pages, int metricId)
    {
        int p = metricId >>> PAGE_SHIFT;
        if (p >= pages.length)
            return 0;
        long[] page = pages[p];
        return page == null ? 0 : page[metricId & PAGE_MASK];
    }

    private static void cleanupOneReference() throws InterruptedException
    {
        Object obj = referenceQueue.remove(100);
        if (obj instanceof MetricIdReference)
        {
            ((MetricIdReference) obj).release();
            phantomReferences.remove(obj);
        }
        else if (obj instanceof MetricCleanerReference)
        {
            ((MetricCleanerReference) obj).release();
            phantomReferences.remove(obj);
        }
    }

    private static class MetricIdReference extends PhantomReference<Object>
    {
        private final int metricId;

        public MetricIdReference(Object referent, ReferenceQueue<? super Object> q, int metricId)
        {
            super(referent, q);
            this.metricId = metricId;
        }

        public void release()
        {
            recycleMetricId(metricId);
        }
    }

    private static class MetricCleanerReference extends PhantomReference<Object>
    {
        private final MetricCleaner metricCleaner;

        public MetricCleanerReference(Object referent, ReferenceQueue<? super Object> q, MetricCleaner metricCleaner)
        {
            super(referent, q);
            this.metricCleaner = metricCleaner;
        }

        public void release()
        {
            metricCleaner.clean();
        }
    }

    interface MetricCleaner
    {
        void clean();
    }

    static void destroyWhenUnreachable(Object referent, int metricId)
    {
        phantomReferences.add(new MetricIdReference(referent, referenceQueue, metricId));
    }

    static void destroyWhenUnreachable(Object referent, MetricCleaner metricCleaner)
    {
        phantomReferences.add(new MetricCleanerReference(referent, referenceQueue, metricCleaner));
    }

    @VisibleForTesting
    public static void shutdownCleaner(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdownAndWait(timeout, unit, of(cleaner));
    }

    public void release()
    {
        // Using this lock while moving we want to avoid races with readers in getCount
        // such races can cause a transfered value lost or its double-counting by a reader
        Lock lock = summaryLock.writeLock();
        lock.lock();
        try
        {
            // we may try to release ThreadLocalMetrics 2 times: onRemoval and by PhantomReference
            // so this if check is needed to avoid a potential double release
            if (allThreadLocalMetrics.remove(this))
            {
                long[][] current = pages;
                for (int p = 0; p < current.length; p++)
                {
                    long[] page = current[p];
                    if (page == null)
                        continue;
                    for (int i = 0; i < PAGE_SIZE; i++)
                    {
                        if (page[i] != 0)
                            updateSummary((p << PAGE_SHIFT) + i, page[i]);
                    }
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * If we already have ThreadLocalMetrics instance looked up for the current thread
     * we can use this method to avoid thread local lookup costs.
     * It can be used if you need to update several counters at the same time.
     * @param metricId metric to add a value
     * @param n valuen to add, can be negative number as well
     */
    public void addNonStatic(int metricId, long n)
    {
        page(metricId)[metricId & PAGE_MASK] += n;
    }

    public static void add(int metricId, long n)
    {
        ThreadLocalMetrics.get().page(metricId)[metricId & PAGE_MASK] += n;
    }

    private static long getCount(int metricId, boolean resetToZero)
    {
        long result;
        Lock readLock = summaryLock.readLock();
        readLock.lock();
        try
        {
            result = getSummaryValue(metricId);
            // a thread's page for metricId exists once the thread reported a value for it
            for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
                result += valueOf(threadLocalMetrics.pages, metricId);
            if (resetToZero)
                updateSummary(metricId, -result); // compensative reset without writing to thread local values
        }
        finally
        {
            readLock.unlock();
        }
        return result;
    }

    // must be executed under summaryLock
    private static long getSummaryValue(int metricId)
    {
        return summaryValues.get(metricId);
    }

    // must be executed under summaryLock
    private static void updateSummary(int metricId, long value)
    {
        summaryValues.getAndAdd(metricId, value);
    }

    public static long getCount(int metricId)
    {
        return getCount(metricId, false);
    }

    public static long getCountAndReset(int metricId)
    {
        return getCount(metricId, true);
    }

    public static ThreadLocalMetrics get()
    {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CassandraThread)
            return ((CassandraThread)currentThread).getThreadLocalMetrics();
        return threadLocalMetricsCurrent.get();
    }

    // The page holding metricId, allocated on first use. Only the owning thread calls this.
    private long[] page(int metricId)
    {
        long[][] current = pages;
        int p = metricId >>> PAGE_SHIFT;
        if (p < current.length)
        {
            long[] page = current[p];
            if (page != null)
                return page;
        }
        return addPage(p);
    }

    private long[] addPage(int p)
    {
        // to avoid a race condition with a metric value reset within recycleMetricId logic
        synchronized (this)
        {
            long[][] current = pages;
            if (p >= current.length)
            {
                long[][] grown = new long[calculateNewCapacity(p)][];
                System.arraycopy(current, 0, grown, 0, current.length);
                current = grown;
            }
            long[] page = new long[PAGE_SIZE];
            current[p] = page;
            pages = current;
            return page;
        }
    }

    private static int calculateNewCapacity(int metricId)
    {
        return Math.max(metricId + 1, (int)(metricId * 1.1) );
    }

    static int allocateMetricId()
    {
        int metricId = freeMetricIdSetTracker.getFreeMetricId();
        if (metricId < 0)
            metricId = idGenerator.getAndIncrement();

        if (metricId >= summaryValues.length()) // double-checked locking
        {
            Lock lock = summaryLock.writeLock();
            lock.lock();
            try
            {
                if (metricId >= summaryValues.length())
                {
                    AtomicLongArray newSummaryValues = new AtomicLongArray(calculateNewCapacity(metricId));
                    for (int i = 0; i < summaryValues.length(); i++)
                        newSummaryValues.set(i, summaryValues.get(i));
                    summaryValues = newSummaryValues;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return metricId;
    }

    static void recycleMetricId(int metricId)
    {
        // we use lock here to avoid potential issues when a metric is releasing and a thread is detected as dead at the same time
        // in this case we may clean a summary value and later the thread removal logic may re-add a non-zero summary value
        Lock lock = summaryLock.writeLock();
        lock.lock();
        try
        {
            for (ThreadLocalMetrics threadLocalMetrics : allThreadLocalMetrics)
            {
                // to avoid a race condition with a page addition by a metric updating thread within addPage
                synchronized (threadLocalMetrics)
                {
                    long[][] current = threadLocalMetrics.pages;
                    int p = metricId >>> PAGE_SHIFT;
                    if (p < current.length && current[p] != null)
                        current[p][metricId & PAGE_MASK] = 0;
                }
            }
            summaryValues.set(metricId, 0);
        }
        finally
        {
            lock.unlock();
        }

        freeMetricIdSetTracker.markAsFree(metricId);
    }

    @VisibleForTesting
    static class FreeMetricIdSetTracker
    {
        private final BitSet freeMetricIdSet = new BitSet();

        private final BitSet tickDelayedToFreeMetricIdSet = new BitSet();
        private final BitSet tockDelayedToFreeMetricIdSet = new BitSet();

        private BitSet delayedToFreeMetricIdSet = tickDelayedToFreeMetricIdSet;

        private ScheduledFuture<?> cleanupTask;

        @VisibleForTesting
        synchronized void triggerRecycling()
        {
            cleanupTask = null;
            BitSet toProcess = otherSet(delayedToFreeMetricIdSet);
            freeMetricIdSet.or(toProcess);
            toProcess.clear();
            if (!delayedToFreeMetricIdSet.isEmpty())
                scheduleCleanupTask();
            delayedToFreeMetricIdSet = toProcess;
        }

        private BitSet otherSet(BitSet set)
        {
            return set == tickDelayedToFreeMetricIdSet ? tockDelayedToFreeMetricIdSet : tickDelayedToFreeMetricIdSet;
        }

        public synchronized int getFreeMetricId()
        {
            int metricId = freeMetricIdSet.nextSetBit(0);
            if (metricId >= 0)
                freeMetricIdSet.clear(metricId);
            return metricId;
        }

        public synchronized void markAsFree(int metricId)
        {
            // there's no an obvious happens-before relation between currentCounterValues[metricId] = 0 write we just did
            // and an initial read of the entry by a thread which updates the reused metric
            // as a workaround we introduce a delay in recyling to provide the write visibility in practice
            //  even if it is not formally guaranteed by the JMM
            delayedToFreeMetricIdSet.set(metricId);
            scheduleCleanupTask();
        }

        // must be called while holding this monitor (from a synchronized method)
        @VisibleForTesting
        protected void scheduleCleanupTask()
        {
            try
            {
                if (cleanupTask == null)
                    cleanupTask = ScheduledExecutors.scheduledTasks.schedule(this::triggerRecycling, 5, TimeUnit.SECONDS);
            }
            catch (RejectedExecutionException e)
            {
               // ignore theoretically possible rejections during a shutdown
            }
        }

        public synchronized int getFreeMetricSetCardinality()
        {
            return freeMetricIdSet.cardinality();
        }

        @Override
        public synchronized String toString()
        {
            return "FreeMetricIdSetTracker{" +
                   "freeMetricIdSet=" + freeMetricIdSet +
                   ", tickDelayedToFreeMetricIdSet=" + tickDelayedToFreeMetricIdSet +
                   ", tockDelayedToFreeMetricIdSet=" + tockDelayedToFreeMetricIdSet +
                   ", delayedToFreeMetricIdSet=" + (delayedToFreeMetricIdSet == tickDelayedToFreeMetricIdSet ? "tick" : "tock") +
                   '}';
        }
    }

    @VisibleForTesting
    static int getAllocatedMetricsCount()
    {
        int freeCount = freeMetricIdSetTracker.getFreeMetricSetCardinality();
        return idGenerator.get() - freeCount;
    }

    @VisibleForTesting
    static int getThreadLocalMetricsObjectsCount()
    {
        return allThreadLocalMetrics.size();
    }

    @VisibleForTesting
    int getCounterCapacity()
    {
        return pages.length * PAGE_SIZE;
    }

    @VisibleForTesting
    int getAllocatedPageCount()
    {
        int count = 0;
        for (long[] page : pages)
            if (page != null)
                count++;
        return count;
    }
}
