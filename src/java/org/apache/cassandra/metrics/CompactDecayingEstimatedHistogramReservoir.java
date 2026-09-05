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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DecayingBucketsOnlySnapshot;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.MAX_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.MEAN_LIFETIME_IN_S;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.findIndex;

/**
 * Allocates counter pages on first use and additional stripes after update contention.
 * Empty observations retain the original decay landmark without allocating counter arrays.
 * Common bucket definitions are shared and copied when exported.
 */
public final class CompactDecayingEstimatedHistogramReservoir implements ClearableReservoir
{
    private static final int[] DISTRIBUTION_PRIMES = { 17, 19, 23, 29 };
    private static final long[] DEFAULT_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, false);
    private static final long[] DEFAULT_ZERO_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, true);
    private static final long[] LOW_OFFSETS = EstimatedHistogram.newOffsets(LOW_BUCKET_COUNT, false);
    private static final long[] LOW_ZERO_OFFSETS = EstimatedHistogram.newOffsets(LOW_BUCKET_COUNT, true);
    private static final AtomicReferenceFieldUpdater<CompactDecayingEstimatedHistogramReservoir, DecayingBuckets> decayingBucketsUpdater =
        AtomicReferenceFieldUpdater.newUpdater(CompactDecayingEstimatedHistogramReservoir.class, DecayingBuckets.class, "decayingBuckets");

    private final int nStripes;
    private final int distributionPrime;
    private final long[] bucketOffsets;
    private final StripedBuckets buckets;
    private final MonotonicClock clock;
    private final long landmarkResetIntervalInNs;
    private volatile DecayingBuckets decayingBuckets;
    private volatile boolean contended;

    public CompactDecayingEstimatedHistogramReservoir()
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT);
    }

    public CompactDecayingEstimatedHistogramReservoir(boolean considerZeroes)
    {
        this(considerZeroes, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT);
    }

    public CompactDecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, int stripes)
    {
        this(considerZeroes, bucketCount, stripes, MonotonicClock.Global.approxTime, LANDMARK_RESET_INTERVAL_IN_NS);
    }

    public CompactDecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, int stripes,
                                                      MonotonicClock clock, long landmarkResetIntervalInNs)
    {
        assert bucketCount <= MAX_BUCKET_COUNT : "bucket count cannot exceed: " + MAX_BUCKET_COUNT;
        bucketOffsets = offsets(considerZeroes, bucketCount);
        nStripes = stripes;
        this.clock = clock;
        buckets = new StripedBuckets(bucketOffsets.length + 1, nStripes);
        decayingBuckets = new DecayingBuckets(clock.now());
        this.landmarkResetIntervalInNs = landmarkResetIntervalInNs;
        int distributionPrime = 1;
        for (int prime : DISTRIBUTION_PRIMES)
        {
            if (buckets.length % prime != 0)
            {
                distributionPrime = prime;
                break;
            }
        }
        this.distributionPrime = distributionPrime;
    }

    private static long[] offsets(boolean considerZeroes, int bucketCount)
    {
        if (bucketCount == DEFAULT_BUCKET_COUNT)
            return considerZeroes ? DEFAULT_ZERO_OFFSETS : DEFAULT_OFFSETS;
        if (bucketCount == LOW_BUCKET_COUNT)
            return considerZeroes ? LOW_ZERO_OFFSETS : LOW_OFFSETS;
        return EstimatedHistogram.newOffsets(bucketCount, considerZeroes);
    }

    public void update(long value)
    {
        long now = clock.now();
        DecayingBuckets decaying = rescaleIfNeeded(now);
        int index = physicalIndex(findIndex(bucketOffsets, value));
        boolean detectContention = nStripes > 1 && !contended;
        int stripe = detectContention ? 0 : (int) (Thread.currentThread().getId() & (nStripes - 1));
        if (detectContention)
        {
            boolean decayContended = decaying.values.addAndDetectContention(index, decaying.forwardDecayWeight(now));
            boolean cumulativeContended = buckets.addAndDetectContention(index, 1);
            if (decayContended || cumulativeContended)
                contended = true;
        }
        else
        {
            decaying.values.stripe(stripe, true).add(index, decaying.forwardDecayWeight(now));
            buckets.stripe(stripe, true).add(index, 1);
        }
    }

    public int size()
    {
        return bucketOffsets.length + 1;
    }

    public int stripeCount()
    {
        return nStripes;
    }

    public long[] buckets(int length)
    {
        return length == bucketOffsets.length
               ? Arrays.copyOf(bucketOffsets, bucketOffsets.length)
               : EstimatedHistogram.newOffsets(length, bucketOffsets[0] == 0);
    }

    public BucketStrategy bucketStrategy()
    {
        return bucketOffsets[0] == 0 ? BucketStrategy.exp_12 : BucketStrategy.exp_12_nozero;
    }

    public Snapshot getSnapshot()
    {
        long[] decayed = new long[size()];
        long[] cumulative = new long[size()];
        DecayingBuckets decaying = rescaleIfNeeded(clock.now());
        double factor = decaying.forwardDecayWeight(clock.now());
        for (int i = 0; i < size(); i++)
        {
            decayed[i] = Math.round(bucketValue(i, decaying.values) / factor);
            cumulative[i] = bucketValue(i, buckets);
        }
        return new EstimatedHistogramReservoirSnapshot(bucketOffsets, decayed, cumulative, decaying.landmark, this::rebase);
    }

    public Snapshot getPercentileSnapshot()
    {
        long[] decayed = new long[size()];
        DecayingBuckets decaying = rescaleIfNeeded(clock.now());
        double factor = decaying.forwardDecayWeight(clock.now());
        for (int i = 0; i < size(); i++)
            decayed[i] = Math.round(bucketValue(i, decaying.values) / factor);
        return new DecayingBucketsOnlySnapshot(bucketOffsets, decayed);
    }

    private int physicalIndex(int index)
    {
        return (index * distributionPrime) % buckets.length;
    }

    private long bucketValue(int index, StripedBuckets values)
    {
        int physicalIndex = physicalIndex(index);
        long value = values.get(physicalIndex);
        AtomicReferenceArray<PagedBuckets> secondary = values.secondary;
        if (secondary != null)
        {
            for (int i = 0; i < secondary.length(); i++)
            {
                PagedBuckets stripe = secondary.get(i);
                if (stripe != null)
                    value += stripe.get(physicalIndex);
            }
        }
        return value;
    }

    private DecayingBuckets rescaleIfNeeded(long now)
    {
        DecayingBuckets current = decayingBuckets;
        while (now - current.landmark > landmarkResetIntervalInNs)
        {
            double factor = current.forwardDecayWeight(now);
            DecayingBuckets replacement = new DecayingBuckets(now);
            for (int stripe = 0; stripe < nStripes; stripe++)
            {
                PagedBuckets source = current.values.stripe(stripe, false);
                if (source == null)
                    continue;
                for (int i = 0; i < buckets.length; i++)
                {
                    long value = Math.round(source.get(i) / factor);
                    if (value != 0)
                        replacement.values.stripe(stripe, true).set(i, value);
                }
            }
            if (decayingBucketsUpdater.compareAndSet(this, current, replacement))
                return replacement;
            current = decayingBuckets;
        }
        return current;
    }

    public void clear()
    {
        buckets.clear();
        decayingBucketsUpdater.set(this, new DecayingBuckets(clock.now()));
    }

    /** As with the reference implementation, callers must exclude concurrent updates during rebase. */
    private void rebase(EstimatedHistogramReservoirSnapshot snapshot)
    {
        if (size() != snapshot.decayingBuckets.length)
            throw new IllegalStateException("Unable to merge two DecayingEstimatedHistogramReservoirs with different bucket sizes");
        if (!Arrays.equals(bucketOffsets, snapshot.bucketOffsets))
            throw new IllegalStateException("Merge is only supported with equal bucketOffsets");
        DecayingBuckets replacement = new DecayingBuckets(snapshot.getSnapshotLandmark());
        long[] cumulative = snapshot.getValues();
        buckets.clear();
        for (int i = 0; i < size(); i++)
        {
            replacement.values.set(physicalIndex(i), snapshot.decayingBuckets[i]);
            buckets.set(physicalIndex(i), cumulative[i]);
        }
        decayingBucketsUpdater.set(this, replacement);
    }

    @VisibleForTesting
    int allocatedCounterCells()
    {
        return buckets.totalAllocatedCells() + decayingBuckets.values.totalAllocatedCells();
    }

    @VisibleForTesting
    int allocatedStripeCount()
    {
        int count = 0;
        StripedBuckets decaying = decayingBuckets.values;
        for (int i = 0; i < nStripes; i++)
        {
            PagedBuckets cumulativeStripe = buckets.stripe(i, false);
            PagedBuckets decayingStripe = decaying.stripe(i, false);
            if ((cumulativeStripe != null && cumulativeStripe.allocatedCells() > 0)
                || (decayingStripe != null && decayingStripe.allocatedCells() > 0))
                count++;
        }
        return count;
    }

    @VisibleForTesting
    boolean isContended()
    {
        return contended;
    }

    @VisibleForTesting
    long[] decayingStripeValues(long value)
    {
        StripedBuckets current = decayingBuckets.values;
        int index = physicalIndex(findIndex(bucketOffsets, value));
        long[] counts = new long[nStripes];
        for (int stripe = 0; stripe < nStripes; stripe++)
        {
            PagedBuckets values = current.stripe(stripe, false);
            if (values != null)
                counts[stripe] = values.get(index);
        }
        return counts;
    }

    private final class DecayingBuckets
    {
        private final long landmark;
        private final StripedBuckets values = new StripedBuckets(buckets.length, nStripes);

        private DecayingBuckets(long landmark)
        {
            this.landmark = landmark;
        }

        private long forwardDecayWeight(long now)
        {
            return Math.round(Math.exp(TimeUnit.NANOSECONDS.toSeconds(now - landmark) / MEAN_LIFETIME_IN_S));
        }
    }

    private static final class StripedBuckets extends PagedBuckets
    {
        private static final AtomicReferenceFieldUpdater<StripedBuckets, AtomicReferenceArray> secondaryUpdater =
            AtomicReferenceFieldUpdater.newUpdater(StripedBuckets.class, AtomicReferenceArray.class, "secondary");

        private final int stripes;
        private volatile AtomicReferenceArray<PagedBuckets> secondary;

        private StripedBuckets(int length, int stripes)
        {
            super(length);
            this.stripes = stripes;
        }

        private PagedBuckets stripe(int stripe, boolean create)
        {
            if (stripe == 0)
                return this;
            AtomicReferenceArray<PagedBuckets> current = secondary;
            if (current == null)
            {
                if (!create)
                    return null;
                current = new AtomicReferenceArray<>(stripes - 1);
                if (!secondaryUpdater.compareAndSet(this, null, current))
                    current = secondary;
            }
            PagedBuckets values = current.get(stripe - 1);
            if (values == null && create)
            {
                values = new PagedBuckets(length);
                if (!current.compareAndSet(stripe - 1, null, values))
                    values = current.get(stripe - 1);
            }
            return values;
        }

        private void clear()
        {
            for (int stripe = 0; stripe < stripes; stripe++)
            {
                PagedBuckets values = stripe(stripe, false);
                if (values != null)
                {
                    for (int i = 0; i < length; i++)
                        values.set(i, 0);
                }
            }
        }

        private int totalAllocatedCells()
        {
            int count = allocatedCells();
            AtomicReferenceArray<PagedBuckets> current = secondary;
            if (current != null)
            {
                for (int i = 0; i < current.length(); i++)
                {
                    PagedBuckets values = current.get(i);
                    if (values != null)
                        count += values.allocatedCells();
                }
            }
            return count;
        }
    }

    private static class PagedBuckets
    {
        private static final int PAGE_SHIFT = 4;
        private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
        private static final int SPARSE_UPDATE_LIMIT = 64;

        final int length;
        private volatile SparsePages pages;
        private volatile AtomicLongArray dense;

        private PagedBuckets(int length)
        {
            if (length < 0)
                throw new NegativeArraySizeException(Integer.toString(length));
            this.length = length;
        }

        final long get(int index)
        {
            AtomicLongArray full = dense;
            if (full != null)
                return full.get(index);
            SparsePages current = pages;
            if (current == null)
            {
                // Promotion publishes dense before removing the sparse directory.
                full = dense;
                return full == null ? 0 : full.get(index);
            }
            AtomicLongArray page = current.get(index >>> PAGE_SHIFT);
            return page == null ? 0 : page.get(index & (PAGE_SIZE - 1));
        }

        final void add(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full != null)
                full.addAndGet(index, value);
            else
                addSparse(index, value);
        }

        final boolean addAndDetectContention(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full == null)
            {
                addSparse(index, value);
                return false;
            }
            long previous = full.get(index);
            if (full.compareAndSet(index, previous, previous + value))
                return false;
            // Keep this event on its original stripe; only later events change routing.
            full.addAndGet(index, value);
            return true;
        }

        private synchronized void addSparse(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full != null)
            {
                full.addAndGet(index, value);
                return;
            }
            SparsePages current = sparsePages();
            page(current, index).addAndGet(index & (PAGE_SIZE - 1), value);
            if (++current.updates >= SPARSE_UPDATE_LIMIT || current.allocatedCells * 4 >= length * 3)
                promote(current);
        }

        final void set(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full != null)
                full.set(index, value);
            else
                setSparse(index, value);
        }

        private synchronized void setSparse(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full != null)
            {
                full.set(index, value);
                return;
            }
            if (value != 0)
            {
                SparsePages current = sparsePages();
                page(current, index).set(index & (PAGE_SIZE - 1), value);
                if (current.allocatedCells * 4 >= length * 3)
                    promote(current);
            }
            else
            {
                SparsePages current = pages;
                AtomicLongArray page = current == null ? null : current.get(index >>> PAGE_SHIFT);
                if (page != null)
                    page.set(index & (PAGE_SIZE - 1), 0);
            }
        }

        private SparsePages sparsePages()
        {
            SparsePages current = pages;
            if (current == null)
                pages = current = new SparsePages((length + PAGE_SIZE - 1) >>> PAGE_SHIFT);
            return current;
        }

        private AtomicLongArray page(SparsePages current, int index)
        {
            int pageIndex = index >>> PAGE_SHIFT;
            AtomicLongArray page = current.get(pageIndex);
            if (page == null)
            {
                page = new AtomicLongArray(Math.min(PAGE_SIZE, length - (pageIndex << PAGE_SHIFT)));
                current.set(pageIndex, page);
                current.allocatedCells += page.length();
            }
            return page;
        }

        private void promote(SparsePages current)
        {
            AtomicLongArray full = new AtomicLongArray(length);
            for (int i = 0; i < current.length(); i++)
            {
                AtomicLongArray page = current.get(i);
                if (page != null)
                {
                    for (int j = 0; j < page.length(); j++)
                        full.set((i << PAGE_SHIFT) + j, page.get(j));
                }
            }
            dense = full;
            pages = null;
        }

        final synchronized int allocatedCells()
        {
            return dense != null ? length : pages == null ? 0 : pages.allocatedCells;
        }

        private static final class SparsePages extends AtomicReferenceArray<AtomicLongArray>
        {
            private static final long serialVersionUID = 1L;
            private int allocatedCells;
            private int updates;

            private SparsePages(int length)
            {
                super(length);
            }
        }
    }
}
