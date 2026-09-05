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
 * Preserves the reference histogram's physical stripes and decay arithmetic while allocating
 * counter pages only when used. Empty observations retain the original decay landmark without
 * allocating counter arrays. Common bucket definitions are shared and copied when exported.
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
    private final PagedBuckets buckets;
    private final MonotonicClock clock;
    private final long landmarkResetIntervalInNs;
    private volatile DecayingBuckets decayingBuckets;

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
        buckets = new PagedBuckets((bucketOffsets.length + 1) * nStripes);
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
        int index = findIndex(bucketOffsets, value);
        int stripe = (int) (Thread.currentThread().getId() & (nStripes - 1));
        int physicalIndex = stripedIndex(index, stripe);
        decaying.values.add(physicalIndex, decaying.forwardDecayWeight(now));
        buckets.add(physicalIndex, 1);
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

    private int stripedIndex(int index, int stripe)
    {
        return ((index * nStripes + stripe) * distributionPrime) % buckets.length;
    }

    private long bucketValue(int index, PagedBuckets values)
    {
        long value = 0;
        for (int stripe = 0; stripe < nStripes; stripe++)
            value += values.get(stripedIndex(index, stripe));
        return value;
    }

    private DecayingBuckets rescaleIfNeeded(long now)
    {
        DecayingBuckets current = decayingBuckets;
        while (now - current.landmark > landmarkResetIntervalInNs)
        {
            double factor = current.forwardDecayWeight(now);
            DecayingBuckets replacement = new DecayingBuckets(now);
            for (int i = 0; i < buckets.length; i++)
                replacement.values.set(i, Math.round(current.values.get(i) / factor));
            if (decayingBucketsUpdater.compareAndSet(this, current, replacement))
                return replacement;
            current = decayingBuckets;
        }
        return current;
    }

    public void clear()
    {
        for (int i = 0; i < buckets.length; i++)
            buckets.set(i, 0);
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
        for (int i = 0; i < size(); i++)
        {
            replacement.values.set(stripedIndex(i, 0), snapshot.decayingBuckets[i]);
            buckets.set(stripedIndex(i, 0), cumulative[i]);
            for (int stripe = 1; stripe < nStripes; stripe++)
            {
                replacement.values.set(stripedIndex(i, stripe), 0);
                buckets.set(stripedIndex(i, stripe), 0);
            }
        }
        decayingBucketsUpdater.set(this, replacement);
    }

    @VisibleForTesting
    int allocatedCounterCells()
    {
        return buckets.allocatedCells() + decayingBuckets.values.allocatedCells();
    }

    private final class DecayingBuckets
    {
        private final long landmark;
        private final PagedBuckets values = new PagedBuckets(buckets.length);

        private DecayingBuckets(long landmark)
        {
            this.landmark = landmark;
        }

        private long forwardDecayWeight(long now)
        {
            return Math.round(Math.exp(TimeUnit.NANOSECONDS.toSeconds(now - landmark) / MEAN_LIFETIME_IN_S));
        }
    }

    private static final class PagedBuckets
    {
        private static final int PAGE_SHIFT = 4;
        private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
        private static final int SPARSE_UPDATE_LIMIT = 64;

        private final int length;
        private volatile SparsePages pages;
        private volatile AtomicLongArray dense;

        private PagedBuckets(int length)
        {
            if (length < 0)
                throw new NegativeArraySizeException(Integer.toString(length));
            this.length = length;
        }

        private long get(int index)
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

        private void add(int index, long value)
        {
            AtomicLongArray full = dense;
            if (full != null)
                full.addAndGet(index, value);
            else
                addSparse(index, value);
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
            if (++current.updates >= SPARSE_UPDATE_LIMIT || current.allocatedCells * 2 >= length)
                promote(current);
        }

        private void set(int index, long value)
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
                if (current.allocatedCells * 2 >= length)
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

        private synchronized int allocatedCells()
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
