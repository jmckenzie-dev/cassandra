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

import java.lang.ref.Reference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.MonotonicClock;

/** First-use counter IDs; rate storage and ticking remain in the eager implementation. */
final class LazyThreadLocalMeter extends ThreadLocalMeter
{
    private volatile int lazyCountMetricId = -1;
    private int lazyUncountedMetricId = -1;

    LazyThreadLocalMeter(MonotonicClock clock)
    {
        super(clock, false);
        registerForTicking();
    }

    @Override
    public void mark(long n)
    {
        try
        {
            int id = lazyCountMetricId;
            if (id < 0)
                id = initialize();
            ThreadLocalMetrics context = ThreadLocalMetrics.get();
            context.addNonStatic(id, n);
            context.addNonStatic(lazyUncountedMetricId, n);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }

    private synchronized int initialize()
    {
        if (lazyCountMetricId < 0)
        {
            int count = ThreadLocalMetrics.allocateMetricId();
            ThreadLocalMetrics.destroyWhenUnreachable(this, count);
            int uncounted = ThreadLocalMetrics.allocateMetricId();
            ThreadLocalMetrics.destroyWhenUnreachable(this, uncounted);
            lazyUncountedMetricId = uncounted;
            // Publish both IDs after their cleanup registrations are installed.
            lazyCountMetricId = count;
        }
        return lazyCountMetricId;
    }

    @Override
    protected long getUncountedAndReset()
    {
        try
        {
            return lazyCountMetricId < 0 ? 0 : ThreadLocalMetrics.getCountAndReset(lazyUncountedMetricId);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }

    @VisibleForTesting
    @Override
    int[] counterIds()
    {
        return new int[] { lazyCountMetricId, lazyUncountedMetricId };
    }

    @Override
    public long getCount()
    {
        try
        {
            int id = lazyCountMetricId;
            return id < 0 ? 0 : ThreadLocalMetrics.getCount(id);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }

}
