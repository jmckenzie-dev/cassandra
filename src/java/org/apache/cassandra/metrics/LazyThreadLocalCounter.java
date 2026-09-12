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

/** Counter IDs and cleanup state are installed only on the first update. */
public class LazyThreadLocalCounter extends ThreadLocalCounter
{
    private volatile int lazyMetricId = -1;

    public LazyThreadLocalCounter()
    {
        super(-1);
    }

    private synchronized int initialize()
    {
        if (lazyMetricId < 0)
        {
            int allocated = ThreadLocalMetrics.allocateMetricId();
            ThreadLocalMetrics.destroyWhenUnreachable(this, allocated);
            lazyMetricId = allocated;
        }
        return lazyMetricId;
    }

    @Override
    int metricIdForTesting()
    {
        return lazyMetricId;
    }

    @Override
    public void inc()
    {
        inc(1);
    }

    @Override
    public void inc(long n)
    {
        try
        {
            int id = lazyMetricId;
            ThreadLocalMetrics.add(id < 0 ? initialize() : id, n);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }

    @Override
    public void dec()
    {
        inc(-1);
    }

    @Override
    public void dec(long n)
    {
        inc(-n);
    }

    @Override
    public long getCount()
    {
        try
        {
            int id = lazyMetricId;
            return id < 0 ? 0 : ThreadLocalMetrics.getCount(id);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }

    @Override
    public void reset()
    {
        try
        {
            int id = lazyMetricId;
            if (id >= 0)
                ThreadLocalMetrics.getCountAndReset(id);
        }
        finally
        {
            Reference.reachabilityFence(this);
        }
    }
}
