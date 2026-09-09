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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;

/** Shared recording destinations for metrics with no enabled consumer. */
public final class NoOpMetrics
{
    private static final long[] EMPTY = new long[0];
    private static final Snapshot SNAPSHOT = new UniformSnapshot(EMPTY);

    /** Empty snapshots cannot participate in LatencyMetrics' mutable histogram merges. */
    public static final ClearableReservoir RESERVOIR = new ClearableReservoir()
    {
        @Override
        public Snapshot getPercentileSnapshot()
        {
            return SNAPSHOT;
        }

        @Override
        public long[] buckets(int length)
        {
            return EMPTY;
        }

        @Override
        public BucketStrategy bucketStrategy()
        {
            return BucketStrategy.none;
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public void update(long value)
        {
        }

        @Override
        public Snapshot getSnapshot()
        {
            return SNAPSHOT;
        }

        @Override
        public void clear()
        {
        }
    };

    public static final com.codahale.metrics.Counter COUNTER = new NoOpCounter();
    public static final com.codahale.metrics.Meter METER = new NoOpMeter();
    public static final OverrideHistogram HISTOGRAM = new NoOpHistogram();
    public static final SnapshottingTimer TIMER = new NoOpTimer();

    private NoOpMetrics()
    {
    }

    public static boolean isNoOp(Metric metric)
    {
        return metric == COUNTER || metric == METER || metric == HISTOGRAM || metric == TIMER;
    }

    private static final class NoOpCounter extends com.codahale.metrics.Counter implements Counter
    {
        @Override
        public void inc()
        {
        }

        @Override
        public void inc(long value)
        {
        }

        @Override
        public void dec()
        {
        }

        @Override
        public void dec(long value)
        {
        }

        @Override
        public long getCount()
        {
            return 0;
        }
    }

    private static final class NoOpMeter extends OverrideMeter
    {
        private NoOpMeter()
        {
            super(MetricClock.defaultClock());
        }

        @Override
        public void mark()
        {
        }

        @Override
        public void mark(long value)
        {
        }

        @Override
        public long getCount()
        {
            return 0;
        }

        @Override
        public double getMeanRate()
        {
            return 0;
        }

        @Override
        public double getOneMinuteRate()
        {
            return 0;
        }

        @Override
        public double getFiveMinuteRate()
        {
            return 0;
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return 0;
        }
    }

    private static final class NoOpHistogram extends OverrideHistogram
    {
        private NoOpHistogram()
        {
            super(RESERVOIR);
        }

        @Override
        public void update(int value)
        {
        }

        @Override
        public void update(long value)
        {
        }

        @Override
        public long getCount()
        {
            return 0;
        }

        @Override
        public Snapshot getSnapshot()
        {
            return SNAPSHOT;
        }
    }

    private static final class NoOpTimer extends SnapshottingTimer
    {
        private NoOpTimer()
        {
            super(RESERVOIR);
        }

        @Override
        public void update(long duration, TimeUnit unit)
        {
        }

        @Override
        public void update(Duration duration)
        {
        }

        @Override
        public <T> T time(Callable<T> event) throws Exception
        {
            return event.call();
        }

        @Override
        public <T> T timeSupplier(Supplier<T> event)
        {
            return event.get();
        }

        @Override
        public void time(Runnable event)
        {
            event.run();
        }

        @Override
        public long getCount()
        {
            return 0;
        }

        @Override
        public double getMeanRate()
        {
            return 0;
        }

        @Override
        public double getOneMinuteRate()
        {
            return 0;
        }

        @Override
        public double getFiveMinuteRate()
        {
            return 0;
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return 0;
        }

        @Override
        public Snapshot getSnapshot()
        {
            return SNAPSHOT;
        }
    }
}
