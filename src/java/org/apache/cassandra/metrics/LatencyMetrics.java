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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Snapshot;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics about latencies
 */
public class LatencyMetrics
{
    /** Latency */
    public final LatencyMetricsTimer latency;
    /** Total latency in micro sec */
    public final Counter totalLatency;

    /** parent metrics to replicate any updates to **/
    private List<LatencyMetrics> parents = Collections.emptyList();
    private volatile Set<LatencyMetrics> children = Collections.emptySet();

    protected final MetricNameFactory factory;
    protected final MetricNameFactory aliasFactory;
    protected final String namePrefix;

    private LatencyMetrics()
    {
        factory = null;
        aliasFactory = null;
        namePrefix = "";
        totalLatency = NoOpMetrics.COUNTER;
        latency = new LatencyMetricsTimer((Meter) NoOpMetrics.METER, NoOpMetrics.HISTOGRAM)
        {
            @Override
            public Snapshot getSnapshot()
            {
                return NoOpMetrics.HISTOGRAM.getSnapshot();
            }
        };
    }

    public static LatencyMetrics noop()
    {
        return NoOpHolder.INSTANCE;
    }

    public boolean isRecording()
    {
        return true;
    }

    private static final class NoOpHolder
    {
        private static final LatencyMetrics INSTANCE = new LatencyMetrics()
        {
            @Override
            public void addNano(long nanos)
            {
            }

            @Override
            public void release()
            {
            }

            @Override
            public boolean isRecording()
            {
                return false;
            }
        };
    }

    /**
     * Create LatencyMetrics with given group, type, and scope. Name prefix for each metric will be empty.
     *
     * @param type Type name
     * @param scope Scope
     */
    public LatencyMetrics(String type, String scope)
    {
        this(type, "", scope);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param type Type name
     * @param namePrefix Prefix to append to each metric name
     * @param scope Scope of metrics
     */
    public LatencyMetrics(String type, String namePrefix, String scope)
    {
        this(new DefaultNameFactory(type, scope), namePrefix);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix)
    {
        this(factory, null, namePrefix);
    }

    public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix)
    {
        this.factory = factory;
        this.aliasFactory = aliasFactory;
        this.namePrefix = namePrefix;

        LatencyMetricsTimer timer = new LatencyMetrics.LatencyMetricsTimer(CassandraMetricsRegistry.createReservoir(TimeUnit.MICROSECONDS));
        Counter counter = org.apache.cassandra.config.CassandraRelevantProperties.LAZY_METRIC_IDS.getBoolean()
                          ? new LazyLatencyMetricsCounter() : new LatencyMetricsCounter();

        if (aliasFactory == null)
        {
            latency = Metrics.register(factory.createMetricName(namePrefix + "Latency"), timer);
            totalLatency = Metrics.register(factory.createMetricName(namePrefix + "TotalLatency"), counter);
        }
        else
        {
            latency = Metrics.register(factory.createMetricName(namePrefix + "Latency"), timer, aliasFactory.createMetricName(namePrefix + "Latency"));
            totalLatency = Metrics.register(factory.createMetricName(namePrefix + "TotalLatency"), counter, aliasFactory.createMetricName(namePrefix + "TotalLatency"));
        }
    }
    
    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.  Any updates
     * to this will also run on parent
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     * @param parents any amount of parents to replicate updates to
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix, LatencyMetrics ... parents)
    {
        this(factory, null, namePrefix);
        this.parents = Arrays.asList(parents);
        for (LatencyMetrics parent : parents)
        {
            parent.addChildren(this);
        }
    }

    private synchronized void addChildren(LatencyMetrics latencyMetric)
    {
        if (!isRecording())
            return;
        if (children.isEmpty())
            children = ConcurrentHashMap.newKeySet();
        this.children.add(latencyMetric);
    }

    private synchronized void removeChildren(LatencyMetrics toRelease)
    {
        if (!children.contains(toRelease))
            return;
        /*
        Merge details of removed children metrics and add them to our local copy to prevent metrics from going
        backwards. Synchronized since these methods are not thread safe to prevent multiple simultaneous removals.
        Will not protect against simultaneous updates, but since these methods are used by linked parent instances only,
        they should not receive any updates.
         */
        this.latency.releasedLatencyCount += toRelease.latency.getCount();

        DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot childSnapshot = (DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot) toRelease.latency.getSnapshot();
        DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot snapshot = this.latency.getSnapshotForRelease();

        snapshot.add(childSnapshot);
        snapshot.rebaseReservoir();

        this.totalLatency.inc(toRelease.totalLatency.getCount());

        // Now we can remove the reference
        this.children.remove(toRelease);
    }

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds. 1 millionth
        latency.update(nanos, TimeUnit.NANOSECONDS);
        totalLatency.inc(nanos / 1000);
    }

    public void release()
    {
        // Notify parent metrics that this metric is being released
        for (LatencyMetrics parent : this.parents)
        {
            parent.removeChildren(this);
        }
        // Aliases are already known to the parent metrics, so we don't need to remove them here.
        Metrics.remove(factory.createMetricName(namePrefix + "Latency"));
        Metrics.remove(factory.createMetricName(namePrefix + "TotalLatency"));
    }

    public class LatencyMetricsTimer extends ThreadLocalTimer implements org.apache.cassandra.metrics.Timer
    {
        long releasedLatencyCount = 0;

        public LatencyMetricsTimer(CassandraReservoir reservoir)
        {
            super(reservoir);
        }

        private LatencyMetricsTimer(Meter meter, OverrideHistogram histogram)
        {
            super(meter, histogram, MetricClock.defaultClock());
        }

        @Override
        public long getCount()
        {
            long count = super.getCount() + releasedLatencyCount;
            for (LatencyMetrics child : children)
            {
                count += child.latency.getCount();
            }

            return count;
        }

        @Override
        public double getFifteenMinuteRate()
        {
            double rate = super.getFifteenMinuteRate();
            for (LatencyMetrics child : children)
            {
                rate += child.latency.getFifteenMinuteRate();
            }
            return rate;
        }

        @Override
        public double getFiveMinuteRate()
        {
            double rate = super.getFiveMinuteRate();
            for (LatencyMetrics child : children)
            {
                rate += child.latency.getFiveMinuteRate();
            }
            return rate;
        }

        @Override
        public double getMeanRate()
        {
            // Not necessarily 100% accurate, but close enough
            double rate = super.getMeanRate();
            for (LatencyMetrics child : children)
            {
                rate += child.latency.getMeanRate();
            }
            return rate;
        }

        @Override
        public double getOneMinuteRate()
        {
            double rate = super.getOneMinuteRate();
            for (LatencyMetrics child : children)
            {
                rate += child.latency.getOneMinuteRate();
            }
            return rate;
        }

        @Override
        public Snapshot getSnapshot()
        {
            DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot parent = (DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot) super.getSnapshot();
            return withChildren(parent);
        }

        private DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot getSnapshotForRelease()
        {
            // Live children remain attached; merge only the released child's history into our own reservoir.
            return (DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot) super.getSnapshot();
        }

        private DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot withChildren(DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot parent)
        {
            for (LatencyMetrics child : children)
            {
                parent.add(child.latency.getSnapshot());
            }

            return parent;
        }
    }

    class LatencyMetricsCounter extends ThreadLocalCounter
    {
        @Override
        public long getCount()
        {
            long count = super.getCount();
            for (LatencyMetrics child : children)
            {
                count += child.totalLatency.getCount();
            }
            return count;
        }
    }

    class LazyLatencyMetricsCounter extends LazyThreadLocalCounter
    {
        @Override
        public long getCount()
        {
            long count = super.getCount();
            for (LatencyMetrics child : children)
            {
                count += child.totalLatency.getCount();
            }
            return count;
        }
    }
}
