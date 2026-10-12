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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Health of the background ticker of {@link ThreadLocalMeter}.
 * A stalled ticker freezes meter rates and stops removal of cleared meter references, and is otherwise only
 * visible in the log.
 * <p>
 * The counters live in {@link TickerStats}, not in the meter classes, so that reading a gauge never loads a
 * meter class: loading one starts its ticker.
 */
public final class MeterTickerMetrics
{
    public static final String TYPE_NAME = "MeterTicker";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);
    private static boolean registered;

    private MeterTickerMetrics()
    {
    }

    /** Registers the gauges once. Call after the metrics registry is up. */
    public static synchronized void register()
    {
        if (registered)
            return;
        registered = true;

        gauge("RegisteredMeters", () -> TickerStats.THREAD_LOCAL.registered.get());
        gauge("ClearedReferencesRemoved", () -> TickerStats.THREAD_LOCAL.clearedRemoved.get());
        gauge("TickFailures", () -> TickerStats.THREAD_LOCAL.tickFailures.get());
        gauge("LastTickPassMicros", () -> TimeUnit.NANOSECONDS.toMicros(TickerStats.THREAD_LOCAL.lastPassNanos));
    }

    private static void gauge(String name, Gauge<Long> gauge)
    {
        Metrics.register(factory.createMetricName(name), gauge);
    }

    /** Counters for the ticker of {@link ThreadLocalMeter}. */
    static final class TickerStats
    {
        static final TickerStats THREAD_LOCAL = new TickerStats();

        /** Meters waiting for ticks, including ones collected but not yet removed. */
        final AtomicLong registered = new AtomicLong();
        final AtomicLong clearedRemoved = new AtomicLong();
        final AtomicLong tickFailures = new AtomicLong();
        volatile long lastPassNanos;

        private TickerStats()
        {
        }
    }
}
