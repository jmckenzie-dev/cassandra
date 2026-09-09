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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.management.JMX;
import javax.management.ObjectName;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxHistogramMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AdaptiveJmxHistogramHistoryTest
{
    private Config previous;
    private MBeanWrapper.InstanceMBeanWrapper server;
    private int registration;

    @BeforeClass
    public static void initializeConfiguration()
    {
        if (DatabaseDescriptor.getRawConfig() == null)
        {
            assertFalse(DatabaseDescriptor.getAdaptiveJmxHistogramHistoryEnabled());
            DatabaseDescriptor.setConfig(new Config());
        }
    }

    @Before
    public void setUp()
    {
        previous = DatabaseDescriptor.getRawConfig();
        server = new MBeanWrapper.InstanceMBeanWrapper("adaptive-histogram-history-test");
    }

    @After
    public void tearDown()
    {
        server.close();
        DatabaseDescriptor.setConfig(previous);
    }

    @Test
    public void configurationDefaultsOffAndDoesNotSelectTheRecorderBackend() throws Exception
    {
        assertFalse(new Config().adaptive_jmx_histogram_history_enabled);
        DatabaseDescriptor.setConfig(new Config());
        assertFalse(DatabaseDescriptor.getAdaptiveJmxHistogramHistoryEnabled());
        Files.createDirectories(Paths.get("tmp"));
        Path yaml = Files.createTempFile(Paths.get("tmp"), "adaptive-history-config-", ".yaml");
        try
        {
            for (boolean optimized : new boolean[]{ false, true })
            {
                for (boolean adaptive : new boolean[]{ false, true })
                {
                    Files.writeString(yaml, "optimized_metrics_enabled: " + optimized + '\n'
                                            + "adaptive_jmx_histogram_history_enabled: " + adaptive + '\n');
                    DatabaseDescriptor.setConfig(new YamlConfigurationLoader().loadConfig(yaml.toUri().toURL()));
                    assertEquals(adaptive, DatabaseDescriptor.getAdaptiveJmxHistogramHistoryEnabled());
                    assertEquals(optimized, DatabaseDescriptor.getOptimizedMetricsEnabled());
                    assertEquals(optimized ? CompactDecayingEstimatedHistogramReservoir.class : DecayingEstimatedHistogramReservoir.class,
                                 CassandraMetricsRegistry.createHistogramReservoir(false).getClass());
                }
            }
        }
        finally
        {
            Files.delete(yaml);
        }
    }

    @Test
    public void cumulativeHistogramAndTimerAliasesKeepIndependentExactHistory() throws Exception
    {
        for (boolean timer : new boolean[]{ false, true })
        {
            SnapshotReservoir reservoir = new SnapshotReservoir();
            Views views = timer ? timerViews(new SnapshottingTimer(reservoir)) : histogramViews(new OverrideHistogram(reservoir));
            long[][] snapshots = { {}, { 0, 0, 0 }, { 1, 2, 3 }, { 127, -128, 0 }, { 128, -129, 1 },
                                   { 32767, -32768, 1 }, { 32768, -32769, 2 },
                                   { Integer.MAX_VALUE, Integer.MIN_VALUE, 3 }, { Long.MAX_VALUE, Long.MIN_VALUE, 4 },
                                   { Long.MIN_VALUE, Long.MAX_VALUE, 5 }, { 0, 0, 0 }, { 1 }, {}, { 7, 8, 9, 10 } };
            long[] previousPrimary = null;
            long[] previousAlias = null;
            for (int step = 0; step < snapshots.length; step++)
            {
                long[] now = snapshots[step];
                reservoir.set(now);
                long[] expected = CassandraMetricsRegistry.delta(now, previousPrimary);
                views.assertPrimary(expected);
                previousPrimary = now;
                if (step % 3 == 2 || step == snapshots.length - 1)
                {
                    views.assertAliases(CassandraMetricsRegistry.delta(now, previousAlias));
                    previousAlias = now;
                }
                views.assertPrimary(new long[now.length]);
            }
        }
    }

    @Test
    public void noncumulativeHistogramAndTimerReturnTheCurrentSnapshotEveryTime() throws Exception
    {
        for (boolean timer : new boolean[]{ false, true })
        {
            SnapshotReservoir reservoir = new SnapshotReservoir();
            Views views;
            if (timer)
            {
                views = timerViews(new SnapshottingTimer(reservoir)
                {
                    @Override
                    public boolean isCumulative()
                    {
                        return false;
                    }
                });
            }
            else
            {
                views = histogramViews(new OverrideHistogram(reservoir)
                {
                    @Override
                    public boolean isCumulative()
                    {
                        return false;
                    }
                });
            }
            for (long[] values : new long[][]{ { 1, 2, 3 }, { 128, -129, 0 }, {}, { Long.MAX_VALUE, Long.MIN_VALUE }, { 0 } })
            {
                reservoir.set(values);
                views.assertPrimary(values);
                views.assertPrimary(values);
                views.assertAliases(values);
                views.assertAliases(values);
            }
        }
    }

    @Test
    public void realRecordersAndResetsMatchWithEitherHistogramBackend() throws Exception
    {
        for (boolean optimized : new boolean[]{ false, true })
        {
            configure(false, optimized);
            ClearableReservoir histogramReservoir = CassandraMetricsRegistry.createHistogramReservoir(false);
            ClearableReservoir timerReservoir = CassandraMetricsRegistry.createHistogramReservoir(false);
            ClearableHistogram histogram = new ClearableHistogram(histogramReservoir);
            SnapshottingTimer timer = new SnapshottingTimer(timerReservoir);
            Views histogramViews = histogramViews(histogram);
            Views timerViews = timerViews(timer);
            long[] previousHistogram = null;
            long[] previousTimer = null;
            for (int step = 0; step < 4; step++)
            {
                if (step == 2)
                {
                    histogram.clear();
                    timerReservoir.clear();
                }
                else
                {
                    for (int update = 0; update < 130; update++)
                    {
                        histogram.update(10);
                        timer.update(10, TimeUnit.NANOSECONDS);
                    }
                }
                long[] histogramNow = histogram.getSnapshot().getValues();
                long[] timerNow = timer.getSnapshot().getValues();
                histogramViews.assertPrimary(CassandraMetricsRegistry.delta(histogramNow, previousHistogram));
                histogramViews.assertAliases(CassandraMetricsRegistry.delta(histogramNow, previousHistogram));
                timerViews.assertPrimary(CassandraMetricsRegistry.delta(timerNow, previousTimer));
                timerViews.assertAliases(CassandraMetricsRegistry.delta(timerNow, previousTimer));
                previousHistogram = histogramNow;
                previousTimer = timerNow;
            }
        }
    }

    private Views histogramViews(OverrideHistogram histogram) throws Exception
    {
        return registerViews(histogram, JmxHistogramMBean.class, JmxHistogramMBean::getRecentValues);
    }

    private Views timerViews(SnapshottingTimer timer) throws Exception
    {
        return registerViews(timer, JmxTimerMBean.class, JmxTimerMBean::getRecentValues);
    }

    private <T> Views registerViews(Metric metric, Class<T> type, Function<T, long[]> read) throws Exception
    {
        boolean optimized = DatabaseDescriptor.getOptimizedMetricsEnabled();
        List<Supplier<long[]>> readers = new ArrayList<>();
        int id = registration++;
        for (int view = 0; view < 8; view++)
        {
            configure(view % 4 >= 2, optimized);
            ObjectName name = new ObjectName("test.metrics:type=History,id=" + id + ",view=" + view);
            CassandraMetricsRegistry.Metrics.registerMBean(metric, name, server, false, view >= 4);
            assertTrue(server.isRegistered(name));
            T bean = JMX.newMBeanProxy(server.getMBeanServer(), name, type);
            readers.add(() -> read.apply(bean));
        }
        return new Views(readers);
    }

    private static void configure(boolean adaptive, boolean optimized)
    {
        Config config = new Config();
        config.adaptive_jmx_histogram_history_enabled = adaptive;
        config.optimized_metrics_enabled = optimized;
        DatabaseDescriptor.setConfig(config);
    }

    private static class Views
    {
        private final List<Supplier<long[]>> readers;

        private Views(List<Supplier<long[]>> readers)
        {
            this.readers = readers;
        }

        private void assertPrimary(long[] expected)
        {
            for (int i = 0; i < readers.size(); i += 2)
                assertValues(expected, readers.get(i).get());
        }

        private void assertAliases(long[] expected)
        {
            for (int i = 1; i < readers.size(); i += 2)
                assertValues(expected, readers.get(i).get());
        }

        private static void assertValues(long[] expected, long[] actual)
        {
            assertArrayEquals(expected, actual);
            Arrays.fill(actual, 987654321L);
        }
    }

    /** Supplies controlled bucket counts through the reservoir interface, including overflow and changing lengths. */
    private static class SnapshotReservoir implements ClearableReservoir
    {
        private long[] values = new long[0];

        private void set(long[] values)
        {
            this.values = values.clone();
        }

        public int size()
        {
            return values.length;
        }

        public void update(long value)
        {
            throw new UnsupportedOperationException();
        }

        public Snapshot getSnapshot()
        {
            return new LogLinearHistogram.LogLinearSnapshot(values.clone(), Arrays.stream(values).sum());
        }

        public Snapshot getPercentileSnapshot()
        {
            return getSnapshot();
        }

        public long[] buckets(int length)
        {
            return new long[length];
        }

        public BucketStrategy bucketStrategy()
        {
            return BucketStrategy.none;
        }

        public void clear()
        {
            Arrays.fill(values, 0);
        }
    }
}
