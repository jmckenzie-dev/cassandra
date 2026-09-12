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

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.management.JMX;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxHistogramMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.service.reads.PercentileSpeculativeRetryPolicy;
import org.apache.cassandra.utils.EstimatedHistogram;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompactMetricsIntegrationTest
{
    private Config previous;

    @Before
    public void saveConfiguration()
    {
        previous = DatabaseDescriptor.getRawConfig();
    }

    @After
    public void restoreConfiguration()
    {
        DatabaseDescriptor.setConfig(previous);
    }

    @Test
    public void bootstrapUsesLegacyUntilConfigurationIsAvailable()
    {
        DatabaseDescriptor.setConfig(null);
        assertFalse(DatabaseDescriptor.getOptimizedMetricsEnabled());
        ClearableReservoir bootstrap = CassandraMetricsRegistry.createHistogramReservoir(false);
        assertEquals(DecayingEstimatedHistogramReservoir.class, bootstrap.getClass());

        DatabaseDescriptor.setConfig(new Config());
        assertTrue(DatabaseDescriptor.getOptimizedMetricsEnabled());
        assertEquals(CompactDecayingEstimatedHistogramReservoir.class,
                     CassandraMetricsRegistry.createHistogramReservoir(false).getClass());
        assertEquals(DecayingEstimatedHistogramReservoir.class, bootstrap.getClass());
    }

    @Test
    public void resettingConfigurationAlsoResetsMetricProfile()
    {
        Config config = new Config();
        config.metrics_config_file = "org/apache/cassandra/metrics/test-metrics-profile.yml";
        DatabaseDescriptor.setConfig(config);
        assertFalse(DatabaseDescriptor.getMetricProfile().isEnabled(MetricProfile.Scope.TABLE, "WriteLatency"));

        DatabaseDescriptor.setConfig(null);
        assertSame(MetricProfile.ALL, DatabaseDescriptor.getMetricProfile());
        assertFalse(DatabaseDescriptor.getOptimizedMetricsEnabled());
    }

    @Test
    public void yamlDefaultAndExplicitSelectionReachFactory() throws Exception
    {
        DatabaseDescriptor.setConfig(loadYaml("{}\n"));
        assertTrue(DatabaseDescriptor.getOptimizedMetricsEnabled());
        assertEquals(CompactDecayingEstimatedHistogramReservoir.class,
                     CassandraMetricsRegistry.createHistogramReservoir(false).getClass());

        for (boolean optimized : new boolean[]{ false, true })
        {
            DatabaseDescriptor.setConfig(loadYaml("optimized_metrics_enabled: " + optimized + '\n'));
            assertEquals(optimized, DatabaseDescriptor.getOptimizedMetricsEnabled());
            Class<?> expected = optimized ? CompactDecayingEstimatedHistogramReservoir.class : DecayingEstimatedHistogramReservoir.class;
            for (boolean zeroes : new boolean[]{ false, true })
                assertEquals(expected, CassandraMetricsRegistry.createHistogramReservoir(zeroes).getClass());
            assertEquals(expected, new ThreadLocalTimer().histogram.reservoir.getClass());
        }
    }

    @Test
    public void registeredHistogramsPreserveExportsAndClear() throws Exception
    {
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        for (boolean optimized : new boolean[]{ false, true })
        {
            DatabaseDescriptor.setConfig(loadYaml("optimized_metrics_enabled: " + optimized + '\n'));
            for (boolean zeroes : new boolean[]{ false, true })
            {
                String scope = "compactIntegration" + optimized + zeroes;
                MetricName name = new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, scope).createMetricName("Histogram");
                MetricName alias = new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, scope).createMetricName("HistogramAlias");
                try
                {
                    ClearableHistogram histogram = (ClearableHistogram) registry.histogram(name, alias, zeroes);
                    assertEquals(optimized ? CompactDecayingEstimatedHistogramReservoir.class : DecayingEstimatedHistogramReservoir.class,
                                 histogram.reservoir.getClass());
                    assertSame(histogram, registry.histogram(name, zeroes));
                    JmxHistogramMBean export = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), name.getMBeanName(), JmxHistogramMBean.class);
                    JmxHistogramMBean aliasExport = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), alias.getMBeanName(), JmxHistogramMBean.class);
                    DecayingEstimatedHistogramReservoir reference = new DecayingEstimatedHistogramReservoir(zeroes);
                    long[] values = { 0, 1, 2, 100, 1000, 1000000 };
                    for (long value : values)
                    {
                        histogram.update(value);
                        reference.update(value);
                    }
                    long[] expected = reference.getSnapshot().getValues();
                    assertEquals(values.length, export.getCount());
                    assertEquals(values.length, population(export.values()));
                    assertArrayEquals(expected, export.values());
                    assertArrayEquals(expected, export.rawValues());
                    assertArrayEquals(expected, export.getRecentValues());
                    assertArrayEquals(new long[expected.length], export.getRecentValues());
                    assertArrayEquals(expected, aliasExport.values());
                    assertEquals(reference.bucketStrategy().name(), export.bucketsId());
                    assertArrayEquals(reference.buckets(expected.length), export.rawBuckets(expected.length));

                    histogram.clear();
                    assertEquals(0, histogram.getCount());
                    assertEquals(0, export.getCount());
                    assertArrayEquals(new long[expected.length], export.values());
                    histogram.update(100);
                    reference.clear();
                    reference.update(100);
                    assertEquals(1, export.getCount());
                    assertArrayEquals(reference.getSnapshot().getValues(), export.values());
                }
                finally
                {
                    registry.remove(alias);
                    registry.remove(name);
                }
            }
        }
    }

    @Test
    public void optimizedParentPreservesExactBucketsWhenChildrenAreReleased() throws Exception
    {
        DatabaseDescriptor.setConfig(loadYaml("optimized_metrics_enabled: true\n"));
        LatencyMetrics parent = new LatencyMetrics(new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, "compactParent"), "");
        LatencyMetrics first = new LatencyMetrics(new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, "compactFirst"), "", parent);
        LatencyMetrics second = new LatencyMetrics(new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, "compactSecond"), "", parent);
        boolean firstReleased = false;
        boolean secondReleased = false;
        try
        {
            DecayingEstimatedHistogramReservoir expected = new DecayingEstimatedHistogramReservoir(false,
                                                                                                   DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT,
                                                                                                   DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT);
            for (int i = 0; i < 3; i++)
            {
                first.addNano(1000);
                expected.update(1);
            }
            for (int i = 0; i < 2; i++)
            {
                second.addNano(9000);
                expected.update(9);
            }
            assertEquals(5, parent.latency.getCount());
            assertArrayEquals(expected.getSnapshot().getValues(), parent.latency.getSnapshot().getValues());

            first.release();
            firstReleased = true;
            assertEquals(5, parent.latency.getCount());
            assertEquals(5, population(parent.latency.getSnapshot().getValues()));
            assertArrayEquals(expected.getSnapshot().getValues(), parent.latency.getSnapshot().getValues());

            second.addNano(99000);
            expected.update(99);
            assertArrayEquals(expected.getSnapshot().getValues(), parent.latency.getSnapshot().getValues());
            second.release();
            secondReleased = true;
            assertEquals(6, parent.latency.getCount());
            assertEquals(120, parent.totalLatency.getCount());
            assertArrayEquals(expected.getSnapshot().getValues(), parent.latency.getSnapshot().getValues());
        }
        finally
        {
            if (!firstReleased)
                first.release();
            if (!secondReleased)
                second.release();
            parent.release();
        }
    }

    @Test
    public void speculativeRetryUsesMicrosecondPercentilesFromEitherImplementation() throws Exception
    {
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        long[] offsets = EstimatedHistogram.newOffsets(DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT, false);
        int index = Arrays.binarySearch(offsets, 9000L);
        long expected = offsets[index >= 0 ? index : -index - 1];
        for (boolean optimized : new boolean[]{ false, true })
        {
            DatabaseDescriptor.setConfig(loadYaml("optimized_metrics_enabled: " + optimized + '\n'));
            MetricName name = new DefaultNameFactory(ClientRequestMetrics.TYPE_NAME, "compactRetry" + optimized).createMetricName("Latency");
            try
            {
                SnapshottingTimer timer = registry.timer(name);
                assertEquals(123, PercentileSpeculativeRetryPolicy.NINETY_NINE_P.calculateThreshold(timer, 123));
                for (int i = 0; i < 1000; i++)
                    timer.update(i < 980 ? 1000 : 9000, TimeUnit.MICROSECONDS);
                assertEquals(expected, PercentileSpeculativeRetryPolicy.NINETY_NINE_P.calculateThreshold(timer, 123));
                assertEquals(1000, timer.getCount());
            }
            finally
            {
                registry.remove(name);
            }
        }
    }

    private static Config loadYaml(String yaml) throws Exception
    {
        Files.createDirectories(Paths.get("tmp"));
        Path path = Files.createTempFile(Paths.get("tmp"), "compact-metrics-config-", ".yaml");
        try
        {
            Files.write(path, yaml.getBytes(StandardCharsets.UTF_8));
            return new YamlConfigurationLoader().loadConfig(path.toUri().toURL());
        }
        finally
        {
            Files.delete(path);
        }
    }

    private static long population(long[] values)
    {
        long total = 0;
        for (long value : values)
            total += value;
        return total;
    }
}
