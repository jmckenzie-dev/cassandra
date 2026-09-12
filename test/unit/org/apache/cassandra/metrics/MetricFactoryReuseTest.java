/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetricFactoryReuseTest
{
    @BeforeClass
    public static void configure()
    {
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(new Config());
    }

    private static CassandraMetricsRegistry.MetricName name(String name)
    {
        return new CassandraMetricsRegistry.MetricName("org.apache.cassandra.metrics", "Table", name, "factory-reuse");
    }

    @Test
    public void repeatedFactoriesAvoidCounterIdsAndRegisterNewAliases()
    {
        CassandraMetricsRegistry.MetricName histogramName = name("Histogram");
        CassandraMetricsRegistry.MetricName timerName = name("Timer");
        CassandraMetricsRegistry.MetricName histogramAlias = name("HistogramAlias");
        CassandraMetricsRegistry.MetricName timerAlias = name("TimerAlias");
        try
        {
            OverrideHistogram histogram = Metrics.histogram(histogramName, false);
            SnapshottingTimer timer = Metrics.timer(timerName);
            int allocated = ThreadLocalMetrics.getAllocatedMetricsCount();
            for (int i = 0; i < 100; i++)
            {
                assertSame(histogram, Metrics.histogram(histogramName, histogramAlias, false));
                assertSame(timer, Metrics.timer(timerName, timerAlias));
            }
            assertEquals(allocated, ThreadLocalMetrics.getAllocatedMetricsCount());
            assertSame(histogram, Metrics.getMetrics().get(histogramAlias.getMetricName()));
            assertSame(timer, Metrics.getMetrics().get(timerAlias.getMetricName()));
        }
        finally
        {
            Metrics.remove(histogramName);
            Metrics.remove(timerName);
            Metrics.remove(histogramAlias);
            Metrics.remove(timerAlias);
        }
    }

    @Test
    public void concurrentFactoriesReturnRegisteredWinner() throws Exception
    {
        CassandraMetricsRegistry.MetricName name = name("ConcurrentHistogram");
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try
        {
            List<Future<OverrideHistogram>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++)
                results.add(executor.submit(() -> { start.await(); return Metrics.histogram(name, false); }));
            start.countDown();
            for (Future<OverrideHistogram> result : results)
                assertSame(results.get(0).get(), result.get());
            assertSame(results.get(0).get(), Metrics.getMetrics().get(name.getMetricName()));
        }
        finally
        {
            executor.shutdownNow();
            Metrics.remove(name);
        }
    }

    @Test
    public void disabledFactoriesKeepIndependentHiddenRecorders()
    {
        Config previous = DatabaseDescriptor.getRawConfig();
        Config simple = new Config();
        simple.metrics_config_file = java.nio.file.Paths.get("conf", "simple_metrics.yml").toAbsolutePath().toString();
        CassandraMetricsRegistry.MetricName histogramName = name("SSTablesPerRangeReadHistogram");
        CassandraMetricsRegistry.MetricName timerName = name("CasPrepareLatency");
        try
        {
            DatabaseDescriptor.setConfig(simple);
            OverrideHistogram first = Metrics.histogram(histogramName, false);
            OverrideHistogram second = Metrics.histogram(histogramName, false);
            SnapshottingTimer firstTimer = Metrics.timer(timerName);
            SnapshottingTimer secondTimer = Metrics.timer(timerName);
            assertNotSame(first, second);
            assertNotSame(firstTimer, secondTimer);
            first.update(1);
            firstTimer.update(1, java.util.concurrent.TimeUnit.MILLISECONDS);
            assertEquals(1, first.getCount());
            assertEquals(0, second.getCount());
            assertEquals(1, firstTimer.getCount());
            assertEquals(0, secondTimer.getCount());
            assertFalse(Metrics.getMetrics().containsKey(histogramName.getMetricName()));
            assertFalse(Metrics.getMetrics().containsKey(timerName.getMetricName()));
        }
        finally
        {
            DatabaseDescriptor.setConfig(previous);
        }
    }

    @Test
    public void incompatibleExistingTypeRetainsFactoryFailure()
    {
        CassandraMetricsRegistry.MetricName name = name("ConflictingType");
        try
        {
            com.codahale.metrics.Counter counter = Metrics.counter(name);
            try
            {
                Metrics.histogram(name, false);
                fail("Expected histogram type conflict");
            }
            catch (ClassCastException expected)
            {
                assertSame(counter, Metrics.getMetrics().get(name.getMetricName()));
            }
            try
            {
                Metrics.timer(name);
                fail("Expected timer type conflict");
            }
            catch (ClassCastException expected)
            {
                assertSame(counter, Metrics.getMetrics().get(name.getMetricName()));
            }
        }
        finally
        {
            Metrics.remove(name);
        }
    }

    @Test
    public void reusedMetricsRestoreMissingAndCustomMBeans()
    {
        CassandraMetricsRegistry.MetricName histogramName = name("RestoredHistogram");
        CassandraMetricsRegistry.MetricName timerName = name("RestoredTimer");
        CassandraMetricsRegistry.MetricName customHistogram = new CassandraMetricsRegistry.MetricName(
        "org.apache.cassandra.metrics", "Table", "RestoredHistogram", "factory-reuse", "factory.reuse:type=Histogram");
        CassandraMetricsRegistry.MetricName customTimer = new CassandraMetricsRegistry.MetricName(
        "org.apache.cassandra.metrics", "Table", "RestoredTimer", "factory-reuse", "factory.reuse:type=Timer");
        try
        {
            OverrideHistogram histogram = Metrics.histogram(histogramName, false);
            SnapshottingTimer timer = Metrics.timer(timerName);
            MBeanWrapper.instance.unregisterMBean(histogramName.getMBeanName(), MBeanWrapper.OnException.THROW);
            MBeanWrapper.instance.unregisterMBean(timerName.getMBeanName(), MBeanWrapper.OnException.THROW);
            assertSame(histogram, Metrics.histogram(histogramName, false));
            assertSame(timer, Metrics.timer(timerName));
            assertTrue(MBeanWrapper.instance.isRegistered(histogramName.getMBeanName()));
            assertTrue(MBeanWrapper.instance.isRegistered(timerName.getMBeanName()));
            assertSame(histogram, Metrics.histogram(customHistogram, false));
            assertSame(timer, Metrics.timer(customTimer));
            assertTrue(MBeanWrapper.instance.isRegistered(customHistogram.getMBeanName()));
            assertTrue(MBeanWrapper.instance.isRegistered(customTimer.getMBeanName()));
        }
        finally
        {
            Metrics.remove(histogramName);
            Metrics.remove(timerName);
            MBeanWrapper.instance.unregisterMBean(customHistogram.getMBeanName(), MBeanWrapper.OnException.IGNORE);
            MBeanWrapper.instance.unregisterMBean(customTimer.getMBeanName(), MBeanWrapper.OnException.IGNORE);
        }
    }
}
