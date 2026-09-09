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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.metrics;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactJmxRegistrationTest
{
    @BeforeClass
    public static void initialize()
    {
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(new Config());
    }

    @Test
    public void configurationDefaultsOffAndLoadsIndependently() throws Exception
    {
        assertFalse(new Config().compact_jmx_registration_enabled);
        Config previous = DatabaseDescriptor.getRawConfig();
        Files.createDirectories(Paths.get("tmp"));
        Path yaml = Files.createTempFile(Paths.get("tmp"), "compact-jmx-", ".yaml");
        try
        {
            for (boolean compact : new boolean[] { false, true })
            {
                Files.writeString(yaml, "compact_jmx_registration_enabled: " + compact + '\n');
                DatabaseDescriptor.setConfig(new YamlConfigurationLoader().loadConfig(yaml.toUri().toURL()));
                assertEquals(compact, DatabaseDescriptor.getCompactJmxRegistrationEnabled());
                assertFalse(DatabaseDescriptor.getAdaptiveJmxHistogramHistoryEnabled());
                MBeanWrapper.InstanceMBeanWrapper wrapper = new MBeanWrapper.InstanceMBeanWrapper("configured-registration");
                try
                {
                    ObjectName name = new ObjectName("configured:type=Counter");
                    CassandraMetricsRegistry.Metrics.registerMBean(new Counter(), name, wrapper, false);
                    assertEquals(compact, wrapper.getMBeanServer().isInstanceOf(name, TransientMetricMBean.class.getName()));
                }
                finally
                {
                    wrapper.close();
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setConfig(previous);
            Files.delete(yaml);
        }
    }

    @Test
    public void metadataValuesOperationsAndFailuresMatchLegacy() throws Exception
    {
        Counter counter = new Counter();
        counter.inc(17);
        exercise(counter, "Count");
        exercise((Gauge<Object>) () -> null, "Value");
        exercise((Gauge<Object>) () -> "text", "Value");
        exercise((Gauge<Object>) () -> { throw new IllegalStateException("fixture"); }, "Value");
        exercise((Gauge<Object>) () -> { throw new AssertionError("fixture"); }, "Value");
        exercise(new ClearableHistogram(CassandraMetricsRegistry.createHistogramReservoir(false)), "Count");
        exercise(new SnapshottingTimer(CassandraMetricsRegistry.createHistogramReservoir(false)), "Count");
        exercise((Metric) Meter.create(), "Count");
        exercise((Metric) Meter.create(), "Value", true);
    }

    private static void exercise(Metric metric, String attribute) throws Exception
    {
        exercise(metric, attribute, false);
    }

    private static void exercise(Metric metric, String attribute, boolean gaugeCompatible) throws Exception
    {
        try (Views views = new Views(metric, gaugeCompatible))
        {
            assertEquals(views.legacy.getMBeanInfo(views.name), views.compact.getMBeanInfo(views.name));
            assertEquals(views.legacy.getObjectInstance(views.name), views.compact.getObjectInstance(views.name));
            assertEquals(views.legacy.queryMBeans(views.name, null), views.compact.queryMBeans(views.name, null));
            String originalClass = views.legacy.getMBeanInfo(views.name).getClassName();
            assertTrue(views.compact.isInstanceOf(views.name, originalClass));
            assertTrue(views.compact.isInstanceOf(views.name, TransientMetricMBean.class.getName()));
            MBeanAttributeInfo[] attributes = views.compact.getMBeanInfo(views.name).getAttributes();
            for (MBeanAttributeInfo info : attributes)
                views.compare(server -> server.getAttribute(views.name, info.getName()));
            Arrays.fill(attributes, null);
            assertEquals(views.legacy.getMBeanInfo(views.name), views.compact.getMBeanInfo(views.name));
            views.compare(server -> server.getAttribute(views.name, attribute));
            views.compare(server -> server.getAttribute(views.name, "missing"));
            views.compare(server -> server.getAttribute(views.name, null));
            views.compare(server -> server.getAttributes(views.name, new String[] { attribute, "missing", attribute, null }));
            views.compare(server -> server.getAttributes(views.name, null));
            views.compare(server -> { server.setAttribute(views.name, new Attribute(attribute, 9)); return null; });
            views.compare(server -> { server.setAttribute(views.name, new Attribute("missing", 9)); return null; });
            views.compare(server -> { server.setAttribute(views.name, null); return null; });
            AttributeList values = new AttributeList();
            values.add(new Attribute(attribute, 9));
            values.add(new Attribute("missing", 9));
            views.compare(server -> server.setAttributes(views.name, values));
            views.compare(server -> server.setAttributes(views.name, null));
            views.compare(server -> server.invoke(views.name, "objectName", null, null));
            views.compare(server -> server.invoke(views.name, "objectName", new Object[] { 1 }, new String[0]));
            views.compare(server -> server.invoke(views.name, "objectName", new Object[] { "x" }, new String[] { "java.lang.String" }));
            views.compare(server -> server.invoke(views.name, "missing", null, null));
            views.compare(server -> server.invoke(views.name, null, null, null));
            for (String operation : new String[] { "values", "rawValues", "bucketsId" })
                views.compare(server -> server.invoke(views.name, operation, null, null));
            views.compare(server -> server.invoke(views.name, "rawBuckets", new Object[] { 4 }, new String[] { "int" }));
            views.compare(server -> server.invoke(views.name, "rawBuckets", new Object[] { -1 }, new String[] { "int" }));
        }
    }

    @Test
    public void duplicateRegistrationAndRetryAfterRemovalKeepValues() throws Exception
    {
        try (Views views = new Views(new Counter()))
        {
            Counter replacement = new Counter();
            replacement.inc(19);
            for (boolean compact : new boolean[] { false, true })
            {
                MBeanWrapper.InstanceMBeanWrapper wrapper = compact ? views.newWrapper : views.oldWrapper;
                MBeanServer server = wrapper.getMBeanServer();
                CassandraMetricsRegistry.Metrics.registerMBean(replacement, views.name, wrapper, false, compact);
                assertEquals(0L, server.getAttribute(views.name, "Count"));
                server.unregisterMBean(views.name);
                CassandraMetricsRegistry.Metrics.registerMBean(replacement, views.name, wrapper, false, compact);
                assertEquals(19L, server.getAttribute(views.name, "Count"));
            }
        }
    }

    private interface Call
    {
        Object run(MBeanServer server) throws Exception;
    }

    private static Object outcome(Call call, MBeanServer server)
    {
        try
        {
            Object value = call.run(server);
            return value instanceof long[] ? Arrays.toString((long[]) value) : value;
        }
        catch (Throwable failure)
        {
            StringBuilder result = new StringBuilder();
            for (Throwable cause = failure; cause != null; cause = cause.getCause())
                result.append(cause.getClass().getName()).append(':').append(cause.getMessage()).append('\n');
            return result.toString();
        }
    }

    private static class Views implements AutoCloseable
    {
        final MBeanWrapper.InstanceMBeanWrapper oldWrapper = new MBeanWrapper.InstanceMBeanWrapper("legacy-registration");
        final MBeanWrapper.InstanceMBeanWrapper newWrapper = new MBeanWrapper.InstanceMBeanWrapper("compact-registration");
        final MBeanServer legacy = oldWrapper.getMBeanServer();
        final MBeanServer compact = newWrapper.getMBeanServer();
        final ObjectName name;

        Views(Metric metric) throws Exception
        {
            this(metric, false);
        }

        Views(Metric metric, boolean gaugeCompatible) throws Exception
        {
            name = new ObjectName("org.apache.cassandra.metrics:type=Table,keyspace=registration,scope=t,name=Value");
            CassandraMetricsRegistry.Metrics.registerMBean(metric, name, oldWrapper, gaugeCompatible, false);
            CassandraMetricsRegistry.Metrics.registerMBean(metric, new ObjectName(name.toString()), newWrapper, gaugeCompatible, true);
            assertTrue(legacy.isRegistered(name));
            assertTrue(compact.isRegistered(name));
        }

        void compare(Call call)
        {
            assertEquals(outcome(call, legacy), outcome(call, compact));
        }

        public void close()
        {
            oldWrapper.close();
            newWrapper.close();
        }
    }

    public static class Properties
    {
        @BeforeClass
        public static void configure()
        {
            initialize();
        }

        @Test
        public void generatedCounterAndGaugeValues() throws Exception
        {
            for (int seed = 0; seed < 16; seed++)
            {
                Random random = new Random(seed);
                Counter counter = new Counter();
                AtomicReference<Object> gauge = new AtomicReference<>();
                try (Views counters = new Views(counter); Views gauges = new Views((Gauge<Object>) gauge::get))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        counter.inc(random.nextLong());
                        gauge.set(i % 4 == 0 ? null : i % 4 == 1 ? random.nextLong() : i % 4 == 2 ? random.nextDouble() : "v" + random.nextInt());
                        counters.compare(server -> server.getAttribute(counters.name, "Count"));
                        gauges.compare(server -> server.getAttribute(gauges.name, "Value"));
                        assertEquals(counter.getCount(), counters.compact.getAttribute(counters.name, "Count"));
                        assertEquals(gauge.get(), gauges.compact.getAttribute(gauges.name, "Value"));
                    }
                }
            }
        }

        @Test
        public void generatedHistogramTimerAndMeterReads() throws Exception
        {
            for (boolean adaptive : new boolean[] { false, true })
            {
                Config previous = DatabaseDescriptor.getRawConfig();
                Config config = new Config();
                config.adaptive_jmx_histogram_history_enabled = adaptive;
                DatabaseDescriptor.setConfig(config);
                try
                {
                    for (int seed = 0; seed < 8; seed++)
                    {
                        Random random = new Random(seed);
                        ClearableHistogram histogram = new ClearableHistogram(CassandraMetricsRegistry.createHistogramReservoir(false));
                        SnapshottingTimer timer = new SnapshottingTimer(CassandraMetricsRegistry.createHistogramReservoir(false));
                        Meter meter = Meter.create();
                        try (Views histograms = new Views(histogram); Views timers = new Views(timer);
                             Views meters = new Views((Metric) meter); Views compatible = new Views((Metric) meter, true))
                        {
                            for (int step = 0; step < 200; step++)
                            {
                                int value = random.nextInt(100000);
                                histogram.update(value);
                                timer.update(value, TimeUnit.NANOSECONDS);
                                meter.mark(value);
                                for (Views views : new Views[] { histograms, timers, meters, compatible })
                                {
                                    views.compare(server -> server.getAttribute(views.name, "Count"));
                                    views.compare(server -> server.getAttribute(views.name, "RecentValues"));
                                    views.compare(server -> server.getAttribute(views.name, "RateUnit"));
                                    views.compare(server -> server.invoke(views.name, "rawBuckets", new Object[] { 4 }, new String[] { "int" }));
                                }
                            }
                        }
                    }
                }
                finally
                {
                    DatabaseDescriptor.setConfig(previous);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        initialize();
        if (args.length == 1 && args[0].equals("--monitoring"))
        {
            benchmarkMonitoring();
            return;
        }
        Counter counter = new Counter();
        counter.inc(7);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemorySupported())
            throw new IllegalStateException("Thread allocation counters are required");
        allocation.setThreadAllocatedMemoryEnabled(true);
        try (Views views = new Views(counter))
        {
            System.out.println("round,mode,reads,ns_per_read,allocated_bytes_per_read");
            for (int round = 0; round < 3; round++)
            {
                MBeanServer[] order = round % 2 == 0 ? new MBeanServer[] { views.legacy, views.compact }
                                                   : new MBeanServer[] { views.compact, views.legacy };
                for (MBeanServer server : order)
                {
                    for (int i = 0; i < 100000; i++)
                        server.getAttribute(views.name, "Count");
                    long startAllocation = allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
                    long start = System.nanoTime();
                    int reads = 200000;
                    long total = 0;
                    for (int i = 0; i < reads; i++)
                        total += (Long) server.getAttribute(views.name, "Count");
                    long elapsed = System.nanoTime() - start;
                    long allocated = allocation.getThreadAllocatedBytes(Thread.currentThread().getId()) - startAllocation;
                    assertEquals(reads * 7L, total);
                    System.out.println(String.format(java.util.Locale.ROOT, "%d,%s,%d,%d,%d", round,
                                                     server == views.legacy ? "legacy" : "compact", reads,
                                                     elapsed / reads, allocated / reads));
                }
            }
        }
    }

    private static void benchmarkMonitoring() throws Exception
    {
        Config config = new Config();
        config.adaptive_jmx_histogram_history_enabled = true;
        config.optimized_metrics_enabled = true;
        DatabaseDescriptor.setConfig(config);
        MBeanWrapper.InstanceMBeanWrapper wrapper = new MBeanWrapper.InstanceMBeanWrapper("monitoring-benchmark");
        try
        {
            MBeanServer server = wrapper.getMBeanServer();
            List<ObjectName> names = new ArrayList<>();
            for (int i = 0; i < 1000; i++)
            {
                Metric metric;
                switch (i % 5)
                {
                    case 0: metric = new Counter(); break;
                    case 1: metric = (Gauge<Long>) () -> 7L; break;
                    case 2: metric = new ClearableHistogram(CassandraMetricsRegistry.createHistogramReservoir(false)); break;
                    case 3: metric = new SnapshottingTimer(CassandraMetricsRegistry.createHistogramReservoir(false)); break;
                    default: metric = (Metric) Meter.create(); break;
                }
                ObjectName name = new ObjectName("benchmark:type=Table,keyspace=k,scope=t" + i + ",name=Metric");
                CassandraMetricsRegistry.Metrics.registerMBean(metric, name, wrapper, false, true);
                names.add(name);
            }
            List<String[]> attributes = new ArrayList<>();
            for (ObjectName name : names)
                attributes.add(Arrays.stream(server.getMBeanInfo(name).getAttributes()).map(MBeanAttributeInfo::getName).toArray(String[]::new));
            System.out.println("sample,case,operations,ns_per_operation,allocated_bytes_per_operation,result");
            for (int sample = 0; sample < 6; sample++)
            {
                for (int type = 0; type < 5; type++)
                {
                    ObjectName name = names.get(type);
                    String attribute = type == 1 ? "Value" : "Count";
                    measure(sample, "attribute-" + type, 100000, () -> ((Number) server.getAttribute(name, attribute)).longValue());
                }
                measure(sample, "full-scrape", 10, () -> {
                    long count = 0;
                    for (int i = 0; i < names.size(); i++)
                    {
                        for (String attribute : attributes.get(i))
                        {
                            server.getAttribute(names.get(i), attribute);
                            count++;
                        }
                    }
                    return count;
                });
                for (String pattern : new String[] { "benchmark:scope=t0,*", "benchmark:keyspace=missing,*", "benchmark:*" })
                {
                    ObjectName query = new ObjectName(pattern);
                    measure(sample, pattern, 100, () -> server.queryNames(query, null).size());
                }
            }
        }
        finally
        {
            wrapper.close();
        }
    }

    private interface Sample
    {
        long run() throws Exception;
    }

    private static void measure(int sample, String label, int repetitions, Sample operation) throws Exception
    {
        for (int i = 0; i < Math.max(3, repetitions / 10); i++)
            operation.run();
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        long thread = Thread.currentThread().getId();
        long allocatedBefore = allocation.getThreadAllocatedBytes(thread);
        long start = System.nanoTime();
        long result = 0;
        for (int i = 0; i < repetitions; i++)
            result += operation.run();
        long elapsed = System.nanoTime() - start;
        long allocated = allocation.getThreadAllocatedBytes(thread) - allocatedBefore;
        System.out.println(String.format(java.util.Locale.ROOT, "%d,\"%s\",%d,%.3f,%.3f,%d", sample, label, repetitions,
                                         (double) elapsed / repetitions, (double) allocated / repetitions, result));
    }
}
