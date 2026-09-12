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

import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;

import javax.management.ObjectName;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import com.sun.management.ThreadMXBean;

import org.apache.commons.io.output.TeeOutputStream;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Paired timing and allocation measurements for cumulative JMX histogram history.
 * Run with .build/sh/ai-benchmark-jmx-history. Each operation includes a fresh
 * snapshot-values array; fixture construction, logging, and registration are untimed.
 * JMX cases use a local MBean server and a supplied reservoir, without transport or decay costs.
 */
public final class AdaptiveJmxHistoryBenchmark
{
    private static volatile long[] observed;

    private AdaptiveJmxHistoryBenchmark()
    {
    }

    public static void main(String[] args) throws Exception
    {
        Path logs = Files.createDirectories(Path.of("logs"));
        Path log = logs.resolve(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))
                                + "-adaptive-jmx-history-benchmark.log");
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        try (OutputStream file = Files.newOutputStream(log))
        {
            PrintStream output = new PrintStream(new TeeOutputStream(originalOut, file), true, StandardCharsets.UTF_8);
            PrintStream error = new PrintStream(new TeeOutputStream(originalErr, file), true, StandardCharsets.UTF_8);
            System.setOut(output);
            System.setErr(error);
            try
            {
                Options options = Options.parse(args);
                DatabaseDescriptor.clientInitialization();
                System.setOut(output);
                System.setErr(error);
                run(options);
                System.out.println("# log=" + log.toAbsolutePath());
            }
            catch (Exception | Error failure)
            {
                failure.printStackTrace(error);
                throw failure;
            }
            finally
            {
                output.flush();
                error.flush();
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
        }
    }

    private static void run(Options options) throws Exception
    {
        java.lang.management.ThreadMXBean platform = ManagementFactory.getThreadMXBean();
        if (!(platform instanceof ThreadMXBean) || !((ThreadMXBean) platform).isThreadAllocatedMemorySupported())
            throw new UnsupportedOperationException("This benchmark requires per-thread allocation counters");
        ThreadMXBean allocations = (ThreadMXBean) platform;
        if (!allocations.isThreadAllocatedMemoryEnabled())
            allocations.setThreadAllocatedMemoryEnabled(true);

        System.out.printf(Locale.ROOT, "# java=%s vm=%s processors=%d max_heap_bytes=%d%n",
                          Runtime.version(), ManagementFactory.getRuntimeMXBean().getVmName(),
                          Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory());
        System.out.println("# jvm_args=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        ManagementFactory.getGarbageCollectorMXBeans().forEach(gc -> System.out.println("# gc=" + gc.getName()));
        System.out.printf(Locale.ROOT, "# iterations=%d warmup_rounds=%d measured_rounds=%d buckets=%d%n",
                          options.iterations, options.warmupRounds, options.rounds, options.buckets);
        System.out.println("# Each operation clones snapshot values, samples the last delta bucket, and publishes the array to a volatile sink.");
        System.out.println("# Full-array equality and returned-array independence checks run outside timing.");
        System.out.println("# Allocation covers the benchmark thread only; JMX is local without RMI or reservoir decay.");
        System.out.println("# Alternate legacy/adaptive execution order. No samples or GC pauses are discarded.");
        System.out.println("sample,path,case,implementation,round,order,ns_per_op,bytes_per_op,checksum");
        System.out.println("summary,path,case,implementation,metric,min,median,max,stddev");
        System.out.println("ratio,path,case,metric,min,median,max,stddev");

        for (String path : options.helperOnly ? new String[]{ "helper" } : new String[]{ "helper", "histogram", "timer" })
        {
            for (Workload workload : Workload.values())
            {
                try (Operation legacy = operation(path, workload, options.buckets, false);
                     Operation adaptive = operation(path, workload, options.buckets, true))
                {
                    verifyPair(legacy, adaptive);
                    for (int round = 0; round < options.warmupRounds; round++)
                        measurePair(legacy, adaptive, allocations, options.iterations, round);

                    double[][] nanos = new double[2][options.rounds];
                    double[][] bytes = new double[2][options.rounds];
                    double[] ratios = new double[options.rounds];
                    for (int round = 0; round < options.rounds; round++)
                    {
                        Sample[] pair = measurePair(legacy, adaptive, allocations, options.iterations, round);
                        for (int implementation = 0; implementation < pair.length; implementation++)
                        {
                            Sample sample = pair[implementation];
                            nanos[implementation][round] = sample.nanos;
                            bytes[implementation][round] = sample.bytes;
                            System.out.printf(Locale.ROOT, "sample,%s,%s,%s,%d,%d,%.3f,%.3f,%d%n",
                                              path, workload, implementation == 0 ? "legacy" : "adaptive", round,
                                              (round + implementation) % 2, sample.nanos, sample.bytes, sample.checksum);
                        }
                        ratios[round] = pair[1].nanos / pair[0].nanos;
                    }
                    for (int implementation = 0; implementation < 2; implementation++)
                    {
                        String prefix = "summary," + path + ',' + workload + ',' + (implementation == 0 ? "legacy" : "adaptive");
                        summarize(prefix + ",ns_per_op", nanos[implementation]);
                        summarize(prefix + ",bytes_per_op", bytes[implementation]);
                    }
                    summarize("ratio," + path + ',' + workload + ",adaptive_over_legacy_ns", ratios);
                }
            }
        }
        observed = null;
    }

    private static void verifyPair(Operation legacy, Operation adaptive) throws Exception
    {
        for (int i = 0; i < 32; i++)
        {
            long[] expected = legacy.next();
            long[] actual = adaptive.next();
            if (!Arrays.equals(expected, actual))
                throw new AssertionError("Benchmark paths return different recent values at operation " + i);
            Arrays.fill(expected, Long.MIN_VALUE);
            Arrays.fill(actual, Long.MAX_VALUE);
        }
        if (!Arrays.equals(legacy.next(), adaptive.next()))
            throw new AssertionError("Returned-array mutation changed subsequent benchmark values");
    }

    private static Sample[] measurePair(Operation legacy, Operation adaptive, ThreadMXBean allocations,
                                        int iterations, int round) throws Exception
    {
        Sample[] samples = new Sample[2];
        int first = round % 2;
        samples[first] = measure(first == 0 ? legacy : adaptive, allocations, iterations);
        samples[1 - first] = measure(first == 0 ? adaptive : legacy, allocations, iterations);
        if (samples[0].checksum != samples[1].checksum)
            throw new AssertionError("Benchmark paths produced different sampled checksums in round " + round);
        return samples;
    }

    private static Sample measure(Operation operation, ThreadMXBean allocations, int iterations) throws Exception
    {
        long thread = Thread.currentThread().getId();
        long checksum = 1;
        long beforeBytes = allocations.getThreadAllocatedBytes(thread);
        long beforeNanos = System.nanoTime();
        for (int i = 0; i < iterations; i++)
        {
            long[] delta = operation.next();
            checksum = 31 * checksum + (delta.length == 0 ? 0 : delta[delta.length - 1]);
            observed = delta;
        }
        long elapsed = System.nanoTime() - beforeNanos;
        long allocated = allocations.getThreadAllocatedBytes(thread) - beforeBytes;
        return new Sample((double) elapsed / iterations, (double) allocated / iterations, checksum);
    }

    private static void summarize(String prefix, double[] values)
    {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        double mean = Arrays.stream(values).average().getAsDouble();
        double variance = Arrays.stream(values).map(value -> (value - mean) * (value - mean)).average().getAsDouble();
        double median = (sorted[(sorted.length - 1) / 2] + sorted[sorted.length / 2]) / 2;
        System.out.printf(Locale.ROOT, "%s,%.3f,%.3f,%.3f,%.3f%n", prefix,
                          sorted[0], median, sorted[sorted.length - 1], Math.sqrt(variance));
    }

    private static Operation operation(String path, Workload workload, int buckets, boolean adaptive) throws Exception
    {
        SequenceReservoir source = new SequenceReservoir(workload.snapshots(buckets));
        if (path.equals("helper"))
            return adaptive ? new AdaptiveOperation(source) : new LegacyOperation(source);
        return new JmxOperation(source, path.equals("timer"), adaptive);
    }

    private interface Operation extends AutoCloseable
    {
        long[] next() throws Exception;

        default void close()
        {
        }
    }

    private static final class LegacyOperation implements Operation
    {
        private final SequenceReservoir source;
        private long[] last;

        private LegacyOperation(SequenceReservoir source)
        {
            this.source = source;
        }

        public long[] next()
        {
            long[] now = source.getSnapshot().getValues();
            long[] result = CassandraMetricsRegistry.delta(now, last);
            last = now;
            return result;
        }
    }

    private static final class AdaptiveOperation implements Operation
    {
        private final SequenceReservoir source;
        private Object last;

        private AdaptiveOperation(SequenceReservoir source)
        {
            this.source = source;
        }

        public long[] next()
        {
            long[] now = source.getSnapshot().getValues();
            long[] result = AdaptiveHistogramHistory.delta(now, last);
            last = AdaptiveHistogramHistory.pack(now, last);
            return result;
        }
    }

    private static final class JmxOperation implements Operation
    {
        private final MBeanWrapper.InstanceMBeanWrapper server = new MBeanWrapper.InstanceMBeanWrapper("jmx-history-benchmark");
        private final ObjectName name;

        private JmxOperation(SequenceReservoir source, boolean timer, boolean adaptive) throws Exception
        {
            name = new ObjectName("org.apache.cassandra.metrics:type=JmxHistoryBenchmark,name=" + (timer ? "Timer" : "Histogram"));
            Config original = DatabaseDescriptor.getRawConfig();
            Config config = new Config();
            config.adaptive_jmx_histogram_history_enabled = adaptive;
            try
            {
                DatabaseDescriptor.setConfig(config);
                Metric metric = timer ? new SnapshottingTimer(source) : new OverrideHistogram(source);
                CassandraMetricsRegistry.Metrics.registerMBean(metric, name, server, false);
                if (!server.isRegistered(name))
                    throw new AssertionError("The benchmark MBean was not registered: " + name);
            }
            catch (Exception | Error failure)
            {
                server.close();
                throw failure;
            }
            finally
            {
                DatabaseDescriptor.setConfig(original);
            }
        }

        public long[] next() throws Exception
        {
            return (long[]) server.getMBeanServer().getAttribute(name, "RecentValues");
        }

        public void close()
        {
            server.close();
        }
    }

    /** Cached snapshots supply independently owned values arrays on each measured read. */
    private static final class SequenceReservoir implements CassandraReservoir
    {
        private final Snapshot[] snapshots;
        private int index;

        private SequenceReservoir(Snapshot[] snapshots)
        {
            this.snapshots = snapshots;
        }

        public Snapshot getSnapshot()
        {
            Snapshot snapshot = snapshots[index];
            if (++index == snapshots.length)
                index = 0;
            return snapshot;
        }

        public Snapshot getPercentileSnapshot()
        {
            return getSnapshot();
        }

        public long[] buckets(int length)
        {
            return BucketStrategy.exp_12_nozero.bucketsWithLength.apply(length);
        }

        public BucketStrategy bucketStrategy()
        {
            return BucketStrategy.exp_12_nozero;
        }

        public int size()
        {
            return snapshots[index].size();
        }

        public void update(long value)
        {
            throw new UnsupportedOperationException("Benchmark samples are supplied at construction");
        }
    }

    private enum Workload
    {
        ZERO_LENGTH(0),
        EMPTY(0),
        BYTE(Byte.MAX_VALUE),
        SHORT(Short.MAX_VALUE),
        INT(Integer.MAX_VALUE),
        LONG((long) Integer.MAX_VALUE + 1),
        WIDEN_RESET(0, Byte.MAX_VALUE, Byte.MAX_VALUE + 1, Short.MAX_VALUE, Short.MAX_VALUE + 1,
                    Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1, 0);

        private final long[] values;

        Workload(long... values)
        {
            this.values = values;
        }

        private Snapshot[] snapshots(int buckets)
        {
            Snapshot[] result = new Snapshot[values.length];
            for (int sample = 0; sample < values.length; sample++)
            {
                long[] data = new long[this == ZERO_LENGTH ? 0 : buckets];
                for (int bucket = 0; bucket < data.length; bucket += 8)
                    data[bucket] = values[sample];
                result[sample] = new UniformSnapshot(data);
            }
            return result;
        }
    }

    private static final class Sample
    {
        private final double nanos;
        private final double bytes;
        private final long checksum;

        private Sample(double nanos, double bytes, long checksum)
        {
            this.nanos = nanos;
            this.bytes = bytes;
            this.checksum = checksum;
        }
    }

    private static final class Options
    {
        private int iterations = 20000;
        private int warmupRounds = 5;
        private int rounds = 9;
        private int buckets = 160;
        private boolean helperOnly;

        private static Options parse(String[] args)
        {
            Options options = new Options();
            for (int i = 0; i < args.length; i++)
            {
                String option = args[i];
                if (option.equals("--helper-only"))
                {
                    options.helperOnly = true;
                    continue;
                }
                if (i + 1 == args.length)
                    throw new IllegalArgumentException("Missing value for " + option);
                int value = Integer.parseInt(args[++i]);
                if (value <= 0)
                    throw new IllegalArgumentException(option + " must be positive");
                switch (option)
                {
                    case "--iterations": options.iterations = value; break;
                    case "--warmup-rounds": options.warmupRounds = value; break;
                    case "--rounds": options.rounds = value; break;
                    case "--buckets": options.buckets = value; break;
                    default: throw new IllegalArgumentException("Unknown option " + option);
                }
            }
            return options;
        }
    }
}
