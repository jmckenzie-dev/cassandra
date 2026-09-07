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

package org.apache.cassandra.distributed.test;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.ThreadLocalMetrics;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.MBeanWrapper;

/** Separates empty table, JMX collection, and synthetic worker metric residency in live heap dumps. */
public final class HeapOwnershipCensusHarness extends ProfiledClusterHarness
{
    private static final String KEYSPACE = "heap_census";
    private static final int WORKERS = 8;

    // This field belongs to the node's isolated class loader, not the harness driver.
    private static Worker[] workers;

    private final Config config;
    private final Map<String, Object> checkpoints = new LinkedHashMap<>();
    private Map<String, Object> effective;

    private HeapOwnershipCensusHarness(Config config)
    {
        super(config.out, "Heap ownership census", "Heap ownership census summary",
              "heap-ownership-" + config.tables + "t", config.args, true, true);
        this.config = config;
    }

    public static void main(String[] args) throws Throwable
    {
        new HeapOwnershipCensusHarness(Config.parse(args)).execute();
    }

    @Override
    protected void configureCluster(Cluster.Builder builder)
    {
        builder.withSubnet(config.subnet);
    }

    @Override
    protected void configureNode(IInstanceConfig node)
    {
        node.set("memtable", Map.of("configurations", Map.of("default", Map.of("class_name", "TrieMemtable",
                                                                                            "parameters", Map.of("lazy_initialization", "true")))));
        node.set("sstable", Map.of("selected_format", "bti"));
        node.set("cursor_compaction_enabled", false);
        node.set("optimized_metrics_enabled", true);
    }

    @Override
    protected void postCluster()
    {
        int tables = config.tables;
        effective = cluster.get(1).callOnInstance(() -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("optimizedMetricsEnabled", DatabaseDescriptor.getOptimizedMetricsEnabled());
            values.put("memtableParameters", DatabaseDescriptor.getMemtableConfigurations().get("default").parameters);
            values.put("heapMaxBytes", Runtime.getRuntime().maxMemory());
            values.put("processors", Runtime.getRuntime().availableProcessors());
            values.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
            HotSpotDiagnosticMXBean diagnostic = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            for (String option : List.of("UseCompressedOops", "UseCompressedClassPointers", "ObjectAlignmentInBytes"))
                values.put(option, diagnostic.getVMOption(option).getValue());
            workers = new Worker[WORKERS];
            for (int i = 0; i < WORKERS; i++)
            {
                workers[i] = new Worker(i, tables);
                workers[i].thread.start();
            }
            return values;
        });
    }

    @Override
    protected List<Phase> definePhases()
    {
        List<Phase> phases = new ArrayList<>();
        phases.add(checkedPhase("00-baseline", phase -> checkpoint("baseline", phase, 0, false)));
        phases.add(checkedPhase("01-create", phase -> {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                                 "{'class':'SimpleStrategy','replication_factor':1}");
            for (int i = 0; i < config.tables; i++)
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + tableName(i) +
                                     " (pk int, c int, v text, PRIMARY KEY (pk,c))" +
                                     " WITH compaction = {'class':'SizeTieredCompactionStrategy'}" +
                                     " AND caching = {'keys':'NONE','rows_per_partition':'NONE'}");
        }));
        phases.add(checkedPhase("02-created", phase -> checkpoint("created", phase, 0, true)));
        phases.add(checkedPhase("03-scrape", phase -> scrape("scraped")));
        phases.add(checkedPhase("04-scraped", phase -> checkpoint("scraped", phase, 0, true)));
        phases.add(checkedPhase("05-worker-1", phase -> activate(0, 1)));
        phases.add(checkedPhase("06-worker-1", phase -> checkpoint("worker-1", phase, 1, true)));
        phases.add(checkedPhase("07-workers-8", phase -> activate(1, WORKERS)));
        phases.add(checkedPhase("08-workers-8", phase -> checkpoint("workers-8", phase, WORKERS, true)));
        phases.add(checkedPhase("09-rescrape", phase -> scrape("rescraped")));
        phases.add(checkedPhase("10-rescraped", phase -> checkpoint("rescraped", phase, WORKERS, true)));
        phases.add(checkedPhase("11-close-workers", phase -> stopWorkers()));
        return phases;
    }

    private Phase checkedPhase(String name, ResourceProfiler.PhaseBody body)
    {
        return new Phase(name, false, phase -> {
            try
            {
                body.run(phase);
            }
            catch (Throwable failure)
            {
                try
                {
                    stopWorkers();
                }
                catch (Throwable cleanupFailure)
                {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }, null);
    }

    private void stopWorkers()
    {
        cluster.get(1).runOnInstance(() -> {
            if (workers == null)
                return;
            for (Worker worker : workers)
            {
                worker.stopping = true;
                worker.start.countDown();
                worker.release.countDown();
            }
            for (Worker worker : workers)
            {
                try
                {
                    worker.thread.join(TimeUnit.SECONDS.toMillis(30));
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted stopping census workers", e);
                }
                if (worker.thread.isAlive())
                    throw new IllegalStateException("Worker did not stop: " + worker.thread.getName());
            }
            workers = null;
        });
    }

    private void activate(int first, int end)
    {
        cluster.get(1).runOnInstance(() -> {
            for (int i = first; i < end; i++)
            {
                workers[i].start.countDown();
                try
                {
                    if (!workers[i].completed.await(60, TimeUnit.SECONDS))
                        throw new IllegalStateException("Worker recording timed out: " + i);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted awaiting census worker", e);
                }
                if (workers[i].failure != null)
                    throw new IllegalStateException("Worker recording failed: " + i, workers[i].failure);
            }
        });
    }

    private void scrape(String name) throws Exception
    {
        boolean inspectNameProperties = config.inspectNameProperties;
        Map<String, Object> result = cluster.get(1).callOnInstance(() -> {
            try
            {
                return scrapeMetrics(inspectNameProperties);
            }
            catch (Exception e)
            {
                throw new IllegalStateException("Could not scrape metric MBeans", e);
            }
        });
        Files.writeString(runDirectory.resolve("scrape-" + name + ".json"), JsonUtils.writeAsJsonString(result),
                          StandardOpenOption.CREATE_NEW);
        System.out.println("JMX scrape " + name + ": " + result);
        if (((Number) result.get("failedAttributes")).longValue() != 0)
            throw new IllegalStateException("Readable metric attributes failed; see scrape-" + name + ".json");
    }

    private static Map<String, Object> scrapeMetrics(boolean inspectNameProperties) throws Exception
    {
        MBeanServer server = MBeanWrapper.instance.getMBeanServer();
        List<ObjectName> names = new ArrayList<>(server.queryNames(new ObjectName("org.apache.cassandra.metrics:*"), null));
        if (inspectNameProperties)
            Collections.sort(names);
        else
            names.sort(Comparator.comparing(ObjectName::getCanonicalName));
        long attributes = 0;
        long recentAttributes = 0;
        long userBeans = 0;
        List<String> failures = new ArrayList<>();
        for (ObjectName name : names)
        {
            boolean userKeyspace = inspectNameProperties ? KEYSPACE.equals(name.getKeyProperty("keyspace"))
                                                        : hasUserKeyspace(name.getCanonicalName());
            if (userKeyspace)
                userBeans++;
            for (MBeanAttributeInfo attribute : server.getMBeanInfo(name).getAttributes())
            {
                if (!attribute.isReadable())
                    continue;
                try
                {
                    server.getAttribute(name, attribute.getName());
                    attributes++;
                    if (attribute.getName().startsWith("Recent"))
                        recentAttributes++;
                }
                catch (Exception e)
                {
                    failures.add(name + " attribute=" + attribute.getName() + " error=" + e + " cause=" + e.getCause());
                }
            }
        }
        if (userBeans == 0 || attributes == 0 || recentAttributes == 0)
            throw new IllegalStateException("Scrape did not cover user metrics and recent-value attributes");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("inspectNameProperties", inspectNameProperties);
        result.put("metricMBeans", names.size());
        result.put("userKeyspaceMBeans", userBeans);
        result.put("successfulAttributes", attributes);
        result.put("recentAttributes", recentAttributes);
        result.put("failedAttributes", failures.size());
        result.put("failures", failures);
        return result;
    }

    static boolean hasUserKeyspace(String canonicalName)
    {
        String property = "keyspace=" + KEYSPACE;
        int offset = canonicalName.indexOf(property);
        int end = offset + property.length();
        return offset > 0 && (canonicalName.charAt(offset - 1) == ':' || canonicalName.charAt(offset - 1) == ',') &&
               (end == canonicalName.length() || canonicalName.charAt(end) == ',');
    }

    private void checkpoint(String name, ResourceProfiler.PhaseResult phase, int activeWorkers, boolean created) throws Exception
    {
        int tables = config.tables;
        Map<String, Object> state = cluster.get(1).callOnInstance(() -> inspectState(tables, activeWorkers, created));
        System.gc();
        TimeUnit.MILLISECONDS.sleep(500);
        profiler.histogram("histogram-" + name + ".txt");
        Path histogram = runDirectory.resolve("histogram-" + name + ".txt");
        if (!Files.isRegularFile(histogram) || !Files.readString(histogram).contains("Total"))
            throw new IllegalStateException("Missing or failed class histogram: " + histogram);
        phase.postGc = profiler.checkpoint();
        state.put("heapUsedBytes", phase.postGc.heapUsed);
        state.put("heapCommittedBytes", phase.postGc.heapCommitted);
        state.put("settledPostGc", true);
        state.put("liveHeapDump", config.heapDumps);
        checkpoints.put(name, state);
        Files.writeString(runDirectory.resolve("checkpoint-" + name + ".json"), JsonUtils.writeAsJsonString(state),
                          StandardOpenOption.CREATE_NEW);
        if (config.heapDumps)
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                             .dumpHeap(runDirectory.resolve(name + ".hprof").toString(), true);
        System.out.println("Checkpoint " + name + ": " + state);
    }

    private static Map<String, Object> inspectState(int tables, int activeWorkers, boolean created)
    {
        long metricBeans = MBeanWrapper.instance.getMBeanServer().getMBeanCount();
        Set<ThreadLocalMetrics> stores = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Map<String, Object>> workerStates = new ArrayList<>();
        for (int i = 0; i < workers.length; i++)
        {
            Worker worker = workers[i];
            if (!worker.thread.isAlive() || worker.failure != null)
                throw new IllegalStateException("Worker not healthy: " + i, worker.failure);
            if (i < activeWorkers)
            {
                if (worker.completed.getCount() != 0 || worker.counters == null || !stores.add(worker.counters))
                    throw new IllegalStateException("Worker lacks distinct completed metric storage: " + i);
            }
            else if (worker.counters != null)
                throw new IllegalStateException("Inactive worker allocated metric storage: " + i);
            workerStates.add(Map.of("name", worker.thread.getName(), "threadId", worker.thread.getId(),
                                    "recordedTables", worker.recordedTables, "hasMetricStorage", worker.counters != null));
        }
        if (created)
        {
            for (int i = 0; i < tables; i++)
            {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName(i));
                TableMetrics metric = cfs.metric;
                if (!cfs.getCurrentMemtable().isClean() || !cfs.getLiveSSTables().isEmpty())
                    throw new IllegalStateException("Unexpected user data: " + cfs.name);
                if (!(cfs.getCurrentMemtable() instanceof TrieMemtable) || ((TrieMemtable) cfs.getCurrentMemtable()).isInitialized())
                    throw new IllegalStateException("Expected uninitialized TrieMemtable: " + cfs.name);
                if (metric.totalRowsRead.getCount() != activeWorkers || metric.readRepairRequests.getCount() != activeWorkers ||
                    metric.sstablesPerReadHistogram.cf.getCount() != activeWorkers || metric.readLatency.latency.getCount() != activeWorkers)
                    throw new IllegalStateException("Synthetic observations not preserved for " + cfs.name);
            }
        }
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("tables", created ? tables : 0);
        state.put("mBeanCount", metricBeans);
        state.put("activeWorkers", activeWorkers);
        state.put("workerStates", workerStates);
        state.put("distinctWorkerMetricStores", stores.size());
        state.put("verifiedEmptyUserMemtables", created ? tables : 0);
        state.put("verifiedUninitializedTrieMemtables", created ? tables : 0);
        state.put("userSSTables", 0);
        state.put("observationsPerSelectedMetricPerTable", activeWorkers);
        return state;
    }

    private static String tableName(int table)
    {
        return String.format(Locale.ROOT, "t%06d", table);
    }

    @Override
    protected Map<String, Object> runParameters()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tables", config.tables);
        result.put("keyspace", KEYSPACE);
        result.put("subnet", config.subnet);
        result.put("inspectNameProperties", config.inspectNameProperties);
        result.put("effectiveConfiguration", effective);
        result.put("workload", "Synthetic actual table metric updates; no user queries or SSTables; workers activate sequentially");
        result.put("selectedMetrics", List.of("TotalRowsRead", "ReadRepairRequests", "SSTablesPerReadHistogram", "ReadLatency"));
        result.put("memoryScope", "Whole JVM; eight workers exist before baseline and remain alive at every checkpoint");
        result.put("checkpoints", checkpoints);
        return result;
    }

    @Override
    protected String profiledArtifactNote()
    {
        return "checkpoint JSON, full JMX scrape reports, class histograms, optional live heap dumps";
    }

    private static final class Worker implements Runnable
    {
        private final int tables;
        private final Thread thread;
        private final CountDownLatch start = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile ThreadLocalMetrics counters;
        private volatile int recordedTables;
        private volatile Throwable failure;
        private volatile boolean stopping;

        private Worker(int id, int tables)
        {
            this.tables = tables;
            thread = new Thread(this, "census-worker-" + id);
            thread.setDaemon(true);
        }

        @Override
        public void run()
        {
            try
            {
                start.await();
                if (stopping)
                    return;
                record();
                completed.countDown();
                release.await();
            }
            catch (Throwable t)
            {
                failure = t;
                completed.countDown();
            }
        }

        private void record()
        {
            for (int i = 0; i < tables; i++)
            {
                TableMetrics metric = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName(i)).metric;
                metric.totalRowsRead.inc();
                metric.readRepairRequests.mark();
                metric.sstablesPerReadHistogram.update(1);
                metric.readLatency.addNano(1000);
                recordedTables++;
            }
            counters = ThreadLocalMetrics.get();
        }
    }

    static final class Config
    {
        int tables = 100;
        int subnet = 0;
        boolean heapDumps = true;
        boolean inspectNameProperties = true;
        Path out = Paths.get("logs");
        String[] args;

        static Config parse(String[] args)
        {
            Config config = new Config();
            config.args = args.clone();
            for (int i = 0; i < args.length; i++)
            {
                String option = args[i];
                if (option.equals("--no-heap-dumps"))
                    config.heapDumps = false;
                else if (option.equals("--attributes-only"))
                    config.inspectNameProperties = false;
                else
                {
                    if (++i == args.length)
                        throw new IllegalArgumentException("Missing value for " + option);
                    switch (option)
                    {
                        case "--tables": config.tables = Integer.parseInt(args[i]); break;
                        case "--subnet": config.subnet = Integer.parseInt(args[i]); break;
                        case "--out": config.out = Paths.get(args[i]); break;
                        default: throw new IllegalArgumentException("Unknown option: " + option);
                    }
                }
            }
            if (config.tables < 1 || config.tables > 1000 || config.subnet < 0 || config.subnet > 255)
                throw new IllegalArgumentException("Require 1..1000 tables and subnet 0..255");
            return config;
        }
    }
}
