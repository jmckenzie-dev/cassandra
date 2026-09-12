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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.metrics.MetricProfile;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.ThreadLocalMetrics;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.MBeanWrapper;

/** Separates empty table, JMX collection, and synthetic worker metric residency in live heap dumps. */
public final class HeapOwnershipCensusHarness extends ProfiledClusterHarness
{
    private static final String KEYSPACE = "heap_census";
    private static final int WORKERS = 8;
    private static final Set<String> LATENCY_PREFIXES = Set.of("Read", "Write", "Range", "CasPrepare", "CasPropose", "CasCommit",
                                                               "KeyMigration", "AccordRepair", "AccordPostStreamRepair", "ViewSSTableIntervalTree");
    private static final Set<String> DEPRECATED_GAUGES = Set.of("MemtableOnHeapDataSize", "MemtableOffHeapDataSize",
                                                               "AllMemtablesOnHeapDataSize", "AllMemtablesOffHeapDataSize");
    private static final Set<String> TRIE_METRICS = Set.of("Uncontended memtable puts", "Contended memtable puts",
                                                         "Contention timeLatency", "Contention timeTotalLatency",
                                                         "Shard sizes during last flushMin", "Shard sizes during last flushMax",
                                                         "Shard sizes during last flushAvg", "Shard sizes during last flushStdDev",
                                                         "Shard sizes during last flushNumSamples");

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
                                                                                            "parameters", config.stock ? Map.of() : Map.of("lazy_initialization", "true")))));
        node.set("sstable", Map.of("selected_format", "bti"));
        if (config.stock)
            return;
        node.set("cursor_compaction_enabled", false);
        node.set("optimized_metrics_enabled", true);
        node.set("metrics_config_file", config.metricsConfig);
        node.set("adaptive_jmx_histogram_history_enabled", config.adaptiveJmxHistory);
        node.set("compact_jmx_registration_enabled", config.compactJmxRegistration);
    }

    @Override
    protected void postCluster()
    {
        int tables = config.tables;
        boolean stock = config.stock;
        effective = cluster.get(1).callOnInstance(() -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("stock", stock);
            values.put("productionCodeSource", ColumnFamilyStore.class.getProtectionDomain().getCodeSource().getLocation().toString());
            if (!stock)
            {
                values.put("optimizedMetricsEnabled", DatabaseDescriptor.getOptimizedMetricsEnabled());
                values.put("metricsConfigFile", DatabaseDescriptor.getRawConfig().metrics_config_file);
                values.put("adaptiveJmxHistogramHistoryEnabled", DatabaseDescriptor.getRawConfig().adaptive_jmx_histogram_history_enabled);
                values.put("compactJmxRegistrationEnabled", DatabaseDescriptor.getCompactJmxRegistrationEnabled());
            }
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
            {
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + tableName(i) +
                                     " (pk int, c int, v text, PRIMARY KEY (pk,c))" +
                                     " WITH compaction = {'class':'SizeTieredCompactionStrategy'}" +
                                     " AND caching = {'keys':'NONE','rows_per_partition':'NONE'}");
                if ((i + 1) % 500 == 0)
                    System.out.println("Created " + (i + 1) + " tables");
            }
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
        boolean propertyQueries = config.propertyQueries;
        Map<String, Object> result = cluster.get(1).callOnInstance(() -> {
            try
            {
                return scrapeMetrics(inspectNameProperties, propertyQueries);
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

    private static Map<String, Object> scrapeMetrics(boolean inspectNameProperties, boolean propertyQueries) throws Exception
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
        if (propertyQueries)
            result.put("propertyQueries", exercisePropertyQueries(server, names));
        return result;
    }

    private static List<Map<String, Object>> exercisePropertyQueries(MBeanServer server, List<ObjectName> known) throws Exception
    {
        List<Map<String, Object>> measurements = new ArrayList<>();
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemorySupported())
            throw new IllegalStateException("Property-query measurements require thread allocation counters");
        allocation.setThreadAllocatedMemoryEnabled(true);
        for (String text : new String[] { "org.apache.cassandra.metrics:keyspace=" + KEYSPACE + ",scope=" + tableName(0) + ",*",
                                          "org.apache.cassandra.metrics:type=ThreadPools,*",
                                          "org.apache.cassandra.metrics:keyspace=missing,*" })
        {
            ObjectName pattern = new ObjectName(text);
            Set<ObjectName> expected = new HashSet<>();
            for (ObjectName name : known)
                if (pattern.apply(name))
                    expected.add(name);
            long beforeAllocation = allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
            long before = System.nanoTime();
            Set<ObjectName> actual = server.queryNames(pattern, null);
            long elapsed = System.nanoTime() - before;
            long allocated = allocation.getThreadAllocatedBytes(Thread.currentThread().getId()) - beforeAllocation;
            if (!actual.equals(expected))
                throw new IllegalStateException("Property-query membership changed for " + pattern);
            Set<ObjectName> instances = new HashSet<>();
            for (javax.management.ObjectInstance instance : server.queryMBeans(pattern, null))
                instances.add(instance.getObjectName());
            if (!instances.equals(expected))
                throw new IllegalStateException("queryMBeans membership changed for " + pattern);
            for (ObjectName name : actual)
            {
                ObjectName operation = (ObjectName) server.invoke(name, "objectName", null, null);
                if (!operation.equals(name))
                    throw new IllegalStateException("Metric objectName operation changed for " + name);
                operation.getKeyPropertyList();
            }
            measurements.add(Map.of("pattern", text, "matches", actual.size(), "elapsedNanos", elapsed, "allocatedBytes", allocated));
        }
        return measurements;
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
        boolean stock = config.stock;
        Map<String, Object> state = cluster.get(1).callOnInstance(() -> inspectState(tables, activeWorkers, created, stock));
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

    private static Map<String, Object> inspectState(int tables, int activeWorkers, boolean created, boolean stock)
    {
        long metricBeans = MBeanWrapper.instance.getMBeanServer().getMBeanCount();
        int readRepairRequests = stock || DatabaseDescriptor.getMetricProfile().isEnabled(MetricProfile.Scope.TABLE, "ReadRepairRequests") ? activeWorkers : 0;
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
                if (!(cfs.getCurrentMemtable() instanceof TrieMemtable) || (!stock && ((TrieMemtable) cfs.getCurrentMemtable()).isInitialized()))
                    throw new IllegalStateException("Expected uninitialized TrieMemtable: " + cfs.name);
                if (metric.totalRowsRead.getCount() != activeWorkers || metric.readRepairRequests.getCount() != readRepairRequests ||
                    metric.sstablesPerReadHistogram.cf.getCount() != activeWorkers || metric.readLatency.latency.getCount() != activeWorkers)
                    throw new IllegalStateException("Synthetic observations not preserved for " + cfs.name);
            }
            long aggregateCount = (long) tables * activeWorkers;
            if (Keyspace.open(KEYSPACE).metric.sstablesPerReadHistogram.getCount() != aggregateCount ||
                Keyspace.open(KEYSPACE).metric.readLatency.latency.getCount() != aggregateCount)
                throw new IllegalStateException("Synthetic observations not preserved in keyspace aggregates");
        }
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("tables", created ? tables : 0);
        state.put("mBeanCount", metricBeans);
        state.put("activeWorkers", activeWorkers);
        state.put("workerStates", workerStates);
        state.put("distinctWorkerMetricStores", stores.size());
        state.put("verifiedEmptyUserMemtables", created ? tables : 0);
        state.put("verifiedUninitializedTrieMemtables", created && !stock ? tables : 0);
        state.put("userSSTables", 0);
        state.put("attemptedObservationsPerSelectedMetricPerTable", activeWorkers);
        state.put("recordedObservationsPerMetricPerTable", Map.of("TotalRowsRead", activeWorkers,
                                                                  "ReadRepairRequests", readRepairRequests,
                                                                  "SSTablesPerReadHistogram", activeWorkers,
                                                                  "ReadLatency", activeWorkers));
        state.put("observationsPerSelectedKeyspaceAggregate", (long) (created ? tables : 0) * activeWorkers);
        if (created)
            state.putAll(verifyRegistrations(tables, stock));
        return state;
    }

    private static Map<String, Object> verifyRegistrations(int tables, boolean stock)
    {
        Map<String, String> expected = stock ? null : RegistrationInventory.expectedRegistrations(DatabaseDescriptor.getMetricProfile(), tables);
        Set<String> actualMBeans = new HashSet<>();
        try
        {
            for (ObjectName name : MBeanWrapper.instance.getMBeanServer().queryNames(new ObjectName("org.apache.cassandra.metrics:*"), null))
            {
                String canonical = name.getCanonicalName();
                if (hasUserKeyspace(canonical))
                    actualMBeans.add(canonical);
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Could not inspect metric registrations", e);
        }
        if (!stock)
            requireNames("JMX", expected.keySet(), actualMBeans);
        Set<String> actualRegistry = new HashSet<>();
        for (String name : CassandraMetricsRegistry.Metrics.getMetrics().keySet())
        {
            if (name.endsWith('.' + KEYSPACE) || name.contains('.' + KEYSPACE + '.'))
                actualRegistry.add(name);
        }
        if (stock)
        {
            // The upstream all-metrics inventory includes 249 exports per TrieMemtable table and 101 keyspace exports.
            if (actualMBeans.size() != 249 * tables + 101 || actualRegistry.size() != actualMBeans.size())
                throw new IllegalStateException("Unexpected stock registrations: JMX=" + actualMBeans.size() + ", registry=" + actualRegistry.size());
        }
        else
            requireNames("registry", new HashSet<>(expected.values()), actualRegistry);
        return Map.of("verifiedUserMetricMBeans", actualMBeans.size(), "verifiedUserRegistryMetrics", actualRegistry.size());
    }

    private static void requireNames(String kind, Set<String> expected, Set<String> actual)
    {
        if (expected.equals(actual))
            return;
        Set<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        Set<String> unexpected = new TreeSet<>(actual);
        unexpected.removeAll(expected);
        throw new IllegalStateException(kind + " registrations differ: missing=" + missing + ", unexpected=" + unexpected);
    }

    // Keep branch-only method signatures out of the class deserialized by stock nodes.
    static final class RegistrationInventory
    {
        static Map<String, String> expectedRegistrations(MetricProfile profile, int tables)
        {
            Map<String, String> names = new LinkedHashMap<>();
            for (String name : MetricProfile.knownNames(MetricProfile.Scope.KEYSPACE))
            {
                if (profile.isEnabled(MetricProfile.Scope.KEYSPACE, name))
                    addRegistration(names, "Keyspace", name, null);
            }
            for (int i = 0; i < tables; i++)
            {
                String table = tableName(i);
                for (String name : MetricProfile.knownNames(MetricProfile.Scope.TABLE))
                {
                    if (!profile.isEnabled(MetricProfile.Scope.TABLE, name))
                        continue;
                    addRegistration(names, "Table", name, table);
                    boolean latency = LATENCY_PREFIXES.stream().anyMatch(prefix -> name.equals(prefix + "Latency") || name.equals(prefix + "TotalLatency"));
                    if (latency || !profile.includesLegacyAliases())
                        continue;
                    Set<String> aliases = MetricProfile.aliases(MetricProfile.Scope.TABLE, name);
                    if (aliases.isEmpty() || DEPRECATED_GAUGES.contains(name))
                        addRegistration(names, "ColumnFamily", name, table);
                    for (String alias : aliases)
                    {
                        addRegistration(names, "ColumnFamily", alias, table);
                        if (DEPRECATED_GAUGES.contains(name))
                            addRegistration(names, "Table", alias, table);
                    }
                }
                for (String name : TRIE_METRICS)
                    addRegistration(names, "TrieMemtable", name, table);
            }
            return names;
        }
    }

    private static void addRegistration(Map<String, String> names, String type, String name, String table)
    {
        String group = "org.apache.cassandra.metrics";
        String mBean = group + ":keyspace=" + KEYSPACE + ",name=" + name + (table == null ? "" : ",scope=" + table) + ",type=" + type;
        String scope = table == null ? KEYSPACE : KEYSPACE + '.' + table;
        String registryType = type.equals("Keyspace") ? KeyspaceMetrics.TYPE_NAME : type;
        names.put(mBean, new CassandraMetricsRegistry.MetricName(group, registryType, name, scope, mBean).getMetricName());
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
        result.put("stock", config.stock);
        result.put("keyspace", KEYSPACE);
        result.put("subnet", config.subnet);
        result.put("inspectNameProperties", config.inspectNameProperties);
        result.put("metricsConfigFile", config.metricsConfig);
        result.put("adaptiveJmxHistogramHistoryEnabled", config.adaptiveJmxHistory);
        result.put("compactJmxRegistrationEnabled", config.compactJmxRegistration);
        result.put("propertyQueries", config.propertyQueries);
        result.put("effectiveConfiguration", effective);
        result.put("workload", "Synthetic actual table metric updates; no user queries or SSTables; workers activate sequentially");
        result.put("selectedMetrics", List.of("TotalRowsRead", "ReadRepairRequests", "SSTablesPerReadHistogram", "ReadLatency"));
        result.put("disabledIndependentProbe", config.stock ? "none" : "ReadRepairRequests");
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
        boolean adaptiveJmxHistory;
        boolean compactJmxRegistration;
        boolean propertyQueries;
        boolean stock;
        String metricsConfig;
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
                else if (option.equals("--adaptive-jmx-history"))
                    config.adaptiveJmxHistory = true;
                else if (option.equals("--compact-jmx-registration"))
                    config.compactJmxRegistration = true;
                else if (option.equals("--property-queries"))
                    config.propertyQueries = true;
                else if (option.equals("--stock"))
                    config.stock = true;
                else
                {
                    if (++i == args.length)
                        throw new IllegalArgumentException("Missing value for " + option);
                    switch (option)
                    {
                        case "--tables": config.tables = Integer.parseInt(args[i]); break;
                        case "--subnet": config.subnet = Integer.parseInt(args[i]); break;
                        case "--out": config.out = Paths.get(args[i]); break;
                        case "--metrics-config":
                            if (args[i].isBlank())
                                throw new IllegalArgumentException("Metrics configuration must not be blank");
                            config.metricsConfig = args[i];
                            break;
                        default: throw new IllegalArgumentException("Unknown option: " + option);
                    }
                }
            }
            if (config.tables < 1 || config.tables > 5000 || config.subnet < 0 || config.subnet > 255)
                throw new IllegalArgumentException("Require 1..5000 tables and subnet 0..255");
            if (config.stock && (config.metricsConfig != null || config.adaptiveJmxHistory || config.compactJmxRegistration))
                throw new IllegalArgumentException("Stock mode cannot use branch-only metric options");
            return config;
        }
    }
}
