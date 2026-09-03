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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

/**
 * Profiles many-table scale: creates a keyspace and N tables, optionally writes rows, holds steady,
 * and tears down. Each phase records one multi-event async-profiler JFR plus JDK JFR, and checkpoints
 * heap and allocation metrics via {@link ResourceProfiler}. HTML flame graphs are derived from the
 * recorded JFRs post-run with jfrconv (see the post-run hint in summary.txt).
 */
public final class ManyTablesProfileHarness extends ProfiledClusterHarness
{
    private static final String POST_RUN_VIEWS_HINT =
        "post-run views: tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv -o html --alloc <phase>.ap.jfr " +
        "out.html (also --wall, --cpu; requires one-time download of async-profiler 4.2 release tarball)";

    private final Config config;

    private ManyTablesProfileHarness(Config config)
    {
        super(config.outputDirectory, "Many-tables profile run", "Many-tables profiling summary",
              "many-tables-" + config.tables + "t", config.originalArgs, config.noProfile, config.skipCpu);
        this.config = config;
    }

    public static void main(String[] args) throws Throwable
    {
        new ManyTablesProfileHarness(Config.parse(args)).execute();
    }

    @Override
    protected List<Phase> definePhases()
    {
        List<Phase> phases = new ArrayList<>();
        phases.add(new Phase("00-baseline", phase -> {
            TimeUnit.SECONDS.sleep(5);
            System.gc();
            TimeUnit.SECONDS.sleep(2);
            phase.postGc = profiler.checkpoint();
        }));
        phases.add(new Phase("01-create-keyspace", phase -> cluster.schemaChange(String.format(Locale.ROOT,
                                                                                 "CREATE KEYSPACE %s WITH replication = " +
                                                                                 "{'class':'SimpleStrategy','replication_factor':1}",
                                                                                 config.keyspace)),
                             () -> profiler.histogram("histogram-02-before.txt")));
        phases.add(new Phase("02-create-tables", phase -> createTables(),
                             () -> profiler.histogram("histogram-02-after.txt")));
        if (config.writesPerTable > 0)
            phases.add(new Phase("03-writes", phase -> writeRows()));
        phases.add(new Phase("04-steady-hold", phase -> holdSteady(phase),
                             () -> profiler.histogram("histogram-04-after.txt")));
        phases.add(new Phase("05-teardown", phase -> teardownCluster()));
        return phases;
    }

    private void createTables() throws IOException
    {
        try (BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve("create-times.csv"),
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE_NEW,
                                                             StandardOpenOption.WRITE))
        {
            for (int i = 0; i < config.tables; i++)
            {
                long startedAt = System.nanoTime();
                cluster.schemaChange(String.format(Locale.ROOT,
                                                   "CREATE TABLE %s.t%06d " +
                                                   "(pk int, c int, v text, PRIMARY KEY (pk, c))",
                                                   config.keyspace, i));
                writer.write(i + "," + (System.nanoTime() - startedAt));
                writer.newLine();
            }
        }
    }

    private void writeRows()
    {
        for (int table = 0; table < config.tables; table++)
        {
            String query = String.format(Locale.ROOT,
                                         "INSERT INTO %s.t%06d (pk,c,v) VALUES (?,?,?)",
                                         config.keyspace, table);
            for (int row = 0; row < config.writesPerTable; row++)
            {
                cluster.coordinator(1).execute(query, ConsistencyLevel.ONE,
                                               table, row, "v-" + table + '-' + row);
            }
        }
    }

    private void holdSteady(ResourceProfiler.PhaseResult phase) throws Exception
    {
        Path samples = runDirectory.resolve("hold-samples.csv");
        long startedAt = System.nanoTime();
        long durationNanos = TimeUnit.SECONDS.toNanos(config.holdSeconds);
        try (BufferedWriter writer = Files.newBufferedWriter(samples,
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE_NEW,
                                                             StandardOpenOption.WRITE))
        {
            while (System.nanoTime() - startedAt < durationNanos)
            {
                long offsetSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedAt);
                ResourceProfiler.HeapSnapshot sample = profiler.checkpoint();
                writer.write(offsetSeconds + "," + sample.heapUsed + ',' + sample.mBeanCount + ',' +
                             sample.threadAllocatedBytes);
                writer.newLine();
                writer.flush();

                long remaining = durationNanos - (System.nanoTime() - startedAt);
                if (remaining > 0)
                    TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(5), remaining));
            }
        }

        System.gc();
        TimeUnit.SECONDS.sleep(2);
        phase.postGc = profiler.checkpoint();
    }

    private void teardownCluster() throws IOException
    {
        cluster.schemaChange("DROP KEYSPACE " + config.keyspace);
        try
        {
            cluster.get(1).logs().mark();
        }
        catch (Throwable t)
        {
            profiler.warn("Could not locate the node log through the dtest log API: " + t);
        }

        cluster.close();
        cluster = null;
        copySystemLog();
    }

    private void copySystemLog()
    {
        String testTag = CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG.getString();
        Path logRoot = Paths.get("build", "test", "logs", testTag);
        if (!Files.isDirectory(logRoot))
        {
            profiler.warn("Node log directory does not exist: " + logRoot);
            return;
        }

        try (Stream<Path> files = Files.walk(logRoot, FileVisitOption.FOLLOW_LINKS))
        {
            Path systemLog = files.filter(path -> path.getFileName().toString().equals("system.log"))
                                  .max(Comparator.comparingLong(ManyTablesProfileHarness::lastModified))
                                  .orElse(null);
            if (systemLog == null)
            {
                profiler.warn("No system.log file exists under " + logRoot);
                return;
            }
            Files.copy(systemLog, runDirectory.resolve("system.log"), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            profiler.warn("Could not copy the node system.log: " + e);
        }
    }

    private static long lastModified(Path path)
    {
        try
        {
            return Files.getLastModifiedTime(path).toMillis();
        }
        catch (IOException e)
        {
            return Long.MIN_VALUE;
        }
    }

    @Override
    protected Map<String, Object> runParameters()
    {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("tables", config.tables);
        parameters.put("writesPerTable", config.writesPerTable);
        parameters.put("holdSeconds", config.holdSeconds);
        parameters.put("keyspace", config.keyspace);
        parameters.put("postRunViews", POST_RUN_VIEWS_HINT);
        return parameters;
    }

    @Override
    protected void writeSummaryDetails(BufferedWriter writer) throws IOException
    {
        writer.write("Tables: " + config.tables + ", writes/table: " + config.writesPerTable +
                     ", hold seconds: " + config.holdSeconds);
        writer.newLine();
        writer.write(POST_RUN_VIEWS_HINT);
        writer.newLine();
    }

    @Override
    protected String profiledArtifactNote()
    {
        return "jfrconv renders HTML views on demand";
    }

    private static final class Config
    {
        private int tables = 100;
        private int writesPerTable;
        private int holdSeconds = 60;
        private String keyspace = "many_tables_harness";
        private Path outputDirectory = Paths.get("logs");
        private boolean skipCpu;
        private boolean noProfile;
        private String[] originalArgs;

        private static Config parse(String[] args)
        {
            Config config = new Config();
            config.originalArgs = args.clone();
            for (int i = 0; i < args.length; i++)
            {
                switch (args[i])
                {
                    case "--tables":
                        config.tables = parseNonNegative(args, ++i, "--tables");
                        break;
                    case "--writes-per-table":
                        config.writesPerTable = parseNonNegative(args, ++i, "--writes-per-table");
                        break;
                    case "--hold-seconds":
                        config.holdSeconds = parseNonNegative(args, ++i, "--hold-seconds");
                        break;
                    case "--keyspace":
                        config.keyspace = value(args, ++i, "--keyspace");
                        break;
                    case "--out":
                        config.outputDirectory = Paths.get(value(args, ++i, "--out"));
                        break;
                    case "--skip-cpu":
                        config.skipCpu = true;
                        break;
                    case "--no-profile":
                        config.noProfile = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }

            if (!config.keyspace.matches("[A-Za-z_][A-Za-z0-9_]*"))
                throw new IllegalArgumentException("Invalid unquoted keyspace name: " + config.keyspace);
            return config;
        }

        private static int parseNonNegative(String[] args, int index, String option)
        {
            String value = value(args, index, option);
            int parsed = Integer.parseInt(value);
            if (parsed < 0)
                throw new IllegalArgumentException(option + " must be non-negative");
            return parsed;
        }

        private static String value(String[] args, int index, String option)
        {
            if (index >= args.length)
                throw new IllegalArgumentException("Missing value for " + option);
            return args[index];
        }
    }
}
