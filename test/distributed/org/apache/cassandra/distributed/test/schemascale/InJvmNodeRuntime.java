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

package org.apache.cassandra.distributed.test.schemascale;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.ClusterUtils;

/** One node in a dtest cluster. Config overrides go through the instance config, not system properties. */
public final class InJvmNodeRuntime implements NodeRuntime
{
    private final Map<String, Object> config;
    private Cluster cluster;
    private InJvmSchemaExecutor executor;
    private InJvmMetricsProbe probe;
    private Path logFile;

    public InJvmNodeRuntime(Map<String, Object> config)
    {
        this.config = new LinkedHashMap<>(config);
    }

    @Override
    public void start() throws IOException
    {
        Set<Path> existingLogs = systemLogs();
        cluster = Cluster.build(1).withConfig(c -> {
            c.with(Feature.values());
            config.forEach(c::set);
        }).start();
        executor = new InJvmSchemaExecutor(cluster);
        probe = new InJvmMetricsProbe(cluster);
        Set<Path> created = systemLogs();
        created.removeAll(existingLogs);
        logFile = created.stream().max(Comparator.comparingLong(InJvmNodeRuntime::lastModified)).orElse(null);
    }

    @Override
    public void stopGraceful() throws Exception
    {
        cluster.get(1).nodetool("drain");
        ClusterUtils.stopUnchecked(cluster.get(1));
    }

    @Override
    public void kill()
    {
        ClusterUtils.stopAbrupt(cluster, cluster.get(1));
    }

    @Override
    public void restart()
    {
        cluster.get(1).startup();
    }

    @Override
    public SchemaExecutor executor()
    {
        return executor;
    }

    @Override
    public MetricsProbe probe()
    {
        return probe;
    }

    @Override
    public long pid()
    {
        return ProcessHandle.current().pid();
    }

    @Override
    public Path logFile()
    {
        return logFile;
    }

    // Every node log under the test log root; the log of this cluster is the one that start() adds.
    private static Set<Path> systemLogs()
    {
        Path root = Path.of("build", "test", "logs", CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG.getString());
        Set<Path> logs = new HashSet<>();
        if (!Files.isDirectory(root))
            return logs;
        try (Stream<Path> files = Files.walk(root))
        {
            files.filter(path -> path.getFileName().toString().equals("system.log")).forEach(logs::add);
        }
        catch (IOException e)
        {
            // no logs known
        }
        return logs;
    }

    @Override
    public void close()
    {
        if (cluster == null)
            return;
        try
        {
            cluster.close();
        }
        catch (Throwable t)
        {
            System.err.println("Could not close the cluster: " + t);
        }
        cluster = null;
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
}
