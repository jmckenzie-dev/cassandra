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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.JsonUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs the smoke profile without profiling. The node must reach 2000 tables and a 2000-column table
 * without a break point, and steps.csv must show a non-decreasing TCM epoch.
 * <p>
 * The harness runs in a child JVM. The dtest JVM has a 1 GiB heap, which 2000 in-JVM tables do not fit in.
 */
public class SchemaScaleSmokeTest extends TestBaseImpl
{
    private static final String CHILD_HEAP = "-Xmx6g";

    @Test
    public void smokeProfileReachesTargetScale() throws Throwable
    {
        Path runDirectory = Path.of("tmp", "schemascale-smoke-" + System.nanoTime()).toAbsolutePath();
        Files.createDirectories(runDirectory);
        int exitCode = runHarness(runDirectory);
        Path launcherLog = runDirectory.resolve("launcher.log");
        assertEquals("Harness failed or hit a break point; see " + launcherLog, 0, exitCode);

        JsonNode summary = JsonUtils.JSON_OBJECT_MAPPER.readTree(runDirectory.resolve("summary.json").toFile());
        assertEquals("Break points: " + summary.get("breakPoints"), 0, summary.get("breakPoints").size());

        Path steps = runDirectory.resolve("steps.csv");
        assertTrue("steps.csv is missing", Files.isRegularFile(steps));
        List<String> lines = Files.readAllLines(steps);
        List<String> header = List.of(lines.get(0).split(","));
        int epochColumn = header.indexOf("tcm_epoch");
        int tablesColumn = header.indexOf("tables");
        int widthColumn = header.indexOf("max_cols_table");
        assertTrue(epochColumn >= 0 && tablesColumn >= 0 && widthColumn >= 0);

        List<Long> epochs = new ArrayList<>();
        long maxTables = 0;
        long maxWidth = 0;
        for (String line : lines.subList(1, lines.size()))
        {
            String[] cells = line.split(",");
            epochs.add(Long.parseLong(cells[epochColumn]));
            maxTables = Math.max(maxTables, Long.parseLong(cells[tablesColumn]));
            maxWidth = Math.max(maxWidth, Long.parseLong(cells[widthColumn]));
        }
        assertFalse(epochs.isEmpty());
        for (int i = 1; i < epochs.size(); i++)
            assertTrue("tcm_epoch went backwards at row " + i + ": " + epochs, epochs.get(i) >= epochs.get(i - 1));
        assertTrue("tables reached " + maxTables, maxTables >= 2000);
        assertTrue("widest table reached " + maxWidth, maxWidth >= 2000);
        assertEquals("One row per restart", 2, Files.readAllLines(runDirectory.resolve("restart.csv")).size() - 1);
    }

    private static int runHarness(Path runDirectory) throws Exception
    {
        List<String> command = new ArrayList<>();
        command.add(Path.of(CassandraRelevantProperties.JAVA_HOME.getString(), "bin", "java").toString());
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments())
        {
            if (!argument.startsWith("-Xmx"))
                command.add(argument);
        }
        command.add(CHILD_HEAP);
        command.add("-cp");
        command.add(CassandraRelevantProperties.JAVA_CLASS_PATH.getString());
        command.add(SchemaScaleHarness.class.getName());
        command.addAll(List.of("--profile", "smoke", "--variant", "stock", "--spread", "1", "--fail-on-break",
                               "--run-dir", runDirectory.toString(), "--set", "profiling.asyncProfiler=false"));

        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                                                     .redirectOutput(runDirectory.resolve("launcher.log").toFile())
                                                     .start();
        if (!process.waitFor(25, TimeUnit.MINUTES))
        {
            process.destroyForcibly();
            throw new AssertionError("Harness did not finish in 25 minutes; see " + runDirectory.resolve("launcher.log"));
        }
        return process.exitValue();
    }
}
