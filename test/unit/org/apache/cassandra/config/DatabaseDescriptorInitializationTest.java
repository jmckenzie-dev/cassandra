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
package org.apache.cassandra.config;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class DatabaseDescriptorInitializationTest
{
    @Test
    public void laterInvalidSettingsPreserveTheFirstError()
    {
        DatabaseDescriptor.daemonInitialization();
        Config restoreConfig = new YamlConfigurationLoader().loadConfig();
        List<Consumer<Config>> invalidSettings = Arrays.asList(
            config -> config.auto_snapshot_ttl = "-1s",
            config -> config.commitlog_sync = null,
            config -> config.disk_access_mode = Config.DiskAccessMode.direct,
            config -> config.phi_convict_threshold = 4,
            config -> config.concurrent_reads = 1,
            config -> config.memtable_heap_space = new DataStorageSpec.IntMebibytesBound(0));
        List<String> errorSettings = Arrays.asList("auto_snapshot_ttl", "CommitLogSync", "DiskAccessMode",
                                                 "phi_convict_threshold", "concurrent_reads", "memtable_heap_space");

        try
        {
            qt().withFixedSeed(18424)
                .withExamples(64)
                .forAll(integers().between(0, invalidSettings.size() - 2),
                        integers().between(1, invalidSettings.size() - 1))
                .checkAssert((first, offset) -> {
                    int later = first + 1 + (offset - 1) % (invalidSettings.size() - first - 1);
                    Config singleInvalid = new YamlConfigurationLoader().loadConfig();
                    invalidSettings.get(first).accept(singleInvalid);
                    String expected = initializationError(singleInvalid);
                    assertTrue(expected, expected.contains(errorSettings.get(first)));

                    Config multipleInvalid = new YamlConfigurationLoader().loadConfig();
                    invalidSettings.get(first).accept(multipleInvalid);
                    invalidSettings.get(later).accept(multipleInvalid);
                    assertEquals("First setting " + first + ", later setting " + later,
                                 expected, initializationError(multipleInvalid));
                });
        }
        finally
        {
            // Applied encryption options cannot be initialized a second time.
            DatabaseDescriptor.unsafeDaemonInitialization(() -> restoreConfig);
        }
    }

    private static String initializationError(Config config)
    {
        try
        {
            DatabaseDescriptor.unsafeDaemonInitialization(() -> config);
        }
        catch (ConfigurationException e)
        {
            return e.getMessage();
        }
        fail("Invalid configuration was accepted");
        return null;
    }
}
