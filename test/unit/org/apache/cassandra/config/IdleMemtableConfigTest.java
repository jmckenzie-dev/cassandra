/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class IdleMemtableConfigTest
{
    @Test
    public void defaultsAndYamlUnits()
    {
        Config defaults = new Config();
        DatabaseDescriptor.validateIdleMemtableOptions(defaults);
        assertEquals(0, defaults.memtable_idle_timeout.toMilliseconds());
        assertEquals(2, defaults.memtable_idle_flush_max_concurrent);
        assertEquals(100, defaults.memtable_idle_flush_max_per_second);
        assertEquals(16 * 1024 * 1024, defaults.memtable_idle_flush_throughput.toBytesPerSecond(), 0);
        Config configured = YamlConfigurationLoader.fromMap(Map.of("memtable_idle_timeout", "30s",
                                                                  "memtable_idle_flush_max_per_second", 5,
                                                                  "memtable_idle_flush_throughput", "32KiB/s"), Config.class);
        DatabaseDescriptor.validateIdleMemtableOptions(configured);
        assertEquals(30000, configured.memtable_idle_timeout.toMilliseconds());
        assertEquals(5, configured.memtable_idle_flush_max_per_second);
        assertEquals(32768, configured.memtable_idle_flush_throughput.toBytesPerSecond(), 0);
    }

    @Test
    public void invalidSettingsFailEvenWhenIdleFlushingIsDisabled()
    {
        for (int value : new int[] { 0, -1, Integer.MIN_VALUE })
        {
            Config config = new Config();
            config.memtable_idle_flush_max_per_second = value;
            assertThatThrownBy(() -> DatabaseDescriptor.validateIdleMemtableOptions(config)).isInstanceOf(ConfigurationException.class);
            config.memtable_idle_flush_max_per_second = 1;
            config.memtable_idle_flush_max_concurrent = value;
            assertThatThrownBy(() -> DatabaseDescriptor.validateIdleMemtableOptions(config)).isInstanceOf(ConfigurationException.class);
        }
        for (DataRateSpec.LongBytesPerSecondBound rate : new DataRateSpec.LongBytesPerSecondBound[] {
            null, new DataRateSpec.LongBytesPerSecondBound("0B/s")
        })
        {
            Config config = new Config();
            config.memtable_idle_flush_throughput = rate;
            assertThatThrownBy(() -> DatabaseDescriptor.validateIdleMemtableOptions(config)).isInstanceOf(ConfigurationException.class);
        }
        Config config = new Config();
        config.memtable_idle_timeout = null;
        assertThatThrownBy(() -> DatabaseDescriptor.validateIdleMemtableOptions(config)).isInstanceOf(ConfigurationException.class);
    }
}
