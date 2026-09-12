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

import java.nio.file.Paths;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.metrics.MetricProfile;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HeapOwnershipCensusHarnessTest
{
    @Test
    public void boundsAllTableCountsAroundTheSupportedRange()
    {
        for (int tables = -10; tables <= 5010; tables++)
        {
            String[] args = { "--tables", Integer.toString(tables) };
            if (tables >= 1 && tables <= 5000)
                assertEquals(tables, HeapOwnershipCensusHarness.Config.parse(args).tables);
            else
                assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void stockModeRejectsBranchOnlyOptions()
    {
        assertFalse(HeapOwnershipCensusHarness.Config.parse(new String[0]).stock);
        assertTrue(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--stock", "--tables", "5000" }).stock);
        for (String option : new String[]{ "--adaptive-jmx-history", "--compact-jmx-registration" })
            assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--stock", option }))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--stock", "--metrics-config", "all_metrics.yml" }))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void validatesSubnetRangeAndArguments()
    {
        for (int subnet = -10; subnet <= 270; subnet++)
        {
            String[] args = { "--subnet", Integer.toString(subnet) };
            if (subnet >= 0 && subnet <= 255)
                assertEquals(subnet, HeapOwnershipCensusHarness.Config.parse(args).subnet);
            else
                assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--tables" })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--unknown", "x" })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--tables", "x" })).isInstanceOf(IllegalArgumentException.class);
        HeapOwnershipCensusHarness.Config defaults = HeapOwnershipCensusHarness.Config.parse(new String[0]);
        assertEquals(100, defaults.tables);
        assertNull(defaults.metricsConfig);
        assertTrue(defaults.heapDumps);
        assertTrue(defaults.inspectNameProperties);
        assertFalse(defaults.adaptiveJmxHistory);
        assertFalse(defaults.compactJmxRegistration);
        assertFalse(defaults.propertyQueries);
        assertTrue(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--compact-jmx-registration" }).compactJmxRegistration);
        assertTrue(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--property-queries" }).propertyQueries);
        assertFalse(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--no-heap-dumps" }).heapDumps);
        assertFalse(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--attributes-only" }).inspectNameProperties);
    }

    @Test
    public void selectsMetricsProfileWithoutChangingItsLocation()
    {
        for (String profile : new String[]{ "all_metrics.yml", "simple_metrics.yml", "/config/metrics.yml", "file:/config/metrics.yml" })
            assertEquals(profile, HeapOwnershipCensusHarness.Config.parse(new String[]{ "--metrics-config", profile }).metricsConfig);
        for (String profile : new String[]{ "", " ", "\t\n" })
            assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--metrics-config", profile }))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--metrics-config" }))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void selectsAdaptiveHistoryIndependentlyOfScrapeAndMetricSelection()
    {
        for (String profile : new String[]{ "all_metrics.yml", "simple_metrics.yml" })
        {
            HeapOwnershipCensusHarness.Config adaptive = HeapOwnershipCensusHarness.Config.parse(new String[]{ "--adaptive-jmx-history",
                                                                                                               "--metrics-config", profile });
            assertTrue(adaptive.adaptiveJmxHistory);
            assertTrue(adaptive.inspectNameProperties);
            assertEquals(profile, adaptive.metricsConfig);
            HeapOwnershipCensusHarness.Config attributes = HeapOwnershipCensusHarness.Config.parse(new String[]{ "--metrics-config", profile,
                                                                                                                 "--attributes-only", "--adaptive-jmx-history",
                                                                                                                 "--tables", "17" });
            assertTrue(attributes.adaptiveJmxHistory);
            assertFalse(attributes.inspectNameProperties);
            assertEquals(17, attributes.tables);
            assertEquals(profile, attributes.metricsConfig);
        }
    }

    @Test
    public void derivesExpectedRegistrationCountsFromSelectedProfiles() throws Exception
    {
        MetricProfile all = MetricProfile.load(Paths.get("conf", "all_metrics.yml").toAbsolutePath().toString());
        MetricProfile simple = MetricProfile.load(Paths.get("conf", "simple_metrics.yml").toAbsolutePath().toString());
        for (int tables : new int[]{ 1, 2, 10 })
        {
            Map<String, String> allNames = HeapOwnershipCensusHarness.RegistrationInventory.expectedRegistrations(all, tables);
            Map<String, String> simpleNames = HeapOwnershipCensusHarness.RegistrationInventory.expectedRegistrations(simple, tables);
            assertEquals(249 * tables + 101, allNames.size());
            assertEquals(30 * tables + 33, simpleNames.size());
            assertEquals(allNames, HeapOwnershipCensusHarness.RegistrationInventory.expectedRegistrations(MetricProfile.ALL, tables));
            assertTrue(allNames.entrySet().containsAll(simpleNames.entrySet()));
        }
    }

    @Test
    public void preservesCanonicalAndUnfilteredMetricShapesWithoutLegacyExports() throws Exception
    {
        MetricProfile simple = MetricProfile.load(Paths.get("conf", "simple_metrics.yml").toAbsolutePath().toString());
        Map<String, String> names = HeapOwnershipCensusHarness.RegistrationInventory.expectedRegistrations(simple, 1);
        String table = "org.apache.cassandra.metrics:keyspace=heap_census,name=";
        assertTrue(names.containsKey(table + "AllMemtablesOnHeapDataSize,scope=t000000,type=Table"));
        assertFalse(names.containsKey(table + "AllMemtablesHeapSize,scope=t000000,type=Table"));
        assertFalse(names.containsKey(table + "AllMemtablesHeapSize,scope=t000000,type=ColumnFamily"));
        assertTrue(names.containsKey(table + "ReadLatency,scope=t000000,type=Table"));
        assertFalse(names.containsKey(table + "ReadLatency,scope=t000000,type=ColumnFamily"));
        assertFalse(names.containsKey(table + "ReadRepairRequests,scope=t000000,type=Table"));
        assertTrue(names.containsKey(table + "Contention timeLatency,scope=t000000,type=TrieMemtable"));
        assertTrue(names.containsKey(table + "ReadLatency,type=Keyspace"));
        assertEquals("org.apache.cassandra.metrics.keyspace.ReadLatency.heap_census", names.get(table + "ReadLatency,type=Keyspace"));
        assertTrue(names.containsKey(table + "ReadTotalLatency,type=Keyspace"));
    }

    @Test
    public void recognizesUserKeyspaceWithoutParsingObjectNameProperties()
    {
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census,name=ReadLatency,type=Table"));
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:foo=x,keyspace=heap_census,name=ReadLatency"));
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census_extra,name=ReadLatency"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:otherkeyspace=heap_census,name=ReadLatency"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=system,name=ReadLatency"));
    }
}
