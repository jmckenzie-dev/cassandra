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
package org.apache.cassandra.metrics;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.metrics.MetricProfile.Scope.KEYSPACE;
import static org.apache.cassandra.metrics.MetricProfile.Scope.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetricProfileTest
{
    static final String SOURCE = "test-metrics-profile.yml";
    static final List<String> REQUIRED = Collections.unmodifiableList(Arrays.asList("CompressionRatio",
                                                                                   "CoordinatorReadLatency",
                                                                                   "CoordinatorWriteLatency",
                                                                                   "TotalDiskSpaceUsed"));

    @Test
    public void omittedConfigurationEnablesAllMetrics()
    {
        assertSame(MetricProfile.ALL, MetricProfile.load(null));
        for (MetricProfile.Scope scope : MetricProfile.Scope.values())
        {
            for (String name : MetricProfile.knownNames(scope))
                assertTrue(scope + "." + name, MetricProfile.ALL.isEnabled(scope, name));
        }
        assertTrue(MetricProfile.ALL.isEnabled(TABLE, "FutureMetric"));
    }

    @Test
    public void shippedProfilesLoad()
    {
        MetricProfile all = MetricProfile.load(Paths.get("conf", "all_metrics.yml").toAbsolutePath().toString());
        MetricProfile simple = MetricProfile.load(Paths.get("conf", "simple_metrics.yml").toAbsolutePath().toString());
        for (MetricProfile.Scope scope : MetricProfile.Scope.values())
        {
            for (String name : MetricProfile.knownNames(scope))
                assertTrue(scope + "." + name, all.isEnabled(scope, name));
        }

        for (String name : REQUIRED)
            assertTrue(name, simple.isEnabled(TABLE, name));
        assertTrue(simple.isEnabled(TABLE, "ReadLatency"));
        assertTrue(simple.isEnabled(TABLE, "ReadTotalLatency"));
        assertTrue(simple.isEnabled(TABLE, "WriteTotalLatency"));
        assertTrue(simple.isEnabled(KEYSPACE, "ReadTotalLatency"));
        assertTrue(simple.isEnabled(KEYSPACE, "WriteTotalLatency"));
        assertTrue(all.includesLegacyAliases());
        assertFalse(simple.includesLegacyAliases());
        assertFalse(simple.isEnabled(TABLE, "CoordinatorScanLatency"));
        assertFalse(simple.isEnabled(TABLE, "SSTablesPerRangeReadHistogram"));
        assertTrue(simple.isEnabled(KEYSPACE, "SSTablesPerRangeReadHistogram"));
    }

    @Test
    public void loadsProfileFromClasspath()
    {
        MetricProfile profile = MetricProfile.load("org/apache/cassandra/metrics/test-metrics-profile.yml");
        for (String name : REQUIRED)
            assertTrue(name, profile.isEnabled(TABLE, name));
        assertTrue(profile.isEnabled(TABLE, "ReadLatency"));
        assertFalse(profile.isEnabled(TABLE, "WriteLatency"));
        assertFalse(profile.isEnabled(KEYSPACE, "ReadLatency"));
    }

    @Test
    public void loadsAbsolutePathsAndFileUris() throws Exception
    {
        Path directory = Files.createDirectories(Paths.get("tmp"));
        Path profile = Files.createTempFile(directory, "metric profile ", ".yml").toAbsolutePath();
        try
        {
            Files.write(profile, yaml("[ReadLatency]", "[]", "[]", "[]").getBytes(StandardCharsets.UTF_8));
            for (String location : Arrays.asList(profile.toString(), profile.toUri().toString()))
            {
                MetricProfile loaded = MetricProfile.load(location);
                assertTrue(location, loaded.isEnabled(TABLE, "ReadLatency"));
                assertFalse(location, loaded.isEnabled(TABLE, "WriteLatency"));
                assertFalse(location, loaded.isEnabled(KEYSPACE, "ReadLatency"));
            }
        }
        finally
        {
            Files.deleteIfExists(profile);
        }
    }

    @Test
    public void explicitMissingLocationsFail()
    {
        for (String location : Arrays.asList("missing-metric-profile-for-unit-test.yml",
                                             Paths.get("tmp", "missing-metric-profile-for-unit-test.yml").toAbsolutePath().toString(),
                                             Paths.get("tmp", "missing-metric-profile-for-unit-test.yml").toAbsolutePath().toUri().toString()))
        {
            try
            {
                MetricProfile.load(location);
                fail("Expected missing profile to fail: " + location);
            }
            catch (ConfigurationException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains(location));
            }
        }
    }

    @Test
    public void omittedNamesAreDisabledAndScopesAreIndependent()
    {
        MetricProfile profile = parse(yaml("[ReadLatency]", "[WriteLatency]", "[WriteLatency]", "[ReadLatency]"));
        assertTrue(profile.isEnabled(TABLE, "ReadLatency"));
        assertFalse(profile.isEnabled(TABLE, "ReadTotalLatency"));
        assertFalse(profile.isEnabled(TABLE, "WriteLatency"));
        assertFalse(profile.isEnabled(TABLE, "TombstoneScannedHistogram"));
        assertFalse(profile.isEnabled(KEYSPACE, "ReadLatency"));
        assertTrue(profile.isEnabled(KEYSPACE, "WriteLatency"));
        assertFalse(profile.isEnabled(KEYSPACE, "WriteTotalLatency"));
        assertFalse(profile.isEnabled(TABLE, "FutureMetric"));
    }

    @Test
    public void deprecatedAliasesFollowTheirCanonicalSelection()
    {
        MetricProfile profile = parse(yaml("[MemtableOnHeapDataSize, EstimatedPartitionCount]", "[]", "[]", "[]"));
        assertTrue(MetricProfile.aliases(TABLE, "MemtableOnHeapDataSize").contains("MemtableOnHeapSize"));
        assertTrue(MetricProfile.aliases(TABLE, "EstimatedPartitionCount").contains("EstimatedRowCount"));
        assertTrue(profile.isEnabled(TABLE, "MemtableOnHeapSize"));
        assertTrue(profile.isEnabled(TABLE, "EstimatedRowCount"));
        assertFalse(profile.isEnabled(TABLE, "MemtableOffHeapDataSize"));
        assertFalse(profile.isEnabled(TABLE, "MemtableOffHeapSize"));
        assertFalse(profile.isEnabled(KEYSPACE, "MemtableOnHeapDataSize"));
        assertFalse(MetricProfile.knownNames(TABLE).contains("MemtableOnHeapSize"));
    }

    @Test
    public void aliasOptionDefaultsToCompatibilityAndFiltersOnlyLegacyExports()
    {
        String yaml = yaml("[AllMemtablesOnHeapDataSize]", "[]", "[ReadLatency]", "[]");
        assertTrue(parse(yaml).includesLegacyAliases());
        for (boolean include : new boolean[]{ false, true })
        {
            MetricProfile profile = parse("include_legacy_aliases: " + include + '\n' + yaml);
            assertTrue(profile.isEnabled(TABLE, "AllMemtablesOnHeapDataSize"));
            assertEquals(include, profile.isEnabled(TABLE, "AllMemtablesHeapSize"));
            for (String scope : Arrays.asList("ks.table", "all"))
            {
                for (String type : Arrays.asList("Table", "IndexTable", "ColumnFamily", "IndexColumnFamily"))
                {
                    boolean legacyType = type.equals("ColumnFamily") || type.equals("IndexColumnFamily");
                    assertEquals(type, include || !legacyType,
                                 profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, type, "AllMemtablesOnHeapDataSize", scope)));
                    assertEquals(type, include,
                                 profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, type, "AllMemtablesHeapSize", scope)));
                }
            }
            assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "Table", "KeyMigrationLatency", "all")));
            assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "keyspace", "ReadLatency", "ks")));
            assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "ClientRequest", "ReadLatency", "Read")));
            assertTrue(profile.isEnabled(name("another.metrics", "ColumnFamily", "AllMemtablesHeapSize", "ks.table")));
        }
    }

    @Test
    public void rejectsInvalidAliasOptions()
    {
        String yaml = yaml("[]", "[]", "[]", "[]");
        for (String value : Arrays.asList("null", "1", "[]", "{}", "'false'", "unexpected"))
            assertInvalid("include_legacy_aliases: " + value + '\n' + yaml, "include_legacy_aliases");
        assertInvalid("include_legacy_aliases: true\ninclude_legacy_aliases: false\n" + yaml, "include_legacy_aliases");
        assertInvalid("null: false\n" + yaml, "null");
    }

    @Test
    public void metricNamesFilterTableAndKeyspaceScopesOnly()
    {
        MetricProfile profile = parse(yaml("[MemtableOnHeapDataSize]", "[]", "[WriteLatency]", "[]"));
        for (String type : Arrays.asList("Table", "IndexTable", "ColumnFamily", "IndexColumnFamily"))
        {
            assertTrue(type, profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, type, "MemtableOnHeapDataSize", "ks.table")));
            assertTrue(type, profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, type, "MemtableOnHeapSize", "ks.table")));
            assertFalse(type, profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, type, "ReadLatency", "ks.table")));
        }
        assertFalse(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "keyspace", "ReadLatency", "ks")));
        assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "keyspace", "WriteLatency", "ks")));

        assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "Table", "ReadLatency", "all")));
        assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "ColumnFamily", "ReadLatency", "all")));
        assertTrue(profile.isEnabled(name(DefaultNameFactory.GROUP_NAME, "ClientRequest", "ReadLatency", "Read")));
        assertTrue(profile.isEnabled(name("another.metrics", "Table", "ReadLatency", "ks.table")));
    }

    @Test
    public void rejectsMissingAndUnexpectedSections()
    {
        String valid = yaml("[]", "[]", "[]", "[]");
        assertInvalid("", SOURCE);
        assertInvalid("[]", SOURCE);
        assertInvalid(valid.replace("mode: allowlist\n", ""), "mode");
        assertInvalid(valid.replace("mode: allowlist", "mode: all"), "mode");
        assertInvalid(valid + "tables: {}\n", "tables");
        assertInvalid(valid.substring(0, valid.indexOf("keyspace:")), "keyspace");
        assertInvalid(valid.replace("  optional: []\n", ""), "optional");
        assertInvalid(valid.replace("  disabled: []", "  disabled: []\n  extra: []"), "extra");
    }

    @Test
    public void rejectsYamlDuplicateKeysAndWrongTypes()
    {
        String valid = yaml("[]", "[]", "[]", "[]");
        assertInvalid(valid + "mode: allowlist\n", "mode");
        assertInvalid(valid.replace("  optional: []", "  optional: []\n  optional: []"), "optional");
        for (String invalidList : Arrays.asList("null", "ReadLatency", "{}", "[null]", "[1]", "[true]", "[{}]"))
            assertInvalid(yaml(invalidList, "[]", "[]", "[]"), "table", "optional");
        assertInvalid(valid.replace("mode: allowlist", "mode: [allowlist]"), "mode");
        assertInvalid(valid.replace("table:\n", "table: []\n"), SOURCE);
        assertInvalid("!!java.util.HashMap {}", SOURCE);
    }

    @Test
    public void rejectsUnknownDuplicateAndOverlappingMetricNames()
    {
        assertInvalid(yaml("[ReadLatncy]", "[]", "[]", "[]"), "table", "ReadLatncy");
        assertInvalid(yaml("[]", "[ReadLatncy]", "[]", "[]"), "table", "ReadLatncy");
        assertInvalid(yaml("[]", "[]", "[ReadLatncy]", "[]"), "keyspace", "ReadLatncy");
        assertInvalid(yaml("[ReadLatency, ReadLatency]", "[]", "[]", "[]"), "table", "ReadLatency");
        assertInvalid(yaml("[ReadLatency]", "[ReadLatency]", "[]", "[]"), "table", "ReadLatency");
        assertInvalid(yaml("[CompressionRatio]", "[]", "[]", "[]"), "table", "CompressionRatio");
        assertInvalid(yaml("[MemtableOnHeapSize]", "[]", "[]", "[]"), "table", "MemtableOnHeapSize");
        assertInvalid(yaml("[]", "[]", "[CoordinatorReadLatency]", "[]"), "keyspace", "CoordinatorReadLatency");
        assertInvalid(yaml("[RepairTime]", "[]", "[]", "[]"), "table", "RepairTime");
    }

    @Test
    public void requiredMetricsCannotBeOmittedDisabledOrReclassified()
    {
        String valid = yaml("[]", "[]", "[]", "[]");
        for (String required : REQUIRED)
        {
            List<String> missing = new ArrayList<>(REQUIRED);
            missing.remove(required);
            assertInvalid(valid.replace(REQUIRED.toString(), missing.toString()), "table", required);
            assertInvalid(yaml('[' + required + ']', "[]", "[]", "[]").replace(REQUIRED.toString(), missing.toString()), "table", required);
            assertInvalid(yaml("[]", '[' + required + ']', "[]", "[]").replace(REQUIRED.toString(), missing.toString()), "table", required);
        }
        List<String> duplicate = new ArrayList<>(REQUIRED);
        duplicate.add("CompressionRatio");
        assertInvalid(valid.replace(REQUIRED.toString(), duplicate.toString()), "table", "CompressionRatio");
        assertInvalid(valid.replace(REQUIRED.toString(), "[ReadLatency]"), "table", "required");
        assertInvalid(valid.replace("  required: []", "  required: [ReadLatency]"), "keyspace", "required");
    }

    static MetricProfile parse(String yaml)
    {
        return MetricProfile.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), SOURCE);
    }

    static String yaml(String tableOptional, String tableDisabled, String keyspaceOptional, String keyspaceDisabled)
    {
        return "mode: allowlist\n"
               + "table:\n"
               + "  required: " + REQUIRED + '\n'
               + "  optional: " + tableOptional + '\n'
               + "  disabled: " + tableDisabled + '\n'
               + "keyspace:\n"
               + "  required: []\n"
               + "  optional: " + keyspaceOptional + '\n'
               + "  disabled: " + keyspaceDisabled + '\n';
    }

    static void assertInvalid(String yaml, String... messageFragments)
    {
        try
        {
            parse(yaml);
            fail("Expected invalid metric profile to fail:\n" + yaml);
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains(SOURCE));
            for (String fragment : messageFragments)
                assertTrue(e.getMessage() + " must identify " + fragment, e.getMessage().contains(fragment));
        }
    }

    static CassandraMetricsRegistry.MetricName name(String group, String type, String metric, String scope)
    {
        return new CassandraMetricsRegistry.MetricName(group, type, metric, scope);
    }
}
