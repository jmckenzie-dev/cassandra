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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import static org.apache.cassandra.metrics.MetricProfile.Scope.TABLE;
import static org.junit.Assert.assertEquals;

public class MetricProfilePropertyTest
{
    @Test
    public void generatedSelectionsPreserveScopeAndAliasDecisions()
    {
        for (long seed : new long[]{ 1, 42, 1009, 8675309 })
        {
            Random random = new Random(seed);
            for (int example = 0; example < 40; example++)
            {
                Map<MetricProfile.Scope, Set<String>> expected = new EnumMap<>(MetricProfile.Scope.class);
                StringBuilder yaml = new StringBuilder("mode: allowlist\n");
                for (MetricProfile.Scope scope : MetricProfile.Scope.values())
                {
                    List<String> required = scope == TABLE ? new ArrayList<>(MetricProfileTest.REQUIRED) : new ArrayList<>();
                    List<String> optional = new ArrayList<>();
                    List<String> disabled = new ArrayList<>();
                    List<String> names = new ArrayList<>(MetricProfile.knownNames(scope));
                    Collections.sort(names);
                    for (String name : names)
                    {
                        if (required.contains(name))
                            continue;
                        int selection = random.nextInt(3);
                        if (selection == 0)
                            optional.add(name);
                        else if (selection == 1)
                            disabled.add(name);
                    }
                    Set<String> enabled = new HashSet<>(required);
                    enabled.addAll(optional);
                    expected.put(scope, enabled);

                    Collections.shuffle(required, random);
                    Collections.shuffle(optional, random);
                    Collections.shuffle(disabled, random);
                    yaml.append(scope == TABLE ? "table:\n" : "keyspace:\n");
                    yaml.append("  disabled: ").append(disabled).append('\n');
                    yaml.append("  required: ").append(required).append('\n');
                    yaml.append("  optional: ").append(optional).append('\n');
                }

                MetricProfile profile = MetricProfileTest.parse(yaml.toString());
                MetricProfile modern = MetricProfileTest.parse("include_legacy_aliases: false\n" + yaml);
                for (MetricProfile.Scope scope : MetricProfile.Scope.values())
                {
                    for (String name : MetricProfile.knownNames(scope))
                    {
                        String context = "seed=" + seed + ", example=" + example + ", scope=" + scope + ", name=" + name;
                        boolean enabled = expected.get(scope).contains(name);
                        assertEquals(context, enabled, profile.isEnabled(scope, name));
                        assertEquals(context, enabled, modern.isEnabled(scope, name));
                        for (String alias : MetricProfile.aliases(scope, name))
                        {
                            assertEquals(context + ", alias=" + alias, enabled, profile.isEnabled(scope, alias));
                            assertEquals(context + ", modern alias=" + alias, false, modern.isEnabled(scope, alias));
                        }
                        if (scope == TABLE)
                        {
                            for (String type : List.of("Table", "IndexTable", "ColumnFamily", "IndexColumnFamily"))
                            {
                                boolean legacy = type.equals("ColumnFamily") || type.equals("IndexColumnFamily");
                                assertEquals(context + ", type=" + type, enabled && !legacy,
                                             modern.isEnabled(MetricProfileTest.name(DefaultNameFactory.GROUP_NAME, type, name, "ks.table")));
                                assertEquals(context + ", global type=" + type, !legacy,
                                             modern.isEnabled(MetricProfileTest.name(DefaultNameFactory.GROUP_NAME, type, name, "all")));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void everyCatalogNameRejectsDuplicateAndConflictingSelections()
    {
        for (MetricProfile.Scope scope : MetricProfile.Scope.values())
        {
            for (String name : MetricProfile.knownNames(scope))
            {
                if (scope == TABLE && MetricProfileTest.REQUIRED.contains(name))
                    continue;
                String duplicate = '[' + name + ", " + name + ']';
                String selected = '[' + name + ']';
                String scopeName = scope == TABLE ? "table" : "keyspace";
                String duplicated = scope == TABLE ? MetricProfileTest.yaml(duplicate, "[]", "[]", "[]")
                                                   : MetricProfileTest.yaml("[]", "[]", duplicate, "[]");
                String overlapping = scope == TABLE ? MetricProfileTest.yaml(selected, selected, "[]", "[]")
                                                    : MetricProfileTest.yaml("[]", "[]", selected, selected);
                MetricProfileTest.assertInvalid(duplicated, scopeName, name);
                MetricProfileTest.assertInvalid(overlapping, scopeName, name);
            }
        }
    }
}
