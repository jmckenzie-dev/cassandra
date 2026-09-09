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
package org.apache.cassandra.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import org.apache.cassandra.exceptions.ConfigurationException;

/** Immutable startup selection for the table and keyspace metric exports. */
public final class MetricProfile
{
    public enum Scope
    {
        TABLE("table"), KEYSPACE("keyspace");

        final String key;

        Scope(String key)
        {
            this.key = key;
        }
    }

    public static final MetricProfile ALL = new MetricProfile(null, true);
    private static final Set<String> REQUIRED_TABLE = Set.of("CoordinatorReadLatency", "CoordinatorWriteLatency",
                                                              "CompressionRatio", "TotalDiskSpaceUsed");
    private final Map<Scope, Set<String>> enabled;
    private final boolean includeLegacyAliases;

    private MetricProfile(Map<Scope, Set<String>> enabled, boolean includeLegacyAliases)
    {
        this.enabled = enabled;
        this.includeLegacyAliases = includeLegacyAliases;
    }

    public static MetricProfile load(String location)
    {
        if (location == null)
            return ALL;
        check(!location.isBlank(), "metrics_config_file", "must be a nonempty filename or null");
        try
        {
            URL url;
            if (location.startsWith("file:"))
                url = Path.of(URI.create(location)).toUri().toURL();
            else if (Path.of(location).isAbsolute())
                url = Path.of(location).toUri().toURL();
            else
                url = MetricProfile.class.getClassLoader().getResource(location);
            check(url != null, location, "cannot locate metric profile");
            try (InputStream input = url.openStream())
            {
                return parse(input, location);
            }
        }
        catch (IOException | IllegalArgumentException e)
        {
            throw new ConfigurationException("Cannot load metric profile " + location + ": " + e.getMessage(), e);
        }
    }

    static MetricProfile parse(InputStream input, String source)
    {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Object loaded;
        try
        {
            loaded = new Yaml(new SafeConstructor(options)).load(input);
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid metric profile " + source + ": " + e.getMessage(), e);
        }
        Map<?, ?> profile = mapping(loaded, source);
        check(profile.keySet().containsAll(Set.of("mode", "table", "keyspace"))
              && profile.keySet().stream().allMatch(key -> key instanceof String
                                                          && Set.of("mode", "table", "keyspace", "include_legacy_aliases").contains(key)), source,
              "expected mode, table, and keyspace, with optional include_legacy_aliases; found " + profile.keySet());
        check("allowlist".equals(profile.get("mode")), source, "expected mode: allowlist");
        boolean includeAliases = true;
        if (profile.containsKey("include_legacy_aliases"))
        {
            check(profile.get("include_legacy_aliases") instanceof Boolean, source, "include_legacy_aliases must be a boolean");
            includeAliases = (Boolean) profile.get("include_legacy_aliases");
        }
        Map<Scope, Set<String>> selected = new EnumMap<>(Scope.class);
        for (Scope scope : Scope.values())
        {
            String context = source + ": " + scope.key;
            Map<?, ?> sections = mapping(profile.get(scope.key), context);
            check(sections.keySet().equals(Set.of("required", "optional", "disabled")), context,
                  "expected required, optional, and disabled lists; found " + sections.keySet());
            Set<String> required = scope == Scope.TABLE ? REQUIRED_TABLE : Collections.emptySet();
            Set<String> foundRequired = new HashSet<>();
            Set<String> seen = new HashSet<>();
            Set<String> exposed = new HashSet<>();
            for (String section : List.of("required", "optional", "disabled"))
            {
                Object values = sections.get(section);
                check(values instanceof List, context + '.' + section, "must be a list; use [] for an empty list");
                for (Object value : (List<?>) values)
                {
                    check(value instanceof String, context + '.' + section, "metric names must be strings: " + value);
                    String name = (String) value;
                    check(knownNames(scope).contains(name), context, "unknown canonical metric name: " + name);
                    check(seen.add(name), context, "duplicate metric: " + name);
                    if (section.equals("required"))
                        foundRequired.add(name);
                    if (!section.equals("disabled"))
                        exposed.add(name);
                }
            }
            check(foundRequired.equals(required), context, "required must contain exactly " + required + "; found " + foundRequired);
            selected.put(scope, Set.copyOf(exposed));
        }
        return new MetricProfile(Collections.unmodifiableMap(selected), includeAliases);
    }

    public boolean includesLegacyAliases()
    {
        return includeLegacyAliases;
    }

    public boolean isEnabled(Scope scope, String name)
    {
        if (!includeLegacyAliases && Catalog.canonical.get(scope).containsKey(name))
            return false;
        return enabled == null || enabled.get(scope).contains(Catalog.canonical.get(scope).getOrDefault(name, name));
    }

    public boolean isEnabled(CassandraMetricsRegistry.MetricName name)
    {
        if (!DefaultNameFactory.GROUP_NAME.equals(name.getGroup()))
            return true;
        switch (name.getType())
        {
            case "Table":
            case "ColumnFamily":
            case "IndexTable":
            case "IndexColumnFamily":
                if (!includeLegacyAliases && (name.getType().equals("ColumnFamily")
                                              || name.getType().equals("IndexColumnFamily")
                                              || Catalog.canonical.get(Scope.TABLE).containsKey(name.getName())))
                    return false;
                return "all".equals(name.getScope()) || isEnabled(Scope.TABLE, name.getName());
            case "keyspace":
                return isEnabled(Scope.KEYSPACE, name.getName());
            default:
                return true;
        }
    }

    public static Set<String> knownNames(Scope scope)
    {
        return Catalog.aliases.get(scope).keySet();
    }

    public static Set<String> aliases(Scope scope, String name)
    {
        return Catalog.aliases.get(scope).getOrDefault(name, Collections.emptySet());
    }

    private static Map<?, ?> mapping(Object value, String source)
    {
        check(value instanceof Map, source, "expected a mapping");
        return (Map<?, ?>) value;
    }

    private static void check(boolean condition, String source, String message)
    {
        if (!condition)
            throw new ConfigurationException("Invalid metric profile " + source + ": " + message, false);
    }

    /** Generated from the same declarations as conf/metrics_ref.md; it ships inside the Cassandra jar. */
    private static final class Catalog
    {
        static final Map<Scope, Map<String, Set<String>>> aliases = new EnumMap<>(Scope.class);
        static final Map<Scope, Map<String, String>> canonical = new EnumMap<>(Scope.class);

        static
        {
            Properties properties = new Properties();
            try (InputStream input = MetricProfile.class.getResourceAsStream("metrics-catalog.properties"))
            {
                if (input == null)
                    throw new IllegalStateException("Missing bundled metric catalog");
                properties.load(new InputStreamReader(input, StandardCharsets.UTF_8));
            }
            catch (IOException e)
            {
                throw new ExceptionInInitializerError(e);
            }
            for (Scope scope : Scope.values())
            {
                Map<String, Set<String>> names = new HashMap<>();
                Map<String, String> reverse = new HashMap<>();
                String prefix = scope.key + '.';
                for (String key : properties.stringPropertyNames())
                {
                    if (!key.startsWith(prefix))
                        continue;
                    String name = key.substring(prefix.length());
                    String value = properties.getProperty(key);
                    Set<String> oldNames = value.isEmpty() ? Collections.emptySet() : Set.of(value.split(","));
                    names.put(name, oldNames);
                    for (String alias : oldNames)
                        reverse.put(alias, name);
                }
                aliases.put(scope, Collections.unmodifiableMap(names));
                canonical.put(scope, Collections.unmodifiableMap(reverse));
            }
        }
    }
}
