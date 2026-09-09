/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import com.sun.source.doctree.DocCommentTree;
import com.sun.source.tree.AssignmentTree;
import com.sun.source.tree.BinaryTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.DocTrees;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreePath;
import com.sun.source.util.TreeScanner;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Parses metric declarations without compiling or starting Cassandra. Run through ai-generate-metrics-reference. */
public class GenerateMetricsReference
{
    private static final String METRICS = "src/java/org/apache/cassandra/metrics/";
    private static final String CATALOG = "src/resources/org/apache/cassandra/metrics/metrics-catalog.properties";
    private static final Set<String> TYPES = Set.of("Gauge", "GaugeProvider", "Counter", "Meter", "Timer", "Histogram",
                                                    "TableMeter", "TableTimer", "TableHistogram", "SnapshottingTimer", "LatencyMetrics");
    private static final Set<String> HELPERS = Set.of("createTableGauge", "createTableGaugeWithDeprecation",
                                                      "createTableCounter", "createTableMeter", "createTableTimer",
                                                      "createTableHistogram", "createHistogram", "createLatencyMetrics",
                                                      "createKeyspaceGauge", "createKeyspaceCounter", "createKeyspaceMeter",
                                                      "createKeyspaceTimer", "createKeyspaceHistogram", "newGaugeProvider");

    private static final class Metric
    {
        final String name;
        final String type;
        final String description;
        final String alias;
        final String source;

        Metric(String name, String type, String description, String alias, String source)
        {
            this.name = name;
            this.type = type;
            this.description = description;
            this.alias = alias;
            this.source = source;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Path root = Path.of("").toAbsolutePath();
        boolean check = false;
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equals("--check"))
                check = true;
            else if (args[i].equals("--root") && i + 1 < args.length)
                root = Path.of(args[++i]).toAbsolutePath();
            else
                throw new IllegalArgumentException("Usage: ai-generate-metrics-reference [--check] [--root PATH]");
        }

        Map<String, Metric> table = readMetrics(root, METRICS + "TableMetrics.java");
        Map<String, Metric> keyspace = readMetrics(root, METRICS + "KeyspaceMetrics.java");
        // Discover built-in provider declarations, including private provider fields.
        try (var paths = Files.walk(root.resolve("src/java/org/apache/cassandra/io/sstable")))
        {
            for (Path path : paths.filter(p -> p.toString().endsWith("Metrics.java")).sorted().collect(Collectors.toList()))
            {
                for (Metric metric : readMetrics(root, root.relativize(path).toString()).values())
                {
                    add(table, metric);
                    add(keyspace, metric);
                }
            }
        }
        validateLatencyNames(root);
        Map<String, Map<String, String>> all = readProfile(root.resolve("conf/all_metrics.yml"));
        Map<String, Map<String, String>> simple = readProfile(root.resolve("conf/simple_metrics.yml"));
        StringBuilder out = new StringBuilder(header());
        appendScope(out, "table", table, all, simple);
        appendScope(out, "keyspace", keyspace, all, simple);
        String reference = out.toString().stripTrailing() + '\n';
        Map<String, String> outputs = new TreeMap<>();
        outputs.put("conf/metrics_ref.md", reference);
        outputs.put(CATALOG, catalog(table, keyspace));
        for (Map.Entry<String, String> output : outputs.entrySet())
        {
            Path path = root.resolve(output.getKey());
            if (check)
                require(Files.exists(path) && Files.readString(path).equals(output.getValue()),
                        output.getKey() + " is stale; run .build/sh/ai-generate-metrics-reference");
            else
            {
                Files.createDirectories(path.getParent());
                Files.writeString(path, output.getValue());
                System.out.println("Wrote " + path);
            }
        }
        System.out.println("Reference and runtime catalog " + (check ? "are current: " : "contain: ")
                           + table.size() + " table and " + keyspace.size() + " keyspace metrics.");
    }

    private static String catalog(Map<String, Metric> table, Map<String, Metric> keyspace)
    {
        StringBuilder out = new StringBuilder("# Licensed to the Apache Software Foundation (ASF) under one\n"
                                             + "# or more contributor license agreements. See the NOTICE file\n"
                                             + "# distributed with this work for additional information\n"
                                             + "# regarding copyright ownership. The ASF licenses this file\n"
                                             + "# to you under the Apache License, Version 2.0 (the\n"
                                             + "# \"License\"); you may not use this file except in compliance\n"
                                             + "# with the License. You may obtain a copy of the License at\n"
                                             + "#\n"
                                             + "# http://www.apache.org/licenses/LICENSE-2.0\n"
                                             + "#\n"
                                             + "# Unless required by applicable law or agreed to in writing,\n"
                                             + "# software distributed under the License is distributed on an\n"
                                             + "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
                                             + "# KIND, either express or implied. See the License for the\n"
                                             + "# specific language governing permissions and limitations\n"
                                             + "# under the License.\n\n"
                                             + "# Generated by .build/sh/ai-generate-metrics-reference.\n"
                                             + "# Keys are scoped canonical metric names; values are comma-separated deprecated aliases.\n");
        appendCatalog(out, "table", table);
        appendCatalog(out, "keyspace", keyspace);
        return out.toString();
    }

    private static void appendCatalog(StringBuilder out, String scope, Map<String, Metric> metrics)
    {
        for (Metric metric : metrics.values())
        {
            require(!metric.alias.contains(","), scope + "." + metric.name + ": alias cannot contain a comma");
            out.append(property(scope + '.' + metric.name)).append('=').append(property(metric.alias)).append('\n');
        }
    }

    private static String property(String value)
    {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
                    .replace("\f", "\\f").replace(" ", "\\ ").replace("=", "\\=").replace(":", "\\:")
                    .replace("#", "\\#").replace("!", "\\!");
    }

    private static Map<String, Metric> readMetrics(Path root, String source) throws IOException
    {
        Map<String, Metric> metrics = new TreeMap<>();
        parse(root.resolve(source), (unit, docs) -> {
            for (Tree declaration : unit.getTypeDecls())
            {
                if (!(declaration instanceof ClassTree))
                    continue;
                ClassTree clazz = (ClassTree) declaration;
                Map<String, VariableTree> fields = new TreeMap<>();
                Set<String> assigned = new TreeSet<>();
                for (Tree member : clazz.getMembers())
                {
                    if (!(member instanceof VariableTree))
                        continue;
                    VariableTree field = (VariableTree) member;
                    String type = field.getType().toString().replaceAll("<.*>", "");
                    if (TYPES.contains(type) && !field.getModifiers().getFlags().contains(Modifier.STATIC))
                        fields.put(field.getName().toString(), field);
                }
                fields.forEach((name, field) -> {
                    if (field.getInitializer() != null)
                        register(metrics, assigned, field, field.getInitializer(), source, unit, docs);
                });
                for (Tree member : clazz.getMembers())
                {
                    if (!(member instanceof MethodTree) || ((MethodTree) member).getReturnType() != null)
                        continue;
                    new TreeScanner<Void, Void>()
                    {
                        @Override
                        public Void visitAssignment(AssignmentTree assignment, Void unused)
                        {
                            String name = assignment.getVariable().toString().replaceFirst("^this\\.", "");
                            if (fields.containsKey(name))
                                register(metrics, assigned, fields.get(name), assignment.getExpression(), source, unit, docs);
                            return super.visitAssignment(assignment, unused);
                        }
                    }.scan(member, null);
                }
                Set<String> missing = new TreeSet<>(fields.keySet());
                missing.removeAll(assigned);
                require(missing.isEmpty(), source + ": metric fields without supported registration: " + missing);
            }
        });
        return metrics;
    }

    private static void register(Map<String, Metric> metrics, Set<String> assigned, VariableTree field,
                                 ExpressionTree value, String source, CompilationUnitTree unit, DocTrees docs)
    {
        if (value.getKind() == Tree.Kind.NULL_LITERAL)
            return; // Conditional metrics can be absent, for example view timers on materialized views.
        String context = source + ": " + field.getName();
        require(value instanceof MethodInvocationTree, context + ": unsupported metric initializer " + value);
        MethodInvocationTree call = (MethodInvocationTree) value;
        String helper = call.getMethodSelect().toString();
        require(HELPERS.contains(helper), context + ": unsupported metric helper " + helper);
        require(!call.getArguments().isEmpty(), context + ": missing metric name");
        String name = literal(call.getArguments().get(0));
        require(name != null, context + ": metric name must be a string literal");
        DocCommentTree comment = docs.getDocCommentTree(TreePath.getPath(unit, field));
        require(comment != null && !comment.getFullBody().isEmpty(), context + ": missing metric description");
        String description = comment.getFullBody().stream().map(Object::toString).collect(Collectors.joining());
        description = description.replaceAll("\\{@(?:link|code|literal)\\s+([^}]+)}", "`$1`").replaceAll("\\s+", " ").trim();
        long line = unit.getLineMap().getLineNumber(docs.getSourcePositions().getStartPosition(unit, field));
        String link = "../" + source + "#L" + line;
        String type = field.getType().toString();
        if (helper.equals("createLatencyMetrics"))
        {
            require(type.equals("LatencyMetrics"), context + ": unexpected latency field type " + type);
            add(metrics, new Metric(name + "Latency", "Timer", description + " Duration distribution, operation count, and rates.", "", link));
            add(metrics, new Metric(name + "TotalLatency", "Counter", description + " Cumulative duration in microseconds; each recorded duration is truncated to whole microseconds.", "", link));
        }
        else
        {
            type = type.replace("GaugeProvider", "Gauge").replace("SnapshottingTimer", "Timer").replaceFirst("^Table", "");
            String alias = call.getArguments().size() > 1 ? literal(call.getArguments().get(1)) : null;
            add(metrics, new Metric(name, type, description, alias == null || alias.equals(name) ? "" : alias, link));
        }
        assigned.add(field.getName().toString());
    }

    private static void add(Map<String, Metric> metrics, Metric metric)
    {
        require(metrics.putIfAbsent(metric.name, metric) == null, "Duplicate source metric " + metric.name);
    }

    private static String literal(ExpressionTree tree)
    {
        if (!(tree instanceof LiteralTree))
            return null;
        Object value = ((LiteralTree) tree).getValue();
        return value instanceof String ? (String) value : null;
    }

    private interface ParsedSource
    {
        void accept(CompilationUnitTree unit, DocTrees docs);
    }

    private static void parse(Path path, ParsedSource consumer) throws IOException
    {
        var compiler = ToolProvider.getSystemJavaCompiler();
        require(compiler != null, "A full JDK is required");
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, null, java.nio.charset.StandardCharsets.UTF_8))
        {
            var task = (JavacTask) compiler.getTask(null, files, diagnostics, List.of("-proc:none"), null,
                                                   files.getJavaFileObjects(path.toFile()));
            var units = task.parse();
            require(diagnostics.getDiagnostics().stream().noneMatch(d -> d.getKind() == Diagnostic.Kind.ERROR),
                    "Java parse failed for " + path + ": " + diagnostics.getDiagnostics());
            for (CompilationUnitTree unit : units)
                consumer.accept(unit, DocTrees.instance(task));
        }
    }

    private static void validateLatencyNames(Path root) throws IOException
    {
        Set<String> suffixes = new TreeSet<>();
        parse(root.resolve(METRICS + "LatencyMetrics.java"), (unit, docs) -> new TreeScanner<Void, Void>()
        {
            @Override
            public Void visitMethodInvocation(MethodInvocationTree call, Void unused)
            {
                if (call.getMethodSelect().toString().endsWith(".createMetricName"))
                    for (ExpressionTree argument : call.getArguments())
                        if (argument.getKind() == Tree.Kind.PLUS)
                        {
                            BinaryTree plus = (BinaryTree) argument;
                            if (plus.getLeftOperand().toString().equals("namePrefix") && literal(plus.getRightOperand()) != null)
                                suffixes.add(literal(plus.getRightOperand()));
                        }
                return super.visitMethodInvocation(call, unused);
            }
        }.scan(unit, null));
        require(suffixes.equals(Set.of("Latency", "TotalLatency")), "Unsupported LatencyMetrics registration suffixes: " + suffixes);
    }

    private static Map<String, Map<String, String>> readProfile(Path path) throws IOException
    {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Object loaded;
        try (Reader reader = Files.newBufferedReader(path))
        {
            loaded = new Yaml(new SafeConstructor(options)).load(reader);
        }
        Map<?, ?> profile = mapping(loaded, path.toString());
        require(profile.keySet().containsAll(Set.of("mode", "table", "keyspace"))
                && profile.keySet().stream().allMatch(key -> key instanceof String
                                                            && Set.of("mode", "table", "keyspace", "include_legacy_aliases").contains(key)),
                path + ": expected mode, table, keyspace, with optional include_legacy_aliases");
        require(!profile.containsKey("include_legacy_aliases") || profile.get("include_legacy_aliases") instanceof Boolean,
                path + ": include_legacy_aliases must be a boolean");
        require("allowlist".equals(profile.get("mode")), path + ": expected mode: allowlist");
        Map<String, Map<String, String>> result = new TreeMap<>();
        for (String scope : List.of("table", "keyspace"))
        {
            Map<?, ?> sections = mapping(profile.get(scope), path + ": " + scope);
            require(sections.keySet().equals(Set.of("required", "optional", "disabled")), path + ": invalid sections for " + scope);
            Map<String, String> selections = new TreeMap<>();
            for (String section : List.of("required", "optional", "disabled"))
            {
                Object names = sections.get(section);
                require(names instanceof List, path + ": " + scope + "." + section + " must be a list (use [] for empty)");
                for (Object name : (List<?>) names)
                {
                    require(name instanceof String && !((String) name).isBlank(), path + ": metric names must be nonempty strings");
                    require(selections.putIfAbsent((String) name, section) == null, path + ": duplicate metric " + scope + "." + name);
                }
            }
            result.put(scope, selections);
        }
        return result;
    }

    private static Map<?, ?> mapping(Object value, String context)
    {
        require(value instanceof Map, context + ": expected a YAML mapping");
        return (Map<?, ?>) value;
    }

    private static void appendScope(StringBuilder out, String scope, Map<String, Metric> metrics,
                                    Map<String, Map<String, String>> all, Map<String, Map<String, String>> simple)
    {
        for (Map<String, Map<String, String>> profile : List.of(all, simple))
        {
            Set<String> missing = new TreeSet<>(metrics.keySet());
            missing.removeAll(profile.get(scope).keySet());
            Set<String> unknown = new TreeSet<>(profile.get(scope).keySet());
            unknown.removeAll(metrics.keySet());
            require(missing.isEmpty() && unknown.isEmpty(), scope + ": profile/source mismatch; missing=" + missing + ", unknown=" + unknown);
        }
        out.append("## ").append(scope.equals("table") ? "Table and IndexTable" : "Keyspace")
           .append("\n\n").append(metrics.size()).append(" canonical metric names.\n\n")
           .append("| Metric | Type | All | Simple | Description |\n| --- | --- | --- | --- | --- |\n");
        for (Metric metric : metrics.values())
        {
            String full = all.get(scope).get(metric.name);
            String small = simple.get(scope).get(metric.name);
            require(!full.equals("disabled"), "all_metrics.yml disables " + scope + "." + metric.name);
            require(full.equals("required") == small.equals("required"), "Required selection differs for " + scope + "." + metric.name);
            out.append("| [").append(metric.name).append("](").append(metric.source).append(") | ")
               .append(cell(metric.type)).append(" | ").append(full).append(" | ").append(small).append(" | ")
               .append(cell(metric.description));
            if (!metric.alias.isEmpty())
                out.append(" Deprecated alias: `").append(cell(metric.alias)).append("`.");
            out.append(" |\n");
        }
        out.append('\n');
    }

    private static String cell(String value)
    {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "\\|");
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new IllegalArgumentException(message);
    }

    private static String header()
    {
        return "<!--\nLicensed to the Apache Software Foundation (ASF) under one or more contributor\n"
               + "license agreements. See the NOTICE file distributed with this work for additional\n"
               + "information regarding copyright ownership. The ASF licenses this file to you\n"
               + "under the Apache License, Version 2.0 (the \"License\"); you may not use this file\n"
               + "except in compliance with the License. You may obtain a copy of the License at\n"
               + "http://www.apache.org/licenses/LICENSE-2.0\n"
               + "Unless required by applicable law or agreed to in writing, software distributed\n"
               + "under the License is distributed on an \"AS IS\" BASIS, WITHOUT WARRANTIES OR\n"
               + "CONDITIONS OF ANY KIND, either express or implied. See the License for the\n"
               + "specific language governing permissions and limitations under the License.\n-->\n\n"
               + "# Metrics configuration reference\n\n"
               + "Generated by `.build/sh/ai-generate-metrics-reference`. Edit the Java field Javadoc or the profiles, "
               + "then regenerate this file and the runtime metric-name catalog. Use `--check` to detect stale output. Names and types come from Java declarations "
               + "and registration calls; descriptions come from their Javadoc. Generation checks both profiles against the source catalog.\n\n"
               + "This reference covers [all_metrics.yml](all_metrics.yml) and [simple_metrics.yml](simple_metrics.yml). "
               + "Select a profile with `metrics_config_file` in `cassandra.yaml`. See [README.txt](README.txt) for the selection rules.\n\n"
               + "## Choosing metrics\n\n"
               + "Start with the simple profile for routine monitoring. Move a diagnostic metric from `disabled` to `optional` "
               + "when you need its signal. `required` entries preserve database dependencies; keep them in that section. "
               + "`optional` means enabled and operator-selectable. `disabled` means excluded by that profile. "
               + "These selections do not measure memory cost, and an enabled aggregate can still require state from disabled child exports.\n\n"
               + "Use the case-sensitive canonical name in the table below. Each name selects all instances in its scope. "
               + "Table includes IndexTable. Set `include_legacy_aliases: false` to omit ColumnFamily/IndexColumnFamily and deprecated metric-name exports, "
               + "including node-wide Table aliases. The setting defaults to true when omitted; all_metrics.yml enables it and simple_metrics.yml disables it. "
               + "When enabled, aliases follow the canonical selection. Recording and aggregation do not change. "
               + "A timer's percentile attributes cannot be selected separately. `TotalLatency` is a separate counter.\n\n"
               + "Required entries protect database decisions. See the [internal dependency audit](../research/internal_metric_dependencies.md) "
               + "for their uses and the [aggregate inventory](../research/metric_aggregates.md) for parent/child dependencies.\n\n"
               + "## Reading the values\n\n"
               + "All scopes describe this node. Table metrics describe one local table or index; Keyspace metrics describe activity or "
               + "resources across local tables in that keyspace. Keyspace histograms combine observations, not table percentiles. "
               + "Replica read/write latency and coordinator request latency cover different work.\n\n"
               + "| Type | Interpretation |\n| --- | --- |\n"
               + "| Gauge | Value computed when queried. Array gauges contain histogram bucket counts, not a list of raw observations. |\n"
               + "| Counter | Integer count or accounting total. Disk usage and pending work can decrease; not every counter is a lifetime event total. |\n"
               + "| Meter | Recorded count, mean rate, and 1-, 5-, and 15-minute moving rates. Rate units follow the recorded values, such as events or bytes per second. |\n"
               + "| Histogram | Distribution of recorded values. Units and observation boundaries are in the description. Runtime distributions use a reservoir; they are not a full event log. |\n"
               + "| Timer | Duration distribution plus operation count and rates. Java Management Extensions (JMX) exports durations in microseconds and rates per second. `*Nanos` gauges explicitly use nanoseconds. |\n\n"
               + "Counts and histories follow metric/object lifetime, not durable database history. A zero can mean no observations or an inactive feature; "
               + "it does not prove a workload is healthy. Threshold metrics depend on the corresponding checks being enabled. "
               + "Some metrics exist only for applicable objects: base-table view timers and format-specific gauges are examples. "
               + "Bloom-filter gauges operate on compatible live SSTables (sorted string tables); index-summary and key-cache gauges apply to the Big format. "
               + "Recent Bloom-filter gauges consume shared per-reader sampling cursors, so polling one scope or attribute can affect another.\n\n"
               + "Node-wide Table aggregates are outside the metric-selection lists; only `include_legacy_aliases` changes their compatibility exports. "
               + "TrieMemtable, Storage Attached Indexing (SAI), and other node/service families are outside these profiles and this reference. "
               + "Links on metric names lead to their source declarations.\n\n";
    }
}
