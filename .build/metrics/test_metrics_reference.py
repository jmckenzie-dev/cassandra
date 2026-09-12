# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Exercise the source-file generator through its logging wrapper and --root input."""

import json
from pathlib import Path
import random
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / ".build/sh/ai-generate-metrics-reference"
METRICS = "src/java/org/apache/cassandra/metrics/"
CATALOG = "src/resources/org/apache/cassandra/metrics/metrics-catalog.properties"


class ReferenceFixture(unittest.TestCase):
    def setUp(self):
        (ROOT / "tmp").mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix="metrics-reference-", dir=ROOT / "tmp")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.table = '''class TableMetrics {
            /** Used bytes, including {@code a | b}. */
            public final Gauge<Long> used;
            /** Local read execution. */
            public final LatencyMetrics read;
            /** Cached rows per query. */
            public final Histogram rows;
            static final Timer GLOBAL = ignored();
            TableMetrics(boolean enabled) {
                this.used = createTableGaugeWithDeprecation("Used", "OldUsed", () -> 0L, null);
                read = createLatencyMetrics("Read");
                if (enabled) rows = createHistogram("Rows", true); else rows = null;
                // ignored = createTableCounter("CommentOnly");
                String ignored = "createTableCounter(\\"StringOnly\\")";
            }
        }'''
        self.write(METRICS + "TableMetrics.java", self.table)
        self.write(METRICS + "KeyspaceMetrics.java", '''class KeyspaceMetrics {
            /** Current used bytes across tables. */
            final Gauge<Long> used = createKeyspaceGauge("Used", x -> x.used);
        }''')
        self.write(METRICS + "LatencyMetrics.java", '''class LatencyMetrics {
            void register(String namePrefix) {
                factory.createMetricName(namePrefix + "Latency");
                factory.createMetricName(namePrefix + "TotalLatency");
            }
        }''')
        self.write("src/java/org/apache/cassandra/io/sstable/format/ExampleMetrics.java", '''class ExampleMetrics {
            /** Off-heap filter bytes. */
            private final GaugeProvider<Long> memory = newGaugeProvider("FilterMemory", 0L, x -> 0, Long::sum);
        }''')
        self.all = self.profile(["FilterMemory", "ReadLatency", "ReadTotalLatency", "Rows", "Used"], ["FilterMemory", "Used"])
        self.simple = json.loads(json.dumps(self.all))
        self.simple["table"]["optional"].remove("Rows")
        self.simple["table"]["disabled"].append("Rows")
        self.write_profiles()

    @staticmethod
    def profile(table, keyspace):
        return {"mode": "allowlist", **{scope: {"required": [], "optional": names, "disabled": []}
                                      for scope, names in [("table", table), ("keyspace", keyspace)]}}

    def write(self, relative, value):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")

    def write_profiles(self):
        self.write("conf/all_metrics.yml", json.dumps(self.all))
        self.write("conf/simple_metrics.yml", json.dumps(self.simple))

    def run_generator(self, *args, root=None, error=None):
        result = subprocess.run([str(WRAPPER), "--root", str(root or self.root), *args],
                                cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
        if error:
            self.assertNotEqual(0, result.returncode, result.stdout)
            self.assertIn(error, result.stdout)
        else:
            self.assertEqual(0, result.returncode, result.stdout)
        return result

    def output(self):
        return (self.root / "conf/metrics_ref.md").read_text(encoding="utf-8")

    def catalog_entries(self):
        text = (self.root / CATALOG).read_text(encoding="utf-8")
        return dict(line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#"))


class ReferenceTests(ReferenceFixture):
    def test_repository_reference_is_current(self):
        self.run_generator("--check", root=ROOT)

    def test_types_aliases_latency_provider_and_conditional_registration(self):
        self.run_generator()
        output = self.output()
        self.assertIn("Gauge&lt;Long&gt;", output)
        self.assertIn("Deprecated alias: `OldUsed`", output)
        self.assertIn("Used bytes, including `a \\| b`.", output)
        self.assertRegex(output, r"\[ReadTotalLatency\].*Counter.*microseconds")
        self.assertRegex(output, r"\[Rows\].*Histogram \| optional \| disabled")
        self.assertEqual(2, output.count("[FilterMemory]"))
        self.assertNotIn("CommentOnly", output)
        self.assertNotIn("StringOnly", output)
        self.assertNotIn("GLOBAL", output)
        self.assertEqual({"table.FilterMemory": "", "table.ReadLatency": "", "table.ReadTotalLatency": "",
                          "table.Rows": "", "table.Used": "OldUsed", "keyspace.FilterMemory": "", "keyspace.Used": ""},
                         self.catalog_entries())
        for target, line in re.findall(r"\]\(\.\./([^)#]+)#L(\d+)\)", output):
            declaration = (self.root / target).read_text().splitlines()[int(line) - 1]
            self.assertIn("final", declaration)
        self.run_generator("--check")
        self.assertEqual(output, self.output())

    def test_stale_check_does_not_rewrite(self):
        for relative in ["conf/metrics_ref.md", CATALOG]:
            with self.subTest(output=relative):
                self.run_generator()
                path = self.root / relative
                stale = path.read_text(encoding="utf-8") + "stale\n"
                self.write(relative, stale)
                self.run_generator("--check", error=relative + " is stale")
                self.assertEqual(stale, path.read_text(encoding="utf-8"))

    def test_missing_catalog_check_does_not_write(self):
        self.run_generator()
        (self.root / CATALOG).unlink()
        self.run_generator("--check", error=CATALOG + " is stale")
        self.assertFalse((self.root / CATALOG).exists())

    def test_source_and_profile_must_change_together(self):
        self.write(METRICS + "TableMetrics.java", self.table.replace(
            "static final Timer GLOBAL", '/** New events. */ final Counter added = createTableCounter("Added");\n'
            "static final Timer GLOBAL"))
        self.run_generator(error="missing=[Added]")
        self.assertFalse((self.root / CATALOG).exists())
        self.all["table"]["optional"].append("Added")
        self.simple["table"]["disabled"].append("Added")
        self.write_profiles()
        self.run_generator()
        self.assertIn("table.Added", self.catalog_entries())
        self.write(METRICS + "TableMetrics.java", self.table)
        self.run_generator(error="unknown=[Added]")

    def test_catalog_is_independent_of_profile_selections(self):
        self.run_generator()
        catalog = (self.root / CATALOG).read_text(encoding="utf-8")
        self.simple["table"]["optional"].remove("Used")
        self.simple["table"]["disabled"].append("Used")
        self.write_profiles()
        self.run_generator()
        self.assertEqual(catalog, (self.root / CATALOG).read_text(encoding="utf-8"))

    def test_missing_description_and_unsupported_source_fail(self):
        for replacement, error in [
            (self.table.replace("/** Cached rows per query. */", ""), "missing metric description"),
            (self.table.replace('createHistogram("Rows", true)', 'newRecorder("Rows")'), "unsupported metric helper"),
            (self.table.replace('createHistogram("Rows", true)', 'createHistogram(computedName, true)'), "string literal"),
            (self.table.replace('createHistogram("Rows", true)', 'createHistogram("Used", true)'), "Duplicate source metric"),
            (self.table.replace('createHistogram("Rows", true)', 'null'), "without supported registration"),
            (self.table + "broken syntax {", "Java parse failed"),
        ]:
            with self.subTest(error=error):
                self.write(METRICS + "TableMetrics.java", replacement)
                self.run_generator(error=error)
                self.assertFalse((self.root / "conf/metrics_ref.md").exists())
                self.assertFalse((self.root / CATALOG).exists())

    def test_profile_validation(self):
        original = json.dumps(self.simple)
        cases = [
            (lambda p: p["table"]["optional"].append("Unknown"), "unknown=[Unknown]"),
            (lambda p: p["table"]["optional"].remove("Used"), "missing=[Used]"),
            (lambda p: p["table"]["optional"].append("Used"), "duplicate metric table.Used"),
            (lambda p: p["table"]["disabled"].append("Used"), "duplicate metric table.Used"),
            (lambda p: p["table"].update(required=None), "must be a list"),
            (lambda p: p["table"]["optional"].append(42), "nonempty strings"),
            (lambda p: p.update(mode="all"), "expected mode: allowlist"),
            (lambda p: p.update(include_legacy_aliases="false"), "include_legacy_aliases must be a boolean"),
            (lambda p: p.update(include_legacy_aliases=None), "include_legacy_aliases must be a boolean"),
            (lambda p: p["table"].update(unexpected=[]), "invalid sections"),
        ]
        for edit, error in cases:
            with self.subTest(error=error):
                self.simple = json.loads(original)
                edit(self.simple)
                self.write_profiles()
                self.run_generator(error=error)
        self.simple = json.loads(original)
        self.simple["table"]["optional"].remove("Used")
        self.simple["table"]["required"].append("Used")
        self.write_profiles()
        self.run_generator(error="Required selection differs")
        self.simple = json.loads(original)
        self.all["table"]["optional"].remove("Used")
        self.all["table"]["disabled"].append("Used")
        self.write_profiles()
        self.run_generator(error="all_metrics.yml disables")

    def test_duplicate_yaml_key(self):
        self.write("conf/simple_metrics.yml", json.dumps(self.simple).replace('"mode": "allowlist"', '"mode": "allowlist", "mode": "allowlist"'))
        self.run_generator(error="duplicate key mode")

    def test_alias_options_do_not_change_canonical_catalog(self):
        for include in (True, False):
            with self.subTest(include=include):
                self.simple["include_legacy_aliases"] = include
                self.write_profiles()
                self.run_generator()
                self.run_generator("--check")

    def test_changed_latency_registration_requires_support(self):
        path = self.root / (METRICS + "LatencyMetrics.java")
        path.write_text(path.read_text().replace('"TotalLatency"', '"TotalTime"'))
        self.run_generator(error="Unsupported LatencyMetrics registration suffixes")


class ReferenceProperties(ReferenceFixture):
    def test_declaration_order_formatting_and_profile_order_preserve_catalog(self):
        for seed in range(4):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                fields, calls, names = [], [], []
                for i in range(32):
                    name = f"Sample{i:02}"
                    names.append(name)
                    space = rng.choice([" ", "\n    ", " /* registration */ "])
                    fields.append(f"/** Description {i}. */ final Gauge<Long> field{i};")
                    calls.append(f'field{i} = createTableGaugeWithDeprecation{space}("{name}", "Old{i}", () -> 0, null);')
                rng.shuffle(fields)
                rng.shuffle(calls)
                self.write(METRICS + "TableMetrics.java", "class TableMetrics {\n" + "\n".join(fields)
                           + "\nTableMetrics() {\n" + "\n".join(calls) + "\n}}")
                rng.shuffle(names)
                self.all = self.profile(names + ["FilterMemory"], ["FilterMemory", "Used"])
                self.simple = json.loads(json.dumps(self.all))
                rng.shuffle(self.simple["table"]["optional"])
                self.write_profiles()
                self.run_generator()
                rows = re.findall(r"^\| \[(Sample\d+)\].*$", self.output(), flags=re.MULTILINE)
                self.assertEqual(sorted(names), rows)
                entries = self.catalog_entries()
                self.assertEqual(sorted(names), [name.removeprefix("table.") for name in entries
                                                if name.startswith("table.Sample")])
                for i in range(32):
                    self.assertRegex(self.output(), rf"\[Sample{i:02}\].*Description {i}\. Deprecated alias: `Old{i}`")
                    self.assertEqual(f"Old{i}", entries[f"table.Sample{i:02}"])
                self.assertEqual(len(names) + 3, len(entries))


if __name__ == "__main__":
    suite = ReferenceProperties if sys.argv[1:] == ["--property"] else ReferenceTests
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(suite))
    sys.exit(not result.wasSuccessful())
