#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tests for schema-scale-growth.py with synthetic histograms. Run from the repo root:
#   uv run --no-project --quiet --with pandas python test/bin/schema-scale-growth-test.py
# Scratch files go to ./tmp/growth-test.

import importlib.util
import shutil
import sys
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("growth", Path(__file__).with_name("schema-scale-growth.py"))
growth = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(growth)

SCRATCH = Path("tmp") / "growth-test"


def histogram(rows):
    lines = [" num     #instances         #bytes  class name (module)",
             "-------------------------------------------------------"]
    total_i = total_b = 0
    for n, (name, inst, size) in enumerate(rows, 1):
        lines.append(f"{n:>4}: {inst:>14} {size:>14}  {name}")
        total_i += inst
        total_b += size
    lines.append(f"Total {total_i:>14} {total_b:>14}")
    return "\n".join(lines) + "\n"


def make_run(name, tables_list):
    run = SCRATCH / name
    shutil.rmtree(run, ignore_errors=True)
    for t in tables_list:
        d = run / "steps" / f"t{t:07d}"
        d.mkdir(parents=True)
        (d / "class-histogram.txt").write_text(histogram([
            ("org.apache.cassandra.schema.TableMetadata (app)", 2 * t, 1000 * t + 500),
            ("[B (java.base@25)", 5 * t, 300 * t + t * t // 20),
            ("java.lang.String (java.base@25)", 100, 9000),
            ("[Ljava.lang.Object; (java.base@25)", 3 * t, 50 * t),
        ]))
    return run


class GrowthTest(unittest.TestCase):
    def test_slopes_and_flags(self):
        run = make_run("linear", [1000 * i for i in range(1, 13)])
        self.assertEqual(0, growth.main([str(run)]))
        import pandas as pd
        df = pd.read_csv(run / "growth-all.csv").set_index("class")
        self.assertAlmostEqual(1000.0, df.loc["org.apache.cassandra.schema.TableMetadata", "bytes_per_table"], places=3)
        self.assertAlmostEqual(2.0, df.loc["org.apache.cassandra.schema.TableMetadata", "instances_per_table"], places=6)
        self.assertGreater(df.loc["org.apache.cassandra.schema.TableMetadata", "r2_bytes"], 0.9999)
        self.assertAlmostEqual(0.0, df.loc["java.lang.String", "bytes_per_table"], places=6)
        self.assertNotEqual("SUPERLINEAR", str(df.loc["org.apache.cassandra.schema.TableMetadata", "flag"]))
        self.assertEqual("SUPERLINEAR", df.loc["byte[]", "flag"])
        self.assertIn("java.lang.Object[]", df.index)
        top = pd.read_csv(run / "growth.csv")
        self.assertEqual("org.apache.cassandra.schema.TableMetadata", top.iloc[0]["class"])
        self.assertIn("byte[]", list(top["class"]))
        self.assertTrue((run / "growth.md").is_file())

    def test_tolerates_partial_steps(self):
        run = make_run("partial", [1000, 2000, 3000, 4000])
        (run / "steps" / "t0005000").mkdir()
        (run / "steps" / "t0005000" / "class-histogram.txt").write_text("")
        (run / "steps" / "t0006000").mkdir()
        (run / "steps" / "t0006000" / "class-histogram.txt").write_text("12345:\njunk\n")
        (run / "steps" / "t0007000").mkdir()
        (run / "steps" / "notastep").mkdir()
        self.assertEqual(0, growth.main([str(run)]))
        self.assertTrue((run / "growth.csv").is_file())

    def test_too_few_steps(self):
        run = make_run("single", [1000])
        self.assertEqual(1, growth.main([str(run)]))
        self.assertFalse((run / "growth.csv").exists())
        self.assertEqual(2, growth.main([str(SCRATCH / "missing-dir")]))

    def test_class_names(self):
        self.assertEqual("byte[]", growth.class_name("[B (java.base@25)"))
        self.assertEqual("java.lang.String[][]", growth.class_name("[[Ljava.lang.String; (java.base@25)"))
        self.assertEqual("org.A$B", growth.class_name("org.A$B (app)"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
