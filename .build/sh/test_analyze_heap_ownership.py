#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Focused checks for primitive histogram history in HPROF records."""

import contextlib
import datetime
import importlib.util
from pathlib import Path
import sys
import unittest


spec = importlib.util.spec_from_file_location("heap_ownership", Path(__file__).with_name("analyze-heap-ownership.py"))
analyzer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyzer)


class HistoryArrayStorageTest(unittest.TestCase):
    def test_null_history_has_no_payload(self):
        self.assertEqual(("null", 0), analyzer.history_array_storage(None))

    def test_signed_array_widths(self):
        for typ, expected in ((8, ("byte", 148)), (9, ("short", 296)), (10, ("int", 592)), (11, ("long", 1184))):
            for kind in (35, 195):
                with self.subTest(kind=kind, typ=typ):
                    self.assertEqual(expected, analyzer.history_array_storage((kind, typ, 148, 1000)))

    def test_nonintegral_histories_are_rejected(self):
        for record in ((33, 11, 0, 0), (34, 11, 148, 0), (35, 4, 148, 0), (35, 5, 148, 0),
                       (35, 6, 148, 0), (35, 7, 148, 0), (35, 2, 148, 0)):
            with self.subTest(record=record), self.assertRaises(ValueError):
                analyzer.history_array_storage(record)


class HistoryArrayStoragePropertyTest(unittest.TestCase):
    def test_payload_scales_with_length_for_each_width(self):
        for length in range(1025):
            for typ, width in ((8, 1), (9, 2), (10, 4), (11, 8)):
                for kind in (35, 195):
                    with self.subTest(kind=kind, typ=typ, length=length):
                        self.assertEqual(length * width, analyzer.history_array_storage((kind, typ, length, 0))[1])


if __name__ == "__main__":
    logs = Path("logs")
    logs.mkdir(exist_ok=True)
    path = logs / f"{datetime.datetime.now():%Y%m%d-%H%M%S-%f}-test-analyze-heap-ownership.log"
    with path.open("x") as log, contextlib.redirect_stdout(analyzer.Tee(sys.stdout, log)), contextlib.redirect_stderr(analyzer.Tee(sys.stderr, log)):
        print(f"Test log: {path}")
        program = unittest.main(exit=False)
        raise SystemExit(0 if program.result.wasSuccessful() else 1)
