#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Attribute matched post-retirement dumps with the existing Memory Analyzer."""

import argparse
from collections import Counter
import contextlib
import csv
import datetime
import importlib
import io
import json
import os
from pathlib import Path
import subprocess
import struct
import sys
import zipfile

from hprof_reader import Hprof

Tee = importlib.import_module("analyze-heap-ownership").Tee


def query(heap, command):
    process = subprocess.run(["bash", ".build/sh/ai-analyze-heap-dominators", str(heap), command],
                             env=dict(os.environ, MAT_QUERY_LIMIT="10000"), text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(process.stdout, end="")
    process.check_returncode()
    report = Path(next(line.removeprefix("Report: ") for line in process.stdout.splitlines()
                       if line.startswith("Report: ")))
    with zipfile.ZipFile(report) as archive:
        entry = next(name for name in archive.namelist() if name.endswith(".csv"))
        rows = list(csv.DictReader(io.StringIO(archive.read(entry).decode())))
    return {"command": command, "report": str(report), "rows": rows}


def field_query(heap, cls, fields, predicate):
    selection = ", ".join(f"s.{field}.@retainedHeapSize AS {label}" for label, field in fields.items())
    selection = "s.@retainedHeapSize AS objectRetained, " + selection
    sql = f"SELECT {selection} FROM {cls} s WHERE {predicate}"
    result = query(heap, 'oql "' + sql.replace('"', '\\"') + '"')
    rows = result.pop("rows")
    if not rows or len(rows) >= 10000:
        raise ValueError("Empty or possibly truncated ownership query")
    columns = ("objectRetained", *fields)
    result["count"] = len(rows)
    result["bytes"] = {key: {"sum": sum(int(row[key]) for row in rows),
                             "min": min(int(row[key]) for row in rows),
                             "max": max(int(row[key]) for row in rows)} for key in columns}
    return result


def histogram_storage(path, expected_readers):
    with Hprof(path) as heap:
        def field(oid, key):
            return heap.values(oid)[key][1]

        def string(oid):
            _, typ, length, offset = heap.objects[field(oid, "value")]
            if typ != 8 or field(oid, "coder") != 0:
                raise ValueError("Expected Latin-1 keyspace string")
            return heap.data[offset:offset + length].decode("latin1")

        def longs(oid):
            kind, typ, length, offset = heap.objects[oid]
            if (kind, typ) != (35, 11):
                raise ValueError("Expected long array")
            return [value for value, in struct.iter_unpack(">q", heap.data[offset:offset + 8 * length])]

        counts = {key: Counter() for key in ("partitionBucketLengths", "partitionNonzeroBuckets",
                                           "cellBucketLengths", "cellNonzeroBuckets",
                                           "tombstoneCapacity", "tombstoneUsed")}
        readers = 0
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33 or heap.names[cid] != "org.apache.cassandra.io.sstable.format.bti.BtiTableReader":
                continue
            if string(field(field(oid, "descriptor"), "ksname")) != "memtable_residency":
                continue
            readers += 1
            stats = field(oid, "sstableMetadata")
            for prefix, key in (("partition", "estimatedPartitionSize"), ("cell", "estimatedCellPerPartitionCount")):
                buckets = longs(field(field(field(stats, key), "buckets"), "array"))
                counts[prefix + "BucketLengths"][len(buckets)] += 1
                counts[prefix + "NonzeroBuckets"][sum(value != 0 for value in buckets)] += 1
            points = longs(field(field(field(stats, "estimatedTombstoneDropTime"), "bin"), "points"))
            counts["tombstoneCapacity"][len(points)] += 1
            counts["tombstoneUsed"][sum(value != (1 << 63) - 1 for value in points)] += 1
        if readers != expected_readers:
            raise ValueError("HPROF reader count differs from settled live count")
        return {"readers": readers, **counts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_root", type=Path)
    args = parser.parse_args()
    runs = json.loads((args.run_root / "results.json").read_text())
    if len(runs) != 3 or any(run["exitCode"] or run["validationError"] for run in runs):
        raise ValueError("Expected three completed, validated census runs")
    output = {"limits": "Root dominator groups are disjoint. Field retained sizes are nested and must not be added to parent totals. Includes system state; user CFS/readers are filtered by keyspace.",
              "runs": {}}
    for run in runs:
        summary_path, = (args.run_root / run["mode"]).rglob("summary.json")
        summary = json.loads(summary_path.read_text())
        row = output["runs"][run["mode"]] = {"summary": str(summary_path), "checkpoints": {}}
        for phase in ("created", "settled"):
            heap = summary_path.with_name(phase + ".hprof")
            checkpoint = row["checkpoints"][phase] = {"counters": summary["checkpoints"][phase]}
            checkpoint["dominators"] = query(heap, "dominator_tree -groupby BY_CLASS")
            checkpoint["userCfs"] = field_query(heap, "org.apache.cassandra.db.ColumnFamilyStore", {
                "tracker": "data", "compaction": "compactionStrategyManager",
                "dictionary": "compressionDictionaryManager", "tableMetricObject": "metric"},
                'toString(s.keyspace.name) = "memtable_residency"')
            if checkpoint["userCfs"]["count"] != summary["tables"]:
                raise ValueError("User table ownership count mismatch")
            if phase == "settled":
                checkpoint["userReaders"] = field_query(heap, "org.apache.cassandra.io.sstable.format.bti.BtiTableReader", {
                    "stats": "sstableMetadata",
                    "partitionHistogram": "sstableMetadata.estimatedPartitionSize",
                    "cellHistogram": "sstableMetadata.estimatedCellPerPartitionCount",
                    "partitionOffsets": "sstableMetadata.estimatedPartitionSize.bucketOffsets",
                    "cellOffsets": "sstableMetadata.estimatedCellPerPartitionCount.bucketOffsets",
                    "tombstones": "sstableMetadata.estimatedTombstoneDropTime"},
                    'toString(s.descriptor.ksname) = "memtable_residency"')
                if checkpoint["userReaders"]["count"] != checkpoint["counters"]["sstables"]:
                    raise ValueError("Live reader ownership count mismatch")
                checkpoint["histogramStorage"] = histogram_storage(heap, checkpoint["counters"]["sstables"])
        destination = log_path.with_suffix(".json")
        destination.write_text(json.dumps(output, indent=2) + "\n")
        print("Saved", run["mode"], "to", destination)


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    log_path = Path("logs") / f"{datetime.datetime.now():%Y%m%d-%H%M%S-%f}-sstable-residency.log"
    with log_path.open("x") as log, contextlib.redirect_stdout(Tee(sys.stdout, log)), contextlib.redirect_stderr(Tee(sys.stderr, log)):
        print("Log:", log_path)
        main()
