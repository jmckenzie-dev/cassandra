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
"""Summarize settled heap, UCS compaction, and observed request service time."""

import csv
import datetime
import json
import math
from pathlib import Path
from statistics import median
import sys


def percentiles(values):
    values = sorted(values)
    return {str(p): values[max(0, math.ceil(p * len(values)) - 1)] / 1e6
            for p in (0.5, 0.99)} if values else {}


def analyze(path):
    data = json.loads(path.read_text())
    points = data.get("checkpoints", {})
    final = points.get("settled", {})
    created = points.get("created", {})
    result = {"path": str(path), "failure": data.get("failure"), "tables": data.get("tables"),
              "writes": data.get("completedWrites"), "failedWrites": data.get("failedWriteRequests"),
              "heapMiB": final.get("heapUsedBytes", 0) / 1048576,
              "heapAboveCreatedMiB": (final.get("heapUsedBytes", 0) - created.get("heapUsedBytes", 0)) / 1048576}
    for key in ("initialized_trie_memtables", "sstables", "sstable_data_disk_bytes", "bytes_flushed",
                "compaction_bytes_written", "pending_compactions", "compactionHistory"):
        result[key] = final.get(key)
    flushed = final.get("bytes_flushed", 0)
    result["compactionOutputPerFlushByte"] = final.get("compaction_bytes_written", 0) / flushed if flushed else None
    result["cycles"] = [{"name": name, **point} for name, point in points.items()
                        if name.startswith("03-") and name.endswith("-policy")]
    first_writes, warm_writes, reads = [], [], []
    for trace in path.parent.glob("writes-*.csv"):
        if trace.name == "writes-000.csv":
            continue
        with trace.open() as source:
            for row in csv.DictReader(source):
                target = first_writes if int(row["operation"]) < data["activeTables"] else warm_writes
                target.append(int(row["service_ns"]))
    for trace in path.parent.glob("reads-*.csv"):
        if trace.name.startswith("reads-0-"):
            continue
        with trace.open() as source:
            reads.extend(int(row["service_ns"]) for row in csv.DictReader(source))
    result["serviceMillisecondsAfterCycleZero"] = {"firstWriteEachTable": percentiles(first_writes),
                                                 "remainingWrites": percentiles(warm_writes),
                                                 "reads": percentiles(reads)}
    result["idleDrainPhases"] = [phase for phase in data.get("phases", []) if "idle-drain" in phase.get("name", "")]
    durations = sorted(phase["elapsedNanos"] / 1e9 for phase in result["idleDrainPhases"])
    if durations:
        result["idleDrainSeconds"] = {"min": durations[0], "median": median(durations), "max": durations[-1]}
    result["cycleMilestones"] = [{"cycle": i + 1, "heapMiB": point["heapUsedBytes"] / 1048576,
                                  "sstables": point["sstables"],
                                  "compactionOutputPerFlushByte": point["compaction_bytes_written"] / point["bytes_flushed"]}
                                 for i, point in enumerate(result["cycles"])
                                 if (i + 1) % 12 == 0 and point.get("bytes_flushed", 0)]
    return result


def main():
    results = [analyze(path) for directory in sys.argv[1:] for path in sorted(Path(directory).rglob("summary.json"))]
    logs = Path(__file__).resolve().parents[2] / "logs"
    logs.mkdir(exist_ok=True)
    output = logs / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "-analyze-ucs-idle.json")
    output.write_text(json.dumps(results, indent=2) + "\n")
    lines = [str(output)]
    for result in results:
        brief = {key: value for key, value in result.items() if key not in ("cycles", "idleDrainPhases")}
        lines.append(json.dumps(brief))
    text = "\n".join(lines) + "\n"
    print(text, end="")
    output.with_suffix(".log").write_text(text)


if __name__ == "__main__":
    main()
