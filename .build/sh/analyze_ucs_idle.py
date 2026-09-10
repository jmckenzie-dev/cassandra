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

import argparse
import csv
import datetime
import json
import math
import re
import subprocess
from pathlib import Path
from statistics import mean, median


def allocation_summary(path, converter):
    result = {"windows": 0, "weightedBytes": 0, "compactionWeightedBytes": 0}
    for recording in sorted(path.parent.glob("03-cycle-*.ap.jfr")):
        output = recording.with_suffix(".alloc.collapsed")
        command = [str(converter), "-o", "collapsed", "--alloc", "--total", "--dot", str(recording), str(output)]
        completed = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if completed.returncode:
            raise RuntimeError(f"Allocation conversion failed for {recording}: {completed.stdout}")
        result["windows"] += 1
        for line in output.read_text().splitlines():
            stack, weight = line.rsplit(" ", 1)
            result["weightedBytes"] += int(weight)
            if "CompactionTask." in stack:
                result["compactionWeightedBytes"] += int(weight)
    if not result["weightedBytes"]:
        raise ValueError(f"No allocation samples for {path}")
    return result


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
    if "processCpuNanos" in final:
        result["processCpuSecondsAfterCreation"] = (final["processCpuNanos"] - created["processCpuNanos"]) / 1e9
    result["ucsFinal"] = final.get("ucs")
    peaks = {key: 0 for key in ("sstables", "pending_compactions", "compacting_sstables")}
    for trace in path.parent.glob("*-samples.csv"):
        with trace.open() as source:
            for row in csv.DictReader(source):
                for key in peaks:
                    peaks[key] = max(peaks[key], int(row[key]))
    result["sampledPeaks"] = peaks
    result["peakSettledFiles"] = max((point.get("sstables", 0) for point in points.values()
                                      if point.get("settledPostGc")), default=0)
    flush_sizes, compaction_ms = [], []
    console = path.parent / "console.txt"
    if console.exists():
        with console.open() as source:
            for line in source:
                if "/memtable_residency/" not in line:
                    continue
                match = re.search(r"Completed flushing .*bytes flushed: (\d+)", line)
                if match:
                    flush_sizes.append(int(match[1]))
                match = re.search(r"Compacted .* in (\d+)ms", line)
                if match:
                    compaction_ms.append(int(match[1]))
    positive_flushes = sorted(size for size in flush_sizes if size)
    result["loggedFlushSizes"] = {"writers": len(flush_sizes), "emptyWriters": flush_sizes.count(0),
                                  "positiveMinBytes": min(positive_flushes, default=0),
                                  "positiveMedianBytes": median(positive_flushes) if positive_flushes else 0,
                                  "positiveMaxBytes": max(positive_flushes, default=0)}
    result["loggedCompactionTime"] = {"jobs": len(compaction_ms), "sumMilliseconds": sum(compaction_ms),
                                      "medianMilliseconds": median(compaction_ms) if compaction_ms else 0,
                                      "maxMilliseconds": max(compaction_ms, default=0)}
    for key in ("initialized_trie_memtables", "sstables", "sstable_data_disk_bytes", "bytes_flushed",
                "compaction_bytes_written", "pending_compactions", "compactionHistory"):
        result[key] = final.get(key)
    flushed = final.get("bytes_flushed", 0)
    result["compactionOutputPerFlushByte"] = final.get("compaction_bytes_written", 0) / flushed if flushed else None
    result["cycles"] = [{"name": name, **point} for name, point in points.items()
                        if name.startswith("03-") and name.endswith("-policy")]
    settled_cycles = [point["sstables"] for point in result["cycles"] if point.get("settledPostGc")]
    result["meanSettledFiles"] = mean(settled_cycles) if settled_cycles else None
    result["peakSettledHeapMiB"] = max((point["heapUsedBytes"] / 1048576 for point in points.values()
                                        if point.get("settledPostGc")), default=0)
    result["firstPromotionCycle"] = next((i + 1 for i, point in enumerate(result["cycles"])
                                          if any(level["index"] > 0 and level["files"] > 0
                                                 for group in point.get("ucs", []) for level in group["levels"])), None)
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+")
    parser.add_argument("--brief", action="store_true", help="Print the comparison fields; retain full details in JSON")
    parser.add_argument("--allocation", action="store_true", help="Convert async-profiler allocation samples from cycle windows")
    parser.add_argument("--jfrconv", type=Path, default=Path(__file__).resolve().parents[2] / "tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv")
    args = parser.parse_args()
    results = [analyze(path) for directory in args.directories for path in sorted(Path(directory).rglob("summary.json"))]
    if args.allocation:
        for result in results:
            result["sampledAllocation"] = allocation_summary(Path(result["path"]), args.jfrconv)
    logs = Path(__file__).resolve().parents[2] / "logs"
    logs.mkdir(exist_ok=True)
    output = logs / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "-analyze-ucs-idle.json")
    output.write_text(json.dumps(results, indent=2) + "\n")
    lines = [str(output)]
    for result in results:
        fields = ("path", "heapMiB", "heapAboveCreatedMiB", "sstables", "meanSettledFiles", "peakSettledFiles", "compactionOutputPerFlushByte",
                  "processCpuSecondsAfterCreation", "pending_compactions", "serviceMillisecondsAfterCycleZero", "sampledAllocation")
        brief = {key: value for key, value in result.items()
                 if (key in fields if args.brief else key not in ("cycles", "idleDrainPhases"))}
        lines.append(json.dumps(brief))
    text = "\n".join(lines) + "\n"
    print(text, end="")
    output.with_suffix(".log").write_text(text)


if __name__ == "__main__":
    main()
