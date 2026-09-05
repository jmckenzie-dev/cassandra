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
"""Validate and summarize ai-compare-java-allocation runs and allocation stacks."""
import csv
import datetime
import json
import logging
import math
from pathlib import Path
import subprocess
import sys


def allocation_totals(path):
    totals = dict(total=0, tombstone_spool=0, meter_rate_array=0, meter_registration=0)
    for line in path.read_text().splitlines():
        stack, weight = line.rsplit(" ", 1)
        weight = int(weight)
        totals["total"] += weight
        if "StreamingTombstoneHistogramBuilder$Spool.<init>" in stack:
            totals["tombstone_spool"] += weight
        if "ThreadLocalMeter.allocateRateGroupOffset" in stack:
            totals["meter_rate_array"] += weight
        if "ThreadLocalMeter.<init>" in stack and "CopyOnWriteArrayList.add" in stack:
            totals["meter_registration"] += weight
    return totals


def summarize(batch):
    converter = Path("tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv")
    runs = []
    for path in sorted(batch.glob("*/summary.json")):
        summary = json.loads(path.read_text())
        if summary["failure"] or any(p["errors"] for p in summary["phases"]):
            raise ValueError(f"Failed workload: {path}")
        if not 1 <= summary["tables"] <= 1000:
            raise ValueError(f"Invalid table count: {path}")
        histograms = summary["lazyTombstoneHistograms"]
        meters = summary["geometricMeterArrays"]
        if histograms and meters:
            raise ValueError("Each comparison must isolate one candidate")
        for key in ("lazyTombstoneHistograms", "geometricMeterArrays"):
            if summary[key] != summary["effectiveConfiguration"][key]:
                raise ValueError(f"Requested/effective configuration mismatch: {path}")
        cycles = 0 if summary["scenario"] == "never-written" else summary["cycles"]
        expected_writes = cycles * summary["activeTables"] * summary["rowsPerTablePerCycle"]
        if summary["completedWrites"] != expected_writes or summary["failedWriteRequests"]:
            raise ValueError(f"Write count mismatch: {path}")
        writes = []
        for file in path.parent.glob("writes-*.csv"):
            with file.open() as stream:
                writes.extend(csv.DictReader(stream))
        if len(writes) != expected_writes or any(row["success"] != "true" for row in writes):
            raise ValueError(f"Write CSV mismatch: {path}")
        reads = 0
        for file in path.parent.glob("reads-*.csv"):
            with file.open() as stream:
                reads += sum(1 for _ in csv.DictReader(stream))
        if reads != summary["completedReadQueries"] or reads != summary["tables"] * (cycles + 1):
            raise ValueError(f"Read count mismatch: {path}")
        settled = summary["checkpoints"]["settled"]
        if summary["explicitRetirement"]:
            if summary["completedRetirementRequests"] != cycles * summary["activeTables"]:
                raise ValueError(f"Retirement count mismatch: {path}")
            for name, checkpoint in summary["checkpoints"].items():
                if name.endswith(("-reclaimed", "-read")) or name == "settled":
                    for key in ("initialized_trie_memtables", "dirty_memtables", "flushing_memtables",
                                "pending_flushes", "memtable_accounted_heap_bytes"):
                        if checkpoint[key] != 0:
                            raise ValueError(f"Incomplete retirement: {path} {name} {key}")
        latencies = sorted(int(row["service_ns"]) / 1e6 for row in writes)
        sampled_heap = []
        for file in path.parent.glob("*-samples.csv"):
            with file.open() as stream:
                sampled_heap.extend(int(row["heap_used_bytes"]) for row in csv.DictReader(stream))
        create = next(p for p in summary["phases"] if p["name"] == "01-create")
        row = dict(run=path.parent.name, candidate=histograms or meters,
                   histogram=histograms, meter=meters, profile=summary["profileEnabled"],
                   createdHeapMiB=summary["checkpoints"]["created"]["heapUsedBytes"] / 2**20,
                   settledHeapMiB=settled["heapUsedBytes"] / 2**20,
                   sampledPeakHeapMiB=max(sampled_heap, default=0) / 2**20,
                   createSeconds=create["elapsedNanos"] / 1e9,
                   retirementMillis=[p["elapsedNanos"] / 1e6 for p in summary["phases"]
                                     if p["name"].endswith("-retire") and not p["name"].endswith("-pre-retire")],
                   writeP99Millis=latencies[math.ceil(.99 * len(latencies)) - 1] if latencies else None,
                   writes=expected_writes, reads=reads, sstables=settled["sstables"],
                   warnings=summary["warnings"], allocation=[])
        if summary["profileEnabled"]:
            for phase in summary["phases"]:
                name = phase["name"]
                if name != "01-create" and not (name.endswith("-retire") and not name.endswith("-pre-retire")):
                    continue
                recording = path.parent / f"{name}.ap.jfr"
                result = dict(phase=name)
                for unit, flags in (("samples", []), ("weightedBytes", ["--total"])):
                    output = path.parent / f"{name}-alloc-{unit}.collapsed"
                    command = [str(converter), "-o", "collapsed", "--alloc", "--dot", *flags, str(recording), str(output)]
                    completed = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    logging.info("COMMAND %s\n%s", json.dumps(command), completed.stdout)
                    completed.check_returncode()
                    result[unit] = allocation_totals(output)
                if result["samples"]["total"] <= 0:
                    raise ValueError(f"No allocation samples in {recording}")
                row["allocation"].append(result)
        runs.append(row)
    if len(runs) != 6:
        raise ValueError(f"Expected six complete fresh-JVM runs, found {len(runs)}")
    if [r["candidate"] for r in runs] != [False, True, True, False, False, True]:
        raise ValueError("Unexpected A/B run order")
    if [r["profile"] for r in runs] != [False, False, False, False, True, True]:
        raise ValueError("Unexpected profiling configuration")
    destination = batch / "java-allocation-comparison.json"
    destination.write_text(json.dumps(runs, indent=2) + "\n")
    logging.info("RESULTS\n%s\nWROTE %s", json.dumps(runs, indent=2), destination)


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    log = Path("logs") / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "-analyze-java-allocation.log")
    logging.basicConfig(level=logging.INFO, format="%(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log)])
    try:
        if len(sys.argv) != 2:
            raise ValueError("Usage: analyze-java-allocation.py <comparison-directory>")
        summarize(Path(sys.argv[1]))
    except BaseException:
        logging.exception("Analysis failed")
        sys.exit(1)
