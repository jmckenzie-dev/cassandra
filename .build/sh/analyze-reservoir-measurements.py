#!/usr/bin/env python3
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
"""Summarize raw reservoir probe samples and JMH iteration measurements."""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import statistics
import sys
import traceback


class Tee:
    def __init__(self, console, log):
        self.console = console
        self.log = log

    def write(self, text):
        self.console.write(text)
        self.log.write(text)
        self.flush()

    def flush(self):
        self.console.flush()
        self.log.flush()


def distribution(values):
    finite = [value for value in values if math.isfinite(value)]
    return {"samples": len(values), "unavailable_samples": len(values) - len(finite),
            "median": statistics.median(finite) if finite else None,
            "min": min(finite) if finite else None,
            "max": max(finite) if finite else None}


def implementation(class_name):
    names = {"DecayingEstimatedHistogramReservoir": "legacy",
             "CompactDecayingEstimatedHistogramReservoir": "compact"}
    return names[class_name.rsplit(".", 1)[-1]]


def probe_records(console):
    headers = {}
    storage = defaultdict(list)
    timings = defaultdict(list)
    metadata = None
    current = None
    for line in console.read_text().splitlines():
        match = re.search(r"(?:^| - )reservoir_(measurement|probe|storage|timing) (.*)$", line)
        if not match:
            continue
        kind, payload = match.groups()
        record = dict(part.split("=", 1) for part in payload.split())
        if kind == "measurement":
            if metadata is not None:
                raise ValueError("Duplicate measurement header")
            metadata = record
        elif kind == "probe":
            current = implementation(record["class"])
            if current in headers:
                raise ValueError(f"Duplicate probe header: {current}")
            headers[current] = record
        else:
            if current is None:
                raise ValueError("Probe record precedes its header")
            if kind == "storage":
                numeric = {key: float(value) if key == "amortized_bytes" else int(value)
                           for key, value in record.items()}
                storage[(current, numeric["occupied"], numeric["count"])].append(numeric)
            else:
                numeric = {key: float(value) if key in ("ns_per_op", "bytes_per_op") else int(value)
                           for key, value in record.items() if key != "operation"}
                timings[(current, record["operation"], numeric["threads"], numeric["occupied"])].append(numeric)
    if metadata is None or set(headers) != {"legacy", "compact"}:
        raise ValueError("Expected a measurement header and both reservoir probes")
    storage_summary = []
    for (mode, occupied, count), records in sorted(storage.items()):
        storage_summary.append({"implementation": mode, "occupied": occupied, "count": count,
                                "metrics": {key: distribution([record[key] for record in records])
                                            for key in records[0] if key not in ("occupied", "count")}})
    timing_summary = []
    for (mode, operation, threads, occupied), records in sorted(timings.items()):
        expected = int(headers[mode]["samples"])
        if sorted(record["sample"] for record in records) != list(range(expected)):
            raise ValueError(f"Incomplete samples: {mode}/{operation}/{threads}/{occupied}")
        timing_summary.append({"implementation": mode, "operation": operation,
                               "threads": threads, "occupied": occupied,
                               "metrics": {key: distribution([record[key] for record in records])
                                           for key in records[0] if key not in ("sample", "threads", "occupied")}})
    storage_cases = {mode: {(occupied, count) for candidate, occupied, count in storage if candidate == mode}
                     for mode in headers}
    timing_cases = {mode: {(operation, threads, occupied) for candidate, operation, threads, occupied in timings
                          if candidate == mode} for mode in headers}
    if not storage_cases["legacy"] or storage_cases["legacy"] != storage_cases["compact"]:
        raise ValueError("Missing or mismatched storage scenarios")
    if not timing_cases["legacy"] or timing_cases["legacy"] != timing_cases["compact"]:
        raise ValueError("Missing or mismatched timing scenarios")
    return metadata, headers, storage_summary, timing_summary


def jmh_records(paths):
    result = []
    cases = set()
    for path in paths:
        for benchmark in json.loads(path.read_text()):
            mode = benchmark["params"]["implementation"]
            threads = benchmark["threads"]
            case = (mode, threads, benchmark["benchmark"])
            if case in cases:
                raise ValueError(f"Duplicate JMH result: {case}")
            cases.add(case)
            metrics = {"throughput": benchmark["primaryMetric"], **benchmark["secondaryMetrics"]}
            for name, metric in metrics.items():
                samples = [float(value) for fork in metric["rawData"] for value in fork]
                if not samples:
                    raise ValueError(f"Missing JMH raw samples: {case}/{name}")
                summary = distribution(samples)
                if name == "throughput" and summary["unavailable_samples"]:
                    raise ValueError(f"Non-finite JMH throughput: {case}")
                result.append({"implementation": mode, "threads": threads,
                               "benchmark": benchmark["benchmark"], "metric": name,
                               "unit": metric["scoreUnit"], "statistics": summary,
                               "forks": benchmark["forks"], "warmup_iterations": benchmark["warmupIterations"],
                               "measurement_iterations": benchmark["measurementIterations"],
                               "source": str(path)})
    if {(mode, threads) for mode, threads, _ in cases} != {(mode, threads) for mode in ("legacy", "compact") for threads in (1, 4)}:
        raise ValueError("Expected legacy and compact JMH results with one and four threads")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True, type=Path)
    args = parser.parse_args()
    batch = args.batch.resolve()
    consoles = list(batch.glob("*-console.log"))
    jmh = sorted(batch.glob("*-jmh-t*.json"))
    if len(consoles) != 1 or len(jmh) != 2:
        raise ValueError("Expected one batch console log and two JMH JSON files")
    metadata, headers, storage, timings = probe_records(consoles[0])
    summary = {"metadata": metadata, "probe_headers": headers,
               "sources": {"console": str(consoles[0]), "jmh": [str(path) for path in jmh]},
               "storage": storage, "timings": timings, "jmh": jmh_records(jmh)}
    destination = batch / f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S-%f}-reservoir-summary.json"
    destination.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    print(json.dumps(summary, indent=2, allow_nan=False))
    print(f"Reservoir summary: {destination}")


if __name__ == "__main__":
    logs = Path(__file__).resolve().parents[2] / "logs"
    logs.mkdir(exist_ok=True)
    log_path = logs / f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S-%f}-analyze-reservoir-measurements.log"
    with log_path.open("x") as log:
        stdout, stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = Tee(stdout, log), Tee(stderr, log)
        try:
            print(f"Analysis log: {log_path}")
            main()
        except Exception:
            traceback.print_exc()
            sys.exit(1)
        finally:
            sys.stdout, sys.stderr = stdout, stderr
