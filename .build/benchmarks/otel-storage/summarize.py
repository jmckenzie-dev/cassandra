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
"""Summarize complete paired OTel storage benchmark runs without dropping samples."""
import csv
import datetime
import json
import pathlib
import statistics
import sys
import traceback
from collections import defaultdict


class Tee:
    def __init__(self, console, log):
        self.console, self.log = console, log

    def write(self, value):
        self.console.write(value)
        return self.log.write(value)

    def flush(self):
        self.console.flush()
        self.log.flush()


def distribution(values):
    return dict(min=min(values), median=statistics.median(values), max=max(values),
                stdev=statistics.pstdev(values))


def parse(path):
    content = path.read_text()
    if "# benchmark=PASS" not in content:
        raise ValueError(f"Incomplete run: {path}")
    result = {"source": str(path), "samples": [], "memory": []}
    for row in csv.reader(content.splitlines()):
        if not row:
            continue
        if row[0].startswith("# java="):
            result["settings"] = dict(item.split("=", 1) for item in row[0][2:].split())
        if row[0] == "sample" and row[1] != "implementation":
            _, impl, case, round_id, order, operations, ns, alloc, checksum = row
            result["samples"].append(dict(implementation=impl, case=case, round=int(round_id),
                                          order=int(order), operations=int(operations), ns=float(ns),
                                          alloc=float(alloc), checksum=int(checksum)))
        if row[0] == "memory" and row[1] != "implementation":
            _, impl, case, buckets, population, graph, marginal = row
            result["memory"].append(dict(implementation=impl, case=case, buckets=int(buckets),
                                         population=int(population), graph=int(graph), marginal=int(marginal)))
    rounds = int(result["settings"]["rounds"])
    grouped = defaultdict(list)
    for sample in result["samples"]:
        grouped[(sample["case"], sample["round"])].append(sample)
    if len(grouped) != 13 * rounds:
        raise ValueError(f"Missing cases or rounds: {path}")
    for key, samples in grouped.items():
        if {x["implementation"] for x in samples} != {"LONG", "OTEL", "CASSANDRA"} or len(samples) != 3:
            raise ValueError(f"Unpaired sample {key}")
        if len({(x["operations"], x["checksum"]) for x in samples}) != 1:
            raise ValueError(f"Non-equivalent sample {key}")
        if {x["order"] for x in samples} != {0, 1, 2}:
            raise ValueError(f"Invalid order {key}")
    return result


def main(paths, prefix):
    runs = [parse(pathlib.Path(path)) for path in paths]
    if not runs or any(run["settings"] != runs[0]["settings"] for run in runs):
        raise ValueError("Need complete runs with identical settings")
    if any(run["memory"] != runs[0]["memory"] for run in runs):
        raise ValueError("Memory varied between runs")
    pooled = defaultdict(list)
    paired = defaultdict(dict)
    for run_id, run in enumerate(runs):
        for sample in run["samples"]:
            pooled[(sample["case"], sample["implementation"])].append(sample)
            paired[(sample["case"], run_id, sample["round"])][sample["implementation"]] = sample
    summary = {}
    print("| Case | long[] ns/op | Cassandra ns/op | OTel ns/op | OTel/Cassandra paired median | OTel/long[] paired median | OTel alloc B/op |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for case in dict.fromkeys(s["case"] for s in runs[0]["samples"]):
        case_summary = {}
        for impl in ("LONG", "CASSANDRA", "OTEL"):
            samples = pooled[(case, impl)]
            case_summary[impl] = {key: distribution([s[key] for s in samples]) for key in ("ns", "alloc")}
            case_summary[impl]["jvm_median_ns"] = [statistics.median(s["ns"] for s in run["samples"]
                                                                     if s["case"] == case and s["implementation"] == impl)
                                                        for run in runs]
        for baseline in ("LONG", "CASSANDRA"):
            ratios = [p["OTEL"]["ns"] / p[baseline]["ns"] for key, p in paired.items() if key[0] == case]
            case_summary["OTEL_over_" + baseline] = distribution(ratios)
        summary[case] = case_summary
        print(f"| {case} | {case_summary['LONG']['ns']['median']:.3f} | {case_summary['CASSANDRA']['ns']['median']:.3f} | "
              f"{case_summary['OTEL']['ns']['median']:.3f} | {case_summary['OTEL_over_CASSANDRA']['median']:.3f} | "
              f"{case_summary['OTEL_over_LONG']['median']:.3f} | {case_summary['OTEL']['alloc']['median']:.3f} |")
    output = dict(runs=runs, summary=summary)
    print("\n| Case | OTel JVM medians ns/op | Cassandra JVM medians ns/op | OTel/Cassandra pair min..max |")
    print("|---|---|---|---|")
    for case, values in summary.items():
        otel = ', '.join(f"{v:.3f}" for v in values['OTEL']['jvm_median_ns'])
        cassandra = ', '.join(f"{v:.3f}" for v in values['CASSANDRA']['jvm_median_ns'])
        ratios = values['OTEL_over_CASSANDRA']
        print(f"| {case} | {otel} | {cassandra} | {ratios['min']:.3f}..{ratios['max']:.3f} |")
    prefix.with_suffix(".json").write_text(json.dumps(output, indent=2) + "\n")
    print(f"Runs: {len(runs)}; raw samples: {sum(len(run['samples']) for run in runs)}; all checksums matched.")
    print(f"JSON: {prefix.with_suffix('.json')}")


if __name__ == "__main__":
    prefix = pathlib.Path("logs") / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f") + "-summarize-otel-storage")
    with prefix.with_suffix(".log").open("w") as log:
        out, err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = Tee(out, log), Tee(err, log)
        try:
            main(sys.argv[1:], prefix)
        except BaseException:
            traceback.print_exc()
            raise SystemExit(1)
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout, sys.stderr = out, err
