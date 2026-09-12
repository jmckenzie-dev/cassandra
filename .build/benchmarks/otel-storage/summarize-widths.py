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
"""Validate and summarize actual reservoir width observations."""
import csv
import datetime
import json
import pathlib
import re
import sys
import traceback
from collections import defaultdict

from summarize import Tee

WIDTHS = ("zero", "byte", "short", "int", "long")


def read(path):
    content = path.read_text()
    if "# width_over_time=PASS" not in content:
        raise ValueError(f"Incomplete run: {path}")
    match = re.search(r"# tables=(\d+) seconds=(\d+) spread=(true|false)", content)
    if not match:
        raise ValueError(f"Missing settings: {path}")
    result = dict(source=str(path), tables=int(match[1]), seconds=int(match[2]), spread=match[3] == "true",
                  samples=[], memory=[])
    headers = {}
    for row in csv.reader(content.splitlines()):
        if not row or row[0] not in ("sample", "memory"):
            continue
        kind = row[0]
        if row[1] == "seconds":
            headers[kind] = row[1:]
            continue
        if len(row) - 1 != len(headers[kind]):
            raise ValueError(f"Malformed {kind}: {row}")
        item = {key: value if key in ("cohort", "phase") else int(value)
                for key, value in zip(headers[kind], row[1:])}
        result["samples" if kind == "sample" else "memory"].append(item)
    groups = defaultdict(dict)
    for item in result["samples"]:
        key = (item["seconds"], item["phase"])
        if item["cohort"] in groups[key]:
            raise ValueError("Duplicate observation")
        groups[key][item["cohort"]] = item
        for prefix in ("raw", "cumulative"):
            if sum(item[f"{prefix}_{width}"] for width in WIDTHS) != item["tables"]:
                raise ValueError("Width distribution does not sum to population")
    for (second, phase), group in groups.items():
        if phase not in ("pre", "post") or len(group) < 2 or group["all"]["tables"] != result["tables"]:
            raise ValueError("Missing cohorts or incorrect population")
        children = [value for key, value in group.items() if key != "all"]
        for field in ("tables", "cumulative_total", "allocated_cells",
                      *(f"{prefix}_{width}" for prefix in ("raw", "cumulative") for width in WIDTHS)):
            if sum(item[field] for item in children) != group["all"][field]:
                raise ValueError(f"Invalid aggregate {field}")
        for field in ("raw_max", "cumulative_max", "normalized_max"):
            if max(item[field] for item in children) != group["all"][field]:
                raise ValueError(f"Invalid aggregate {field}")
        if phase == "post":
            before = groups[(second, "pre")]
            for cohort, item in group.items():
                if item["cumulative_total"] != before[cohort]["cumulative_total"]:
                    raise ValueError("Scrape changed cumulative count")
                if item["raw_max"] > before[cohort]["raw_max"]:
                    raise ValueError("Scrape increased raw maximum")
    for second in range(0, result["seconds"] + 1, 60):
        if (second, "post") not in groups:
            raise ValueError("Missing minute checkpoint")
    if (result["seconds"], "post") not in groups:
        raise ValueError("Missing final checkpoint")
    return result


def summarize(run):
    post = [s for s in run["samples"] if s["phase"] == "post"]
    final = [s for s in post if s["seconds"] == run["seconds"]]
    print(f"\nRun: {run['source']}; {run['tables']} instances; {run['seconds']/3600:g} hours; spread={run['spread']}")
    print("\n| Cohort | Final cumulative max | Final raw weighted max | Final normalized max | Largest observed raw count | Final marginal reservoir B |")
    print("|---|---:|---:|---:|---:|---:|")
    peak = {}
    for item in final:
        cohort = item["cohort"]
        peak[cohort] = max(s["raw_max"] for s in run["samples"] if s["cohort"] == cohort)
        sizes = [m["marginal_bytes"] for m in run["memory"]
                 if m["cohort"] == cohort and m["seconds"] == run["seconds"] and m["phase"] == "post"]
        print(f"| {cohort} | {item['cumulative_max']} | {item['raw_max']} | {item['normalized_max']} | {peak[cohort]} | {sizes[0] if sizes else '—'} |")
    checkpoints = [s for s in post if s["cohort"] == "all" and s["seconds"] % 60 == 0]
    denominator = sum(s["tables"] for s in checkpoints)
    shares = {prefix: {width: 100 * sum(s[f"{prefix}_{width}"] for s in checkpoints) / denominator
                       for width in WIDTHS} for prefix in ("raw", "cumulative")}
    print("Minute-checkpoint shares (including never-used and idle instances; not continuous-time fractions):")
    print(json.dumps(shares, sort_keys=True))
    print("Final distributions:")
    last = next(s for s in final if s["cohort"] == "all")
    print(json.dumps({prefix: {width: last[f"{prefix}_{width}"] for width in WIDTHS} for prefix in shares}))
    return dict(final=final, peak_raw=peak, minute_checkpoint_shares=shares)


def main(paths, prefix):
    if not paths:
        raise ValueError("Supply one or more complete width-over-time log paths")
    runs = [read(pathlib.Path(path)) for path in paths]
    summaries = [summarize(run) for run in runs]
    prefix.with_suffix(".json").write_text(json.dumps(dict(runs=runs, summaries=summaries), indent=2) + "\n")
    print(f"JSON: {prefix.with_suffix('.json')}")


if __name__ == "__main__":
    pathlib.Path("logs").mkdir(exist_ok=True)
    prefix = pathlib.Path("logs") / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f") + "-histogram-width-summary")
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
