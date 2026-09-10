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
"""Compare UCS hierarchy sizes with fixed flush boundaries and level traces."""

import argparse
import contextlib
import datetime
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

from benchmark_ucs_idle import validate_run, write_metrics_profile

Tee = importlib.import_module("analyze-heap-ownership").Tee


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("label")
    parser.add_argument("--sizes", nargs="+", choices=("default", "1MiB", "64KiB", "4KiB", "1KiB", "1B"), default=["default"])
    parser.add_argument("--workloads", nargs="+", choices=("append", "overwrite"), default=["append", "overwrite"])
    parser.add_argument("--scaling", choices=("T4", "T8"), default="T4")
    parser.add_argument("--cycles", type=int, default=192)
    parser.add_argument("--tables", type=int, default=20)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--automatic", action="store_true")
    parser.add_argument("--heap-dumps", action="store_true")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.tables <= 1000 or args.cycles < 1 or args.repeats < 1:
        parser.error("Use 1–1000 tables and positive cycles/repeats")
    root = Path(__file__).resolve().parents[2]
    os.chdir(root)
    output = root / "logs" / f"{datetime.datetime.now():%Y%m%d-%H%M%S}-ucs-hierarchy-{args.label}"
    output.mkdir(parents=True)
    profile = output / "metrics.yml"
    write_metrics_profile(profile)
    results = []
    env = dict(os.environ, PROFILE_SKIP_BUILD="true", MANY_TABLES_XMX="8g" if args.tables > 100 else "2g",
               PROFILE_TRANSIENT_JMX="true", PROFILE_LAZY_METRIC_IDS="true", PROFILE_COMPACT_BOOKKEEPING="true")
    with (output / "console.log").open("w") as log, contextlib.redirect_stdout(Tee(sys.stdout, log)), contextlib.redirect_stderr(Tee(sys.stderr, log)):
        print("Output:", output)
        for repeat in range(args.repeats):
            sizes = args.sizes if repeat % 2 == 0 else list(reversed(args.sizes))
            for workload in args.workloads:
                for size in sizes:
                    name = f"{repeat}-{workload}-{args.scaling}-{size}"
                    command = [".build/sh/ai-profile-memtable-residency", "--scenario", "idle-reactivate",
                               "--tables", str(args.tables), "--rows-per-table", "4", "--cycles", str(args.cycles),
                               "--payload-bytes", "256", "--rate", "10000", "--idle-ms", "0", "--hold-ms", "2000",
                               "--sample-ms", "100", "--subnet", "145", "--ucs-scaling", args.scaling,
                               "--metrics-profile", str(profile), "--compact-jmx", "--cursor-compaction",
                               "--settle-each-cycle", "--ucs-trace", "--memtable-heap-mib", "256",
                               "--out", str(output / name)]
                    if size != "default":
                        command.extend(("--ucs-min-hierarchy", size))
                    if args.automatic:
                        command.extend(("--idle-flush-ms", "300"))
                    else:
                        command.append("--explicit-retirement")
                    if workload == "overwrite":
                        command.append("--overwrite")
                    if args.heap_dumps:
                        command.append("--final-heap-dump")
                    if not args.profile:
                        command.append("--no-profile")
                    print("START", name, "log:", output / (name + ".log"))
                    with (output / (name + ".log")).open("w") as run_log:
                        status = subprocess.run(command, env=env, stdout=run_log, stderr=subprocess.STDOUT).returncode
                    error = None
                    if status == 0:
                        try:
                            validate_run(output / name, "auto" if args.automatic else "explicit", args.tables * args.cycles * 4)
                            summary_path, = (output / name).rglob("summary.json")
                            summary = json.loads(summary_path.read_text())
                            if summary["checkpoints"]["settled"]["flushes"] != args.tables * args.cycles:
                                raise ValueError("Unexpected flush boundaries")
                        except (ValueError, KeyError) as failure:
                            error = str(failure)
                    results.append({"name": name, "command": command, "status": status, "validationError": error})
                    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                    print("END", name, "status:", status, "validation:", error)
                    if status or error:
                        return status or 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
