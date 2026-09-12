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
"""Run matched UCS residency experiments against already-built test classes."""

import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess


def validate_run(directory, mode, writes):
    summaries = list(directory.rglob("summary.json"))
    if len(summaries) != 1:
        raise ValueError("Expected exactly one completed harness summary")
    summary = json.loads(summaries[0].read_text())
    if summary.get("failure") or summary.get("completedWrites") != writes or summary.get("failedWriteRequests") != 0:
        raise ValueError("Harness failed or did not complete the expected writes")
    final = summary["checkpoints"]["settled"]
    if mode.startswith("off"):
        if final["sstables"] != 0 or final["initialized_trie_memtables"] != summary["tables"]:
            raise ValueError("Disabled control flushed tables; check the effective memtable pool and flush reasons")
    elif final["initialized_trie_memtables"] != 0 or final["dirty_memtables"] != 0 or final["pending_compactions"] != 0:
        raise ValueError("Retirement or compaction did not settle")


def write_metrics_profile(profile):
    root = Path(__file__).resolve().parents[2]
    # Expose the same byte counters in every mode, including the disabled control.
    table, keyspace = (root / "conf/simple_metrics.yml").read_text().split("\nkeyspace:", 1)
    names = ("BytesFlushed", "CompactionBytesWritten", "MemtableSwitchCount")
    for name in names:
        table = table.replace("    - " + name + "\n", "")
    table = table.replace("  optional:\n", "  optional:\n" + "".join("    - " + name + "\n" for name in names), 1)
    profile.write_text(table + "\nkeyspace:" + keyspace)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=("baseline", "post", "long", "scale", "smoke", "census"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    os.chdir(root)
    output = root / "logs" / (datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "-ucs-idle-" + args.suite)
    output.mkdir(parents=True)
    profile = output / "metrics.yml"
    write_metrics_profile(profile)

    tables, cycles, idle_ms, timeout_ms = 100, 12, 1000, 1000
    modes = [("off-append", "T4"), ("auto-append", "T4"), ("off-overwrite", "T4"), ("auto-overwrite", "T4")]
    if args.suite == "baseline":
        modes = [(name.replace("auto", "explicit"), scaling) for name, scaling in modes]
    elif args.suite == "long":
        tables, cycles, idle_ms, timeout_ms = 20, 48, 100, 100
        modes = [("auto-append", "T4"), ("auto-append", "T8"), ("auto-append", "T4,L10"), ("auto-overwrite", "T4")]
    elif args.suite == "scale":
        tables, cycles = 1000, 4
        modes = [("off-append", "T4"), ("auto-append", "T4")]
    elif args.suite == "smoke":
        tables, cycles, idle_ms, timeout_ms = 3, 2, 30000, 30000
        modes = [("auto-append", "T4")]
    elif args.suite == "census":
        tables, idle_ms = 1000, 0
        modes = [("explicit-1file", "T8"), ("explicit-3files", "T8"), ("explicit-6files", "T8")]
    env = dict(os.environ, PROFILE_SKIP_BUILD="true", MANY_TABLES_XMX="8g" if args.suite in ("scale", "census") else "2g",
               PROFILE_TRANSIENT_JMX="true", PROFILE_LAZY_METRIC_IDS="true", PROFILE_COMPACT_BOOKKEEPING="true")
    results = []
    with (output / "console.log").open("w") as console:
        def log(message):
            print(message, flush=True)
            print(message, file=console, flush=True)

        for mode, scaling in modes:
            rows = 4
            if args.suite == "census":
                # Fixed logical data, with exact flush boundaries below the T8 threshold.
                cycles = {"explicit-1file": 1, "explicit-3files": 3, "explicit-6files": 6}[mode]
                rows = 24 // cycles
            name = mode + "-" + scaling.replace(",", "_")
            command = [".build/sh/ai-profile-memtable-residency", "--scenario", "idle-reactivate",
                       "--tables", str(tables), "--active-tables", str(tables), "--rows-per-table", str(rows),
                       "--cycles", str(cycles), "--rate", "10000", "--idle-ms", str(idle_ms),
                       "--hold-ms", "2000", "--sample-ms", "250", "--payload-bytes", "256", "--subnet", "145",
                       "--ucs-scaling", scaling, "--ucs-min-size", "100MiB", "--metrics-profile", str(profile),
                       "--compact-jmx", "--cursor-compaction", "--no-profile",
                       "--out", str(output / name)]
            command.append("--heap-dumps" if args.suite == "census" else "--settle-each-cycle")
            if mode.startswith("auto"):
                command.extend(("--idle-flush-ms", str(timeout_ms)))
            if args.suite in ("scale", "census"):
                command.extend(("--memtable-heap-mib", "256"))
            if mode.startswith("explicit"):
                command.append("--explicit-retirement")
            if mode.endswith("overwrite"):
                command.append("--overwrite")
            log("START " + name)
            with (output / (name + ".log")).open("w") as run_log:
                process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                for line in process.stdout:
                    print(line, end="", flush=True)
                    run_log.write(line)
                status = process.wait()
            validation_error = None
            if status == 0:
                try:
                    validate_run(output / name, mode, tables * cycles * rows)
                    if args.suite == "census":
                        summary = json.loads(next((output / name).rglob("summary.json")).read_text())
                        final = summary["checkpoints"]["settled"]
                        if final["sstables"] != tables * cycles or final["compactionHistory"]["jobs"] != 0:
                            raise ValueError("Census file count changed or user compaction ran")
                except (ValueError, KeyError) as error:
                    validation_error = str(error)
                    log("VALIDATION FAILED: " + validation_error)
            results.append({"mode": name, "command": command, "exitCode": status,
                            "validationError": validation_error, "heapLimit": env["MANY_TABLES_XMX"]})
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            log(f"END {name}: exit={status}")
            if status:
                return status
            if validation_error:
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
