#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Which classes grow with the table count. Run it through test/bin/schema-scale-growth.
#
# Reads <run-dir>/steps/t*/class-histogram.txt (jcmd GC.class_histogram) and, when the table count is
# new, <run-dir>/final/class-histogram.txt. For each class it fits bytes and instances against the table
# count by least squares and writes <run-dir>/growth.csv and growth.md with the top 30 classes by bytes
# per table. growth-all.csv holds every class. Empty, cut and unreadable histograms are skipped.
#
# R2 close to 1 means a straight line. Ratio is the slope over the last third of the steps divided by
# the slope over the first third; above 1.5 the class is flagged SUPERLINEAR.

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

TOP = 30
SUPERLINEAR_RATIO = 1.5
MIN_POINTS_FOR_THIRDS = 4
LINE = re.compile(r"^\s*(\d+):\s+(\d+)\s+(\d+)\s+(.+?)\s*$")
MODULE = re.compile(r"\s+\([^()]*\)$")
PRIMITIVES = {"B": "byte", "C": "char", "D": "double", "F": "float", "I": "int", "J": "long", "S": "short", "Z": "boolean"}


def log(msg=""):
    print(msg, flush=True)


def class_name(raw):
    """Readable class name: drop the module suffix and expand array descriptors."""
    name = MODULE.sub("", raw)
    dims = len(name) - len(name.lstrip("["))
    if dims == 0:
        return name
    rest = name[dims:]
    if rest in PRIMITIVES:
        base = PRIMITIVES[rest]
    elif rest.startswith("L") and rest.endswith(";"):
        base = rest[1:-1]
    else:
        base = rest
    return base + "[]" * dims


def parse_histogram(path):
    """Returns {class: (instances, bytes)} or None when the file holds no histogram lines."""
    try:
        text = Path(path).read_text(errors="replace")
    except OSError as e:
        log(f"warning: cannot read {path}: {e}")
        return None
    rows = {}
    for line in text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        name = class_name(m.group(4))
        inst, size = rows.get(name, (0, 0))
        rows[name] = (inst + int(m.group(2)), size + int(m.group(3)))
    return rows or None


def tables_of(step_dir):
    m = re.fullmatch(r"t0*(\d+)", step_dir.name)
    return int(m.group(1)) if m else None


def load_steps(run):
    """Returns a sorted list of (tables, {class: (instances, bytes)})."""
    found = {}
    for d in sorted((run / "steps").glob("t*")):
        tables = tables_of(d)
        hist = d / "class-histogram.txt"
        if tables is None or not hist.is_file():
            continue
        rows = parse_histogram(hist)
        if rows is None:
            log(f"warning: skipping {hist}: no histogram lines")
            continue
        found[tables] = rows
    final = run / "final" / "class-histogram.txt"
    if final.is_file():
        tables = final_tables(run)
        rows = parse_histogram(final)
        if rows is None:
            log(f"warning: skipping {final}: no histogram lines")
        elif tables is None:
            log("note: final histogram ignored: the table count at the stop is unknown (no summary.json)")
        elif tables not in found:
            found[tables] = rows
    return sorted(found.items())


def final_tables(run):
    try:
        return int(json.loads((run / "summary.json").read_text())["tablesAtStop"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def slope_r2(x, y):
    """Least squares over rows of y (classes x points). Returns slope, intercept, r2 arrays."""
    xm = x.mean()
    dx = x - xm
    sxx = float((dx * dx).sum())
    ym = y.mean(axis=1)
    dy = y - ym[:, None]
    slope = (dy @ dx) / sxx
    intercept = ym - slope * xm
    syy = (dy * dy).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        r2 = np.where(syy > 0, slope * slope * sxx / syy, 0.0)
    return slope, intercept, r2


def third_ratio(x, y):
    """Last-third slope over first-third slope per class; NaN when it cannot be told."""
    n = len(x)
    if n < MIN_POINTS_FOR_THIRDS:
        return np.full(y.shape[0], np.nan)
    k = max(2, n // 3)
    first, _, _ = slope_r2(x[:k], y[:, :k])
    last, _, _ = slope_r2(x[-k:], y[:, -k:])
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(first > 0, last / first, np.nan)


def analyse(steps):
    """Builds the per-class table from parsed steps. Needs at least two steps."""
    x = np.array([t for t, _ in steps], dtype=float)
    names = sorted({n for _, rows in steps for n in rows})
    index = {n: i for i, n in enumerate(names)}
    inst = np.zeros((len(names), len(steps)))
    size = np.zeros((len(names), len(steps)))
    for j, (_, rows) in enumerate(steps):
        for n, (i_count, b_count) in rows.items():
            inst[index[n], j] = i_count
            size[index[n], j] = b_count
    b_slope, b_icpt, b_r2 = slope_r2(x, size)
    i_slope, _, i_r2 = slope_r2(x, inst)
    ratio = third_ratio(x, size)
    flag = np.where((ratio > SUPERLINEAR_RATIO) & (b_slope > 0), "SUPERLINEAR", "")
    frame = pd.DataFrame({
        "class": names,
        "bytes_per_table": b_slope,
        "instances_per_table": i_slope,
        "r2_bytes": b_r2,
        "r2_instances": i_r2,
        "bytes_intercept": b_icpt,
        "last_bytes": size[:, -1],
        "last_instances": inst[:, -1],
        "slope_ratio_last_to_first_third": ratio,
        "flag": flag,
        "points": len(steps),
    })
    total = size.sum(axis=0)
    t_slope, _, t_r2 = slope_r2(x, total[None, :])
    return frame.sort_values("bytes_per_table", ascending=False).reset_index(drop=True), (float(t_slope[0]), float(t_r2[0]))


def column_slope(csv, x_col, y_col, where=None):
    """Slope and R2 of y_col against x_col in a csv, or None."""
    if not csv.is_file():
        return None
    try:
        df = pd.read_csv(csv)
    except (ValueError, OSError) as e:
        log(f"warning: cannot read {csv}: {e}")
        return None
    if where is not None:
        df = df[where(df)]
    if x_col not in df or y_col not in df:
        return None
    df = df[(df[y_col] >= 0) & (df[x_col] > 0)].drop_duplicates(subset=[x_col, y_col])
    if len(df) < 2 or df[x_col].nunique() < 2:
        return None
    s, _, r2 = slope_r2(df[x_col].to_numpy(dtype=float), df[y_col].to_numpy(dtype=float)[None, :])
    return float(s[0]), float(r2[0]), len(df), float(df[x_col].min()), float(df[x_col].max())


def fmt(value, digits=1):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"{value:,.{digits}f}"


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return out


def write_report(run, steps, frame, total, heap_fit, cold_fit):
    top = frame.head(TOP)
    cols = ["class", "bytes_per_table", "instances_per_table", "r2_bytes", "r2_instances", "bytes_intercept", "last_bytes",
            "last_instances", "slope_ratio_last_to_first_third", "flag", "points"]
    top[cols].to_csv(run / "growth.csv", index_label="rank", float_format="%.4f")
    frame[cols].to_csv(run / "growth-all.csv", index_label="rank", float_format="%.4f")

    lines = [f"# Heap growth per table: {run.name}", ""]
    lines.append(f"- Histograms: {len(steps)} steps, from {steps[0][0]:,} to {steps[-1][0]:,} tables.")
    lines.append(f"- Live bytes in all histogram classes: {fmt(total[0])} bytes per table (R2 {total[1]:.3f}).")
    if heap_fit:
        lines.append(f"- heap_post_gc in steps.csv: {fmt(heap_fit[0])} bytes per table (R2 {heap_fit[1]:.3f}, "
                     f"{heap_fit[2]} rows, {heap_fit[3]:,.0f} to {heap_fit[4]:,.0f} tables).")
    else:
        lines.append("- heap_post_gc slope: steps.csv missing or too short.")
    if cold_fit:
        lines.append(f"- heap_post_gc after restart (restart.csv): {fmt(cold_fit[0])} bytes per table (R2 {cold_fit[1]:.3f}, "
                     f"{cold_fit[2]} restarts). This is the footprint without DDL garbage.")
    flagged = frame[(frame["flag"] == "SUPERLINEAR")].head(TOP)
    lines += ["", f"## Top {TOP} classes by bytes per table", ""]
    rows = []
    for rank, r in enumerate(top.itertuples(index=False), 1):
        rows.append([rank, f"`{r[0]}`", fmt(r.bytes_per_table), fmt(r.instances_per_table, 2), f"{r.r2_bytes:.3f}",
                     fmt(r.slope_ratio_last_to_first_third, 2), r.flag or ""])
    lines += md_table(["rank", "class", "bytes/table", "instances/table", "R2 bytes", "last/first third", "flag"], rows)
    lines += ["", "## Super-linear classes (top by bytes per table)", ""]
    if len(flagged):
        lines += md_table(["class", "bytes/table", "last/first third"],
                          [[f"`{r.iloc[0]}`", fmt(r["bytes_per_table"]), fmt(r["slope_ratio_last_to_first_third"], 2)]
                           for _, r in flagged.iterrows()])
    else:
        lines.append("None flagged" + ("" if len(steps) >= MIN_POINTS_FOR_THIRDS else
                                      f" (needs at least {MIN_POINTS_FOR_THIRDS} steps)") + ".")
    (run / "growth.md").write_text("\n".join(lines) + "\n")
    return lines


def main(argv):
    if len(argv) != 1:
        print("usage: schema-scale-growth <run-dir>", file=sys.stderr)
        return 2
    run = Path(argv[0])
    if not run.is_dir():
        print(f"schema-scale-growth: not a directory: {run}", file=sys.stderr)
        return 2
    steps = load_steps(run)
    log(f"schema-scale-growth: run={run} histograms={len(steps)}")
    if len(steps) < 2:
        log("schema-scale-growth: need at least two class histograms (steps/t*/class-histogram.txt); nothing written")
        return 1
    frame, total = analyse(steps)
    heap_fit = column_slope(run / "steps.csv", "tables", "heap_post_gc", lambda d: d["ramp"] == "02-ramp-tables")
    cold_fit = column_slope(run / "restart.csv", "tables", "heap_post_gc")
    lines = write_report(run, steps, frame, total, heap_fit, cold_fit)
    log("")
    for line in lines:
        log(line)
    log("")
    log(f"schema-scale-growth: wrote {run / 'growth.csv'}, {run / 'growth.md'}, {run / 'growth-all.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
