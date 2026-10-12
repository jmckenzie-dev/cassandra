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

# Compare schema scale runs.
#
# Run with:
#   uv run --no-project --with pandas,matplotlib test/bin/schema-scale-report.py <run-dir> [<run-dir> ...]
#
# Reads summary.json and steps.csv from each run dir. Missing files and columns are skipped.
# Writes logs/<ts>-compare/report.md and PNG charts. Prints the same text to the console and
# to logs/<ts>-compare/report.log. Override the output dir with --out <dir>.

import json
import sys
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Categorical slots in fixed order (validated reference palette, light surface)
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#e4e3df"
DELTA_METRICS = ["ddl_p99_ms", "heap_per_table", "heap_post_gc", "mbeans", "snapshot_bytes", "alloc_bytes_total"]


class Run:
    def __init__(self, path):
        self.path = Path(path)
        self.summary = {}
        self.steps = pd.DataFrame()
        sj = self.path / "summary.json"
        if sj.is_file():
            try:
                self.summary = json.loads(sj.read_text())
            except (ValueError, OSError) as e:
                log(f"warning: cannot read {sj}: {e}")
        else:
            log(f"warning: {sj} missing")
        sc = self.path / "steps.csv"
        if sc.is_file():
            try:
                self.steps = pd.read_csv(sc)
            except (ValueError, OSError) as e:
                log(f"warning: cannot read {sc}: {e}")
        else:
            log(f"warning: {sc} missing")
        self.steps = self._normalize(self.steps)
        s = self.summary
        self.variant = str(first(s, "variant", "variantName", default=self._from_dir(1)))
        self.spread = str(first(s, "keyspaceSpread", "spread", "keyspace_spread", default=self._from_dir(2)))
        self.profile = str(first(s, "profile", "profileName", "name", default=""))
        self.mode = str(first(s, "mode", default=""))
        self.label = f"{self.variant}/ks{self.spread}"

    def _from_dir(self, which):
        # dir name: <ts>-<profile>-<variant>-ks<spread>; variants may contain '-'
        name = self.path.name
        if "-ks" in name:
            head, _, spread = name.rpartition("-ks")
            if which == 2:
                return spread
            parts = head.split("-")
            return "-".join(parts[3:]) if len(parts) > 3 else head
        return name if which == 1 else "?"

    @staticmethod
    def _normalize(df):
        if df.empty:
            return df
        df = df.copy()
        if "snapshot_bytes" not in df.columns:
            src = [c for c in ("snapshot_bytes_computed", "snapshot_bytes_gauge") if c in df.columns]
            if src:
                col = df[src[0]]
                for c in src[1:]:
                    col = col.where(col.notna() & (col > 0), df[c])
                df["snapshot_bytes"] = col
        for c in df.columns:
            if c != "ramp":
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    def ramp_rows(self, kind):
        df = self.steps
        if df.empty or "ramp" not in df.columns:
            return df if kind in ("", "table", "tables") else df.iloc[0:0]
        mask = df["ramp"].astype(str).str.lower().str.contains(kind)
        return df[mask]

    def breaks(self):
        b = first(self.summary, "breakPoints", "break_points", default=[])
        return b if isinstance(b, list) else []


def first(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d[k]
    return default


LINES = []


def log(msg=""):
    print(msg)
    LINES.append(msg)


def fmt(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    if isinstance(v, float):
        return f"{v:,.1f}" if abs(v) < 1e6 else f"{v:,.0f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c).replace("|", "/").replace("\n", " ") for c in r) + " |")
    return out


def last_val(df, col):
    if df.empty or col not in df.columns:
        return None
    s = df[col].dropna()
    return s.iloc[-1] if len(s) else None


def max_val(df, col):
    if df.empty or col not in df.columns:
        return None
    s = df[col].dropna()
    return s.max() if len(s) else None


def style(ax, xlabel, ylabel, title):
    ax.set_facecolor(SURFACE)
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    ax.set_title(title, color=INK, fontsize=10, loc="left")
    ax.grid(True, color=GRID, linewidth=0.6)
    ax.tick_params(colors=INK2, labelsize=8)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    for sp in ("left", "bottom"):
        ax.spines[sp].set_color(GRID)


def chart(runs, fname, title, panels, outdir):
    """panels: list of (ramp kind, x column, y column, xlabel, ylabel). One axis per panel."""
    fig, axes = plt.subplots(1, len(panels), figsize=(6.2 * len(panels), 4.2), squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    drawn = False
    for ax, (kind, xc, yc, xl, yl) in zip(axes[0], panels):
        n = 0
        for i, r in enumerate(runs):
            df = r.ramp_rows(kind)
            if df.empty or xc not in df.columns or yc not in df.columns:
                continue
            d = df[[xc, yc]].dropna()
            if d.empty:
                continue
            ax.plot(d[xc], d[yc], color=COLORS[i % len(COLORS)], linewidth=2, label=r.label)
            n += 1
        style(ax, xl, yl, f"{title} ({kind} ramps)")
        if n:
            drawn = True
            if n > 1 or len(runs) > 1:
                ax.legend(fontsize=8, frameon=False, labelcolor=INK2)
        else:
            ax.text(0.5, 0.5, "no data", ha="center", va="center", color=INK2, transform=ax.transAxes)
    if drawn:
        fig.tight_layout()
        fig.savefig(outdir / fname, dpi=120, facecolor=SURFACE)
    plt.close(fig)
    return fname if drawn else None


def deltas(runs):
    """Per spread, compare each run with the baseline (stock, else first) at the largest common table count."""
    groups = {}
    for r in runs:
        groups.setdefault(r.spread, []).append(r)
    out = []
    for spread, rs in sorted(groups.items()):
        if len(rs) < 2:
            continue
        rs = [r for r in rs if not r.ramp_rows("table").empty and "tables" in r.steps.columns]
        if len(rs) < 2:
            continue
        base = next((r for r in rs if r.variant == "stock"), rs[0])
        tabs = [r.ramp_rows("table")["tables"].max() for r in rs]
        common = min(tabs)
        rows = []
        for metric in DELTA_METRICS:
            def at(r):
                df = r.ramp_rows("table")
                if metric not in df.columns:
                    return None
                d = df[df["tables"] <= common][metric].dropna()
                return d.iloc[-1] if len(d) else None
            bv = at(base)
            for r in rs:
                if r is base:
                    continue
                v = at(r)
                if bv is None or v is None:
                    continue
                pct = f"{(v - bv) / bv * 100:+.1f}%" if bv else "-"
                rows.append((metric, r.label, fmt(float(bv)), fmt(float(v)), fmt(float(v - bv)), pct))
        out.append((spread, (base.label, int(common)), rows))
    return out


def main(argv):
    outdir = None
    dirs = []
    i = 0
    while i < len(argv):
        if argv[i] == "--out":
            outdir = Path(argv[i + 1])
            i += 2
        else:
            dirs.append(argv[i])
            i += 1
    if not dirs:
        print("usage: schema-scale-report.py [--out DIR] <run-dir> [<run-dir> ...]", file=sys.stderr)
        return 2
    missing = [d for d in dirs if not Path(d).is_dir()]
    if missing:
        print(f"not a directory: {missing}", file=sys.stderr)
        return 2
    if outdir is None:
        outdir = Path("logs") / f"{time.strftime('%Y%m%d-%H%M%S')}-compare"
    outdir.mkdir(parents=True, exist_ok=True)

    runs = [Run(d) for d in dirs]
    labels = [r.label for r in runs]
    dup = {x for x in labels if labels.count(x) > 1}
    for r in runs:
        if r.label in dup:
            r.label = f"{r.label} ({r.path.name[:15]})"

    log("# Schema scale comparison")
    log()
    log(f"Generated {time.strftime('%Y-%m-%d %H:%M:%S')}. Runs: {len(runs)}.")
    log()
    log("## Runs")
    log()
    rows = []
    for r in runs:
        s = r.summary
        df = r.steps
        rows.append((
            r.label, r.profile or "-", r.mode or "-", r.path,
            fmt(max_val(df, "tables")), fmt(max_val(df, "columns_total")),
            fmt(last_val(df, "wall_s")), fmt(first(s, "heapMax")), fmt(first(s, "rssLimit")), len(df),
        ))
    for line in md_table(["run", "profile", "mode", "dir", "tables", "columns", "wall_s", "heapMax", "rssLimit", "rows"], rows):
        log(line)
    log()

    log("## Break points")
    log()
    brows = []
    for r in runs:
        bps = r.breaks()
        if not bps:
            brows.append((r.label, "-", "-", "-", "-", "none recorded", "-", "-", "-"))
        for b in bps:
            ex = str(first(b, "exception", default="-")).splitlines()
            brows.append((
                r.label, first(b, "ramp", default="-"), first(b, "step", default="-"),
                fmt(first(b, "tables", default=None)), fmt(first(b, "columns", default=None)),
                first(b, "reason", default="-"), fmt(first(b, "value", default=None)),
                fmt(first(b, "threshold", default=None)), (ex[0][:100] if ex else "-"),
            ))
    for line in md_table(["run", "ramp", "step", "tables", "columns", "reason", "value", "threshold", "exception"], brows):
        log(line)
    log()

    log("## Variant deltas")
    log()
    log("Delta = run minus baseline (stock, else first run of the group), at the largest table count all runs in the group reached.")
    log()
    any_delta = False
    for spread, head, body in deltas(runs):
        if head is None:
            log("- " + body[0])
            continue
        any_delta = True
        log(f"### ks{spread}: baseline {head[0]}, at {head[1]:,} tables")
        log()
        for line in md_table(["metric", "run", "baseline", "value", "delta", "delta %"], body):
            log(line)
        log()
    if not any_delta:
        log("No group holds two runs with the same keyspace spread; no deltas.")
        log()

    log("## Charts")
    log()
    charts = [
        chart(runs, "ddl_p99.png", "DDL p99 (ms)",
              [("table", "tables", "ddl_p99_ms", "tables", "DDL p99 (ms)"),
               ("column", "max_cols_table", "ddl_p99_ms", "columns in widest table", "DDL p99 (ms)")], outdir),
        chart(runs, "heap_per_table.png", "Heap per table (bytes)",
              [("table", "tables", "heap_per_table", "tables", "heap per table (bytes)"),
               ("column", "max_cols_table", "heap_per_column", "columns in widest table", "heap per column (bytes)")], outdir),
        chart(runs, "snapshot_bytes.png", "TCM snapshot size (bytes)",
              [("table", "tables", "snapshot_bytes", "tables", "snapshot bytes"),
               ("column", "max_cols_table", "snapshot_bytes", "columns in widest table", "snapshot bytes")], outdir),
        chart(runs, "mbeans.png", "MBeans",
              [("table", "tables", "mbeans", "tables", "MBeans"),
               ("", "columns_total", "mbeans", "total columns", "MBeans")], outdir),
    ]
    for c in charts:
        if c:
            log(f"![{c}]({c})")
            log()
    if not any(charts):
        log("No chartable data (steps.csv missing or without the expected columns).")
        log()

    (outdir / "report.md").write_text("\n".join(LINES) + "\n")
    (outdir / "report.log").write_text("\n".join(LINES) + "\n")
    print(f"\nreport: {outdir / 'report.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
