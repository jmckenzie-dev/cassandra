# Many-Tables Harness — Run 1 Report (2026-09-02)

## Validation status: PASSED

| Gate | Result |
|---|---|
| Smoke (1 table, writes) | success, all phases |
| checkstyle + checkstyle-test | 0 violations across 6200 files |
| Primary N=100 | `logs/20260902-095625-many-tables-100t` success |
| Repeatability N=100 | `logs/20260902-095828-many-tables-100t` success, identical artifact sets |

Environment: JDK 21.0.12, kernel perf_event_paranoid=1 (cpu event live), in-JVM 1-node cluster.

## First findings (N=100)

- **Residency (H2)**: post-GC heap 82.4 MB (baseline) → 110.2 MB (steady hold with 100 tables)
  → **~285 KB/table standing cost**. Hold-phase heap flat: +3 KB drift over 60 s (no churn).
  Extrapolation: 100k tables ≈ 28.5 GB heap — consistent with the reported problem scale.
- **Creation cost (H3)**: ~89 ms/table, flat from table 0 to 99 (create-times.csv), one 276 ms
  outlier. Dominated by the schema-agreement path, not allocation rate.
- **Metrics (H1)**: live-object histogram growth for 100 tables is dominated by metrics
  machinery: JmxGauge +84/table, ThreadLocalMeter +44/table,
  DecayingEstimatedHistogramReservoir +33/table, plus JmxMeter/registry structures.
  Full attribution: `02-create-tables-alloc.html` (tables 0–49) flame graph;
  CPU attribution: `02-create-tables-cpu.html` (tables 50–99).
- Repeatability deltas across the two N=100 runs: phase elapsed within ±1%,
  steady-hold post-GC heap within 0.6%.

## Known issues / notes

- `mbeans_delta` column is inert: the in-JVM node does not register metrics into the
  platform MBean server (count constant at 27). H1 must be judged via flame graph +
  histograms (as above). Fix or drop the counter later.
- Node `system.log` not located via the dtest log API (benign warning; node output is
  captured in each run's `console.txt` via the launch log tee).
- Phase 02 HTML halves cover tables [0,50) (alloc) and [50,100) (cpu) by design
  (async-profiler allows one session at a time; multi-event is JFR-only).

## Machine adaptations (all inside the two new files, nothing in src/ or build.xml)

- Wrapper stubs the git-hook installer when the main repo's `.git/hooks` is read-only.
- Wrapper redirects GRADLE_USER_HOME and the maven local repo into `tmp/` when the
  host caches are read-only (`-Dlocal.repository` + `GRADLE_OPTS=-Dmaven.repo.local=...`).
- Wrapper derives JDK-version-specific JPMS flags from build.xml's lists
  (`--add-exports`/`--add-opens` split as two argv tokens); keeps G1 (JVM default)
  rather than test-env ZGC; omits debugrefcount/strict-checks instrumentation.
- Harness: one multi-event `.ap.jfr` session per phase; phase 02 split-half single-event
  HTML sessions; `jdk.ObjectAllocationSample` added to the JDK JFR recording.

## Next decisions

1. Open `02-create-tables-alloc.html` — confirm the allocation stacks behind the metrics
   growth (TableMetrics constructor vs registry vs JMX reporting) and pick the first
   optimization target.
2. Scale probe: 1k tables with `MANY_TABLES_XMX=16g` to check whether the ~285 KB/table
   residency holds at larger N (or grows non-linearly).
3. Then hypothesis iteration: lazy metrics / shared registries (H1), lazy CFS/memtable
   residency (H2), cheaper schema persistence (H3) — one change per run, same commands.
