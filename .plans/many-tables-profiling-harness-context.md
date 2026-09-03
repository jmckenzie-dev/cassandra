# Many Tables — Profiling Harness: Context Collection

Branch `moar_tables` @ `4c79cf739161` (trunk, 7.0-SNAPSHOT). Collected 2026-09-01, pre-plan.

## Goal

Repeatable harness that stands up a node, creates N tables, and profiles CPU and heap
(flame graphs, allocation detail) so we can form and test hypotheses about per-table
overhead: metrics, memtable residency, flush/evict behavior.

## Verified facts

### Already shipped in this branch

- Build is current: `build/apache-cassandra-7.0-SNAPSHOT.jar` plus `build/lib/jars/`
  including `async-profiler-4.2.jar`, `jamm-0.4.0.jar`, `metrics-core-4.2.28.jar`,
  `HdrHistogram`. JDKs 8/11/17/21 under `/usr/lib/jvm`.
- Async-profiler ships with Cassandra since CASSANDRA-20854:
  - `AsyncProfilerService` in `src/java/org/apache/cassandra/service/`.
  - `nodetool profile start|stop|status|list|fetch|purge|execute`.
  - Events: `cpu, alloc, lock, wall, nativemem, cache_misses`.
  - Formats: `flat, traces, collapsed, flamegraph, tree, jfr, otlp`.
  - Enable with `cassandra.async_profiler.enabled=true`; output goes to the log dir
    `profiler/` subdir (override: `cassandra.logdir.async_profiler`).
  - CPU event needs `kernel.perf_event_paranoid <= 1` and `kptr_restrict == 0`
    (`AsyncProfilerKernelParamsCheck` in `StartupChecks.java:1108`).
- In-JVM dtest framework at `test/distributed/org/apache/cassandra/distributed/`.
  Worked example of profiling an in-JVM node: `AsyncProfilerTest.java`
  (`WithProperties.set(ASYNC_PROFILER_ENABLED, true)` + `nodetoolResult("profile", ...)`).
- `main()` harness precedent: `test/distributed/.../test/ForBenchmarks.java` —
  builds an in-JVM cluster and parks. No JMH `benchmarks/` module on this branch.

### Per-table overhead suspects (to be quantified by the harness)

- `src/java/org/apache/cassandra/metrics/TableMetrics.java` — constructor spans
  lines 433–928. ~110 metric fields per table. Most register twice per table
  (table + keyspace alias) plus shared global copies (`TableMeter`, `TableHistogram`,
  `TableTimer` hold table/keyspace/global arrays). Nine partition `Sampler`s,
  format-specific gauges, and JMX names.
- `ColumnFamilyStore` init path (per-table object graph, compaction strategy, sstable lists).
- Memtables per table: `src/java/org/apache/cassandra/db/memtable/` (default
  `TrieMemtable`), slab pools in `src/java/org/apache/cassandra/utils/memory/`.
- Table creation path: schema persistence + listeners on `CREATE TABLE`.

### Machine / environment state

- `kernel.perf_event_paranoid = 2` → blocks the `cpu` event for CPU flame graphs.
  `kptr_restrict = 0` (already correct). Fix needs one-time
  `sudo sysctl kernel.perf_event_paranoid=1`. `alloc`, `wall`, `nativemem` unaffected.
- Session sandbox denies `java`, `ant`, and `.build/sh/ai-*` wrappers; the user runs
  build/test commands (or extends the permission rules).

## Proposed harness shape (for the plan)

- Standalone `main()` harness (ForBenchmarks pattern) using the in-JVM dtest framework:
  1-node cluster inside the harness JVM, fixed config and seeds.
- Phases, each bracketed by measurements:
  0. Baseline snapshot
  1. Create keyspace
  2. `CREATE TABLE` × N (default 100; knobs for 1k/10k/100k)
  3. Optional write workload
  4. Steady-state hold (sampling window)
  5. Teardown
- Instrumentation per phase:
  - async-profiler `alloc` + `wall` (and `cpu` once the kernel allows) around phases,
    via `nodetool profile` on the in-JVM node.
  - JFR recording on the harness JVM (`jdk.ObjectAllocationSample`, `jdk.GCHeapSummary`).
  - Per-phase heap checkpoints: used-heap deltas, `ThreadMXBean` allocated-bytes per phase,
    `jcmd GC.class_histogram` before/after table creation.
- Output per run under `./logs/<datetime>/`: flamegraphs (.html), JFR (.jfr),
  phase metrics table. Console output mirrored to the same place.

## Hypotheses the first run should discriminate

- H1 metrics: `TableMetrics` + registry + JMX dominate per-table heap.
- H2 residency: empty memtables + CFS bookkeeping are the standing cost of idle tables.
- H3 creation cost: CPU during `CREATE TABLE` is dominated by schema persistence,
  not allocation.

## Risks / open

- 100k tables via individual CQL `CREATE TABLE` is slow; start at 100 as requested,
  add batched schema mutation later.
- In-JVM node is not byte-identical to a production install (no `cassandra-env.sh`
  heap defaults); set explicit `-Xmx` and document deltas.
- JUnit/`ai-ci-test` execution is blocked in this session's sandbox; the user runs
  the harness, or we add a permission rule.

## Decisions (2026-09-01)

- Harness form: standalone `main()` using the in-JVM dtest framework (ForBenchmarks pattern).
- CPU flame graphs approved: one-time `sudo sysctl kernel.perf_event_paranoid=1`
  (user runs it; kptr_restrict is already 0; reboot resets it). Required before the
  first `cpu`-event profile; `alloc`/`wall`/`nativemem` need nothing.

## Architecture (2026-09-02, post-refactor)

The harness is a reusable core plus a thin instance, in
`test/distributed/org/apache/cassandra/distributed/test/`:

- `ResourceProfiler` — instrumentation core (no domain knowledge): async-profiler
  sessions (`startSession`/`stopSession` for multi-event `.ap.jfr`,
  `htmlWindow(name, event, runnable)` for single-event HTML around arbitrary code),
  JDK JFR recording incl. `jdk.ObjectAllocationSample`, heap/allocated-bytes
  checkpoints, self-jcmd class histograms, warning + artifact-size tracking,
  kernel pre-check.
- `ProfiledClusterHarness` (abstract) — cluster lifecycle, ordered `Phase` driver
  (each phase: name, body, optional `after` gap hook), run-dir + console tee +
  summary.txt/summary.json, artifact validation, exit codes. Subclass supplies
  `definePhases()`, `runParameters()`, dir suffix, titles.
- `ManyTablesProfileHarness extends ProfiledClusterHarness` — table-creation
  instance: six phases, schema, split-half alloc/cpu HTML for phase 02,
  create-times/hold-samples CSVs. Same class name, main() signature, CLI flags,
  and summary format as v1.

Wrapper: `PROFILE_MAIN_CLASS` env var runs any future subclass under the same
allowed command; classpath/JVM flags/env redirects are shared.

Refactor validation (N=100 vs run-1): baseline post-GC heap +0.03%,
steady-hold post-GC heap −0.04%, phase timings ±2%, identical artifact sets.

To add a new profiling target: extend `ProfiledClusterHarness`, define phases,
reuse the profiler; run with `PROFILE_MAIN_CLASS=<fqcn> .build/sh/ai-profile-many-tables ...`.

## Option A adopted (2026-09-03, validated)

Record-once/derive-views is now the design: every phase records ONE multi-event
`.ap.jfr` (no split-half HTML anywhere); HTML flame graphs are derived post-run:

    tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv -o html --alloc <phase>.ap.jfr out.html
    (also --wall, --cpu; jfrconv comes from the async-profiler 4.2 release tarball, ~430KB)

jfrconv needs per-event selection flags; output format is `-o html`. The JDK
recording also enables jdk.ExecutionSample + jdk.NativeMethodSample @ 10ms so
`jfr view hot-methods` works on <phase>.jdk.jfr with no extra tools.

Validation (N=100 vs run-1): baseline post-GC 82.46MB (−0.2%), steady-hold
post-GC 110.31MB (+0.1%), ~279KB/table residency — instruments unchanged.
Phase-02 .ap.jfr now covers ALL 100 tables; alloc+cpu HTMLs derived from it in
<0.1s each (02-create-tables-alloc.html / 02-create-tables-cpu.html in the run dir).
