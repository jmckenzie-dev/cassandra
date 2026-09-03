# Many-Tables Profiling Harness (in-JVM dtest + async-profiler)

Workspace root: `/home/jmckenzie/src/cassandra/cassandra_asf/wt_moar_tables` (branch `moar_tables` @ 4c79cf739161, 7.0-SNAPSHOT, build current).

## Goal

Goal: A repeatable, JMH-in-spirit profiling harness — a standalone `main()` that stands up one in-JVM Cassandra node, creates N tables (default 100), optionally writes data, holds steady state, and captures CPU + heap instrumentation per phase (async-profiler flame graphs and JFR, allocation counters, heap checkpoints), writing all artifacts under `./logs/<datetime>-many-tables-<N>t/`.

Scope boundaries:
- New files only: one harness class under `test/distributed/...` and one local wrapper script under `.build/sh/`. Zero changes to `src/`, `lib/`, `src/gen-java/`, `build.xml`, or grammar.
- No new dependencies. async-profiler 4.2 and the in-JVM dtest framework are already on the tree.
- v1 targets N=100 comfortably; 1k/10k/100k are CLI knobs with documented heap/time caveats. Batched schema mutation is explicitly out of scope (noted as future work).
- No JUnit test: this is a manually-run tool with the same standing as `ForBenchmarks.java` (no test precedent exists for it); the end-to-end runs in the Validation Plan are the acceptance gate.

Success conditions: `ai-build` (checkstyle + checkstyle-test) passes; an N=100 run produces non-empty artifacts for every executed phase; an identical re-run produces the same file-name set; the `cpu` event either produces a flame graph or is skipped with a clear warning recorded in the summary.

## Current State

Verified evidence, by area:

- **Build/test wrappers.** `.build/sh/ai-build` runs `ant clean jar checkstyle checkstyle-test` (`.build/sh/ai-build:68`) — it wipes `build/` and does NOT compile test sources. `.build/sh/ci-test` is JUnit-class oriented (`ant testclasslist`, `.build/sh/ci-test:148`) and does a `realclean` first — wrong tool for running a `main()`. So the run path must include a test-compile step (`ant build-test`).
- **Test compilation and classpath.** `ant build-test` compiles `test/distributed` into `build/test/classes` and copies test resources there (`build.xml:71`, `build.xml:1293-1304`, copy at `build.xml:1333`). The ant test classpath is: the multi-version main jar **first** (`build/apache-cassandra-7.0-SNAPSHOT.jar`, required for multi-release classes — `build.xml:475`), then `build/lib/jars/*`, `build/test/lib/jars/*` (excluding `ant-*`), test classes, and `test/conf` (`build.xml:1482-1498`, `cassandra.classpath` / `cassandra.classpath.test` at `build.xml:468-481`). Test JVM flags of record: `-ea`, `-javaagent:build/lib/jars/jamm-0.4.0.jar`, `-Xms512M`, `-XX:ActiveProcessorCount=...`, `-Djava.io.tmpdir=...`, `-Dcassandra.testtag=...` (`build.xml:1440-1463`).
- **`main()` precedent.** `test/distributed/org/apache/cassandra/distributed/test/ForBenchmarks.java:26-36`: `Cluster.build(3).withConfig(c -> c.with(Feature.values())).start()` inside try-with-resources, then park. It lives in package `org.apache.cassandra.distributed.test` and has no JUnit test.
- **In-JVM cluster CQL.** `AbstractCluster.schemaChange(String)` executes DDL on instance 1 via `coordinator().execute(query, ConsistencyLevel.ALL)` and blocks on `SchemaChangeMonitor` for schema agreement (`test/distributed/org/apache/cassandra/distributed/impl/AbstractCluster.java:896-937`) — the full production-like `CREATE TABLE` path (schema persistence + propagation), which is exactly what we want to profile. DML: `ICoordinator.execute(String query, ConsistencyLevel, Object... boundValues)` (`test/distributed/org/apache/cassandra/distributed/api/ICoordinator.java:53`).
- **AsyncProfilerService semantics (why we bypass it).** `src/java/org/apache/cassandra/service/AsyncProfilerService.java`:
  - `start()` validates that the parameter map equals exactly `{events, outputFormat, duration, outputFileName}` (`:452-459`) — a duration is mandatory, so exact phase bracketing (manual stop) is not expressible cleanly.
  - Only one recording at a time: `start` returns false when `isRunning()` (`:212`, `:470-484`); no concurrent sessions.
  - Every `start` runs `kernelParamsCheck.execute(null, true)` (`:225`); the check throws when `cassandra.async_profiler.enabled=true` AND `perf_event_paranoid > 1` or `kptr_restrict != 0`, regardless of the requested event (`src/java/org/apache/cassandra/service/StartupChecks.java:1147-1176`). So a bad kernel state blocks even `alloc`.
  - Output files are confined to `logDir` with filename regex `^[a-zA-Z0-9-]*\.?[a-zA-Z0-9-]*$` (`:63`, `:391-400`).
  - The kernel check is public and reusable: `StartupChecks.AsyncProfilerKernelParamsCheck#hasCorrectKernelParams()` (`StartupChecks.java:1132-1138`) — the harness uses it for the `cpu` pre-check.
- **`one.profiler` is directly usable in-process.** `one.profiler.AsyncProfiler` ships in `build/lib/jars/async-profiler-4.2.jar`, loaded on the shared classpath (not an isolated cassandra class), and `AsyncProfilerTest` proves the lib loads fine around an in-JVM node (`test/distributed/org/apache/cassandra/distributed/test/AsyncProfilerTest.java:59-116`, enable via `CassandraRelevantProperties.ASYNC_PROFILER_ENABLED`, `Feature.JMX`, service init via `runOnInstance`). Because the in-JVM node runs in the same JVM, `one.profiler.AsyncProfiler.getInstance().execute(...)` called from the harness main thread profiles the whole process (node + harness) with no `runOnInstance` needed.
- **async-profiler 4.2 capabilities (verified from the upstream changelog, https://raw.githubusercontent.com/async-profiler/async-profiler/master/CHANGELOG.md):** multiple events in one session since 2.0 ("Profile multiple events together (cpu + alloc + lock)", JFR output); `cpu` + `wall` together since 3.0 (#740); mid-session dump since 2.5; `--all` simultaneous collection in 4.1 (#1259); "Two wall-clock profilers interfere with each other" fixed in **4.2** (#1417) — the exact version this branch ships; CPU falls back to the `ctimer` engine when perf_events are unavailable since 4.0 (#1044). Format is inferred from the file extension since 1.6.
- **Node logs.** In-JVM node `system.log` path is `build/test/logs/<cassandra.testtag>/<suite?>/<cluster_id>/<instance_id>/system.log`, defined by `test/conf/logback-dtest.xml` (`test/distributed/org/apache/cassandra/distributed/impl/Instance.java:263-277`); `test/conf` must be on the classpath for this logging config.
- **Per-table suspects to quantify** (from `.plans/many-tables-profiling-harness-context.md`, trusted): `TableMetrics` constructor spans `src/java/org/apache/cassandra/metrics/TableMetrics.java:433-928`, ~110 metric fields per table, most registered twice (table + keyspace alias) plus 9 partition Samplers and JMX names; `ColumnFamilyStore` init; per-table memtables (default `TrieMemtable`); the `CREATE TABLE` schema persistence path.
- **Environment.** JDKs 8/11/17/21 installed under `/usr/lib/jvm` (verified); `set_jdk N` helper exists. `kernel.perf_event_paranoid=1` and `kptr_restrict=0` are already set (user-approved decision). This session's sandbox denies `java`/`ant`/`ai-*`; the user runs all build/run commands.

## Assumptions

- **JDK 11 for harness runs.** The branch supports 11/17/21; 11 is the lowest that satisfies the programmatic `jdk.jfr.Recording` API (any 11+) and matches the CI default. Confirm with `set_jdk 11 && java -version` before the first run. If 17/21 is preferred later, the same command works unchanged (JFR event names used below all exist on 11+).
- The user's kernel sysctl state (`perf_event_paranoid=1`, `kptr_restrict=0`) persists for the run; a reboot resets it — the harness prints the check result and the remediation command either way.
- `build/apache-cassandra-7.0-SNAPSHOT.jar` is current (stated); `ai-build` is re-run as part of validation anyway.
- Confirming an assumption from the context artifact rather than re-deriving: `TableMetrics.java:433-928` metric-constructor span. The harness output (alloc flame graphs) is the instrument that tests it; the line numbers themselves are not load-bearing for this plan.

## Recommended Plan

Selected design: **direct `one.profiler.AsyncProfiler` control from the harness main thread** (bypassing `AsyncProfilerService`), plus one programmatic `jdk.jfr` Recording with per-phase dumps, plus in-process heap/allocation checkpoints. Rationale: the service forbids concurrent recordings, mandates a duration parameter, and gate-checks kernel params on every start regardless of event; the direct API gives exact phase bracketing, concurrent single-event HTML flame graphs + one multi-event JFR per phase, and graceful per-event skip. No `runOnInstance` is needed because `one.profiler` is loaded on the shared classpath of the single JVM that hosts both node and harness.

### Step 1 — Harness class skeleton

- File: `test/distributed/org/apache/cassandra/distributed/test/ManyTablesProfileHarness.java` (new; package `org.apache.cassandra.distributed.test`, next to `ForBenchmarks`).
- Why here: test-source tree already has the dtest framework on its compile classpath; `ant build-test` compiles it to `build/test/classes`; `checkstyle-test` (wired into `ai-build`) covers it; precedent location.
- Contents: standard ASF license header; `public static void main(String[] args)`; no JUnit.
- CLI parsing (plain `args` loop, no dependencies): `--tables N` (default 100), `--writes-per-table W` (default 0 → phase skipped), `--hold-seconds S` (default 60), `--keyspace NAME` (default `many_tables_harness`), `--out DIR` (default `./logs`), `--skip-cpu`, `--no-profile` (runs all phases with no AP sessions — clean-timing comparison mode).
- Run dir: `<out>/<yyyyMMdd-HHmmss>-many-tables-<N>t/` (absolute `Path`, created first).
- Console mirror: tee `System.out`/`System.err` into `<runDir>/console.txt` from the first line (small `PrintStream` wrapper). The harness itself writes all files; no shell redirection dependence.
- Before cluster build: `CassandraRelevantProperties.ASYNC_PROFILER_ENABLED.setBoolean(true)` (service-parity with `AsyncProfilerTest`, harmless for the direct API; use the properties setter, not `System.setProperty`, to stay checkstyle-clean).
- Fixed config for repeatability: `Cluster.build(1).withConfig(c -> c.with(Feature.values())).start()` (ForBenchmarks pattern; includes `Feature.JMX` so optional nodetool cross-checks stay possible); fixed keyspace and table spec — `CREATE KEYSPACE <ks> WITH replication = {'class':'SimpleStrategy','replication_factor':1}`; `CREATE TABLE <ks>.t%06d (pk int, c int, v text, PRIMARY KEY (pk, c))` with all other settings at branch defaults (that is the point — default `TrieMemtable`, default compaction). No randomness anywhere; values are deterministic functions of the loop indices.
- Expected observable result: compiles under `ant build-test`; passes `checkstyle-test`.

### Step 2 — Instrumentation primitives (inside the harness file)

- `ap(String cmd)`: wraps `one.profiler.AsyncProfiler.getInstance().execute(cmd)`; on any `Throwable` records the error into the phase record and returns null (never fatal). Resolve `getInstance()` lazily once; if it fails, mark AP unavailable for the whole run (summary notes it).
- CPU availability: pre-check once with `new StartupChecks.AsyncProfilerKernelParamsCheck().hasCorrectKernelParams()` (public, `StartupChecks.java:1132`); if false (or `--skip-cpu`), skip all `cpu` sessions and write the reason + the `sysctl` remediation hint into the summary and console.
- Heap checkpoints (main thread, per phase before/after):
  - `Runtime` used/committed heap via `ManagementFactory.getMemoryMXBean()`.
  - Allocated bytes: `com.sun.management.ThreadMXBean#getThreadAllocatedBytes(mainThreadId)` delta per phase (guard with `isThreadAllocatedMemorySupported()`).
  - Platform `MBeanServer` `getMBeanCount()` — direct signal for per-table JMX registration growth (H1).
- jdk.jfr: one `jdk.jfr.Recording` for the whole run, started before the cluster; enable `jdk.GCHeapSummary` (period 1 s), `jdk.MetaspaceSummary` (period 5 s), `jdk.GCPhasePause` (threshold). Per phase: `recording.dump(<runDir>/<phase>.jdk.jfr)` then continue. All three events exist on JDK 11; any jdk.jfr failure degrades to a warning, never fatal. No `-XX:StartFlightRecording` needed.
- Class histogram (best-effort, optional): harness runs `jcmd <ownPid> GC.class_histogram` (subprocess, 30 s timeout, own PID via `ProcessHandle.current().pid()`) around phase `02` (before/after) and at the end of `04`; output → `<runDir>/histogram-*.txt`. Note in docs: jcmd histogram triggers a full GC, so it shows live objects. Failure (jcmd missing, attach denied) is recorded and skipped.
- `--no-profile` short-circuits `ap()` sessions and jdk.jfr enablement but keeps every heap/allocation/timing measurement.

### Step 3 — Phase engine

Six sequential phases; each is bracketed identically: snapshot-before → start AP sessions → phase body → stop AP sessions (`stop,file=<same path as start>`, reverse start order) → `jdk.jfr` dump → snapshot-after → append to summary. Per-phase AP sessions (started/stopped via `ap()`):

| session | command | notes |
|---|---|---|
| alloc flame graph | `start,event=alloc,file=<phase>.alloc.html` | always |
| wall flame graph | `start,event=wall,file=<phase>.wall.html` | always |
| cpu flame graph | `start,event=cpu,file=<phase>.cpu.html` | best-effort: skipped with recorded warning if kernel params bad or start throws |
| multi-event JFR | `start,event=alloc,wall,cpu,file=<phase>.ap.jfr` | drop `cpu` from the event list when cpu is unavailable; AP supports comma multi-event + JFR (changelog 2.0/3.0; two-wall-profiler bug fixed in shipped 4.2 #1417) |

Phase bodies (phase id slugs are fixed and ordered):

- `00-baseline`: cluster is up, only system tables; 5 s settle; `System.gc()` + 2 s, then snapshot (clean baseline). Establishes node floor for every counter.
- `01-create-keyspace`: one `cluster.schemaChange(CREATE KEYSPACE ...)`.
- `02-create-tables`: for `i` in `0..N-1`: time one `cluster.schemaChange(CREATE TABLE <ks>.t%06d ...)`; append `(i, elapsedNanos)` to `<runDir>/create-times.csv`. Histogram before/after brackets this phase.
- `03-writes` (only when `--writes-per-table > 0`): for each table, W inserts via `cluster.coordinator(1).execute("INSERT INTO <ks>.t%06d (pk,c,v) VALUES (?,?,?)", ConsistencyLevel.ONE, pk, c, "v-" + pk + "-" + c)` with deterministic values.
- `04-steady-hold`: every 5 s append `(offsetSeconds, heapUsed, mbeanCount, threadAllocatedBytesTotal)` to `<runDir>/hold-samples.csv` until `--hold-seconds` elapses; then `System.gc()` + 2 s and take a post-GC snapshot (the residency number for H2). Histogram at end.
- `05-teardown`: one `cluster.schemaChange(DROP KEYSPACE <ks>)`; then locate the node `system.log` via `cluster.get(1).logs()` (`Instance.java:263`) and copy it into the run dir (best-effort); close cluster (try-with-resources), stop jdk.jfr.

After the last phase: write `<runDir>/summary.txt` (human table: phase, elapsed, alloc-bytes delta, heap before/after/post-GC, mbean count delta, artifacts with sizes, skipped events + reasons) and `<runDir>/summary.json` (same, machine-readable; includes args, JDK version, kernel-param state, table count). Exit 0 on success; on any fatal error, still attempt to stop all started AP sessions and write a partial summary, then exit non-zero.

Expected observable result: for N=100 every executed phase yields `<phase>.alloc.html`, `<phase>.wall.html`, `<phase>.ap.jfr`, `<phase>.jdk.jfr` non-empty (`03-writes` absent when writes=0; `<phase>.cpu.html` present when kernel params OK), plus `console.txt`, `create-times.csv`, `hold-samples.csv`, `summary.txt`, `summary.json`, up to 3 `histogram-*.txt`, and the copied node `system.log`.

### Step 4 — Wrapper script

- File: `.build/sh/ai-profile-many-tables` (new, executable, ASF header; style follows `.build/sh/ci-test`).
- Behavior: cd to repo root; fail fast if `build/apache-cassandra-7.0-SNAPSHOT.jar` is missing (message: run `.build/sh/ai-build` first); run `ant build-test` (test-compile; `ai-build` alone leaves no `build/test/classes` after `clean`); `mkdir -p tmp logs`; then run the harness with `java ... 2>&1 | tee ./logs/<datetime>-many-tables-launch.log` and `exit "${PIPESTATUS[0]}"` (preserves the JVM return code; console log datetime-stamped per repo rule).
- JVM flags (mirroring the ant test JVM, `build.xml:1440-1463`): `-ea`, `-Xms512m`, `-Xmx${MANY_TABLES_XMX:-8g}`, `-XX:ActiveProcessorCount=${MANY_TABLES_CPUS:-8}` (fixed CPU count → repeatable thread-pool sizing), `-javaagent:$PWD/build/lib/jars/jamm-0.4.0.jar`, `-Djava.io.tmpdir=$PWD/tmp` (absolute — AP native-lib extraction breaks on relative tmpdir, AP #1515), `-Dcassandra.testtag=manytables` (isolates node logs under `build/test/logs/manytables/...`).
- Classpath (order matters — multi-version jar first, per `build.xml:475`): `build/apache-cassandra-7.0-SNAPSHOT.jar:build/test/classes:build/classes:conf:test/conf:build/lib/jars/*:build/test/lib/jars/*`.
- Main class: `org.apache.cassandra.distributed.test.ManyTablesProfileHarness`; pass `"$@"` through as harness args.
- Heap guidance encoded as comments: N≤1k → 8g default; 10k → `-Xmx16g`; 100k → expect tens of GB and long runs (see Risks).

### What is intentionally not done

- No changes to `AsyncProfilerService`, `StartupChecks`, `build.xml`, or anything in `src/` — the direct-API harness avoids needing any product change.
- No batched schema mutations, no ccm/python dtests, no CI wiring (`ci-test` is not touched — this is not a JUnit test), no new dependencies, no commits.

## Validation Plan

All commands are run by the user (this session's sandbox denies java/ant). From the repo root:

1. Build + style:
   ```
   set_jdk 11
   .build/sh/ai-build
   ```
   Pass: exit 0; no checkstyle or checkstyle-test violations reported for the two new files.
2. Primary run (N=100):
   ```
   .build/sh/ai-profile-many-tables --tables 100 --hold-seconds 60
   ```
   Pass: exit 0 (wrapper preserves it); a run dir `./logs/<ts>-many-tables-100t/` exists containing, for phases `00,01,02,04,05`: non-empty `.alloc.html`, `.wall.html`, `.ap.jfr`, `.jdk.jfr` (and `.cpu.html` when `perf_event_paranoid <= 1 && kptr_restrict == 0`); `summary.json` parses and contains 5 phase records; `create-times.csv` has exactly 100 rows; `hold-samples.csv` has ~hold-seconds/5 rows; `console.txt` mirrors the run. Fail: any missing/empty artifact, non-zero exit, or a cpu skip without a recorded reason.
3. Repeatability re-run (same args):
   ```
   .build/sh/ai-profile-many-tables --tables 100 --hold-seconds 60
   ```
   Pass: identical file-name set in the new run dir (names are deterministic; only the timestamped directory differs), `summary.json` structurally identical with values within normal run variance. Fail: any phase artifact set differing (e.g., cpu skipped in one run only).
4. Writes + smoke:
   ```
   .build/sh/ai-profile-many-tables --tables 1 --hold-seconds 5 --writes-per-table 10
   ```
   Pass: `03-writes` phase appears with its four artifact files; run completes quickly.
5. CPU-skip path: temporarily revert the sysctl (`sudo sysctl kernel.perf_event_paranoid=2`) and re-run step 4.
   Pass: run still exits 0, all `cpu` sessions (html and the ap.jfr event list) are skipped, `summary.json` records the reason and the `sysctl kernel.perf_event_paranoid=1` remediation hint. Restore the sysctl afterwards.

Analysis workflow after a run (documented in the plan file for the user, not enforced by code): open `02-create-tables.alloc.html` for H1/H3 allocation attribution (expect `TableMetrics`/metrics-registry stacks), `02-create-tables.cpu.html` for H3 CPU attribution, compare `00-baseline` vs `04-steady-hold` post-GC heap and `histogram-*.txt` for H2 residency, read `create-times.csv` for per-table creation cost trend.

## Risks and Mitigations

- **Concurrent AP recordings misbehave.** The design runs 3-4 concurrent AP sessions per phase. Evidence says this is supported (multi-event since 2.0; cpu+wall since 3.0 #740; the two-wall-profiler interference bug is fixed in the shipped 4.2 #1417). Detection: `ap()` records stop errors; the validator checks each file is non-empty. Mitigation/fallback: degrade to one multi-event `.ap.jfr` per phase plus HTML flame graphs only for `02` and `04` as sequential single sessions — a small, localized change to the session table in Step 3.
- **`cpu` event blocked despite sysctl** (containers, per-user limits). AP 4.0+ falls back to the `ctimer` engine (#1044), and the harness pre-checks kernel params and catches start failures — the phase completes with a recorded skip (validated in step 5).
- **Sysctl resets on reboot.** The summary prints the kernel-param state and the exact remediation command on every run.
- **Large N blows up time or heap.** Each `CREATE TABLE` goes through `schemaChange` with a schema-agreement wait (`AbstractCluster.java:923-937`), so 100k individual DDLs would take hours and ~110 metrics/table can demand tens of GB. v1 is scoped to N=100 (1k comfortable with `-Xmx16g`); the `--no-profile` mode gives clean timing runs; a batched-schema-mutation path is noted as the future fix and kept out of scope.
- **In-JVM node is not production.** No `cassandra-env.sh` heap/GC defaults and the harness JVM shares the process (profiler sees harness threads too; harness threads are idle during measurement windows). Deltas between phases are the meaningful numbers, not absolute values; document `-Xmx` alongside results.
- **Profiler overhead distorts timing.** 4 concurrent samplers add overhead to `create-times.csv`. Mitigation: `--no-profile` runs provide the uninstrumented baseline for comparison.
- **jcmd histogram unavailable or slow.** Best-effort subprocess with timeout; failure recorded, run continues; heap checkpoints and AP alloc profiles still cover the same hypotheses.
- **`build/test/classes` missing or stale.** The wrapper always runs `ant build-test` before launching; stale-jar confusion is avoided because `ai-build` is step 1 of validation.

## Open Questions

None.
