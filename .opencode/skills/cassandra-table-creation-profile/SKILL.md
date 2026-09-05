---
name: cassandra-table-creation-profile
description: >-
  Safely run and analyze Apache Cassandra's many-table creation profiling harness,
  including phase monitoring, optional live heap dumps, artifact checks, JFR views,
  MAT reports, and controlled A/B comparisons. Use this skill only when the user
  explicitly names cassandra-table-creation-profile or invokes an equivalent
  /cassandra-table-creation-profile command. Never trigger it for general Cassandra
  schema, table-count, heap, or performance discussion.
compatibility: >-
  Requires an Apache Cassandra worktree, JDK 11, 17, or 21, and the repository's
  .build/sh/ai-profile-many-tables wrapper. HTML views need async-profiler 4.2
  jfrconv. Dominator analysis needs Eclipse Memory Analyzer Tool (MAT).
---

# Cassandra table creation profile

Run the many-table harness as an observational workflow. Do not edit implementation,
tests, configuration, dependencies, plans, research notes, or Git state. Do not treat
this skill as authorization to implement a candidate or change a host setting.

## Fixed workflow

Keep this order:

1. Inspect the worktree and existing artifacts.
2. Select a preset and state the complete run plan.
3. Confirm expensive or externally sensitive work.
4. Build when needed, then run only through the repository wrapper.
5. Watch phase transitions. Capture a requested live dump during the hold.
6. Locate and validate the exact run directory.
7. Derive only the requested Java Flight Recorder (JFR) views.
8. Analyze the evidence and report exact paths and commands.

If a tool call fails, explain the failure, change the approach, and retry. Never claim
that the intended action occurred after a failed call.

## Select a preset

| Preset | Defaults | Use | Gate | Planning estimate |
|---|---|---|---|---|
| Smoke | 4 tables, 5-second hold, 0 writes | Validate startup, phases, recordings, CSV, and summaries | No extra confirmation unless the user requests writes or another gated action | Usually under 2 minutes after build; a prior 4-table artifact set used about 11 MiB |
| Comparison | 100 tables, 60-second hold, 0 writes | Measure creation timing, post-GC heap, artifacts, allocation, and CPU | Confirm only requested heap dumps, MAT, downloads, or permission changes | About 1.5-3 minutes after build; a prior set used about 15 MiB |
| Scale/dominator | 5,000 tables, 60-120-second hold, 0 writes | Measure latency growth and retained owners | Always get explicit confirmation before launch. Also confirm the heap dump and MAT work | Creation alone has measured about 10m38s. Reserve up to 30 minutes for the runner, about 300 MiB without a dump, and several GiB with a dump |

Choose smoke when the user asks only to verify the harness. Choose comparison for a
normal profile or a controlled before/after run. Choose scale/dominator only for
large-scale latency or retained-heap questions. Do not silently promote a request to
a more expensive preset.

Current measured reference points are:

- 100 tables: about 8.9 seconds to create, or about 89 ms per table.
- 5,000 tables: 638.181 seconds for phase 02. The 5,000 timed create windows sum to
  637.317 seconds. Early calls after startup were about 87-107 ms. Late calls were
  mostly about 165-198 ms.
- The measured 5,000-table run completed a 300-second hold. The external 30-minute
  task cap then interrupted teardown.
- `DROP KEYSPACE` had already consumed more than 13 minutes when that task ended.
  This is teardown evidence, not a creation timeout.

Use these values as estimates and comparison context. Do not present them as results
from a new run.

## Preflight

Run read-only checks first:

- Record `git status --short` and `git rev-parse HEAD`. Preserve all unrelated work.
  Do not stage, commit, switch, reset, clean, or otherwise mutate Git.
- Inspect existing `logs/*-many-tables-*t` directories so a new run cannot be
  confused with an old one.
- Inspect file metadata for `build/apache-cassandra-7.0-SNAPSHOT.jar` and verify that
  it exists.
- A read-only `java -version` preflight may be allowed. The wrapper accepts Java
  Development Kit (JDK) 11, 17, or 21. If permission denies `java -version`, let the
  wrapper report its selected JDK. Do not bypass the denial.
- Use a file-read tool to read `/proc/sys/kernel/perf_event_paranoid` and
  `/proc/sys/kernel/kptr_restrict`. Do not use `sysctl` for this preflight.

CPU profiling needs `kernel.perf_event_paranoid <= 1` and `kernel.kptr_restrict = 0`.
If the host blocks CPU profiling, offer an allocation/wall run with `--skip-cpu`.
Ask before requesting a permission or host-setting change. Do not change either
kernel setting yourself.

Use file metadata and file-read tools to inspect requested post-run tools. Do not use
shell `test` commands. Check these paths:

- `tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv`;
- `tmp/mat/mat/ParseHeapDump.sh`;
- `tmp/mat/mat/MemoryAnalyzer`;
- `tmp/mat/mat/MemoryAnalyzer.ini`.

In this workspace, jfrconv and MAT are staged at those paths, but the MAT script or
launcher may not have execute permission. Check their modes. Ask before any download,
installation, execute-bit change, MAT execution, or request for a new execution
permission. Do not install dependencies without explicit approval.

Before a run, state:

- preset, table count, hold duration, and writes per table;
- exact command;
- expected duration and storage;
- JDK, heap, active processor count, and CPU-profile availability;
- whether the user requested a live heap dump and MAT analysis;
- which actions still need confirmation.

Use one focused confirmation that names every expensive approved action. A scale run,
heap dump, and MAT parse each need explicit consent. If any remains unconfirmed, stop
before that action.

## Build and run commands

Cassandra uses Ant. Never invoke `ant`, Maven, or Gradle directly for this harness.
Never invoke the harness main class, MAT, or other code directly with `java`. A
read-only `java -version` preflight is the sole direct Java exception. Run the harness
only through `.build/sh/ai-profile-many-tables`. The wrapper writes the launch log and
run artifacts under `./logs/` and uses project-local `./tmp/`.

If the Cassandra JAR is missing, run the approved repository build wrapper first:

```bash
.build/sh/ai-build
```

The profiling wrapper checks for that JAR and runs its required test build itself.
Do not run the full Cassandra test suite.

Smoke template:

```bash
.build/sh/ai-profile-many-tables --tables 4 --writes-per-table 0 --hold-seconds 5
```

Comparison template:

```bash
.build/sh/ai-profile-many-tables --tables 100 --writes-per-table 0 --hold-seconds 60
```

Scale/dominator template:

```bash
.build/sh/ai-profile-many-tables --tables 5000 --writes-per-table 0 --hold-seconds 90
```

Keep every primary harness command template wrapper-first so it matches the project's
narrow execution permission. The wrapper defaults currently provide the measured
setup. Use `MANY_TABLES_XMX` or `MANY_TABLES_CPUS` environment overrides only when the
user explicitly requests them and execution permission supports that exact prefixed
command. Environment variables are not harness flags.

Add `--skip-cpu` only when CPU profiling is unavailable or the user requests it. Add
`--writes-per-table N` only when the user explicitly requests writes. Supported
harness options are `--tables`, `--writes-per-table`, `--hold-seconds`, `--keyspace`,
`--out`, `--skip-cpu`, and `--no-profile`.

Do not invent a fast-residency, skip-drop, bulk-schema, or other unsupported option.
The current harness performs serial production `schemaChange` calls and clean
`DROP KEYSPACE` teardown.

## Foreground and background discipline

Prefer a foreground blocking run when no action must occur while the harness runs.
Use the command tool's blocking wait instead of starting a background task only
because a run is long.

Use a background task only when useful concurrent inspection is required, such as a
live heap dump during the hold. For that case:

1. Start the wrapper once as a background task.
2. Register one asynchronous `bash_watch` immediately.
3. With zero writes, watch for the exact text `Finished phase 02-create-tables`.
4. With writes enabled, watch for `Finished phase 03-writes`, the last setup phase
   before the steady hold.
5. While the watch is active, verify free storage, prepare the dump path inside the
   new run directory, and check jcmd and MAT availability. Do not duplicate or rerun
   the harness.
6. When notified, ensure `Starting phase 04-steady-hold` has appeared. If needed, use
   one blocking watch for that text. Do not poll task status in a loop.
7. Take the approved dump early in phase 04.

Read the exact run directory from the wrapper output line `Many-tables profile run:`.
Use that path for all later commands. Do not guess a timestamped directory.

## Capture a live heap dump

Confirm the dump before launch because it pauses the process and can consume several
GiB. After phase 04 starts, use `jcmd -l` to identify the single JVM whose command is
`org.apache.cassandra.distributed.test.ManyTablesProfileHarness`. Stop if the process
is absent or ambiguous.

Use jcmd, not a direct Java command:

```bash
jcmd <HARNESS_PID> GC.heap_dump <RUN_DIR>/04-steady-hold-live.hprof
```

Do not add `-all`; the residency question needs a live-object dump. Verify that the
dump exists and is non-empty before MAT analysis. Record its byte size and exact path.

## Validate the run

Validate before deriving or interpreting views:

1. Confirm the exact run directory and inspect `console.txt` for phase starts,
   finishes, fatal errors, and the last created table.
2. Count `create-times.csv` rows. The file has no header, so the count must equal the
   requested table count exactly.
3. For each completed phase, verify that `<phase>.ap.jfr` and `<phase>.jdk.jfr` exist
   and have non-zero size. CPU restriction does not invalidate an `.ap.jfr` that
   contains allocation and wall events.
4. Verify `hold-samples.csv` exists and is non-empty after phase 04 starts. A completed
   phase 04 must also have its recordings and post-GC heap in the summary.
5. If `summary.txt` or `summary.json` exists, report its status, failure, phase errors,
   skipped events, and warnings. A successful summary has no failure.
6. If the runner times out before summaries, use phase boundaries, CSV rows, hold
   samples, and recording sizes as partial evidence. Never call the whole run a
   success without a successful summary.

The normal no-write phases are `00-baseline`, `01-create-keyspace`,
`02-create-tables`, `04-steady-hold`, and `05-teardown`. Writes add `03-writes`.

Treat a 30-minute external task timeout during teardown as a runner limit. If all
create rows exist and phase 04 completed, report creation and hold as complete while
reporting teardown and overall status as incomplete. If the timeout occurs before
phase 04 completes, stop and report an incomplete run.

## Derive requested JFR views

async-profiler 4.2 permits one in-process session. Each phase therefore records one
multi-event `.ap.jfr`. Derive event-specific HTML after the run. Never start parallel
in-process profiler sessions.

Use jfrconv with `-o html` and exactly one event selector:

```bash
tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv -o html --alloc <RUN_DIR>/02-create-tables.ap.jfr <RUN_DIR>/02-create-tables-alloc.html
tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv -o html --cpu <RUN_DIR>/02-create-tables.ap.jfr <RUN_DIR>/02-create-tables-cpu.html
tmp/ap-dist/async-profiler-4.2-linux-x64/bin/jfrconv -o html --wall <RUN_DIR>/02-create-tables.ap.jfr <RUN_DIR>/02-create-tables-wall.html
```

Generate only the views the user or selected preset requires. Skip the CPU command
when CPU was unavailable. If jfrconv is absent, stop and ask before downloading it.

The `jfr view` subcommand varies by JDK release. Before using it, check the selected
JDK's read-only help, such as `jfr help view` or `jfr help`. Run these commands only
when help confirms the view names:

```bash
jfr view allocation-by-site <RUN_DIR>/02-create-tables.jdk.jfr
jfr view hot-methods <RUN_DIR>/02-create-tables.jdk.jfr
```

If `jfr view` or either named view is absent, report the limitation. Use the requested
jfrconv HTML view instead, or use another command documented by that installed JDK,
such as `jfr summary`, after checking its help. Do not claim that a view ran when the
subcommand was absent or denied.

Record the output source. Allocation percentages estimate allocated bytes. CPU
percentages estimate sampled execution time. Never equate the two.

## Run MAT only after approval

Use the live heap dump for retained-owner analysis. Class histograms report shallow
class totals and cannot establish dominators. Eclipse Memory Analyzer Tool (MAT)
calculates the dominator tree and retained sizes while it parses the dump. Its
supported batch reports include:

- `org.eclipse.mat.api:overview`, including the biggest top-level dominator classes;
- `org.eclipse.mat.api:top_components`, analyzing the top retained components;
- `org.eclipse.mat.api:suspects`, including accumulated dominator-tree evidence.

Use the [official MAT batch-mode documentation](https://help.eclipse.org/latest/topic/org.eclipse.mat.ui.help/tasks/batch.html)
as the command authority.

The installed `MemoryAnalyzer.ini` sets only `-Xmx1024m`. That parser heap is likely
too small for a several-GiB 5,000-table dump. Before parsing, inspect the dump's byte
size and read `/proc/meminfo` with a file-read tool. State a bounded `<MAT_HEAP>` that
leaves operating-system headroom, then get explicit approval for that parser heap and
the MAT run. Do not guess that 1 GiB is sufficient and do not consume all available
RAM.

After the user confirms MAT execution and any required narrow permission, ensure the
script and launcher are executable. If either execute bit is missing, ask separately
before running this command:

```bash
chmod u+x tmp/mat/mat/ParseHeapDump.sh tmp/mat/mat/MemoryAnalyzer
```

Then pass application and report arguments first, with JVM arguments last:

```bash
tmp/mat/mat/ParseHeapDump.sh <DUMP> org.eclipse.mat.api:overview org.eclipse.mat.api:top_components org.eclipse.mat.api:suspects -vmargs -Xmx<MAT_HEAP>
```

If the script is present but not executable, report that fact and request the narrow
approval needed before changing its mode. Do not invoke a different downloaded tool
without approval. Do not predict one generated filename. Inspect and report every
produced report ZIP, index, and index-related file. Describe retained size, paths to
garbage collection roots, and top components as MAT evidence. Do not promote
leak-suspect heuristics to proved Cassandra ownership.

## Analyze creation and residency

For creation, report:

- phase 02 elapsed time from the summary or phase finish line;
- row count, sum, and mean of nanoseconds in `create-times.csv`;
- early and late latency using equal-sized bins, with the bin size stated;
- the first-create startup outlier separately when material;
- allocation-by-site and hot-method evidence from the requested views;
- relevant garbage collection (GC) behavior and profiler warnings.

For residency, report:

- phase 04 post-GC heap when phase 04 completed;
- hold drift from the first to last `hold-samples.csv` heap value;
- before/after and hold class-histogram evidence;
- MAT retained-size dominators and root paths when available;
- dump pause, missing data, and other limits.

The in-JVM harness platform MBean count can remain flat even while Cassandra's
instance MBean server retains table metric wrappers. Do not use the flat platform
count as evidence that table MBeans are absent.

Label each conclusion as one of:

- **Measured fact:** directly present in this run's artifacts.
- **Source-confirmed mechanism:** directly present in repository source.
- **Inference:** consistent with measurements and source but not isolated.
- **Unknown:** requires another controlled run or retained-heap analysis.

## A/B comparison rules

Use already prepared baseline and candidate worktrees or artifact sets. Do not edit,
checkout, stash, or switch Git state. Keep these controls identical:

- JDK and Java options;
- wrapper-selected heap and active processor count;
- table schema, count, keyspace pattern, and node count;
- hold duration and writes per table;
- profiler settings, CPU availability, and phase order.

Change one intervention at a time. Run the same preset command in both worktrees.
Record both revisions, dirty states, exact commands, run directories, and artifact
completeness.

Compare total creation time, create-time slope, early and late bins, allocation
pressure, GC behavior, post-GC heap, warnings, and missing artifacts. Use the same bin
definitions and view types for both sides.

For a `ThreadLocalMeter` geometric-growth candidate, expected supporting evidence is:

- a large reduction in allocation attributed to `allocateRateGroupOffset`; and
- a flatter late-versus-early create-latency curve.

Treat those as hypotheses, not promised outcomes. Do not promise a 2-3 minute
5,000-table production-schema run. The measured flat base near 90 ms per table alone
is about 7 minutes 30 seconds.

## Stop and escalate

- **Out of memory (OOM):** stop. Preserve partial artifacts. Do not raise the heap or
  rerun automatically.
- **Failed phase:** stop downstream claims. Report the exception, last completed
  phase, and available artifacts.
- **Wrong create row count:** classify creation as incomplete even if other files
  exist.
- **Missing or empty profiler artifact:** report the affected phase and event. Do not
  derive a view or substitute evidence silently.
- **CPU kernel restriction:** use `--skip-cpu` only with the user's choice. Do not
  change host settings.
- **Task timeout before hold completion:** report an incomplete run and do not infer
  post-GC residency.
- **Task timeout during long teardown:** separate completed creation/hold evidence
  from incomplete teardown and summary status.
- **Missing jfrconv or MAT:** ask before download or installation. Continue only with
  evidence supported by available tools.
- **MAT execution denied:** request only the narrow permission needed after the user
  approves MAT. Do not bypass the denial.

## Report format

Use this order:

1. **Run plan** — preset, controls, command, gate approvals, duration/storage estimate.
2. **Run status** — exact run directory, completed phases, summary status, warnings.
3. **Creation evidence** — timings, early/late behavior, allocation, CPU, and GC.
4. **Residency evidence** — post-GC heap, hold drift, histograms, and MAT when present.
5. **Artifact validation** — expected count, non-empty recordings, derived views, dump,
   and report paths.
6. **Interpretation** — measured facts, source-confirmed mechanisms, inferences, and
   unknowns kept separate.
7. **Commands used** — exact build, run, dump, conversion, and analysis commands.
8. **Limitations** — observer overhead, one-node in-JVM scope, sampling, timeout, and
   missing evidence.

Do not make implementation changes or Git changes after analysis. Ask for a separate,
explicit implementation request if the evidence suggests a code change.
