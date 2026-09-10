<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Simplify four new production methods

Start at 9237b39014. Preserve behavior and storage while reducing cognitive
complexity in the four new production methods scoring at least 18. Aim for at
most 10 per method, including extracted helpers. Do not move the same complexity
into another large method. Do not change existing upstream algorithms outside
these four methods. No commit requested for this refactor.

The baseline PMD 7.27.0 report is in
logs/20260910-013715-branch-pmd/measurements.json. The branch merge base is
88fd0f6a0eaed8943f05ac9e8f947882b8ddc8f1. Rank new production methods separately
from tests, modified upstream methods, and implementations moved into overloads.

## ObjectNamePropertyPattern.matches: 30

| Approach | Advantages | Costs / risks |
|---|---|---|
| Extract value-end scanning, key lookup, and value comparison | Names the parsing steps; retains canonical-string ranges, wildcard algorithm, and allocation behavior | A few private calls; key lookup stays linear in pattern keys |
| Parse candidate properties into a map and use normal lookups | Short code using JDK property access | Can populate ObjectName caches or allocate maps/strings; defeats the residency work |
| Sort pattern keys and merge them with canonical candidate keys | Can reduce repeated key comparisons | Adds sorting and ordering assumptions; changes the algorithm and needs new performance evidence |

Choose the first. Preserve encoded quotes/backslashes, empty values, wildcard
matching, missing keys, extra keys, and early rejection. No candidate substrings,
maps, or parser objects.

## IdleMemtableFlusher.scan: 24

| Approach | Advantages | Costs / risks |
|---|---|---|
| Extract completed-flush cleanup and one-candidate submission | Makes admission order visible; keeps one monitor and the existing iterator/budgets | Helpers rely on the caller holding the monitor |
| Flatten every branch in the existing method with continue/return | No new methods; preserves locality | Completion cleanup and admission remain interleaved in a long method |
| Introduce a candidate state machine or admission-policy objects | Could support future policies independently | Adds per-candidate state or abstractions, allocation, and lifecycle transitions without a current need |

Choose the first. Keep scan synchronized, preserve the catch boundary, stop on a
failed future or submission exception, and release concurrency only after both
successful flush completion and reader reclamation. Preserve candidate traversal,
time sampling, accepted-submission budget charging, and listener ordering.

## AdaptiveHistogramHistory.pack: 24

| Approach | Advantages | Costs / risks |
|---|---|---|
| Extract one typed packing helper for byte, short, and int | Keeps direct primitive loops and exact array reuse; dispatch remains easy to read | Three similar helpers are required by primitive array types |
| Use one loop with a width switch inside it | Centralizes traversal | Repeats type dispatch for each bucket and obscures typed writes |
| Introduce a storage interface or width-specific objects | Encapsulates storage and conversion | Adds wrappers or indirect calls and changes the compact representation |

Choose the first. Preserve signed width boundaries, null for zero snapshots,
reuse only at the same type and length, and ownership of the input long array.

## MetricProfile.parse: 19

| Approach | Advantages | Costs / risks |
|---|---|---|
| Extract validation and selection for one scope | Removes nested scope traversal from document parsing; keeps current diagnostics and validation order | One helper receives the scope value, scope identity, and source context |
| Bind YAML directly into typed configuration objects | Reduces casts in parsing code | Needs additional checks for unknown keys, strict types, duplicate sections, and compatibility of errors |
| Add reusable schema/section validators and a section enum | Could extend to future profile schemas | More types and generic plumbing for a fixed two-scope, three-section document |

Choose the first. Keep SnakeYAML SafeConstructor, duplicate-key rejection, alias
defaults, required metric checks, canonical-name validation, duplicate detection,
immutable results, and existing error messages.

## Validation

- Inspect existing unit and property coverage before adding tests. Add focused
  cases for uncovered behavior crossed by extraction, especially synchronous
  submission failure and quoted property boundaries.
- Run current histogram width/reuse/property and JMX history tests; metric profile
  unit/property tests; idle flusher lifecycle and budget unit/property tests.
- Run the relevant distributed idle-flush and profile registration/recording
  classes. Run shared-output test wrappers sequentially.
- Run JMX matching and boundary equivalence probes against JDK ObjectName
  behavior, including property tests and authorization checks.
- Build and run production/test Checkstyle through repository wrappers. Preserve
  timestamped logs and test reports. Do not run the entire test suite.
- Re-run PMD on all branch Java files after implementation, including new helpers.
  Report the four before/after scores and the top five new production methods.
- Record whether allocation and throughput were measured; do not infer a speedup
  from reduced complexity. Keep raw outputs in ignored logs/ and tmp/.

## Completed results

All four selected approaches were implemented without a second design iteration.
PMD 7.27.0 analyzed all 108 changed Java files, including the new unit test. The
final passes had no processing or configuration errors. The extraction pass uses
low thresholds to collect numeric values; its records are not defect counts.

| Method | Before | After | Highest extracted helper |
|---|---:|---:|---:|
| ObjectNamePropertyPattern.matches | 30 | 8 | 7 |
| IdleMemtableFlusher.scan | 24 | 8 | 7 |
| AdaptiveHistogramHistory.pack | 24 | 9 | 3 |
| MetricProfile.parse | 19 | 4 | 10 |

New helpers retain primitive loops, canonical-string ranges, and the existing
scope-local sets. No storage wrappers, per-table state, or additional locks were
introduced. The flusher helpers run under scan's monitor and exception handler.
Throughput and allocation were not benchmarked; this change claims simpler code,
not a measured speedup or heap reduction.

Validation on Java 21:

- Clean build and production/test Checkstyle passed through .build/sh/ai-build.
  Log: logs/20260910-084943-ai-build.log.
- All 55 focused unit cases and 24 distributed cases passed, with no failures,
  errors, or skips. Tests cover profiles, histogram history, idle flush admission,
  reclamation/failure, configuration, registration/recording, and JMX authorization.
- Added 360 direct ObjectName comparisons covering empty values, encoded quotes,
  commas, equals signs, backslashes, literal/pattern wildcards, supplementary
  Unicode, missing/extra keys, and prefix keys. Oracle: JDK ObjectName.apply.
- Added checks for histogram input ownership and synchronous submission failure.
  Existing generated histogram/profile/budget/lifecycle tests also passed.
- Four JMX boundary runs passed: ordinary, generated, late installation, and
  protected operation. Generated checks cover 16,000 queries, 1,600 lifecycle
  names, and 1,000 concurrent registration cycles. Query inspection retains no
  additional property-cache entries in the ten-name probe.
- JaCoCo measured the four refactored methods and their helpers at 150/150 lines
  and 123/124 branches. The remaining outcome is a short-circuit branch in scan's
  admission loop. No class-data mismatch warnings occurred.
- git diff --check and bash -n run_tests.sh passed. run_tests.sh --jmx-query now
  includes the new direct matcher test. No full-suite or performance run occurred.

Test commands/results, XML, logs, and coverage HTML/XML:
logs/20260910-085141-complexity-refactor/.
PMD commands/results, rules, scope, raw findings, measurements, and ranked methods:
logs/20260910-085502-complexity-refactor-pmd/.

The top five new production methods now score:

| Method | Cognitive complexity |
|---|---:|
| AdaptiveHistogramHistory.delta | 15 |
| CompactDecayingEstimatedHistogramReservoir.rescaleIfNeeded | 15 |
| ObjectNamePropertyPattern.wildcardMatches | 11 |
| CompactDecayingEstimatedHistogramReservoir.StripedBuckets.stripe | 10 |
| MetricProfile.parseScope | 10 |

TransientMBeanServerBuilder.Handler.invoke also scores 10. Ties sort by source
path, then line. No new production method scores 18 or higher. Ranking excludes
tests, constructors, existing upstream methods, and existing logic moved to a new
overload, including CassandraMetricsRegistry.registerMBean. Baseline declarations
were checked using the prior PMD report, with the high-scoring upstream methods
and moved registerMBean implementation inspected separately.
