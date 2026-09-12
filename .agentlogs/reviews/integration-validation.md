<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Combined validation

Java 21.0.12.1. Runtime and test sources correspond to eeb3e6957e. Later commits
change records, shell tooling, and executable modes only. The frozen JAR hash is
6afb1f7644fb42471393266d081672ab802d124bd6858c9c280e9ccdc996a74e.

## Build and tests

- ai-build passed, including main and test Checkstyle:
  logs/20260912-000545-ai-build.log.
- The focused combined runner passed 57 top-level classes: 333 cases,
  332 passed and one existing DecayingEstimatedHistogramReservoirTest skip.
- Three explicit nested property entrypoints passed five additional cases.
  Total: 337 passed, one existing skip, zero failures or errors.
- The new five-case configuration matrix and the original five registration
  cases passed again in a fresh process with the transient MBean server builder.
  These ten repeated cases are not added to the unique count above.
- The reference generator passed 11 unit tests, one property test, and --check
  for 126 table and 101 keyspace metrics. Heap analyzer tests passed four cases.

The complete class totals are in integration-test-results.tsv. Raw reports are
preserved under logs/20260912-000806-validate-many-tables-split-reports; the
driver log has the same timestamp. The alternate-builder run is preserved in
logs/20260912-002328-many-tables-launch.log and ends with OK (10 tests).

The restored MemtableResidencyHarnessTest passed five methods spanning 14
scenarios. IdleMemtableFlushTest passed all seven methods. Recovery passed both
methods. All original combined assertions remain present.

## Boundary programs

The separate --boundaries run passed:

- 16,000 canonical-name copy cases.
- Local/remote query, notification, and operation boundaries with both builders.
- Late builder installation without changing the existing platform server.
- 16,000 generated queries, 1,600 lifecycle names, and 1,000 concurrent cycles.

Logs: logs/20260912-002601-validate-many-tables-split.log and the five
20260912-002602 through 002605 jmx-names logs.

## Scope and limits

This is targeted validation, not the full Cassandra suite. No numerical code
coverage percentage is claimed. Boundary and property executions complement
the unit/distributed cases; they are not added to the JUnit case total.
The combined build and tests ran before measurements. Measurements remain
pending and must use the frozen binaries without competing build/test work.
