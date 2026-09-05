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

# Memtable residency baseline

## Scope

Implement step 1 of the residency experiments. Add a separate harness using the
existing profiling driver. Keep production memtable, metrics, and flush behavior
unchanged. Capture repeatable eager-allocation baselines before adding policies.

## Implementation

1. Add a pre-start node configuration hook to `ProfiledClusterHarness`.
2. Add `MemtableResidencyProfileHarness` with never-written, written/flushed,
   idle/reactivate, rotating-burst, and continuous-trickle scenarios.
3. Fix table schema, payload bytes, active subset, seed, operation count and
   offered schedule through validated arguments. Record effective configuration.
4. Write operation timings and periodic resource samples to the run directory.
   Distinguish service latency from latency since scheduled arrival. Stop on
   request failure; report missed arrival deadlines rather than reducing load.
5. Capture policy outcomes before any forced flush or GC. Wait for submitted
   flushes and allocator reclamation before post-GC checkpoints. Force flush only
   in the explicit-flush scenario. Verify deterministic row contents.
6. Record table memtable counts/data/accounted memory, flush backlog, SSTables,
   flush/compaction bytes, and JVM heap. Label driver-thread allocation and
   node-wide pool values. Optional heap dumps support retained-owner analysis.
7. Add configuration/schedule property tests and small real-cluster scenarios.
   Add documented launch/test entrypoints without duplicating JVM setup.

## Validation and baseline capture

- Run `.build/sh/ai-build` with checkstyle and compile the distributed tests.
- Run targeted harness tests and generated configuration/schedule cases.
- Smoke all five scenarios and the original creation harness.
- Capture N=100 baseline runs for each scenario, repeat residency checkpoints,
  and capture an allocation profile plus a no-profile control.
- Report exact arguments, effective configuration, artifact paths, observed
  values, failures, and measurement limits in `research/memtable_residency_baseline.md`.
- Use fresh JVMs and node directories for runs. Keep JDK, heap, CPU count,
  memtable, SSTable format, and compaction implementation fixed.
- Keep executed runs at 1,000 tables or fewer, as requested. Baselines use 100.

## Limits and acceptance

The workload driver shares the node JVM. It uses a deterministic paced serial
schedule and records lateness; it is not a saturation load generator. Samples
scan table state and add observation cost. Accounted memtable memory excludes
some empty object overhead; whole-JVM heap includes driver/system state. Heap
dumps provide attribution, not just class counts. No cache eviction is performed.
Read timings describe the existing cache state.

Accept when scenarios verify data, artifacts distinguish policy and settled
outcomes, tests pass, and baseline runs have an evidence-backed report. No
production lazy allocation or eviction policy is implemented in this step.

## Completed

Build and Checkstyle passed. Generated schedule tests and all five real-cluster
scenarios passed. Captured 13 N=100 baseline/control/diagnostic runs, verified the
original creation harness and both alternative memtable configurations, and
validated allocation recordings and heap dumps. The
[baseline report](../research/memtable_residency_baseline.md) records commands,
artifacts, results, limitations, and the observed 1 MiB first-write slab cost.
