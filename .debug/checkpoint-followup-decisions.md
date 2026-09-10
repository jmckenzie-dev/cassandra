<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Checkpoint follow-up decisions — 2026-09-10

## FUL-020: declined for this patch

The user declined the proposed local command-copy optimization. Keep the copy
and its isolation guarantees. Measure its allocation and runtime cost during
final end-to-end performance testing. No copy guard changed in this pass.

## FUL-016: documented

Add reciprocal Javadoc references at initial digest resolution, full-data
resolution, individual replica retries, and response metadata emission.
Explain the owned-page consistency-ONE exception, the reconciliation quorum
requirement, the single-replica retry distinction, and why metadata emission
does not establish acceptance. No predicates or runtime behavior changed.

## FUL-015: proposal only

Implemented subsequently as `TombstonePagingState`; see
`tombstone-paging-state.md` for the final design, tests, and measurements.
The text below records the original proposal.

A command-owned state holder should define lifecycle operations rather than
generic setters: attach a pager, begin local execution, record a local stop,
accept a coordinator continuation, clear for reconciliation, and create fresh
state for a command copy. Copy the pager capability, never a live payload or
the mutable holder itself. Keep payload clearing separate from detaching the
pager. Recording a local stop must remain valid without a pager owner.

This does not require a new lock or promise atomic publication across threads.
Existing execution and response-publication ordering still applies. Merely
moving fields into an object would not codify the lifecycle.

An isolated holder with one object reference, one int, and one boolean measured
24 shallow bytes on the local Java 17 runtime using the existing jamm 0.4.0
agent. This is a prototype size, not a measurement of a refactored ReadCommand
or an end-to-end performance result. Parent-object packing and reference sizes
affect net retained memory. Eager construction adds one allocation per command,
including command copies and reads with paging disabled. Method inlining and
garbage-collection cost need runtime measurement; no zero-cost claim is made.

The temporary probe is `tmp/CheckpointStateSize.java`. Commands used the
existing `distrobox enter dev -- bash tmp/paging-coverage-run.sh` logging wrapper:

- `javac -cp build/lib/jars/jamm-0.4.0.jar -d tmp tmp/CheckpointStateSize.java`
- `java -javaagent:build/lib/jars/jamm-0.4.0.jar -cp tmp:build/lib/jars/jamm-0.4.0.jar CheckpointStateSize`

Both exited zero. The holder is not implemented in production.
