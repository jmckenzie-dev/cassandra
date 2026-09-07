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

# Paging error and performance audit

- Trace exceptions through the native protocol. An internal tombstone exception
  can become ServerError even when a component test treats it as expected.
- Remote checkpoint responses do not imply a cursor on the coordinator command.
  Reject unsupported reconciliation using response metadata and a typed failure.
- Keep checkpoint opt-in explicit. Default local APIs also serve callers that
  cannot resume a partial result, including unpaged and unfiltered reads.
- Deliver captured real messages through the inbound handler to fix response
  order in tests. Use bounded latches and futures instead of sleeps.
- Validate mutation models with independent Cassandra replay, and bound the
  driver traversal itself; a correct model does not prevent a paging loop.
- Successful test stdout may be discarded. Save benchmark measurements to a
  dated artifact and verify that the artifact contains the reported samples.
- Replica read counts were stable while short-run latency ratios varied. Retain
  both observations; do not describe an unstable timing ratio as proven overhead.
- Audit configuration values independently. Equal paging/failure settings in
  tests hid the mismatch between the documented and implemented cutoff.

Evidence: [audit results](../.debug/paging-correctness-and-performance.md).
