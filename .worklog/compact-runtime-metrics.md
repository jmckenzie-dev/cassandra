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

# Compact runtime metrics

- Assign primitive arrays by field ownership. A shallow class census missed
  155,968 bytes of empty reservoir counter payload per user table.
- Sparse pages need a dense fallback. Page headers raised fully populated
  reservoir graphs by 29% in the first candidate.
- Preserve the legacy export representation and snapshot merge contract.
  Cumulative interval exports subtract bucket arrays by position.
- Parent release needs an exact bucket-population assertion. Checking only
  scalar counts and means missed existing double inclusion of child histograms.
- Use repository-relative edit paths. `/home` versus `/var/home` aliases caused
  an unnecessary permission prompt even though both reached the same worktree.
- Keep each benchmark record in a single println. Cassandra's test logger
  splits printf fragments into separate log records.
- Thread identity does not establish contention. Four serial writer handoffs
  should retain a single dense stripe; activate additional stores on a failed
  atomic update instead.
- A count-conservation test can pass with broken stripe activation. Assert that
  contention occurred, then test nonzero rounding on each physical stripe.
- Shared-host timing varies for the unchanged control too. Preserve every batch
  and compare repeated matched runs before attributing a change to the patch.
- Smaller dense arrays change the sparse promotion break-even point. The 50%
  threshold increased N100 counter payload; 75% kept those histograms sparse.
- Pin contended microbenchmarks to a recorded CPU set when cache-group placement
  changes results between forks. Affinity does not reserve cores from other work.
- Use distinct loopback subnets for sequential table measurements. Record failed
  startup attempts separately; do not substitute them for workload measurements.
- Exercise dense user metrics explicitly. Four writes per table do not reach
  dense promotion; 128 writes expose 600 dense counter stores in the N100 heap.
- Narrow counter storage needs a contention policy as well as overflow handling.
  Repeated int CAS loops reduced four-thread throughput; widen busy stores to
  recover direct long atomic additions while retaining narrow quiet stores.
- A representation change must not appear as a failed strong CAS. The reservoir
  uses CAS failures to allocate extra stripes, so migration-only failures can
  silently erase the resident-memory benefit even when counts remain correct.
