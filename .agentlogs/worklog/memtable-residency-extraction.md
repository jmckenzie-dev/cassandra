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

# Memtable residency extraction

- Split lazy initialization from automatic idle flushing at the source commit
  boundary.
- Use the final repaired lazy test blobs after the source rebase.
- Compare blob hashes after full-file extraction to detect transcription drift.
- Keep mixed distributed coverage by method. Leave hierarchy-specific methods
  for integration and retain the default scheduler path on this branch.
- Do not treat a source extraction as verified when a shared baseline defect
  stops the build and runtime gate.
- macOS distributed fixtures need configured loopback addresses. For these two
  serialized single-node tests, subnet 0 avoids host alias changes.
- Preserve generated trace counts separately from JUnit case counts.
- Use passing correctness assertions for the standalone branch. Leave retained
  memory, compaction-byte, and drain-rate measurements to the combined harness,
  where the legacy controls and other feature branches are available.
