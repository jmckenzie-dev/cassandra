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

# Memtable residency extraction notes

- The lazy production files match their f5f830e5d8 source blobs.
- The lazy test files match their final 319995ce5c source blobs.
- The source rebase changed two lazy fixtures. This extraction uses the repaired
  final versions instead of the original f5f830e5d8 versions.
- Apply patches relative to the coordinator root with the worktree prefix.
- Final blob comparisons cover all fully owned production files and unit tests.
- Static searches found no metric configuration or UCS hierarchy option in the
  standalone production and test changes.
- Validation stopped before the branch build. The shared baseline reproduced
  `UnifiedCompactionStrategyTest.testDropExpiredSSTables` with a null task pick.
