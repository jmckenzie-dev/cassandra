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

# Memtable residency validation

State: blocked before build and tests.

The coordinator reproduced this failure on the frozen shared base while another
feature branch ran its planned smoke tests:

`UnifiedCompactionStrategyTest.testDropExpiredSSTables` failed because `pick`
was null at the existing `assertNotNull(pick)` assertion. The sibling feature
branch showed the same failure. The coordinator stopped branch validation to
avoid reporting results past a shared baseline defect.

No memtable branch build, test wrapper, distributed fixture, or measurement ran.
There are no branch validation logs or XML reports. Static checks completed:

- `git diff --check` passed before checkpoint commit.
- Eight fully owned final idle production or unit-test files matched source blob
  hashes from 319995ce5c exactly.
- All seven lazy production and test files matched their recorded source blobs
  before the lazy commit.
- A source-independence search found no metric configuration or UCS hierarchy
  option in the standalone files.

Pending validation remains in TODO.md. Do not describe this branch as verified.
