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

# Tombstone test follow-ups

- Driver paging-state wrappers use object identity. Compare serialized bytes
  to detect repeated checkpoints. Test the guard with separate real responses.
- Empty pages need their own page budget and cursor-order check; counting only
  returned rows cannot bound an empty-page cycle.
- Ant's shell launcher can discard environment names containing dots. Pass
  replay seeds through `test.jvm.args` and assert one replay iteration.
- Fresh partitions preserve divergence when testing blocking read repair.
  The asymmetric mixed-version matrix passed without production changes.
- The archived 4.1 harness cannot access `Field.modifiers` on Java 17. An
  isolated Java 11 build ran the unchanged legacy test successfully.
- Full commands, failures, and final results are in
  [the test report](../.debug/tombstone-test-followups.md).
