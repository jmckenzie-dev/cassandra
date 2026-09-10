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

# Checkpoint ownership reproduction lessons

- A rejected response does not necessarily end a read. A speculative read can
  still obtain a remote quorum. Make those successful replicas disagree to test
  whether rejected local execution state reaches read repair.
- Check the native error contract. An existing direct test that expects a local
  exception can preserve a generic server error visible to clients.
- Coverage of repair setup is insufficient: the first passing native tests
  skipped the checkpoint guard. A second, quorum-specific scenario reached it
  and reproduced FUL-019. Keep that distinction in the evidence.
- Direct coordinator tests must initialize and finish the request's warning
  context. Do not disable its defensive checks to accommodate an incomplete
  fixture.
- A copy hook and local execution need not coexist in a production coordinator.
  Accord's out-of-range copy hook disables local reads. Do not invent a combined
  coordinator to claim a user-visible reproduction.

Evidence: [.debug/tombstone-checkpoint-ownership-reproductions.md](../.debug/tombstone-checkpoint-ownership-reproductions.md).
