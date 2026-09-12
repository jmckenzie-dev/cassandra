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

# Remaining-work extraction

Parent c6631dcef555d278cbb0f6734a887cc94ee90e1b. Source319995ce5c178f5eb0dbaf135b98d395c40a045c.

Keep two production commits: Unified Compaction Strategy (UCS) hierarchy control,
and lazy tombstone histogram construction. Preserve the 1 MiB hierarchy default
and disabled lazy-histogram default. Import only EXTRA-owned paths and hunks.

Build and run the new/changed tests plus existing ShardedMultiWriterTest and
StreamingTombstoneHistogramBuilderTest. Retain generated histogram traces.
Keep source research and combined benchmark scripts in the original worktree.
