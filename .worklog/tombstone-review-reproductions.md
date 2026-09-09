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

# Test review claims directly

- Zero live rows does not mean zero unfiltered Rows. Deleted Rows still advance
  short-read protection's clustering position.
- Full-data repair completes checkpointed responses through short-read
  protection. A missing outer cursor setter alone does not prove truncation.
- Reintroduce replica divergence before testing a second blocking-repair read.
- Range markers need a bounded allowance to reach a row checkpoint. An immediate
  cutoff broke existing generated histories; the fixed allowance passed them.
- Successful replica checkpoints need their own metric. Count failures only
  when the replica aborts, and test both counters explicitly.
- Test fixtures must configure the independent paging threshold. Updating only
  the failure threshold left older paging tests below the actual paging cutoff.
- JUnit assumptions explain the six skipped parameter combinations. Searching
  only for Ignore annotations missed them.
- Allocate continuation storage as components arrive, rather than from an
  untrusted component count before reading any components.
