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

# Integration idle watchdog diagnosis

The failed run is under
`logs/20260912-010422-split-measurements---idle/combined-1000-idle/20260912-011324-residency-idle-reactivate-1000t`.
The first three idle drains completed in 99.5, 101.6, and 103.2 seconds. The
fourth drain reached the 121-second deadline with 211 dirty memtables, one
flushing memtable, and two pending flushes.

The fourth drain made continuous progress. Dirty memtables fell from 1000 at
0 seconds to 901, 803, 705, 606, 510, 413, 315, and 217 at 15-second intervals
through 120 seconds. Flush count rose from 3001 to 3784. The remaining work
needed about 32 seconds at the observed rate. Fourth-cycle compaction reduced
live SSTables from 3000 to 1434 and wrote 3.41 MB. The prior three policy
checkpoints recorded no compaction work.

The primary error was `Automatic idle flushing did not drain` at
05:26:51.388. Cluster shutdown started at 05:26:51.390. About 5.3 seconds later,
active post-flush work submitted to executors that cluster shutdown had already
closed. The
resulting `RejectedExecutionException` messages are cleanup noise after the
watchdog failure. They did not cause the timeout.

The retry is under
`logs/20260912-013238-split-measurements---idle-retry`. It keeps the production
configuration and workload unchanged. It changes only `--settle-seconds` from
120 to 300. This option extends the harness deadlines in `awaitIdleFlushes()`
and `awaitSettled()`.

The run configured cursor compaction, but the log reports that cursor
compaction supports BIG output only and does not support BTI. Cassandra used
the normal compaction path. The retry can measure idle flushing with the BTI
fallback. It cannot support a cursor-compaction result.

## Retry result

The retry passed all four cycles, 16,000 writes, and 5,000 verification queries.
Drain durations were 95.136, 94.766, 97.744, and 148.104 seconds. The final
checkpoint had zero dirty or initialized table memtables, zero accounted
memtable heap bytes, no pending flushes or compactions, and no reclaiming
heap bytes. It recorded 4,000 flushes, 1,000 SSTables, and 1,000 compactions.
No failed write, phase error, warning, or shutdown error was reported.
The longer watchdog confirmed completion without any production change.
