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

Copied 13 EXTRA-owned source/test/documentation paths from final source.
Added only LAZY_TOMBSTONE_HISTOGRAMS to shared CassandraRelevantProperties.
No dependency on memtable residency or metric changes is permitted.

## Test adaptations for integration

ControllerHierarchyTest.realFlushEstimatesPromoteLargeFilesAcrossLevelGaps:
replace post-read !TrieMemtable.isInitialized() with currentMemtable.isClean().
Keep the original dormancy assertion for integration; retain all level, file,
flush-size, and 514-row assertions here.

ControllerHierarchyTest.compactionPreservesDeletesAndTtlAtExplicitReadTimes:
replace post-insert TrieMemtable.isInitialized() with !currentMemtable.isClean().
Keep the original initialization assertion for integration; retain all explicit
time, deletion, concurrent read, final file count, flush, and row assertions here.

README includes the source UCS operator example. Combined benchmark commands and
historical research links remain in the source worktree, not this branch.

## Residual inventory

Historical research remains at ../wt_moar_tables_presplit/research and its other
ARCHIVE paths in the coordinator's 246-path inventory. OpenTelemetry comparisons,
width-over-time studies, combined census/idle drivers, and mixed fixtures belong
to integration. No archived file was deleted.

Future ideas: SSTable snapshot trimming/offset sharing; further JMX object
reductions; worker-owned metrics; lazy aggregation; registration-list copying.
These ideas are not implemented by this split.
