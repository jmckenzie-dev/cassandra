# PMD cognitive complexity

No earlier PMD run was found in this worktree's logs, review notes, plans, or worklog.

PMD 7.27.0 scanned the 23 Java files changed between merge base
`88fd0f6a0eaed8943f05ac9e8f947882b8ddc8f1` and branch head
`4abd8c09a9ae596d5866c5c2fc55d94cb6dd89c1` on 2026-09-10.
The baseline scan covered the 20 files that existed at the merge base.
Both scans completed with no processing or configuration errors.

This run measures cognitive complexity, not the full PMD ruleset.
The built-in Java CognitiveComplexity rule used reportLevel=1 to expose
all positive scores. A local PMD rule exported declaration identities,
zero scores, and class totals using PMD's own cognitive complexity metric.
All 663 positive head scores and 616 positive baseline scores matched the
built-in rule. Inventory records are reported through PMD's violation output;
these record counts are not defect counts.

Declarations were compared by source path, enclosing binary class name,
declaration kind, and method name with whitespace-normalized parameters.
The rankings include production and test code. Class totals sum directly
declared methods and constructors. Nested classes are ranked separately.
There are only three added top-level classes; the top five includes nested classes.

## Top five added methods

| Method | Score | Source |
|---|---:|---|
| TombstoneTriggeredCompactionManager.drain() | 24 | src/java/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManager.java:200 |
| CompactionsTest.testReactiveCompactionRetainsTombstoneOverOlderData() | 23 | test/unit/org/apache/cassandra/db/compaction/CompactionsTest.java:932 |
| PurgeFunctionTest.testPurgeableRangeEvaluationCounts() | 19 | test/unit/org/apache/cassandra/db/partitions/PurgeFunctionTest.java:109 |
| TombstoneTriggeredCompactionManagerTest.assertQueuedAdmissions(List) | 14 | test/unit/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManagerTest.java:617 |
| CompactionStrategyManager.getUserDefinedTasksIfAvailable(Collection, long, OperationType) | 12 | src/java/org/apache/cassandra/db/compaction/CompactionStrategyManager.java:1301 |

## Top five added classes

| Class | Score | Source |
|---|---:|---|
| TombstoneTriggeredCompactionManagerTest | 49 | test/unit/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManagerTest.java:63 |
| TombstoneTriggeredCompactionManager | 41 | src/java/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManager.java:42 |
| TombstoneCompactionShutdownTest | 7 | test/distributed/org/apache/cassandra/distributed/test/TombstoneCompactionShutdownTest.java:70 |
| TombstoneCompactionShutdownTest.GatedScanner | 6 | test/distributed/org/apache/cassandra/distributed/test/TombstoneCompactionShutdownTest.java:320 |
| TombstoneTriggeredCompactionManagerTest.QueueOperation | 5 | test/unit/org/apache/cassandra/db/compaction/TombstoneTriggeredCompactionManagerTest.java:704 |

## Production methods only

| Method | Score |
|---|---:|
| TombstoneTriggeredCompactionManager.drain() | 24 |
| CompactionStrategyManager.getUserDefinedTasksIfAvailable(Collection, long, OperationType) | 12 |
| TombstoneTriggeredCompactionManager.enqueue(TableId, DecoratedKey) | 10 |
| CompactionManager.executeTombstoneTriggeredCompaction(TableId, DecoratedKey) | 6 |
| TombstoneTriggeredCompactionManager.shutdown(boolean) | 5 |

## Local artifacts

- Built-in rule reports: `tmp/pmd-branch/head.json`, `tmp/pmd-branch/base.json`.
- Metric exports: `tmp/pmd-branch/head-metrics.json`, `tmp/pmd-branch/base-metrics.json`.
- Added declarations: `tmp/pmd-branch/added.json`.
- Rules: `tmp/pmd-cognitive.xml`, `tmp/pmd-metrics.xml`, `tmp/BranchCognitiveRule.java`.
- Snapshot preparation: `tmp/pmd-prepare.py`.
- Ranking and cross-check: `tmp/pmd-rank.py`.
- Ranking log: `logs/pmd-20260910-094227-312375.log`.
- Head scan logs: `logs/pmd-20260910-094049-876128.log`, `logs/pmd-20260910-094158-606009.log`.

PMD's default method reporting threshold is 15:
https://pmd.github.io/pmd/pmd_rules_java_design.html#cognitivecomplexity
