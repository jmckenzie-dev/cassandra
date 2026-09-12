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

# Expiration test verification

Java Development Kit (JDK) 21.0.12.1. Both worktrees passed .build/sh/ai-build,
including main/test Checkstyle. The wrapper skips generated docs and license
auditing. Run .build/sh/ai-ci-test
org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTest for this class.

Base: 27 cases per run, three passing runs. Remaining: 28 cases per run, three
passing runs. All six exited zero, with no failures, errors, or skips.
Reports remain in each worktree's logs/ucs-expiration-fix directory.

Base logs: 20260911-213122-ai-build.log, 20260911-213253-ai-ci-test.log,
20260911-213355-ai-ci-test.log, 20260911-213434-ai-ci-test.log.
Remaining logs: 20260911-213256-ai-build.log, 20260911-213530-ai-ci-test.log,
20260911-213621-ai-ci-test.log, 20260911-213700-ai-ci-test.log.
All log names are relative to logs/ in the respective worktree.

The remaining runs include its uncommitted hierarchy extraction. This commit
contains only the same fixture correction validated separately on the base.
It does not claim that the remaining feature branch is fully verified.
