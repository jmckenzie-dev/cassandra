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

# Memtable residency validation task

Validate only the memtable-residency worktree. Use JDK 21 and the ai-build and
ai-ci-test wrappers. Run the ten planned feature test classes and the four
existing domain smoke classes. Add CommitLogTest only when source inspection
shows that its exercised behavior intersects the extracted paths. Preserve XML
reports and command logs. Record passed, skipped, failed, and generated cases.
