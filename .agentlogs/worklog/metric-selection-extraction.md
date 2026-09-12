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

# Metric selection extraction worklog

- Split complete owned paths and shared hunks from source commit
  `319995ce5c178f5eb0dbaf135b98d395c40a045c` onto corrected base
  `f7d5a7e2c0fcf769293ee27073630a3574584b06`.
- Preserved `simple_metrics.yml` as the supplied default and kept absent/null
  profile behavior as the full legacy catalog.
- Removed combined STORE, EXPORT, and MEM settings from standalone distributed
  tests. Recorded both moved TrieMemtable initialization assertions for the
  integration branch.
- Used the legacy map bookkeeping in the first runtime commit. Added compact
  release bookkeeping as a separate review unit, then confirmed the final
  `TableMetrics.java` blob matches the built and tested source tree.
- Adapted the generator runner to use the existing project Python or host
  `python3`. The tests use only the Python standard library.
- Found that `PROFILE_CONFIG_DIR` does not set the in-JVM node profile in the
  generic harness. Classified the completed run as smoke evidence and moved the
  controlled comparison to the integration harness.
