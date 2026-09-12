<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Metric storage extraction worklog

- Extract shared imports with their owned hunks. A missing registry import can
  pass textual review and fail only during compilation.
- Classify combined tests before the first build. A SELECT profile method in a
  STORE test caused test compilation to fail and then moved to the integration
  map.
- Check executable modes after applying source patches. The reservoir launcher
  first returned exit code 126 because text extraction lost mode 100755.
- Preserve each test XML before the wrapper replaces it. The final evidence has
  all 16 class reports and distinguishes 98 passed cases from one skipped case.
- Audit nested property and enclosed runners. Wrapper reports can hide omitted
  entrypoints unless their declared and reported counts are compared.
- Run standard-library analyzers with an existing interpreter. Do not create a
  Python environment or fetch dependencies for these scripts.
