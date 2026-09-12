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

# Metric storage extraction

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c

Corrected base: f7d5a7e2c0fcf769293ee27073630a3574584b06

Original base before the shared test correction:
c6631dcef555d278cbb0f6734a887cc94ee90e1b

Branch: many-tables/metric-storage

## Work

- Extract STORE-owned compact reservoir changes and tests.
- Extract geometric meters before lazy geometric meters.
- Extract unconditional metric factory reuse without metric profiles.
- Extract lazy metric identifiers and the STORE-only latency counter selector.
- Preserve optimized metric defaults and opt-in property defaults.
- Adapt combined fixtures and record each moved assertion.
- Run the focused build, compatibility tests, property tests, and probes.
- Record review, validation, source paths, adaptations, and measurements.

## Boundaries

- Do not import metric selection, no-op, ownership, or release changes.
- Do not import Java Management Extensions export changes.
- Do not import memtable or remaining experiment changes.
- Do not change the frozen source or any other worktree.
