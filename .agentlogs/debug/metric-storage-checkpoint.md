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

# Metric storage checkpoint

Date: 2026-09-11

Branch: many-tables/metric-storage

Original base: c6631dcef555d278cbb0f6734a887cc94ee90e1b

Corrected base: f7d5a7e2c0fcf769293ee27073630a3574584b06

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c

## Extracted

- Compact reservoir configuration, factory selection, snapshot adapters, code,
  tests, property tests, probes, microbenchmark, and analysis tools.
- Geometric meter property, factory, implementation, tests, and property tests.
- Unconditional histogram and timer reuse in the standalone registry.
- Meter registry factory selection.

## Checkpoint status

This checkpoint is superseded by the completed extraction and validation record.

## Validation state

At this checkpoint, no build or test had started. The coordinator later fixed
the shared-base test and resumed this branch.
