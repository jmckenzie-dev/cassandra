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

# Metric storage source review

## Result

All 48 STORE inventory paths are present. A separate read-only ownership review
found no missing STORE production hunk. The only accepted source difference is
the normalized final newline in `Config.java`.

The branch keeps these source behaviors:

- `optimized_metrics_enabled` defaults to true.
- Geometric meter arrays and lazy metric identifiers default to false.
- Histogram and timer factory reuse is unconditional on the STORE branch.
- `LatencyMetrics` changes only the lazy counter selector and subclass. Its
  legacy lifecycle remains unchanged.
- Compact reservoirs, geometric meters, factory reuse, and lazy identifiers
  remain in review order. Lazy geometric storage follows geometric storage.

## Shared-file boundaries

Shared registry files contain only STORE storage, factory, and lazy allocation
hunks. The branch excludes SELECT profiles, no-op recorders, ownership, and
release changes. It also excludes EXPORT Java Management Extensions (JMX) and
MEM code.

The final integration branch must restore the source SELECT profile guards
around histogram and timer reuse. Four combined assertions require SELECT code.
The source manifest maps each assertion and its required merged invariant.

## Standalone changes

The standalone adaptations add a required registry import, place STORE option
text outside the absent SELECT documentation section, and retain the source
launcher mode. The existing standard-library analyzer runs with the host
`python3` because this host has no `uv` or executable `./venv/bin/python`.

No dependency was installed. No source worktree or other split worktree was
changed.
