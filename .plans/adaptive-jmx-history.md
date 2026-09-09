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

# Optional adaptive JMX history

Preserve current metric recording, registration, bucket layout, and independent
alias scrape cursors. Add `adaptive_jmx_histogram_history_enabled`, default false,
independent of `optimized_metrics_enabled` and `metrics_config_file`.

The current path retains a long array after each cumulative histogram/timer
scrape. The optional path retains no array for all-zero snapshots and otherwise
uses the smallest signed primitive width that preserves every value. It may
reuse backing arrays and must shrink after reset. Preserve negative deltas,
long overflow, changing lengths, and independent returned-array ownership.
Keep all record paths unchanged.

Production scope: a package-private AdaptiveHistogramHistory utility and the
two recent-value methods in CassandraMetricsRegistry; Config and
DatabaseDescriptor expose the startup setting. The utility provides
`static long[] delta(long[] now, Object last)` and
`static Object pack(long[] now, Object previous)` for primitive-array history.
Existing registry delta remains the legacy control.

Validation:
- Unit tests compare both paths across signed-width boundaries, resets, length
  changes, empty snapshots, overflow, and returned-array mutation.
- Integration tests exercise histogram and timer JMX wrappers, aliases, and
  noncumulative behavior with both settings.
- A production-helper benchmark pairs legacy and adaptive paths at empty,
  narrow, wide, and changing counter values. Warm up, repeat, report time and
  allocated bytes. Include full recent-value JMX calls where practical.
- Capture a fresh legacy heap census before production edits. Then use the same
  binary with each flag value, simple_metrics.yml, 100 tables, both full and
  attribute-only scrapes. Extend to 1000 tables if the results support it.
- Update heap analysis to count all primitive history widths. Check identical
  registration counts and metric values. Separate array savings from whole-JVM
  heap movement. Keep timing benchmarks separate from builds and heap analysis.

Comparison basis: existing dirty working tree on HEAD 4192a00e1f, including
completed metric-profile work. Do not revert or commit that work. New optional
path remains disabled by default. Record pre/peri/post measurements in research.
