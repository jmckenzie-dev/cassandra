#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

set -euo pipefail
project_root="$(cd "$(dirname "$0")" && pwd)"
cd "$project_root"
mkdir -p logs tmp
run_log="logs/$(date +%Y%m%d-%H%M%S)-run-tests.log"
exec > >(tee "$run_log") 2>&1
report_dir="${run_log%.log}-reports"
mkdir -p "$report_dir"

history=(
  org.apache.cassandra.metrics.AdaptiveHistogramHistoryTest
  org.apache.cassandra.metrics.AdaptiveHistogramHistoryPropertyTest
  org.apache.cassandra.metrics.AdaptiveJmxHistogramHistoryTest
)
registration=(
  org.apache.cassandra.metrics.CompactJmxRegistrationTest
  org.apache.cassandra.metrics.CassandraMetricsRegistryTest
)
names=(
  org.apache.cassandra.utils.ObjectNamePropertyPatternTest
  org.apache.cassandra.auth.jmx.TransientMBeanServerAuthorizationTest
)
case "${1:---all}" in
  --all) tests=("${history[@]}" "${registration[@]}" "${names[@]}") ;;
  --jmx-history) tests=("${history[@]}") ;;
  --jmx-registration) tests=("${registration[@]}") ;;
  --jmx-names) tests=("${names[@]}") ;;
  --properties) tests=(org.apache.cassandra.metrics.AdaptiveHistogramHistoryPropertyTest org.apache.cassandra.metrics.CompactJmxRegistrationTest org.apache.cassandra.utils.ObjectNamePropertyPatternTest) ;;
  *) echo "Usage: $0 [--all|--jmx-history|--jmx-registration|--jmx-names|--properties]" >&2; exit 2 ;;
esac
if [[ $# -gt 1 ]]; then
  echo "Expected at most one test selector" >&2
  exit 2
fi
for test_class in "${tests[@]}"; do
  echo "Running $test_class"
  status=0
  .build/sh/ai-ci-test "$test_class" || status=$?
  report="build/test/output/test/TEST-$test_class.xml"
  if [[ -f "$report" ]]; then
    cp "$report" "$report_dir/"
  elif [[ $status -eq 0 ]]; then
    echo "Missing test report: $report" >&2
    exit 1
  fi
  if [[ $status -ne 0 ]]; then
    exit "$status"
  fi
done
