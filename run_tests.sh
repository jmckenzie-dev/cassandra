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

ucs=(
  org.apache.cassandra.db.compaction.unified.ControllerTest
  org.apache.cassandra.db.compaction.unified.ControllerHierarchyTest
  org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTest
  org.apache.cassandra.db.compaction.unified.ShardedMultiWriterTest
)
tombstone=(
  org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilderTest
  org.apache.cassandra.utils.streamhist.LazyTombstoneHistogramBuilderTest
  org.apache.cassandra.utils.streamhist.LazyTombstoneHistogramBuilderPropertyTest
  org.apache.cassandra.io.sstable.metadata.MetadataCollectorHistogramTest
  org.apache.cassandra.utils.streamhist.TombstoneHistogramAllocationTest
)
case "${1:---all}" in
  --all) tests=("${ucs[@]}" "${tombstone[@]}") ;;
  --ucs-hierarchy) tests=("${ucs[@]}") ;;
  --tombstone) tests=("${tombstone[@]}") ;;
  --properties) tests=(org.apache.cassandra.utils.streamhist.LazyTombstoneHistogramBuilderPropertyTest) ;;
  *) echo "Usage: $0 [--all|--ucs-hierarchy|--tombstone|--properties]" >&2; exit 2 ;;
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
