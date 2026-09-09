#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail
project_root="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$project_root/logs"
exec > >(tee "$project_root/logs/$(date +%Y%m%d-%H%M%S)-run_property_tests.log") 2>&1
if [[ $# == 1 && "$1" == --idle-flush ]]; then
    exec "$project_root/.build/sh/ai-ci-test" org.apache.cassandra.db.memtable.IdleMemtableFlusherTest
fi
if [[ $# == 1 && "$1" == --metric-ids ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.LazyMetricIdPropertyTest
fi
if [[ $# == 1 && "$1" == --metrics-ref ]]; then
    exec "$project_root/.build/sh/ai-generate-metrics-reference" --property
fi
if [[ $# == 1 && "$1" == --metric-profiles ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.MetricProfilePropertyTest \
        'org.apache.cassandra.metrics.NoOpMetricsTest$Properties'
fi
if [[ $# == 1 && "$1" == --jmx-history ]]; then
    "$project_root/venv/bin/python" "$project_root/.build/sh/test_analyze_heap_ownership.py" HistoryArrayStoragePropertyTest
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.AdaptiveHistogramHistoryPropertyTest
fi
if [[ $# == 1 && "$1" == --otel-storage ]]; then
    exec "$project_root/.build/sh/ai-benchmark-otel-storage" --property-only
fi
if [[ $# == 1 && "$1" == --histogram-widths ]]; then
    exec "$project_root/.build/sh/ai-benchmark-otel-storage" --width-over-time --property-only
fi
if [[ $# == 1 && "$1" == --jmx-names ]]; then
    exec bash "$project_root/.build/sh/ai-probe-jmx-names" --property-only
fi
if [[ $# == 1 && "$1" == --jmx-query ]]; then
    exec bash "$project_root/.build/sh/ai-probe-jmx-names" --boundaries --properties
fi
if [[ $# == 1 && "$1" == --jmx-registration ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" 'org.apache.cassandra.metrics.CompactJmxRegistrationTest$Properties'
fi
if [[ $# == 1 && "$1" == --metric-bookkeeping ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" 'org.apache.cassandra.metrics.TableMetricBookkeepingTest$Properties'
fi
if [[ $# == 1 && "$1" == --lazy ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --property
fi
if [[ $# == 1 && "$1" == --histograms ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --histogram-property
fi
if [[ $# == 1 && "$1" == --reservoirs ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --reservoir-property
fi
if [[ $# == 1 && "$1" == --meters ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --meter-property
fi
if [[ $# != 0 ]]; then
    echo 'Usage: run_property_tests.sh [--idle-flush|--lazy|--histograms|--meters|--reservoirs|--metrics-ref|--metric-profiles|--jmx-history|--otel-storage|--histogram-widths|--jmx-names|--jmx-query|--jmx-registration|--metric-ids|--metric-bookkeeping]' >&2
    exit 2
fi
export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
exec "$project_root/.build/sh/ai-profile-many-tables" org.apache.cassandra.distributed.test.MemtableResidencyConfigTest
