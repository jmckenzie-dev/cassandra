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
exec > >(tee "$project_root/logs/$(date +%Y%m%d-%H%M%S)-run_tests.log") 2>&1
if [[ $# == 0 ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --retirement
fi
if [[ $# == 1 && "$1" == --idle-flush ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" --idle-flush
fi
if [[ $# == 1 && "$1" == --metric-ids ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.MetricFactoryReuseTest \
        org.apache.cassandra.metrics.LazyMetricIdTest \
        org.apache.cassandra.metrics.ThreadLocalHistogramTest \
        org.apache.cassandra.metrics.ThreadLocalMeterTest \
        org.apache.cassandra.metrics.GeometricThreadLocalMeterTest
fi
if [[ $# == 1 && "$1" == --metrics-ref ]]; then
    exec "$project_root/.build/sh/ai-generate-metrics-reference" --test
fi
if [[ $# == 1 && "$1" == --metric-profiles ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.MetricProfileTest \
        org.apache.cassandra.metrics.NoOpMetricsTest \
        org.apache.cassandra.metrics.LatencyMetricsTest \
        org.apache.cassandra.distributed.test.HeapOwnershipCensusHarnessTest \
        org.apache.cassandra.distributed.test.MetricProfileRegistrationTest \
        org.apache.cassandra.distributed.test.MetricProfileRecordingTest
fi
if [[ $# == 1 && "$1" == --jmx-history ]]; then
    "$project_root/venv/bin/python" "$project_root/.build/sh/test_analyze_heap_ownership.py" HistoryArrayStorageTest
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.metrics.AdaptiveHistogramHistoryTest \
        org.apache.cassandra.metrics.AdaptiveJmxHistogramHistoryTest \
        org.apache.cassandra.distributed.test.HeapOwnershipCensusHarnessTest
fi
if [[ $# == 1 && "$1" == --otel-storage ]]; then
    "$project_root/.build/sh/ai-benchmark-otel-storage" --verify-only
    exec "$project_root/.build/sh/ai-benchmark-otel-storage" --bucket-ranges
fi
if [[ $# == 1 && "$1" == --histogram-widths ]]; then
    exec "$project_root/.build/sh/ai-benchmark-otel-storage" --width-over-time --verify-only
fi
if [[ $# == 1 && "$1" == --jmx-names ]]; then
    exec bash "$project_root/.build/sh/ai-probe-jmx-names" 10
fi
if [[ $# == 1 && "$1" == --jmx-query ]]; then
    bash "$project_root/.build/sh/ai-probe-jmx-names" --boundaries 10
    bash "$project_root/.build/sh/ai-probe-jmx-names" --boundaries --late
    bash "$project_root/.build/sh/ai-probe-jmx-names" --boundaries --protected 10
    exec "$project_root/.build/sh/ai-ci-test" org.apache.cassandra.auth.jmx.TransientMBeanServerAuthorizationTest
fi
if [[ $# == 1 && "$1" == --jmx-registration ]]; then
    exec "$project_root/.build/sh/ai-ci-test" org.apache.cassandra.metrics.CompactJmxRegistrationTest
fi
if [[ $# == 1 && "$1" == --metric-bookkeeping ]]; then
    "$project_root/.build/sh/ai-ci-test" org.apache.cassandra.metrics.TableMetricBookkeepingTest
    exec "$project_root/.build/sh/ai-ci-test" org.apache.cassandra.metrics.TableMetricBookkeepingLifecycleTest
fi
if [[ $# == 1 && "$1" == --harness ]]; then
    exec "$project_root/run_property_tests.sh"
fi
if [[ $# == 1 && "$1" == --long ]]; then
    export PROFILE_MAIN_CLASS=org.junit.runner.JUnitCore
    exec "$project_root/.build/sh/ai-profile-many-tables" \
        org.apache.cassandra.distributed.test.MemtableResidencyConfigTest \
        org.apache.cassandra.distributed.test.MemtableResidencyHarnessTest
fi
if [[ $# == 1 && "$1" == --lazy ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy"
fi
if [[ $# == 1 && ( "$1" == --histograms || "$1" == --meters || "$1" == --reservoirs ) ]]; then
    exec "$project_root/.build/sh/ai-test-memtable-lazy" "$1"
fi
echo 'Usage: run_tests.sh [--idle-flush|--harness|--long|--lazy|--histograms|--meters|--reservoirs|--metrics-ref|--metric-profiles|--jmx-history|--otel-storage|--histogram-widths|--jmx-names|--jmx-query|--jmx-registration|--metric-ids|--metric-bookkeeping]' >&2
exit 2
