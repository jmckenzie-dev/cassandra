#!/usr/bin/env bash

set -o pipefail

mkdir -p logs
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log="logs/run_property_tests_${timestamp}.log"

bash run_tests.sh org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManagerTest "$@" 2>&1 | tee "$log"
status="${PIPESTATUS[0]}"
exit "$status"
