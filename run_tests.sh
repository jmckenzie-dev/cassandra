#!/usr/bin/env bash

set -o pipefail

mkdir -p logs
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log="logs/run_tests_${timestamp}.log"

run_logged()
{
    printf 'Running: %s\n' "$*" | tee -a "$log"
    "$@" 2>&1 | tee -a "$log"
    local result="${PIPESTATUS[0]}"
    printf 'Exit status: %s\n' "$result" | tee -a "$log"
    return "$result"
}

run_test()
{
    run_logged .build/run-tests.sh -s -a test -t "^${1//./\/}\.java$"
}

if [[ "$1" == "--build" ]]; then
    shift
    run_logged .build/check-code.sh -s "$@"
    status="$?"
else
    if [[ "${2:-}" == "--reuse" && "$#" -eq 2 ]]; then
        set -- "$1"
    else
        if [[ "$#" -gt 1 || "${1:-}" == -* ]]; then
            printf 'Usage: %s [--build | fully.qualified.TestClass [--reuse]]\n' "$0" | tee -a "$log"
            exit 1
        fi
        run_logged .build/build-jars.sh -s --clean
        status="$?"
        if [[ "$status" -ne 0 ]]; then
            exit "$status"
        fi
    fi
    if [[ "$#" -gt 0 ]]; then
        run_test "$1"
        status="$?"
    else
        tests=(
            org.apache.cassandra.config.DatabaseDescriptorTest
            org.apache.cassandra.service.StorageServiceTest
            org.apache.cassandra.db.virtual.SettingsTableTest
            org.apache.cassandra.db.partitions.PurgeFunctionTest
            org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManagerTest
            org.apache.cassandra.db.ReadCommandTest
            org.apache.cassandra.db.compaction.CompactionsTest
            org.apache.cassandra.tools.nodetool.CompactTest
        )
        status=0
        for test in "${tests[@]}"; do
            run_test "$test"
            status="$?"
            if [[ "$status" -ne 0 ]]; then
                break
            fi
        done
    fi
fi

exit "$status"
