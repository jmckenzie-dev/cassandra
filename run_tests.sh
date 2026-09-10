#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o pipefail

cd -- "$(dirname -- "$0")" || exit
mkdir -p logs
timestamp="$(date -u +%Y%m%dT%H%M%S%NZ)"
log="logs/run_tests_${timestamp}.log"
results="logs/run_tests_${timestamp}"

run_logged()
{
    printf 'Running: %s\n' "$*"
    "$@"
    local result="$?"
    printf 'Exit status: %s\n' "$result"
    return "$result"
}

run_tests()
{
    local target="$1"
    run_logged .build/run-tests.sh -s -a "$target" -t "$2"
    local result="$?"
    mkdir -p "$results/$target" || return
    if [[ -f build/run-tests.log ]]; then
        cp build/run-tests.log "$results/$target/" || return
    fi
    if [[ -d build/test/output ]]; then
        cp -a build/test/output "$results/$target/" || return
    fi
    return "$result"
}

usage()
{
    cat <<'EOF'
Usage: bash run_tests.sh [--compaction] [--long] [--reuse]
       bash run_tests.sh fully.qualified.TestClass [--reuse]
       bash run_tests.sh --build [Ant arguments]

Default: changed branch unit classes and the nodetool CompactTest smoke test.
--compaction  Also run all unit tests in the compaction package and subpackages.
--long        Also run TombstoneCompactionShutdownTest (three integration tests).
--reuse       Skip compilation; use only after building the current sources/tests.
--build       Run the full build and code checks without executing tests.

Select a supported JDK before running. Logs and JUnit XML are saved under logs/.
EOF
}

main()
{
    if [[ "${1:-}" == "--build" ]]; then
        shift
        run_logged .build/check-code.sh -s "$@"
        return $?
    fi

    local reuse=false compaction=false long=false test_class=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --reuse) reuse=true ;;
            --compaction) compaction=true ;;
            --long) long=true ;;
            -h|--help) usage; return 0 ;;
            -*) usage; return 2 ;;
            *)
                if [[ -n "$test_class" || ! "$1" =~ ^[a-zA-Z_][a-zA-Z_0-9]*(\.[a-zA-Z_][a-zA-Z_0-9]*)+$ ]]; then
                    usage
                    return 2
                fi
                test_class="$1"
                ;;
        esac
        shift
    done
    if [[ -n "$test_class" ]] && { "$compaction" || "$long"; }; then
        usage
        return 2
    fi

    local pattern="^${test_class//./\/}\.java$"
    if [[ -z "$test_class" ]]; then
        local tests=(
            org.apache.cassandra.config.DatabaseDescriptorTest
            org.apache.cassandra.service.StorageServiceTest
            org.apache.cassandra.db.virtual.SettingsTableTest
            org.apache.cassandra.db.partitions.PurgeFunctionTest
            org.apache.cassandra.db.compaction.TombstoneTriggeredCompactionManagerTest
            org.apache.cassandra.db.compaction.PendingRepairManagerTest
            org.apache.cassandra.db.ReadCommandTest
            org.apache.cassandra.db.compaction.CompactionsTest
            org.apache.cassandra.tools.nodetool.CompactTest
        )
        pattern=""
        local test
        for test in "${tests[@]}"; do
            if "$compaction" && [[ "$test" == org.apache.cassandra.db.compaction.* ]]; then
                continue
            fi
            pattern+="${pattern:+,}^${test//./\/}\.java$"
        done
        if "$compaction"; then
            pattern+=',^org/apache/cassandra/db/compaction/.*Test\.java$'
        fi
    fi

    printf 'Unit selection: %s\n' "$pattern"
    printf 'Include shutdown integration tests: %s\n' "$long"
    printf 'Results: %s\n' "$results"
    export CASSANDRA_HOME="$PWD"
    export CASSANDRA_INCLUDE="${CASSANDRA_INCLUDE:-$PWD/.build/sh/cassandra-test.in.sh}"
    if ! "$reuse"; then
        CASSANDRA_DIR="$PWD"
        DIST_DIR="$PWD/build"
        summary=false
        source .build/sh/_run-ant.sh
        run_logged run_ant build-test || return
    fi

    local status=0 result
    run_tests test "$pattern" || status=$?
    if "$long"; then
        run_tests jvm-dtest '^org/apache/cassandra/distributed/test/TombstoneCompactionShutdownTest\.java$'
        result=$?
        if [[ "$status" -eq 0 ]]; then
            status="$result"
        fi
    fi
    return "$status"
}

main "$@" 2>&1 | tee -a "$log"
exit "${PIPESTATUS[0]}"
