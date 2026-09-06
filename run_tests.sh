#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -o pipefail

mkdir -p logs tmp
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
coverage=false

case "${1:-}" in
    test|coverage)
        [[ "$1" == coverage ]] && coverage=true
        shift
        test_class="${1:-}"
        shift || exit 2
        test_path="${test_class//.//}.java"
        test_type=""
        for candidate in unit distributed long; do
            if [[ -f "test/$candidate/$test_path" ]]; then
                test_type="$candidate"
                break
            fi
        done
        if [[ -z "$test_type" ]]; then
            echo "Cannot find test class: $test_class" >&2
            exit 2
        fi
        test_list="tmp/paging-tests-${timestamp}-$$.txt"
        printf '%s\n' "$test_path" > "$test_list"
        command=(ant testclasslist "-Dtest.classlistprefix=$test_type"
                 "-Dtest.classlistfile=$test_list" "-Dtmp.dir=$PWD/tmp" "$@")
        if $coverage; then
            coverage_dir="$PWD/build/paging-coverage/${timestamp}-$$"
            command+=("-Dusejacoco=yes" "-Djacoco.export.dir=$coverage_dir"
                      "-Dbuild.test.output.dir=$coverage_dir/tests")
        fi
        ;;
    build)
        shift
        command=(.build/build-jars.sh "$@")
        ;;
    *)
        echo "Usage: $0 {test|coverage} <fully-qualified-class> [Ant properties] | build" >&2
        exit 2
        ;;
esac

log="logs/run_tests_${timestamp}-$$.log"

run() {
    printf 'Running:'
    printf ' %q' "${command[@]}"
    printf '\nLog: %s\n' "$log"
    "${command[@]}"
}
run 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
if $coverage; then
    ant jacoco-report "-Djacoco.export.dir=$coverage_dir" 2>&1 | tee -a "$log"
    report_status=${PIPESTATUS[0]}
    if [[ "$status" -eq 0 ]]; then
        status="$report_status"
    fi
    printf 'Coverage: %s/index.html\n' "$coverage_dir" | tee -a "$log"
fi
exit "$status"
