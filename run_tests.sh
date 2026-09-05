#!/usr/bin/env bash

set -o pipefail

mkdir -p logs

case "${1:-}" in
    test)
        shift
        command=(.build/sh/ai-ci-test "$@")
        ;;
    build)
        shift
        command=(.build/sh/ai-build "$@")
        ;;
    *)
        echo "Usage: $0 {test <fully-qualified-class>|build}" >&2
        exit 2
        ;;
esac

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
log="logs/run_tests_${timestamp}.log"

printf 'Running:'
printf ' %q' "${command[@]}"
printf '\nLog: %s\n' "$log"

"${command[@]}" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
exit "$status"
