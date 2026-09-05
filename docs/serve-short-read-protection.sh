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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -o pipefail

SCRIPT_DIRECTORY=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd -- "${SCRIPT_DIRECTORY}/.." && pwd)
LOG_DIRECTORY="${PROJECT_ROOT}/logs"
TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
LOG_FILE="${LOG_DIRECTORY}/serve-short-read-protection-shell-${TIMESTAMP}.log"
UV_BIN=${UV_BIN:-uv}

mkdir -p "${LOG_DIRECTORY}"

{
    printf 'Launcher output is also being written to %s\n' "${LOG_FILE}"
    printf 'Starting Short Read Protection presentation with %s\n' "${UV_BIN}"
    "${UV_BIN}" run --no-project python "${SCRIPT_DIRECTORY}/serve_short_read_protection.py" "$@"
} 2>&1 | tee "${LOG_FILE}"

status=${PIPESTATUS[0]}
exit "${status}"
