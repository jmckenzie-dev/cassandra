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
set -e

if [[ "${1:-}" == --long ]]; then
    shift
    bash run_tests.sh test org.apache.cassandra.service.pager.PagingFuzzTest "$@"
elif [[ "${1:-}" == --mixed-version ]]; then
    shift
    bash run_tests.sh test org.apache.cassandra.distributed.upgrade.storage.paging.MixedVersionPagingAcrossTombstonesFuzzTest "$@"
else
    bash run_tests.sh test org.apache.cassandra.service.pager.RandomizedPagingStateTest "$@"
    bash run_tests.sh test org.apache.cassandra.db.ReadCommandTest "$@"
    bash run_tests.sh test org.apache.cassandra.db.TombstonePageContinuationTest "$@"
    bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest "$@"
fi
