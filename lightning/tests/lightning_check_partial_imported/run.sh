#!/bin/bash
#
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MYDIR=$(dirname "${BASH_SOURCE[0]}")
set -eux

check_cluster_version 4 0 0 'local backend' || exit 0

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailBeforeStartImportingIndexEngine=return"
set +e
if run_lightning; then
    echo "The first import doesn't fail as expected" >&2
    exit 1
fi
set -e

data_records=$(tail -n +2 "${MYDIR}/data/db01.tbl01.csv" | wc -l | xargs echo )
run_sql "SELECT COUNT(*) FROM db01.tbl01 USE INDEX();"
check_contains "${data_records}"

export GO_FAILPOINTS=""
set +e
if run_lightning --check-requirements=1; then
    echo "The pre-check doesn't find out the non-empty table problem"
    exit 2
fi
set -e

run_sql "TRUNCATE TABLE db01.tbl01;"
run_lightning --check-requirements=1
run_sql "SELECT COUNT(*) FROM db01.tbl01;"
check_contains "${data_records}"
run_sql "SELECT COUNT(*) FROM db01.tbl01 USE INDEX();"
check_contains "${data_records}"
