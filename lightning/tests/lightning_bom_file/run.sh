#!/bin/sh
#
# Copyright 2023 PingCAP, Inc.
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

set -eux

mydir=$(dirname "${BASH_SOURCE[0]}")

original_schema_file="${mydir}/original_data/mytest.testtbl-schema.sql"
original_data_file="${mydir}/original_data/mytest.testtbl.csv"
mkdir -p "$TEST_DIR/data"
schema_file="$TEST_DIR/data/mytest.testtbl-schema.sql"
data_file="$TEST_DIR/data/mytest.testtbl.csv"

# add the BOM header
printf '\xEF\xBB\xBF' | cat - <( sed '1s/^\xEF\xBB\xBF//' "${original_schema_file}" ) > "${schema_file}"
printf '\xEF\xBB\xBF' | cat - <( sed '1s/^\xEF\xBB\xBF//' "${original_data_file}" ) > "${data_file}"

# verify the BOM header
if ! grep -q $'^\xEF\xBB\xBF' "${schema_file}"; then
    echo "schema file doesn't contain the BOM header" >&2
    exit 1
fi

if ! grep -q $'^\xEF\xBB\xBF' "${data_file}"; then
    echo "data file doesn't contain the BOM header" >&2
    exit 1
fi

row_count=$( sed '1d' "${data_file}" | wc -l | xargs echo )

run_lightning --backend tidb -d "$TEST_DIR/data"

# Check that everything is correctly imported
run_sql 'SELECT count(*) FROM mytest.testtbl'
check_contains "count(*): ${row_count}"
# Check session variables take effect
run_sql 'show create table mytest.testtbl'
check_contains "NONCLUSTERED"

check_cluster_version 4 0 0 'local backend' || exit 0
run_sql "DROP TABLE mytest.testtbl"

run_lightning --backend local -d "$TEST_DIR/data"

# Check that everything is correctly imported
run_sql 'SELECT count(*) FROM mytest.testtbl'
check_contains "count(*): ${row_count}"
