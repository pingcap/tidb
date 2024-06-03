#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

EXAMPLES_PATH=${EXAMPLES_PATH:-pkg/lightning/mydump/examples}

# Because of issue JENKINS-45544 we can't use the Unicode filename in the
# examples. We are going to rename it in-place.
do_rename() {
    mv "$EXAMPLES_PATH/mocker_test.$1-schema.sql" "$EXAMPLES_PATH/mocker_test.$2-schema.sql"
    mv "$EXAMPLES_PATH/mocker_test.$1.sql" "$EXAMPLES_PATH/mocker_test.$2.sql"
}
do_rename i ı
undo_rename() {
    do_rename ı i
}
trap undo_rename EXIT

do_run_lightning() {
    run_lightning -d $EXAMPLES_PATH --config "$CUR/$1.toml"
}

# Perform the import
run_sql 'DROP DATABASE IF EXISTS mocker_test;'
do_run_lightning 512

# The existing reader_test
run_sql 'use mocker_test; select count(distinct ID) cnt from `tbl_autoid`'
check_contains 'cnt: 10000'
run_sql 'use mocker_test; select count(distinct Name) cnt from `tbl_multi_index`'
check_contains 'cnt: 10000'

# Check if rest of the imported data really match
run_sql 'SELECT * FROM mocker_test.ı LIMIT 2'
check_not_contains '* 2. row *'
check_contains 'ſ: 🤪'

run_sql 'SELECT * FROM mocker_test.report_case_high_risk LIMIT 2'
check_not_contains '* 2. row *'
check_contains 'id: 2'
check_contains 'report_data: 4'
check_contains 'caseType: 6'
check_contains 'total_case: 8'
check_contains 'today_new_case: 10'

run_sql 'select count(*), sum(id), max(name), min(name), sum(crc32(name)) from mocker_test.tbl_autoid;'
check_contains 'count(*): 10000'
check_contains 'sum(id): 50005000'
check_contains 'max(name): 4-9-9'
check_contains 'min(name): 0-0-0'
check_contains 'sum(crc32(name)): 21388950023608'

# Ensure the AUTO_INCREMENT value is properly defined
run_sql "insert into mocker_test.tbl_autoid (name) values ('new');"
run_sql "select id > 10000 from mocker_test.tbl_autoid where name = 'new';"
check_not_contains '* 2. row *'
check_contains 'id > 10000: 1'

run_sql 'select count(*), avg(age), max(name), min(name), sum(crc32(name)) from mocker_test.tbl_multi_index;'
check_contains 'count(*): 10000'
check_contains 'avg(age): 477.7500'
check_contains 'max(name): 4+9+9'
check_contains 'min(name): 0+0+0'
check_contains 'sum(crc32(name)): 21433704622808'

# Ensure the indices are intact
run_sql "select age from mocker_test.tbl_multi_index where name = '1+2+3'"
check_contains 'age: 6'
run_sql "select count(*) from mocker_test.tbl_multi_index where age = 6"
check_contains 'count(*): 20'

# Rest of the existing reader_test
run_sql 'DROP DATABASE mocker_test;'
do_run_lightning 1
run_sql 'use mocker_test; select count(distinct ID) cnt from `tbl_autoid`'
check_contains 'cnt: 10000'
run_sql 'use mocker_test; select count(distinct Name) cnt from `tbl_multi_index`'
check_contains 'cnt: 10000'

run_sql 'DROP DATABASE mocker_test;'
do_run_lightning 131072
run_sql 'use mocker_test; select count(distinct ID) cnt from `tbl_autoid`'
check_contains 'cnt: 10000'
run_sql 'use mocker_test; select count(distinct Name) cnt from `tbl_multi_index`'
check_contains 'cnt: 10000'
