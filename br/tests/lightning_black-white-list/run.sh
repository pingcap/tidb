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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

drop_dbs() {
    run_sql 'DROP DATABASE IF EXISTS firstdb;'
    run_sql 'DROP DATABASE IF EXISTS seconddb;'
}

check_firstdb_only() {
    run_sql 'SHOW DATABASES;'
    check_contains 'Database: firstdb'
    check_not_contains 'Database: seconddb'
    run_sql 'SHOW TABLES IN firstdb;'
    check_contains 'Tables_in_firstdb: first'
    check_contains 'Tables_in_firstdb: second'
    run_sql 'SHOW TABLES IN mysql;'
    check_not_contains 'Tables_in_mysql: testtable'
}

check_even_table_only() {
    run_sql 'SHOW DATABASES;'
    check_contains 'Database: firstdb'
    check_contains 'Database: seconddb'
    run_sql 'SHOW TABLES IN firstdb;'
    check_not_contains 'Tables_in_firstdb: first'
    check_contains 'Tables_in_firstdb: second'
    run_sql 'SHOW TABLES IN seconddb;'
    check_not_contains 'Tables_in_seconddb: third'
    check_contains 'Tables_in_seconddb: fourth'
    run_sql 'SHOW TABLES IN mysql;'
    check_not_contains 'Tables_in_mysql: testtable'
}

# Check if black-white-list works.

drop_dbs
run_lightning --config "tests/$TEST_NAME/firstdb-only.toml"
check_firstdb_only

drop_dbs
run_lightning --config "tests/$TEST_NAME/even-table-only.toml"
check_even_table_only

# Check the same for table-filter

drop_dbs
run_lightning -f 'f*.*'
check_firstdb_only

drop_dbs
run_lightning -f '!firstdb.*' -f '*.second' -f 'seconddb.fourth'
check_even_table_only
