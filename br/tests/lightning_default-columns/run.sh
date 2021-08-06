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

set -eu

run_sql 'DROP DATABASE IF EXISTS defcol'

run_lightning --log-file "$TEST_DIR/defcol-errors.log"

run_sql 'SELECT min(pk), count(pk) FROM defcol.t'
check_contains 'min(pk): 1'
check_contains 'count(pk): 9'

run_sql 'SELECT pk FROM defcol.t WHERE x IS NOT NULL OR y <> 123 OR z IS NULL OR z NOT BETWEEN now() - INTERVAL 5 MINUTE AND now()'
check_not_contains 'pk:'

run_sql 'SELECT xx FROM defcol.u WHERE yy = 40'
check_contains 'xx: 1'

run_sql 'SELECT xx FROM defcol.u WHERE yy = 60'
check_contains 'xx: 2'

grep -q '\["column missing from data file, going to fill with default value"\].*\[colName=xx\]' "$TEST_DIR/defcol-errors.log"
