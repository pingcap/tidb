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

# This test verifies if TOOL-1420 is fixed.
# It involves pre-calculated auto-inc overflowing the tinyint range.

set -eu

run_sql 'drop database if exists EE1472;'
run_lightning

run_sql 'insert into EE1472.pk values ();'
run_sql 'select count(a), max(a) from EE1472.pk;'
check_contains 'count(a): 3'
check_contains 'max(a): 5'

run_sql 'insert into EE1472.notpk (a) values (3333);'
run_sql 'select b from EE1472.notpk where a = 3333;'
check_contains 'b: 10'
