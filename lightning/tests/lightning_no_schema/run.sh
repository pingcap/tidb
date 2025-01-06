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

# keep this test for check the compatibility of noschema config.
run_sql "DROP DATABASE IF EXISTS noschema;"
run_sql "create database noschema;"
run_sql "create table noschema.invalid (x int primary key);"
! run_lightning --no-schema=1
grep -Fq 'schema not found' $TEST_DIR/lightning.log

run_sql "drop table noschema.invalid;"
run_sql "create table noschema.t (x int primary key);"
! run_lightning --no-schema=1
grep -Fq 'invalid schema statement:' $TEST_DIR/lightning.log

run_sql "create table noschema.invalid (x int primary key);"
run_lightning --no-schema=1

run_sql "SELECT sum(x) FROM noschema.t;"
check_contains 'sum(x): 120'
run_sql "SELECT sum(x) FROM noschema.invalid;"
check_contains 'sum(x): 1'
