#!/bin/sh
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

# Basic check for whether routing rules work

set -eux

run_sql 'DROP DATABASE IF EXISTS test1;'
run_sql 'DROP DATABASE IF EXISTS test;'

run_sql 'CREATE DATABASE test1;'
run_sql 'CREATE DATABASE test;'
run_sql 'CREATE TABLE test1.dump_test (x real primary key);'
run_sql 'CREATE TABLE test.u (x real primary key);'

run_lightning

run_sql 'SELECT sum(x) FROM test.u;'
check_contains 'sum(x): 43'
