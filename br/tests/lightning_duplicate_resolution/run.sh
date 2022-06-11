#!/bin/bash
#
# Copyright 2021 PingCAP, Inc.
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

check_cluster_version 5 2 0 'duplicate detection' || exit 0

run_lightning

# Ensure all tables are consistent.
run_sql 'admin check table dup_resolve.a'
run_sql 'admin check table dup_resolve.b'
run_sql 'admin check table dup_resolve.c'

## Table "a" has a clustered integer key and generated column.

# only one row remains (2, 1, 4.csv). all others are duplicates 🤷
run_sql 'select count(*) from dup_resolve.a'
check_contains 'count(*): 1'

run_sql 'select * from dup_resolve.a'
check_contains 'a: 2'
check_contains 'b: 1'
check_contains 'c: 4.csv'
check_contains 'd: 3'

## Table "b" has a nonclustered integer key and nullable unique column.
run_sql 'select count(*) from dup_resolve.b'
check_contains 'count(*): 3'

run_sql 'select * from dup_resolve.b where a = 2'
check_contains 'b: 1'
check_contains 'c: 4.csv'
run_sql 'select * from dup_resolve.b where a = 7'
check_contains 'b: NULL'
check_contains 'c: 8.csv#2'
run_sql 'select * from dup_resolve.b where a = 8'
check_contains 'b: NULL'
check_contains 'c: 8.csv#3'

## Table "c" has a clustered non-integer key.
run_sql 'select count(*) from dup_resolve.c'
check_contains 'count(*): 4'

run_sql 'select c from dup_resolve.c where a = 2 and b = 1'
check_contains 'c: 1.csv#1'
run_sql 'select c from dup_resolve.c where a = 7 and b = 0'
check_contains 'c: 1.csv#4'
run_sql 'select c from dup_resolve.c where a = 7 and b = 1'
check_contains 'c: 1.csv#7'
run_sql 'select c from dup_resolve.c where a = 9 and b = 0'
check_contains 'c: 1.csv#8'
