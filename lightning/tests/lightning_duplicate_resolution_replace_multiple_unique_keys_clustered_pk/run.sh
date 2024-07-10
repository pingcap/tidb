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

run_sql 'select count(*) from dup_resolve.a'
check_contains 'count(*): 3'

run_sql 'select * from dup_resolve.a'
check_contains 'a: 1'
check_contains 'b: 1'
check_contains 'c: 1'
check_contains 'd: 1.csv'
check_contains 'a: 2'
check_contains 'b: 2'
check_contains 'c: 3'
check_contains 'd: 3.csv'
check_contains 'a: 4'
check_contains 'b: 4'
check_contains 'c: 4'
check_contains 'd: 4.csv'
