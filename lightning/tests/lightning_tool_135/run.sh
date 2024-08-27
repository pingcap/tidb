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

# This test verifies if TOOL-135 is fixed.

set -eu

run_sql 'DROP DATABASE IF EXISTS tool_135;'
run_lightning
echo 'Import finished'

run_sql 'SELECT count(a), sum(a), min(a), max(a) FROM tool_135.bar1;'
check_contains 'count(a): 1000'
check_contains 'sum(a): 601500'
check_contains 'min(a): 102'
check_contains 'max(a): 1101'
run_sql 'INSERT INTO tool_135.bar1 () VALUES ();'
run_sql 'SELECT count(a), min(a), max(a) > 1101 FROM tool_135.bar1;'
check_contains 'count(a): 1001'
check_contains 'min(a): 102'
check_contains 'max(a) > 1101: 1'

run_sql 'SELECT count(a), sum(a), min(a), max(a) FROM tool_135.bar2;'
check_contains 'count(a): 1000'
check_contains 'sum(a): 548500'
check_contains 'min(a): 49'
check_contains 'max(a): 1048'
run_sql 'INSERT INTO tool_135.bar2 () VALUES ();'
run_sql 'SELECT count(a), min(a), max(a) > 1048 FROM tool_135.bar2;'
check_contains 'count(a): 1001'
check_contains 'min(a): 49'
check_contains 'max(a) > 1048: 1'

run_sql 'SELECT count(a), sum(a), min(a), max(a), count(b), sum(b), min(b), max(b) FROM tool_135.bar3;'
check_contains 'count(a): 1000'
check_contains 'sum(a): 532218793'
check_contains 'min(a): 1071'
check_contains 'max(a): 1048054'
check_contains 'count(b): 1000'
check_contains 'sum(b): 645500'
check_contains 'min(b): 146'
check_contains 'max(b): 1145'
run_sql 'INSERT INTO tool_135.bar3 (a) VALUES (229267);'
run_sql 'SELECT count(a), sum(a), min(a), max(a), count(b), min(b), max(b) > 1145 FROM tool_135.bar3;'
check_contains 'count(a): 1001'
check_contains 'sum(a): 532448060'
check_contains 'min(a): 1071'
check_contains 'max(a): 1048054'
check_contains 'count(b): 1001'
check_contains 'min(b): 146'
check_contains 'max(b) > 1145: 1'

run_sql 'SELECT count(a), sum(a), min(a), max(a) FROM tool_135.bar4;'
check_contains 'count(a): 1000'
check_contains 'sum(a): 588500'
check_contains 'min(a): 89'
check_contains 'max(a): 1088'
run_sql 'INSERT INTO tool_135.bar4 () VALUES ();'
run_sql 'SELECT count(a), min(a), max(a) > 1088 FROM tool_135.bar4;'
check_contains 'count(a): 1001'
check_contains 'min(a): 89'
check_contains 'max(a) > 1088: 1'

run_sql 'SELECT count(a), sum(a), min(a), max(a), count(b), sum(b), min(b), max(b) FROM tool_135.bar5;'
check_contains 'count(a): 1000'
check_contains 'sum(a): 534846115'
check_contains 'min(a): 970'
check_contains 'max(a): 1045357'
check_contains 'count(b): 1000'
check_contains 'sum(b): 563500'
check_contains 'min(b): 64'
check_contains 'max(b): 1063'
run_sql 'INSERT INTO tool_135.bar5 (a) VALUES (668233);'
run_sql 'SELECT count(a), sum(a), min(a), max(a), count(b), min(b), max(b) > 1063 FROM tool_135.bar5;'
check_contains 'count(a): 1001'
check_contains 'sum(a): 535514348'
check_contains 'min(a): 970'
check_contains 'max(a): 1045357'
check_contains 'count(b): 1001'
check_contains 'min(b): 64'
check_contains 'max(b) > 1063: 1'

