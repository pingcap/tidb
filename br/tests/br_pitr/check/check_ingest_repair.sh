#!/bin/bash
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

set -eu
# check index schema
## check table test.pairs
run_sql "SHOW INDEX FROM test.pairs WHERE Key_name = 'i1' AND Index_type = 'HASH' AND Index_comment = 'edelw;fe?fewfe\nefwe' AND Visible = 'NO';"
check_contains "Column_name: y"
check_contains "Column_name: z"

run_sql "SHOW INDEX FROM test.pairs WHERE Key_name = 'u1' AND Index_type = 'RTREE' AND Index_comment = '' AND Visible = 'YES';"
check_contains "Column_name: x"
check_contains "Column_name: y"

run_sql "SHOW INDEX FROM test.pairs WHERE Key_name = 'i2' AND Index_type = 'BTREE' AND Index_comment = '123' AND Visible = 'YES';"
check_contains "Column_name: y"
check_contains "Expression: \`z\` + 1"

run_sql "SHOW INDEX FROM test.pairs WHERE Key_name = 'u2' AND Index_type = 'HASH' AND Index_comment = '243' AND Visible = 'YES';"
check_contains "Column_name: x"
check_contains "Expression: \`y\` + 1"

## check table test.pairs2
run_sql "SHOW INDEX FROM test.pairs2 WHERE Key_name = 'i1' AND Visible = 'YES';"
check_contains "Column_name: y"
check_contains "Column_name: z"
run_sql "SHOW INDEX FROM test.pairs2 WHERE Column_name = 'z' AND Key_name = 'i1' AND Visible = 'YES';"
check_contains "Sub_part: 10"

run_sql "SHOW INDEX FROM test.pairs2 WHERE Key_name = 'u1' AND Index_type = 'RTREE' AND Visible = 'YES';"
check_contains "Column_name: y"
check_contains "Column_name: z"
check_contains "Expression: \`y\` * 2"
run_sql "SHOW INDEX FROM test.pairs2 WHERE Column_name = 'z' AND Key_name = 'u1' AND Index_type = 'RTREE' AND Visible = 'YES';"
check_contains "Sub_part: 10"

run_sql "SHOW INDEX FROM test.pairs2 WHERE Key_name = 'PRIMARY' AND Index_type = 'HASH' AND Visible = 'YES';"
check_contains "Column_name: x"

## check table test.pairs3
run_sql "SHOW INDEX FROM test.pairs3 WHERE Key_name = 'zips2' AND Index_type = 'BTREE' AND Visible = 'YES';"
check_contains "Expression: cast(json_extract(\`custinfo\`, _utf8'$.zipcode') as unsigned array)"

## check table test.pairs4
run_sql "SHOW INDEX FROM test.pairs4 WHERE Key_name != 'PRIMARY';"
check_not_contains "1. row" ## the result should be empty

## check table test.pairs5
run_sql "SHOW INDEX FROM test.pairs5;"
check_not_contains "1. row" ## the result should be empty

## check table test.pairs7
run_sql "SHOW INDEX FROM test.pairs7 WHERE Key_name = 'zips2' AND Visible = 'YES';"
check_contains "Expression: cast(json_extract(\`cust\`\`;info\`, _utf8'$.zipcode') as unsigned array)"
run_sql "SHOW INDEX FROM test.pairs7 WHERE Key_name = 'i2' AND Seq_in_index = 1 AND Visible = 'YES';"
check_contains "Column_name: nam\`;e"
run_sql "SHOW INDEX FROM test.pairs7 WHERE Key_name = 'i2' AND Seq_in_index = 2 AND Visible = 'YES';"
check_contains "Expression: \`nam\`\`;e\` * 2"

## check table test.pairs11
run_sql "SELECT count(*) AS RESCNT FROM INFORMATION_SCHEMA.TIDB_INDEXES WHERE INDEX_ID = 1 AND TABLE_NAME = 'pairs11' AND KEY_NAME = 'u1';"
check_contains "RESCNT: 2"

# adjust some index to be visible
run_sql "ALTER TABLE test.pairs ALTER INDEX i1 VISIBLE;"

# check index data
run_sql "select count(*) AS RESCNT from test.pairs use index(i1) where y = 0 and z = 0;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs use index(u1) where x = 1 and y = 0;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs use index(i2) where y = 1 and z+1 = 1;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs use index(u2) where x = 1 and y+1 = 1;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs2 use index(i1) where y = 1 and z = '1';"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs2 use index(u1) where y = 1 and z = '1' and y*2=2;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs2 use index(PRIMARY) where x = 1;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs3 use index(zips2) where custinfo->'$.zipcode' = json_array(1,2);"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs7 use index(zips2) where \`cust\`\`;info\`->'$.zipcode' =json_array(1,2);"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs7 use index(i2) where \`nam\`\`;e\` = 1 and \`nam\`\`;e\` * 2 = 2;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs8 use index(i1) where y = '1';"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs9 use index(i1) where y2 = '1';"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs10 use index(i1) where y = 1;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs10 use index(i1) where y = 101;"
check_not_contains "RESCNT: 0"
run_sql "select count(*) AS RESCNT from test.pairs10 use index(i1) where y = 201;"
check_not_contains "RESCNT: 0"
