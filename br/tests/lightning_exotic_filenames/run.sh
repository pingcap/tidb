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

# rebuild the directory and rename the files to use exotic file names.
# (need to do it at runtime but otherwise git behaves erratically on windows)
DBPATH="$TEST_DIR/exotic_filename.mydump"
mkdir -p "$DBPATH"
cp "tests/$TEST_NAME/data/zwk-schema-create.sql" "$DBPATH/中文庫-schema-create.sql"
cp "tests/$TEST_NAME/data/zwk.zwb-schema.sql" "$DBPATH/中文庫.中文表-schema.sql"
cp "tests/$TEST_NAME/data/zwk.zwb.sql" "$DBPATH/中文庫.中文表.sql"
cp "tests/$TEST_NAME/data/xfn-schema-create.sql" "$DBPATH/"'x`f"n-schema-create.sql'
cp "tests/$TEST_NAME/data/xfn.etn-schema.sql" "$DBPATH/"'x`f"n.exotic`table``name-schema.sql'
cp "tests/$TEST_NAME/data/xfn.etn.sql" "$DBPATH/"'x`f"n.exotic`table``name.sql'

run_sql 'DROP DATABASE IF EXISTS `x``f"n`;'
run_sql 'DROP DATABASE IF EXISTS `中文庫`;'
run_lightning -d "$DBPATH"
echo 'Import finished'

run_sql 'SELECT count(*) FROM `x``f"n`.`exotic``table````name`'
check_contains 'count(*): 5'
run_sql 'INSERT INTO `x``f"n`.`exotic``table````name` (a) VALUES ("ffffff"), ("gggggg")'
run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM `x``f"n`.`exotic``table````name` WHERE a = "ffffff"'
check_contains '_tidb_rowid > 80000: 1'
check_contains 'b > 80000: 1'
run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM `x``f"n`.`exotic``table````name` WHERE a = "gggggg"'
check_contains '_tidb_rowid > 80000: 1'
check_contains 'b > 80000: 1'

run_sql 'SELECT * FROM `中文庫`.中文表'
check_contains 'a: 2345'
