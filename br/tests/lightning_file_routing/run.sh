#!/bin/bash
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

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/fr.mydump"

mkdir -p $DBPATH $DBPATH/fr $DBPATH/ff
echo 'CREATE DATABASE fr;' > "$DBPATH/fr/schema.sql"
echo "CREATE TABLE tbl(i TINYINT PRIMARY KEY, j INT);" > "$DBPATH/fr/tbl-table.sql"
# the column orders in data file is different from table schema order.
echo "INSERT INTO tbl (i, j) VALUES (1, 1),(2, 2);" > "$DBPATH/fr/tbl1.sql.0"
echo "INSERT INTO tbl (i, j) VALUES (3, 3),(4, 4);" > "$DBPATH/fr/tbl2.sql.0"
echo "INSERT INTO tbl (i, j) VALUES (5, 5);" > "$DBPATH/fr/tbl.sql"
echo "INSERT INTO tbl (i, j) VALUES (6, 6), (7, 7), (8, 8), (9, 9);" > "$DBPATH/tbl1.sql.1"
echo "INSERT INTO tbl (i, j) VALUES (10, 10);" > "$DBPATH/ff/test.SQL"
echo "INSERT INTO tbl (i, j) VALUES (11, 11);" > "$DBPATH/fr/tbl-noused.sql"

# view schema
echo "CREATE TABLE v(i TINYINT);" > "$DBPATH/fr/v-table.sql"
cat > "$DBPATH/fr/v-view.sql" << '_EOF_'
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS `v`;
DROP VIEW IF EXISTS `v`;
SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
SET character_set_client = utf8;
SET character_set_results = utf8;
SET collation_connection = utf8_general_ci;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`192.168.198.178` SQL SECURITY DEFINER VIEW `v` (`i`) AS SELECT `i` FROM `fr`.`tbl` WHERE i <= 5;
SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
SET collation_connection = @PREV_COLLATION_CONNECTION;
_EOF_

 check_cluster_version 4 0 0 'local backend'

run_sql 'DROP DATABASE IF EXISTS fr'

# Start importing the tables.
run_lightning -d "$DBPATH" 2> /dev/null

run_sql 'SELECT count(*) FROM `fr`.tbl'
check_contains "count(*): 10"
run_sql 'SELECT sum(j) FROM `fr`.tbl'
check_contains "sum(j): 55"

run_sql 'SELECT sum(i), count(*) FROM `fr`.v'
check_contains "sum(i): 15"
check_contains "count(*): 5"
