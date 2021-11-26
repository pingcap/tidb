#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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
DB="$TEST_NAME"
TABLE="usertable"

run_sql "CREATE DATABASE $DB;"

table_names=${cases:-'t0 t1 t2 t_bit t_bool t_tinyint t_smallint t_mediumint t_int t_date t_time t_datetime t_timestamp t_year t_char t_varcher t_text t_binary t_blob t_enum t_set t8 t9 t10 t11 t12'}

run_sql "
USE $DB;

CREATE TABLE t0 (
    id VARCHAR(255),
    data INT,
    PRIMARY KEY(id) CLUSTERED
);
INSERT INTO t0 VALUES ('1', 1);
INSERT INTO t0 VALUES ('2', 2);
INSERT INTO t0 VALUES ('3', 3);
INSERT INTO t0 VALUES ('4', 4);
INSERT INTO t0 VALUES ('5', 5);

CREATE TABLE t1 (
    id VARCHAR(255),
    a INT,
    b CHAR(10),
    PRIMARY KEY(id, b) CLUSTERED,
    UNIQUE KEY(b),
    KEY(a)
);
INSERT INTO t1 VALUES ('111', 111, '111');
INSERT INTO t1 VALUES ('222', 222, '222');
INSERT INTO t1 VALUES ('333', 333, '333');
INSERT INTO t1 VALUES ('444', 444, '444');
INSERT INTO t1 VALUES ('555', 555, '555');

CREATE TABLE t2 (
    id VARCHAR(255),
    a INT,
    b DECIMAL(5,2),
    PRIMARY KEY(id, a) CLUSTERED,
    KEY(id, a),
    UNIQUE KEY(id, a)
);
INSERT INTO t2 VALUES ('aaaa', 1111, 11.0);
INSERT INTO t2 VALUES ('bbbb', 1111, 12.0);
INSERT INTO t2 VALUES ('cccc', 1111, 13.0);
INSERT INTO t2 VALUES ('dddd', 1111, 14.0);
INSERT INTO t2 VALUES ('eeee', 1111, 15.0);

create table t_bit(a bit primary key CLUSTERED, b int);
INSERT INTO t_bit VALUES(1,2);
INSERT INTO t_bit VALUES(0,3);

create table t_bool(a bool primary key CLUSTERED, b int);
INSERT INTO t_bool VALUES(true,2);
INSERT INTO t_bool VALUES(false,3);

create table t_tinyint(a tinyint primary key CLUSTERED, b int);
INSERT INTO t_tinyint VALUES(6,2);
INSERT INTO t_tinyint VALUES(8,3);

create table t_smallint(a smallint primary key CLUSTERED, b int);
INSERT INTO t_smallint VALUES(432,2);
INSERT INTO t_smallint VALUES(125,3);

create table t_mediumint(a mediumint primary key CLUSTERED, b int);
INSERT INTO t_mediumint VALUES(8567,2);
INSERT INTO t_mediumint VALUES(12341,3);

create table t_int(a int primary key CLUSTERED, b int);
INSERT INTO t_int VALUES(123563,2);
INSERT INTO t_int VALUES(6784356,3);

create table t_date(a date primary key CLUSTERED, b int);
INSERT INTO t_date VALUES ('2020-02-20', 1);
INSERT INTO t_date VALUES ('2020-02-21', 2);
INSERT INTO t_date VALUES ('2020-02-22', 3);

create table t_time(a time primary key CLUSTERED, b int);

INSERT INTO t_time VALUES ('11:22:33', 1);
INSERT INTO t_time VALUES ('11:33:22', 2);
INSERT INTO t_time VALUES ('11:43:11', 3);

create table t_datetime(a datetime primary key CLUSTERED, b int);
INSERT INTO t_datetime VALUES ('2020-02-20 11:22:33', 1);
INSERT INTO t_datetime VALUES ('2020-02-21 11:33:22', 2);
INSERT INTO t_datetime VALUES ('2020-02-22 11:43:11', 3);

create table t_timestamp(a timestamp primary key CLUSTERED, b int);
INSERT INTO t_timestamp VALUES ('2020-02-20 11:22:33', 1);
INSERT INTO t_timestamp VALUES ('2020-02-21 11:33:22', 2);
INSERT INTO t_timestamp VALUES ('2020-02-22 11:43:11', 3);

create table t_year(a year primary key CLUSTERED, b int);
INSERT INTO t_year VALUES ('2020', 1);
INSERT INTO t_year VALUES ('2021', 2);
INSERT INTO t_year VALUES ('2022', 3);

create table t_char(a char(20) primary key CLUSTERED, b int);
INSERT INTO t_char VALUES ('abcc', 1);
INSERT INTO t_char VALUES ('sdff', 2);

create table t_varcher(a varchar(255) primary key CLUSTERED, b int);
INSERT INTO t_varcher VALUES ('abcc', 1);
INSERT INTO t_varcher VALUES ('sdff', 2);

create table t_text (a text, b int, primary key(a(5)) CLUSTERED);
INSERT INTO t_text VALUES ('abcc', 1);
INSERT INTO t_text VALUES ('sdff', 2);

create table t_binary(a binary(20) primary key CLUSTERED, b int);
INSERT INTO t_binary VALUES (x'89504E470D0A1A0A',1),(x'89504E470D0A1A0B',2),(x'89504E470D0A1A0C',3);

create table t_blob(a blob, b int, primary key (a(20)) CLUSTERED);
INSERT INTO t_blob VALUES (x'89504E470D0A1A0A',1),(x'89504E470D0A1A0B',2),(x'89504E470D0A1A0C',3);

create table t_enum(e enum('a', 'b', 'c') primary key CLUSTERED, b int);
INSERT INTO t_enum VALUES ('a',1),('b',2),('c',3);

create table t_set(s set('a', 'b', 'c') primary key CLUSTERED, b int);
INSERT INTO t_set VALUES ('a',1),('b,c',2),('a,c',3);


create table t8(a int, b varchar(255) as (concat(a, 'test')) stored, primary key(b) CLUSTERED);
INSERT INTO t8(a) VALUES (2020);
INSERT INTO t8(a) VALUES (2021);
INSERT INTO t8(a) VALUES (2022);

create table t9(a int, b varchar(255), c int, primary key(a ,b) CLUSTERED);
insert into t9 values(1, 'aaa', 1),(2, 'bbb', 2),(3, 'ccc', 3);

create table t10(a int, b int, c int, primary key(a, b) CLUSTERED);
insert into t10 values(1, 1, 1),(2, 2, 2),(3, 3, 3);

create table t11(a int, b float, c int, primary key(a,b) CLUSTERED);
insert into t11 values(1, 1.1, 1),(2, 2.2, 2),(3, 3.3, 3);

create table t12(name char(255) primary key CLUSTERED, b int, c int, index idx(name), unique index uidx(name));
insert into t12 values('aaaa', 1, 1), ('bbb', 2, 2), ('ccc', 3, 3);
"

clustered_table_count=$(run_sql "\
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES \
    WHERE tidb_pk_type = 'CLUSTERED' AND table_schema = '$DB';" \
    | awk '/COUNT/{print $2}')

[ $clustered_table_count -gt 0 ] || { echo No clustered index table; exit 1; }

# backup table
echo "backup start..."
run_br --pd $PD_ADDR backup db -s "local://$TEST_DIR/$DB" --db $DB

# count
echo "count rows..."
row_counts=()
for table_name in $table_names; do
    row_counts+=($(run_sql "SELECT COUNT(*) FROM $DB.$table_name;" | awk '/COUNT/{print $2}'))
done

run_sql "DROP DATABASE $DB;"
run_sql "CREATE DATABASE $DB;"

# restore table
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# check count
echo "check count..."
idx=0
for table_name in $table_names; do
    row_count=$(run_sql "SELECT COUNT(*) FROM $DB.$table_name;" | awk '/COUNT/{print $2}')
    if [[ $row_count -ne ${row_counts[$idx]} ]]; then
        echo "Lost some rows in table $table_name. Expect ${row_counts[$idx]}; Get $row_count."
        exit 1
    fi
    idx=$(( $idx + 1 ))
done

run_sql "DROP DATABASE $DB;"
