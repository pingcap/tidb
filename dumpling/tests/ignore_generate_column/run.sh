#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="ignore_generate"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql "create database $DB_NAME DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

# build data with generate column full_name
run_sql "create table $DB_NAME.$TABLE_NAME(first_name varchar(14) NOT NULL, last_name varchar(16) NOT NULL, full_name VARCHAR(30) AS (CONCAT(first_name,'-',last_name))) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME (first_name, last_name) values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('a', 'b')/g");"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml



# --include-stored-generated-columns only changes data files, not schema files.
STORED_DB_NAME="include_stored_generate"
export DUMPLING_TEST_PORT=4000
export DUMPLING_TEST_DATABASE=""
run_sql "drop database if exists $STORED_DB_NAME;"
run_sql "create database $STORED_DB_NAME;"
run_sql "create table $STORED_DB_NAME.$TABLE_NAME(id int primary key, a int, s int as (a * 2) stored, v int as (a + 1) virtual);"
run_sql "insert into $STORED_DB_NAME.$TABLE_NAME (id, a) values (1, 10), (2, 20);"
export DUMPLING_TEST_DATABASE=$STORED_DB_NAME

data_file="${DUMPLING_OUTPUT_DIR}/${STORED_DB_NAME}.${TABLE_NAME}.000000000.csv"
schema_file="${DUMPLING_OUTPUT_DIR}/${STORED_DB_NAME}.${TABLE_NAME}-schema.sql"
schema_backup="${DUMPLING_TEST_DIR}/include_stored_generate-schema.sql"

echo "Test dumping csv without --include-stored-generated-columns."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv
actual=$(tr -d '\r' < "$data_file")
expected=$(printf '"id","a"\n1,10\n2,20')
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]
cp "$schema_file" "$schema_backup"

echo "Test dumping csv with --include-stored-generated-columns."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv --include-stored-generated-columns
actual=$(tr -d '\r' < "$data_file")
expected=$(printf '"id","a","s"\n1,10,20\n2,20,40')
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]
if ! cmp -s "$schema_backup" "$schema_file"; then
	echo "schema file changed with --include-stored-generated-columns"
	diff "$schema_backup" "$schema_file" || true
	exit 1
fi

echo "Test --include-stored-generated-columns is rejected for sql output."
rm -rf "$DUMPLING_OUTPUT_DIR"
if run_dumpling --filetype sql --include-stored-generated-columns > "${DUMPLING_TEST_DIR}/include_stored_generate.log" 2>&1; then
	echo "dumpling should reject --include-stored-generated-columns with --filetype sql"
	exit 1
fi
grep -q "only supported with --filetype csv or parquet" "${DUMPLING_TEST_DIR}/include_stored_generate.log"

run_sql "drop database if exists $STORED_DB_NAME;"
