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



# --include-generated-columns=stored only changes data files, not schema files.
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
error_log="${DUMPLING_TEST_DIR}/include_stored_generate.log"

echo "Test dumping csv with the default --include-generated-columns."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv
actual=$(tr -d '\r' < "$data_file")
expected=$(printf '"id","a"\n1,10\n2,20')
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]
cp "$schema_file" "$schema_backup"

echo "Test dumping csv with --include-generated-columns=stored."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv --include-generated-columns=stored
actual=$(tr -d '\r' < "$data_file")
expected=$(printf '"id","a","s"\n1,10,20\n2,20,40')
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]
if ! cmp -s "$schema_backup" "$schema_file"; then
	echo "schema file changed with --include-generated-columns=stored"
	diff "$schema_backup" "$schema_file" || true
	exit 1
fi

expect_dumpling_error() {
	expected_error="$1"
	shift
	rm -rf "$DUMPLING_OUTPUT_DIR"
	if run_dumpling "$@" > "$error_log" 2>&1; then
		echo "dumpling should fail with: $expected_error"
		exit 1
	fi
	grep -qF -- "$expected_error" "$error_log"
}

expected=$(printf '"id","a"\n2,20')
echo "Test --where with --include-generated-columns omitted."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv --where "a > 10"
actual=$(tr -d '\r' < "$data_file")
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]

echo "Test --where with an explicit --include-generated-columns=none."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv --where "a > 10" --include-generated-columns=none
actual=$(tr -d '\r' < "$data_file")
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]

echo "Test unsupported --include-generated-columns combinations."
expect_dumpling_error "only supported with --filetype csv or parquet" --filetype sql --include-generated-columns=stored
expect_dumpling_error "and --no-data at the same time" --filetype csv --no-data --include-generated-columns=stored
expect_dumpling_error "and --where at the same time" --filetype csv --where "a > 0" --include-generated-columns=stored
expect_dumpling_error "and --sql at the same time" --sql "select * from $STORED_DB_NAME.$TABLE_NAME" --include-generated-columns=stored
expect_dumpling_error "with --column-filter or --column-filter-file" --filetype csv -m --include-generated-columns=stored \
	--column-filter "{ matcher = [\"$STORED_DB_NAME.$TABLE_NAME\"], columns = [\"*\"] }"
expect_dumpling_error "--include-generated-columns=virtual is not supported yet" --filetype csv --include-generated-columns=virtual

run_sql "drop database if exists $STORED_DB_NAME;"
