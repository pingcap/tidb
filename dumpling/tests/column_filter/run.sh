#!/bin/bash
#
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

DB_NAME="column_filter"
TABLE_NAME="t"

export DUMPLING_TEST_PORT=3306
export DUMPLING_TEST_DATABASE=""

run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (id int primary key, name varchar(32), secret varchar(32));"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (1, 'alice', 'hidden1'), (2, 'bob', 'hidden2');"

export DUMPLING_TEST_DATABASE="$DB_NAME"

check_csv_output() {
	output_file="${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.csv"
	actual=$(tr -d '\r' < "$output_file")
	expected=$(printf '"id","name"\n1,"alice"\n2,"bob"')
	echo "expected ${expected}, actual ${actual}"
	[ "$actual" = "$expected" ]

	if grep -Eq "secret|hidden" "$output_file"; then
		echo "column filter output contains filtered column data"
		exit 1
	fi
}

echo "Test inline --column-filter."
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --filetype csv -m \
	--column-filter "{ matcher = [\"$DB_NAME.$TABLE_NAME\"], columns = [\"*\", \"!secret\"] }"
check_csv_output

echo "Test --column-filter-file."
rm -rf "$DUMPLING_OUTPUT_DIR"
filter_file="${DUMPLING_TEST_DIR}/column-filter.toml"
cat > "$filter_file" << EOF
[[filters]]
matcher = ["$DB_NAME.$TABLE_NAME"]
columns = ["id", "name"]
EOF
run_dumpling --filetype csv -m --column-filter-file "$filter_file"
check_csv_output
