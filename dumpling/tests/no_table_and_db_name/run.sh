#!/bin/sh

set -euv

assert() {
    if ! $@; then
        echo "assert [$@] failed, exiting."
        exit 1
    fi
}

TEST_NAME=no_table_and_db_name
run_sql "drop database if exists $TEST_NAME"
run_sql "create database $TEST_NAME"
export DUMPLING_TEST_DATABASE=no_table_and_db_name
run_sql "create table t (a varchar(255))"

chars_20="1111_0000_1111_0000_"

# insert 100 records, each occupies 20 bytes
run_sql "insert into t values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('$chars_20')/g");"

# dumping with file size = 233 bytes, actually 10 rows
run_dumpling -F 233B --filetype csv --sql "select * from $TEST_NAME.t"

assert [ $( ls -lh $DUMPLING_OUTPUT_DIR | grep -e ".csv$" | wc -l ) -eq 10 ]

# 10 files with header.
assert [ $( cat $DUMPLING_OUTPUT_DIR/*.csv | wc -l ) -eq $(( 100 + 10 )) ]

# dumping with file size = 311 bytes, actually 10 rows
run_dumpling -F 311B --filetype sql --sql "select * from $TEST_NAME.t"

assert [ $( ls -lh $DUMPLING_OUTPUT_DIR | grep -e ".sql$" | wc -l ) -eq 10 ]

# 10 files with header.
assert [ $( cat $DUMPLING_OUTPUT_DIR/*.sql | wc -l ) -eq $(( 100 + 10 * 2 )) ]

echo "TEST: [$TEST_NAME] passed."