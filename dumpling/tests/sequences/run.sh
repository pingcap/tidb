#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

export DUMPLING_TEST_PORT=4000

run_sql "drop database if exists sequences;"
run_sql "create database sequences DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
export DUMPLING_TEST_DATABASE="sequences"

run_sql "create sequence s start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB;"
run_sql "select nextval(s);"

# test --no-sequences
run_dumpling --no-sequences
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s-schema-sequence.sql"

rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --no-sequences=false
diff "$DUMPLING_BASE_NAME/data/sequences.s-schema-sequence-expect.sql" "$DUMPLING_OUTPUT_DIR/sequences.s-schema-sequence.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s.000000000.sql"

# test --no-schemas
rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --no-schemas
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences-schema-create.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s-schema-sequence.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s.000000000.sql"
