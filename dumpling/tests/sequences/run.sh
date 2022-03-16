#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu


DB_NAME="sequences"
SEQUENCE_NAME="s"

run_sql "drop database if exists \`$DB_NAME\`;"
run_sql_file "$DUMPLING_BASE_NAME/data/sequences-schema-create.sql"
export DUMPLING_TEST_DATABASE=$DB_NAME

#run_sql "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"
run_sql_file "$DUMPLING_BASE_NAME/data/sequences.s-schema-sequence.sql"

run_dumpling --no-sequences
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s-schema-sequence.sql"

rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --no-sequences=false
diff "$DUMPLING_BASE_NAME/data/sequences.s-schema-sequence.sql" "$DUMPLING_OUTPUT_DIR/sequences.s-schema-sequence.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/sequences.s.000000000.sql"
