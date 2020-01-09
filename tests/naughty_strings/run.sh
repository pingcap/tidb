#!/bin/sh

set -eu

run_sql "DROP DATABASE IF EXISTS naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings-schema-create.sql"
export DUMPLING_TEST_DATABASE="naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t.sql"
run_dumpling
diff "$DUMPLING_BASE_NAME/data" "$DUMPLING_OUTPUT_DIR"
