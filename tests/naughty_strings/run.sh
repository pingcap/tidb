#!/bin/sh

set -eu

run_sql "DROP DATABASE IF EXISTS naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings-schema-create.sql"
export DUMPLING_TEST_DATABASE="naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t.sql"
run_dumpling
# FIXME should compare the schemas too, but they differ too much among MySQL versions.
diff "$DUMPLING_BASE_NAME/data/naughty_strings.t.sql" "$DUMPLING_OUTPUT_DIR/naughty_strings.t.sql"
