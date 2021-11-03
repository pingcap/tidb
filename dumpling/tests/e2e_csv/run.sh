#!/bin/bash
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="e2e_csv"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

run_sql_file "$DUMPLING_BASE_NAME/data/e2e_csv-schema-create.sql"
export DUMPLING_TEST_DATABASE="e2e_csv"
run_sql_file "$DUMPLING_BASE_NAME/data/e2e_csv.escape-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/e2e_csv.escape.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/e2e_csv.t-schema.sql"

mkdir -p $DUMPLING_TEST_DIR/data
# lightning will omit empty lines without delimiters now, skip these cases
sed "s/('')/-- ('')/g" "$DUMPLING_BASE_NAME/data/e2e_csv.t.sql" | sed "s/(' ')/-- (' ')/g" > $DUMPLING_TEST_DIR/data/e2e_csv.t.sql
run_sql_file "$DUMPLING_TEST_DIR/data/e2e_csv.t.sql"

run() {
    echo "*** running subtest case ***"
    echo "escape_backslash is $escape_backslash"
    echo "csv_delimiter is $csv_delimiter"
    echo "csv_separator is $csv_separator"

    # drop database on tidb
    export DUMPLING_TEST_PORT=4000
    export DUMPLING_TEST_DATABASE=""
    run_sql "drop database if exists $DB_NAME;"

    # dumping
    export DUMPLING_TEST_PORT=3306
    export DUMPLING_TEST_DATABASE=$DB_NAME
    run_dumpling --filetype="csv" --escape-backslash=$escape_backslash --csv-delimiter="$csv_delimiter" --csv-separator="$csv_separator"

    # construct lightning configuration
    mkdir -p $DUMPLING_TEST_DIR/conf
    cp "$cur/conf/lightning.toml" $DUMPLING_TEST_DIR/conf

    sed -i -e "s/separator-place-holder/$csv_separator/g" $DUMPLING_TEST_DIR/conf/lightning.toml
    csv_delimiter_holder=$csv_delimiter
    if [ "$csv_delimiter" = '"' ]; then
        # We want to replace delimiter-place-holder in lightning.toml to \",
        # but sed will identify \" as ", so we need to use \\\" here.
        csv_delimiter_holder='\\\"'
    fi
    sed -i -e "s/delimiter-place-holder/$csv_delimiter_holder/g" $DUMPLING_TEST_DIR/conf/lightning.toml
    escape_backslash_holder="true"
    if [ "$escape_backslash" = "false" ] && [ "$csv_delimiter" != "" ]; then
        escape_backslash_holder="false"
    fi
    sed -i -e "s/backslash-escape-place-holder/$escape_backslash_holder/g" $DUMPLING_TEST_DIR/conf/lightning.toml

    cat "$DUMPLING_TEST_DIR/conf/lightning.toml"
    # use lightning import data to tidb
    run_lightning $DUMPLING_TEST_DIR/conf/lightning.toml

    # check mysql and tidb data
    check_sync_diff $cur/conf/diff_config.toml
}

escape_backslash_arr="true false"
csv_delimiter_arr="\" '"
csv_separator_arr=', a aa |*|'

for escape_backslash in $escape_backslash_arr
do
  for csv_separator in $csv_separator_arr
  do
    for csv_delimiter in $csv_delimiter_arr
    do
      run
    done
    if [ "$escape_backslash" = "true" ]; then
      csv_delimiter=""
      run
    fi
  done
done
