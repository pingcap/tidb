#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="e2e"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql "create database $DB_NAME DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table $DB_NAME.$TABLE_NAME (a int(255), b blob);"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME (a) values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

# insert blob records
run_sql "insert into $DB_NAME.$TABLE_NAME (b) values (x''),(null),('0'),('1');"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml

# test parquet dump and import via lightning
PARQUET_DB="e2e_parquet"
PARQUET_TABLE_NAME="t_types"

run_dumpling_parquet() {
    parquet_dump_log="$DUMPLING_OUTPUT_DIR/dumpling_parquet.log"
    if ! run_dumpling "$@" > "$parquet_dump_log" 2>&1; then
        if grep -q "requires --pd" "$parquet_dump_log"; then
            pd_addr=$(curl -sf "http://127.0.0.1:10080/settings" | sed -n 's/.*"path": "\([^"]*\)".*/\1/p' | head -n1)
            [ -n "$pd_addr" ]
            if ! run_dumpling "$@" --pd "$pd_addr" > "$parquet_dump_log" 2>&1; then
                cat "$parquet_dump_log"
                exit 1
            fi
        else
            cat "$parquet_dump_log"
            exit 1
        fi
    fi
}

verify_parquet_import() {
    # clear target db on TiDB to avoid duplicate-key failures when importing repeatedly.
    export DUMPLING_TEST_PORT=4000
    orig_test_db="${DUMPLING_TEST_DATABASE-}"
    export DUMPLING_TEST_DATABASE=""
    run_sql "drop database if exists \`$PARQUET_DB\`;"
    export DUMPLING_TEST_DATABASE="$orig_test_db"

    cat "$cur/conf/lightning.toml"
    run_lightning $cur/conf/lightning.toml

    parquet_diff_config="$DUMPLING_OUTPUT_DIR/parquet_diff_config.toml"
    cat > "$parquet_diff_config" <<EOF
# diff Configuration.

check-thread-count = 4

export-fix-sql = true

check-struct-only = false

[task]
    output-dir = "./output"

    source-instances = ["mysql1"]

    target-instance = "tidb0"

    target-check-tables = ["$PARQUET_DB.$PARQUET_TABLE_NAME"]

[data-sources]
[data-sources.mysql1]
host = "127.0.0.1"
port = 3306
user = "root"
password = ""

[data-sources.tidb0]
host = "127.0.0.1"
port = 4000
user = "root"
password = ""
EOF
    check_sync_diff "$parquet_diff_config"
}

export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$PARQUET_DB\`;"
run_sql "create database \`$PARQUET_DB\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$PARQUET_DB\`.\`$PARQUET_TABLE_NAME\` (id int primary key, c_tinyint tinyint, c_int int, c_bigint bigint, c_decimal decimal(18,6), c_double double, c_varchar varchar(64), c_text text, c_blob blob, c_date date, c_datetime datetime(6), c_timestamp timestamp(6), c_time time(6), c_year year, c_json json, c_enum enum('small','medium','large'), c_set set('x','y','z'));"
run_sql "insert into \`$PARQUET_DB\`.\`$PARQUET_TABLE_NAME\` values (1,-8,123456,-9876543210,12345.678901,0.5,'alpha','hello parquet',x'00ff10','2026-02-28','2026-02-28 11:22:33.123456','2026-02-28 11:22:33.123456','12:34:56.123456',2026,'{\"k\":\"v\",\"n\":1}','medium','x,z'),(2,NULL,NULL,NULL,-1.000001,-12.25,'beta','second row text',x'','1999-12-31','1999-12-31 23:59:59.000001','1999-12-31 23:59:59.000001','00:00:00.000000',1999,'{\"arr\":[1,2,3]}','small','y');"

export DUMPLING_TEST_DATABASE=$PARQUET_DB
for parquet_compress in "snappy" "gzip" "zstd" "no-compression"
do
    rm -rf "$DUMPLING_OUTPUT_DIR"
    mkdir -p "$DUMPLING_OUTPUT_DIR"
    run_dumpling_parquet --filetype parquet --parquet-compress "$parquet_compress" --filesize 1B
    parquet_file_num=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -iname "$PARQUET_DB.$PARQUET_TABLE_NAME.*.parquet" | wc -l)
    if [ "$parquet_file_num" -lt 2 ]; then
        echo "obtain parquet file number: $parquet_file_num, but expect at least: 2" && exit 1
    fi
    verify_parquet_import
done

rm -rf "$DUMPLING_OUTPUT_DIR"
mkdir -p "$DUMPLING_OUTPUT_DIR"
run_dumpling_parquet --filetype parquet --parquet-page-size "2MiB" --parquet-row-group-size "128MiB"
verify_parquet_import

# test e2e with different compress options
for compress_type in "gzip" "snappy" "zstd"
do
    # drop database on tidb
    export DUMPLING_TEST_PORT=4000
    run_sql "drop database if exists $DB_NAME;"

    export DUMPLING_TEST_PORT=3306

    # dumping
    export DUMPLING_TEST_DATABASE=$DB_NAME
    rm -rf "$DUMPLING_OUTPUT_DIR"
    run_dumpling --compress "$compress_type"

    cat "$cur/conf/lightning.toml"
    # use lightning import data to tidb
    run_lightning $cur/conf/lightning.toml

    # check mysql and tidb data
    check_sync_diff $cur/conf/diff_config.toml
done
