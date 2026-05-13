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

# test parquet dump and import into on tidb
PARQUET_SRC_DB="e2e_parquet_src"
PARQUET_DST_DB="e2e_parquet_dst"
PARQUET_TABLE_NAME="t_types"

export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$PARQUET_SRC_DB\`;"
run_sql "drop database if exists \`$PARQUET_DST_DB\`;"
run_sql "create database \`$PARQUET_SRC_DB\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\` (id int primary key, c_tinyint tinyint, c_int int, c_bigint bigint, c_decimal decimal(18,6), c_double double, c_varchar varchar(64), c_text text, c_blob blob, c_date date, c_datetime datetime(6), c_timestamp timestamp(6), c_time time(6), c_year year, c_json json, c_enum enum('small','medium','large'), c_set set('x','y','z'));"
run_sql "insert into \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\` values (1,-8,123456,-9876543210,12345.678901,0.5,'alpha','hello parquet',x'00ff10','2026-02-28','2026-02-28 11:22:33.123456','2026-02-28 11:22:33.123456','12:34:56.123456',2026,'{\"k\":\"v\",\"n\":1}','medium','x,z'),(2,NULL,NULL,NULL,-1.000001,-12.25,'beta','second row text',x'','1999-12-31','1999-12-31 23:59:59.000001','1999-12-31 23:59:59.000001','00:00:00.000000',1999,'{\"arr\":[1,2,3]}','small','y');"

export DUMPLING_TEST_DATABASE=$PARQUET_SRC_DB
rm -rf "$DUMPLING_OUTPUT_DIR"
mkdir -p "$DUMPLING_OUTPUT_DIR"
PARQUET_DUMP_LOG="$DUMPLING_OUTPUT_DIR/dumpling_parquet.log"
if ! run_dumpling --filetype parquet > "$PARQUET_DUMP_LOG" 2>&1; then
    if grep -q "requires --pd" "$PARQUET_DUMP_LOG"; then
        PD_ADDR=$(curl -sf "http://127.0.0.1:10080/settings" | sed -n 's/.*"path": "\([^"]*\)".*/\1/p' | head -n1)
        [ -n "$PD_ADDR" ]
        run_dumpling --filetype parquet --pd "$PD_ADDR" > "$PARQUET_DUMP_LOG" 2>&1
    else
        cat "$PARQUET_DUMP_LOG"
        exit 1
    fi
fi

run_sql "create database \`$PARQUET_DST_DB\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$PARQUET_DST_DB\`.\`$PARQUET_TABLE_NAME\` like \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\`;"
run_sql "import into \`$PARQUET_DST_DB\`.\`$PARQUET_TABLE_NAME\` from '$DUMPLING_OUTPUT_DIR/$PARQUET_SRC_DB.$PARQUET_TABLE_NAME.*.parquet' format 'parquet';"

src_rows=$(run_sql "select count(*) as cnt from \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\`;" | awk -F': ' '/cnt:/ {print $2}')
dst_rows=$(run_sql "select count(*) as cnt from \`$PARQUET_DST_DB\`.\`$PARQUET_TABLE_NAME\`;" | awk -F': ' '/cnt:/ {print $2}')
[ "$src_rows" = "$dst_rows" ]

diff_rows=$(run_sql "select count(*) as diff_cnt from ((select * from \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\` except select * from \`$PARQUET_DST_DB\`.\`$PARQUET_TABLE_NAME\`) union all (select * from \`$PARQUET_DST_DB\`.\`$PARQUET_TABLE_NAME\` except select * from \`$PARQUET_SRC_DB\`.\`$PARQUET_TABLE_NAME\`)) diff;" | awk -F': ' '/diff_cnt:/ {print $2}')
[ "$diff_rows" = "0" ]

# test e2e with compress option again

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

export DUMPLING_TEST_PORT=3306

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --compress "snappy"

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml

