#!/bin/bash
#
# Copyright 2013-2024 PingCAP, Inc.
#

set -euE

# Set the failpoint to kill the lightning instance as soon as one batch data is written
PKG="github.com/pingcap/tidb/br/pkg/lightning/importer"
export GO_FAILPOINTS="$PKG/SlowDownWriteRows=sleep(1000);$PKG/FailAfterWriteRows=panic;$PKG/SetMinDeliverBytes=return(1)"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS test'

set +e
bash run_lightning --backend tidb --enable-checkpoint=1 2> /dev/null
set -e
run_sql 'SELECT count(*), sum(id) FROM `test`.binary_table'
check_contains "count(*): 1"
check_contains "sum(id): 1"

# check chunk offset and update checkpoint current row id to a higher value so that
# if parse read from start, the generated rows will be different
run_sql "UPDATE checkpoint_test_parquet.chunk_v5 SET prev_rowid_max = prev_rowid_max + 1000, rowid_max = rowid_max + 1000;"

# restart lightning from checkpoint, the second line should be written successfully
export GO_FAILPOINTS=
set +e
bash run_lightning --backend tidb --enable-checkpoint=1 2> /dev/null
set -e

run_sql 'SELECT count(*), sum(id) FROM `test`.binary_table'
check_contains "count(*): 3"
check_contains "sum(id): 6"
