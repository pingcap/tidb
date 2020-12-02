#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eux

WITH_TLS="--ssl-ca=$DUMPLING_TEST_DIR/ca.pem --ssl-cert=$DUMPLING_TEST_DIR/dumpling.pem --ssl-key=$DUMPLING_TEST_DIR/dumpling.key"

# create a user which can only connect using the "dumpling" cert.
export DUMPLING_TEST_PORT=4000
run_sql 'drop user if exists dumper;'
run_sql "create user dumper require subject '/CN=127.0.0.1/OU=dumpling';"
run_sql 'grant all on tls.* to dumper;'

# make some sample data.
export DUMPLING_TEST_USER=dumper
run_sql 'drop database if exists tls;' $WITH_TLS
run_sql 'create database tls;' $WITH_TLS
export DUMPLING_TEST_DATABASE=tls
run_sql 'create table t (a int);' $WITH_TLS
run_sql 'insert into t values (1), (2), (3);' $WITH_TLS

# run dumpling!
# (we need '--consistency none' because dumper does not have SELECT permission of `mysql`.`tidb`)
run_dumpling --ca "$DUMPLING_TEST_DIR/ca.pem" --cert "$DUMPLING_TEST_DIR/dumpling.pem" --key "$DUMPLING_TEST_DIR/dumpling.key" --consistency none

file_should_exist "$DUMPLING_OUTPUT_DIR/tls-schema-create.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/tls.t-schema.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/tls.t.000000000.sql"
