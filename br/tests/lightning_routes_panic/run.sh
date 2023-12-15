#!/bin/sh

# Basic check for whether routing rules work

set -eux

run_sql 'DROP DATABASE IF EXISTS test1;'
run_sql 'DROP DATABASE IF EXISTS test;'

run_sql 'CREATE DATABASE test1;'
run_sql 'CREATE DATABASE test;'
run_sql 'CREATE TABLE test1.dump_test (x real primary key);'
run_sql 'CREATE TABLE test.u (x real primary key);'

run_lightning

run_sql 'SELECT sum(x) FROM test.u;'
check_contains 'sum(x): 43'
