#!/bin/sh

# Basic check for whether routing rules work

set -eux

run_sql 'DROP DATABASE IF EXISTS test1;'
run_sql 'DROP DATABASE IF EXISTS test;'

run_lightning

run_sql 'SELECT sum(x) FROM test.u;'
check_contains 'sum(x): 43'
