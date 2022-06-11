#!/bin/sh

# Basic check for whether routing rules work

set -eux

run_sql 'DROP DATABASE IF EXISTS routes_a0;'
run_sql 'DROP DATABASE IF EXISTS routes_a1;'
run_sql 'DROP DATABASE IF EXISTS routes_b;'

run_lightning

run_sql 'SELECT count(1), sum(x) FROM routes_b.u;'
check_contains 'count(1): 4'
check_contains 'sum(x): 259'

run_sql 'SELECT count(1), sum(x) FROM routes_a1.s1;'
check_contains 'count(1): 1'
check_contains 'sum(x): 1296'

run_sql 'SHOW TABLES IN routes_a1;'
check_not_contains 'Tables_in_routes_a1: t2'

