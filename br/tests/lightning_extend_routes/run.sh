#!/bin/bash

# Basic check for whether routing rules work

set -eux

for BACKEND in tidb local; do
  run_sql 'DROP DATABASE IF EXISTS routes_a0;'
  run_sql 'DROP DATABASE IF EXISTS routes_a1;'
  run_sql 'DROP DATABASE IF EXISTS routes_b;'

  run_sql 'CREATE DATABASE routes_b;'
  run_sql 'CREATE TABLE routes_b.u (x real primary key, c_source varchar(11) not null, c_schema varchar(11) not null, c_table varchar(11) not null);'

  run_lightning --config "tests/$TEST_NAME/config.toml" --backend $BACKEND
  echo Import using $BACKEND finished

  run_sql 'SELECT count(1), sum(x) FROM routes_b.u;'
  check_contains 'count(1): 4'
  check_contains 'sum(x): 259'

  run_sql 'SELECT count(1), sum(x) FROM routes_a1.s1;'
  check_contains 'count(1): 1'
  check_contains 'sum(x): 1296'

  run_sql 'SELECT count(1) FROM routes_b.u where c_table = "0";'
  check_contains 'count(1): 2'
  run_sql 'SELECT count(1) FROM routes_b.u where c_table = "1";'
  check_contains 'count(1): 1'
  run_sql 'SELECT count(1) FROM routes_b.u where c_table = "2";'
  check_contains 'count(1): 1'
  run_sql 'SELECT count(1) FROM routes_b.u where c_schema = "0";'
  check_contains 'count(1): 3'
  run_sql 'SELECT count(1) FROM routes_b.u where c_schema = "1";'
  check_contains 'count(1): 1'
  run_sql 'SELECT count(1) FROM routes_b.u where c_source = "01";'
  check_contains 'count(1): 4'

  run_sql 'SHOW TABLES IN routes_a1;'
  check_not_contains 'Tables_in_routes_a1: t2'
done
