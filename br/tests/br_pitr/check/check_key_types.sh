#!/bin/sh

echo "=== Verifying Data Integrity ==="

# Verify original table key data is intact
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.table_key_test;"
check_contains "cnt: 4"
run_sql "SELECT id, name, value FROM key_types_test.table_key_test ORDER BY id;"
check_contains "id: 1"
check_contains "name: test1"
check_contains "value: 100"
check_contains "id: 2"
check_contains "name: test2"
check_contains "value: 200"
check_contains "id: 3"
check_contains "name: test3"
check_contains "value: 300"

# Verify new table key data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.table_key_test2;"
check_contains "cnt: 1"
run_sql "SELECT id, name, value FROM key_types_test.table_key_test2 ORDER BY id;"
check_contains "id: 1"
check_contains "name: test1"
check_contains "value: 100"

# Verify original auto increment data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.auto_inc_test;"
check_contains "cnt: 4"
run_sql "SELECT id FROM key_types_test.auto_inc_test ORDER BY id;"
check_contains "id: 1"
check_contains "id: 2"
check_contains "id: 3"
check_contains "id: 4"

# Verify new auto increment data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.auto_inc_test2;"
check_contains "cnt: 2"
run_sql "SELECT id FROM key_types_test.auto_inc_test2 ORDER BY id;"
check_contains "id: 1"
check_contains "id: 2"

# Verify original sequence data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.sequence_test;"
check_contains "cnt: 4"
run_sql "SELECT id FROM key_types_test.sequence_test ORDER BY id;"
check_contains "id: 1"
check_contains "id: 3"
check_contains "id: 5"
check_contains "id: 7"

# Verify new sequence data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.sequence_test2;"
check_contains "cnt: 2"
run_sql "SELECT id FROM key_types_test.sequence_test2 ORDER BY id;"
check_contains "id: 1"
check_contains "id: 3"

# Verify original auto random data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.auto_random_test;"
check_contains "cnt: 4"
run_sql "SELECT name FROM key_types_test.auto_random_test ORDER BY id;"
check_contains "name: rand1"
check_contains "name: rand2"
check_contains "name: rand3"
check_contains "name: random4"

# Verify new auto random data
run_sql "SELECT COUNT(*) as cnt FROM key_types_test.auto_random_test2;"
check_contains "cnt: 2"
run_sql "SELECT name FROM key_types_test.auto_random_test2 ORDER BY id;"
check_contains "name: rand1"
check_contains "name: rand2"
