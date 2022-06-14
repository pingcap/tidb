#! /bin/bash

set -eu

# we need to keep backup data after restart service
backup_dir=$TEST_DIR/keep_${TEST_NAME}
incr_backup_dir=${backup_dir}_inc
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
br_log_file=$TEST_DIR/br.log
source tests/_utils/run_services

function run_sql_as() {
	user=$1
  shift
	SQL="$1"
  shift
	echo "[$(date)] Executing SQL with user $user: $SQL"
  mysql -u$user -p123456 -h127.0.0.1 -P4000 \
      "$@" \
      --default-character-set utf8 -E -e "$SQL" > "$TEST_DIR/sql_res.$TEST_NAME.txt" 2>&1
}

restart_services

unset BR_LOG_TO_TERM
run_sql_file tests/${TEST_NAME}/full_data.sql
run_br backup full --log-file $br_log_file -s "local://$backup_dir"

# br.test will add coverage output, so we use head here
LAST_BACKUP_TS=$(run_br validate decode --log-file $br_log_file --field="end-version" -s "local://$backup_dir" 2>/dev/null | head -n1)
run_sql "insert into db2.t1 values(3, 'c'), (4, 'd'), (5, 'e');"
run_br backup full --log-file $br_log_file --lastbackupts $LAST_BACKUP_TS -s "local://$incr_backup_dir"

run_sql "drop database db2"

echo "--> cluster is not fresh"
run_br restore full --log-file $br_log_file -s "local://$backup_dir" > $res_file 2>&1 || true
check_contains "the target cluster is not fresh"

echo "--> non full backup data"
run_br restore full --log-file $br_log_file -s "local://$incr_backup_dir"
run_sql "select count(*) from db2.t1"
check_contains "count(*): 3"

echo "--> restore using filter"
run_sql "drop database db1; drop database db2"
run_br restore full --log-file $br_log_file --filter '*.*' --filter '!mysql.*' -s "local://$backup_dir"
run_sql "select count(*) from db2.t1"
check_contains "count(*): 2"

echo "--> incompatible system table: column count mismatch"
restart_services
# mock incompatible manually
run_sql "alter table mysql.user add column xx int;"
run_br restore full --log-file $br_log_file -s "local://$backup_dir" > $res_file 2>&1 || true
check_contains "the target cluster is not compatible with the backup data"

echo "--> incompatible system table: column type incompatible"
restart_services
# mock incompatible manually
run_sql "alter table mysql.tables_priv modify column Table_priv set('Select') DEFAULT NULL;"
run_br restore full --log-file $br_log_file -s "local://$backup_dir" > $res_file 2>&1 || true
check_contains "the target cluster is not compatible with the backup data"

echo "--> full cluster restore"
restart_services
run_br restore full --log-file $br_log_file -s "local://$backup_dir"
run_sql_as user1 "select count(*) from db1.t1"
check_contains "count(*): 2"
run_sql_as user1 "select count(*) from db2.t1"
check_contains "count(*): 2"
# user2 can select on db1 but not db2
run_sql_as user2 "select count(*) from db1.t1"
check_contains "count(*): 2"
run_sql_as user2 "select count(*) from db2.t1"
check_contains "SELECT command denied to user" || true
# user3 can only query db1.t1 using ssl
run_sql_as user3 "select count(*) from db1.t1" --ssl-mode=DISABLED || true
check_contains "Access denied for user"
run_sql_as user3 "select count(*) from db1.t1" --ssl-mode=REQUIRED
check_contains "count(*): 2"
run_sql_as user3 "select count(*) from db1.t2" --ssl-mode=REQUIRED || true
check_contains "SELECT command denied to user"

echo "clean up kept backup"
rm -rf $backup_dir $incr_backup_dir
