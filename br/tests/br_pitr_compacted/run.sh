#!/bin/bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# tikv-ctl compact-log-backup reads log backup files directly. Keep this test
# focused on compacted PITR restore instead of the BR backup encryption wrapper.
ENCRYPTION_ARGS=""
ENABLE_ENCRYPTION_CHECK=false
export ENCRYPTION_ARGS
export ENABLE_ENCRYPTION_CHECK

set -euo pipefail

. run_services

DB="$TEST_NAME"
TASK_NAME="br_pitr_compacted"
PREFIX="pitr_compacted_backup" # NOTICE: don't start with 'br' because `restart services` removes br* dirs.
FULL_STORAGE="local://$TEST_DIR/$PREFIX/full"
LOG_STORAGE="local://$TEST_DIR/$PREFIX/log"
STATUS_ADDR="127.0.0.1:10081"
restore_log="$TEST_DIR/$TEST_NAME.restore.log"
crr_log="$TEST_DIR/$TEST_NAME.crr_checkpoint.log"
crr_pid=""

restart_services_without_tiflash() {
	stop_services
	start_services --no-tiflash
}

insert_account_rows() {
	local start=$1
	local end=$2
	local status=$3
	local values=""

	for id in $(seq "$start" "$end"); do
		values="$values,($id,$((id * 10)),'$status','seed-$id')"
	done
	run_sql "INSERT INTO $DB.account(id, cents, status, note) VALUES ${values#,};"
}

prepare_full_data() {
	run_sql "UPDATE mysql.tidb SET VARIABLE_VALUE='2400h' WHERE VARIABLE_NAME='tikv_gc_life_time';"
	run_sql "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;"
	run_sql "CREATE TABLE $DB.account (
		id INT PRIMARY KEY,
		cents INT NOT NULL,
		status VARCHAR(16) NOT NULL,
		note VARCHAR(32)
	);"
	insert_account_rows 1 100 "base"
}

write_incremental_data() {
	insert_account_rows 101 180 "inserted"
	run_sql "UPDATE $DB.account SET cents = cents + 7, status = 'updated', note = CONCAT('u-', id) WHERE id BETWEEN 1 AND 25;"
	run_sql "DELETE FROM $DB.account WHERE id BETWEEN 30 AND 40;"
	run_sql "ALTER TABLE $DB.account ADD COLUMN region VARCHAR(8) NOT NULL DEFAULT 'r0';"
	run_sql "UPDATE $DB.account SET region = 'r1' WHERE MOD(id, 3) = 0;"
	run_sql "CREATE INDEX idx_cents ON $DB.account(cents);"
	run_sql "CREATE TABLE $DB.audit (
		id INT PRIMARY KEY,
		account_id INT NOT NULL,
		action VARCHAR(16) NOT NULL,
		KEY idx_account(account_id)
	);"
	run_sql "INSERT INTO $DB.audit VALUES
		(1, 1, 'update'),
		(2, 30, 'delete'),
		(3, 101, 'insert'),
		(4, 150, 'insert');"
	run_sql "UPDATE $DB.audit SET action = 'rewrite' WHERE id IN (2, 4);"
}

start_crr_checkpoint() {
	echo "start CRR checkpoint daemon"
	crr_pid=$(run_br_async "$crr_log" --skip-goleak --pd "$PD_ADDR" operator crr-checkpoint \
		--task-name "$TASK_NAME" \
		--upstream-storage "$LOG_STORAGE" \
		--downstream-storage "$LOG_STORAGE" \
		--check-synced-from-downstream-storage \
		--status-addr "$STATUS_ADDR" \
		--calc.poll-interval 2s)

	for _ in $(seq 1 30); do
		if run_curl "https://$STATUS_ADDR/readyz" >/dev/null 2>&1; then
			return
		fi
		if ! kill -0 "$crr_pid" 2>/dev/null; then
			echo "CRR checkpoint daemon exited unexpectedly"
			cat "$crr_log"
			exit 1
		fi
		sleep 1
	done

	echo "CRR checkpoint daemon is not ready"
	cat "$crr_log"
	exit 1
}

stop_crr_checkpoint() {
	local pid="${crr_pid:-}"
	if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		for _ in $(seq 1 30); do
			if ! kill -0 "$pid" 2>/dev/null; then
				crr_pid=""
				return
			fi
			sleep 1
		done
		echo "CRR checkpoint daemon did not exit after SIGTERM, killing it"
		kill -9 "$pid" 2>/dev/null || true
		for _ in $(seq 1 5); do
			if ! kill -0 "$pid" 2>/dev/null; then
				break
			fi
			sleep 1
		done
	fi
	crr_pid=""
}

trap stop_crr_checkpoint EXIT

get_current_ts() {
	run_sql "show master status;" | awk '/Position:/{print $2; exit}'
}

wait_log_checkpoint_reach() {
	local task_name=$1
	local target_ts=$2
	local checkpoint_ts=""

	echo "wait log backup checkpoint of $task_name to reach $target_ts"
	for _ in $(seq 1 60); do
		local log_backup_status
		log_backup_status=$(
			unset BR_LOG_TO_TERM
			run_br --skip-goleak --pd "$PD_ADDR" log status \
				--task-name "$task_name" \
				--json \
				2>"$TEST_DIR/$TEST_NAME.log_status.err" | sed -n '1p'
		)
		checkpoint_ts=$(echo "$log_backup_status" | jq -r 'if ((.[0].last_errors // []) | length) == 0 then .[0].checkpoint else empty end')
		echo "current log backup checkpoint: ${checkpoint_ts:-empty}"
		if [ -n "$checkpoint_ts" ] && [ "$checkpoint_ts" -ge "$target_ts" ] 2>/dev/null; then
			return
		fi
		sleep 5
	done

	echo "log backup checkpoint ${checkpoint_ts:-empty} does not reach $target_ts"
	exit 1
}

wait_crr_checkpoint_reach() {
	local target_ts=$1
	local safe_checkpoint=""

	echo "wait CRR safe checkpoint to reach $target_ts"
	for _ in $(seq 1 60); do
		if ! kill -0 "$crr_pid" 2>/dev/null; then
			echo "CRR checkpoint daemon exited unexpectedly"
			cat "$crr_log"
			exit 1
		fi

		local status
		status=$(run_curl "https://$STATUS_ADDR/status" 2>/dev/null || true)
		safe_checkpoint=$(echo "$status" | jq -r '.safe_checkpoint // empty' 2>/dev/null || true)
		local state
		state=$(echo "$status" | jq -r '.state // empty' 2>/dev/null || true)
		local last_error
		last_error=$(echo "$status" | jq -r '.last_error // empty' 2>/dev/null || true)
		echo "current CRR safe checkpoint: ${safe_checkpoint:-empty}, state: ${state:-empty}, last_error: ${last_error:-empty}"
		if [ -n "$safe_checkpoint" ] && [ "$safe_checkpoint" -ge "$target_ts" ] 2>/dev/null; then
			return
		fi
		sleep 5
	done

	echo "CRR safe checkpoint ${safe_checkpoint:-empty} does not reach $target_ts"
	cat "$crr_log"
	exit 1
}

capture_expected_results() {
	local account_stats
	account_stats=$(run_sql "SELECT
		COUNT(*) AS ACCOUNT_COUNT,
		SUM(id) AS ACCOUNT_ID_SUM,
		SUM(cents) AS ACCOUNT_CENTS_SUM,
		SUM(CRC32(CONCAT_WS('#', id, cents, status, note, region))) AS ACCOUNT_CRC
	FROM $DB.account;")
	expected_account_count=$(echo "$account_stats" | awk '/ACCOUNT_COUNT:/{print $2}')
	expected_account_id_sum=$(echo "$account_stats" | awk '/ACCOUNT_ID_SUM:/{print $2}')
	expected_account_cents_sum=$(echo "$account_stats" | awk '/ACCOUNT_CENTS_SUM:/{print $2}')
	expected_account_crc=$(echo "$account_stats" | awk '/ACCOUNT_CRC:/{print $2}')

	local audit_stats
	audit_stats=$(run_sql "SELECT
		COUNT(*) AS AUDIT_COUNT,
		SUM(account_id) AS AUDIT_ACCOUNT_SUM,
		SUM(CRC32(CONCAT_WS('#', id, account_id, action))) AS AUDIT_CRC
	FROM $DB.audit;")
	expected_audit_count=$(echo "$audit_stats" | awk '/AUDIT_COUNT:/{print $2}')
	expected_audit_account_sum=$(echo "$audit_stats" | awk '/AUDIT_ACCOUNT_SUM:/{print $2}')
	expected_audit_crc=$(echo "$audit_stats" | awk '/AUDIT_CRC:/{print $2}')
}

compact_log_backup() {
	if ! command -v tikv-ctl >/dev/null; then
		echo "tikv-ctl is required for compact-log-backup"
		exit 1
	fi

	local snapshot_ts=$1
	local until_ts=$2
	local storage_base64
	storage_base64=$(
		unset BR_LOG_TO_TERM
		run_br --pd "$PD_ADDR" operator base64ify -s "$LOG_STORAGE" --load-creds | sed -n '1p'
	)

	for shard in "1/3" "2/3" "3/3"; do
		echo "compact log backup shard $shard from $snapshot_ts until $until_ts"
		tikv-ctl compact-log-backup \
			--from "$snapshot_ts" \
			--until "$until_ts" \
			--storage-base64 "$storage_base64" \
			--shard "$shard" \
			--minimal-compaction-size 0 \
			--cal-shift-ts
	done
}

check_restore_result() {
	run_sql "SELECT
		COUNT(*) AS ACCOUNT_COUNT,
		SUM(id) AS ACCOUNT_ID_SUM,
		SUM(cents) AS ACCOUNT_CENTS_SUM,
		SUM(CRC32(CONCAT_WS('#', id, cents, status, note, region))) AS ACCOUNT_CRC
	FROM $DB.account;"
	check_contains "ACCOUNT_COUNT: $expected_account_count"
	check_contains "ACCOUNT_ID_SUM: $expected_account_id_sum"
	check_contains "ACCOUNT_CENTS_SUM: $expected_account_cents_sum"
	check_contains "ACCOUNT_CRC: $expected_account_crc"

	run_sql "SELECT
		COUNT(*) AS AUDIT_COUNT,
		SUM(account_id) AS AUDIT_ACCOUNT_SUM,
		SUM(CRC32(CONCAT_WS('#', id, account_id, action))) AS AUDIT_CRC
	FROM $DB.audit;"
	check_contains "AUDIT_COUNT: $expected_audit_count"
	check_contains "AUDIT_ACCOUNT_SUM: $expected_audit_account_sum"
	check_contains "AUDIT_CRC: $expected_audit_crc"

	run_sql "SHOW INDEX FROM $DB.account WHERE Key_name = 'idx_cents';"
	check_contains "Column_name: cents"
	run_sql "SELECT COUNT(*) AS INDEX_COUNT FROM $DB.account USE INDEX(idx_cents) WHERE cents >= 0;"
	check_contains "INDEX_COUNT: $expected_account_count"

	run_sql "SHOW INDEX FROM $DB.audit WHERE Key_name = 'idx_account';"
	check_contains "Column_name: account_id"
	run_sql "SELECT COUNT(*) AS AUDIT_INDEX_COUNT FROM $DB.audit USE INDEX(idx_account) WHERE account_id >= 0;"
	check_contains "AUDIT_INDEX_COUNT: $expected_audit_count"

	check_contains "restore log success summary" "$restore_log"
	check_contains "[Compacted SST Restore] Start to restore SST files" "$restore_log"
	check_contains "enabled restoring compacted SSTs with newest MVCC versions only; skip user DML log restore" "$restore_log"
}

restart_services_without_tiflash

echo "prepare full backup data"
prepare_full_data

echo "run full backup"
run_br --pd "$PD_ADDR" backup full -s "$FULL_STORAGE"
snapshot_ts=$(run_br validate decode --field="end-version" -s "$FULL_STORAGE" | grep -oE "^[0-9]+" | sed -n '1p')
echo "snapshot ts: $snapshot_ts"

echo "start log backup task"
run_br --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "$LOG_STORAGE" --start-ts "$snapshot_ts"
start_crr_checkpoint

echo "write incremental data"
write_incremental_data
until_ts=$(get_current_ts)
echo "restore until ts: $until_ts"
capture_expected_results

wait_log_checkpoint_reach "$TASK_NAME" "$until_ts"
wait_crr_checkpoint_reach "$until_ts"
compact_log_backup "$snapshot_ts" "$until_ts"

stop_crr_checkpoint
run_br --pd "$PD_ADDR" log stop --task-name "$TASK_NAME"

echo "restart services for PITR restore"
restart_services_without_tiflash

echo "restore point from compacted log backup"
BR_LOG_TO_TERM=1 run_br --pd "$PD_ADDR" restore point \
	-s "$LOG_STORAGE" \
	--full-backup-storage "$FULL_STORAGE" \
	--restored-ts "$until_ts" \
	--retain-latest-mvcc-version \
	>"$restore_log" 2>&1

check_restore_result
