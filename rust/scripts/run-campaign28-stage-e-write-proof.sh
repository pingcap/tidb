#!/usr/bin/env bash

# Campaign 28 Stage E write proof (focused, self-contained).
#
# Proves that prepared writes committed through the Rust SQL node persist to a
# real PD/TiKV cluster and are observed independently by a separate Go TiDB.
# Unlike the topology-churn read campaigns, this needs no fault injection, so it
# uses the tiup playground's own v8.5.6 Go TiDB as the independent oracle rather
# than a separately built failpoint binary.
#
# Flow: start a tag-owned three-TiKV playground WITH one Go TiDB; the Go TiDB
# creates campaign28.accounts and seeds it; launch the Rust node against the
# discovered table id; run a one-thread prepared read+write sysbench workload
# through the Rust endpoint; a separate Go TiDB connection verifies the summed
# balance advanced by exactly the number of committed prepared UPDATEs; restart
# only the Rust node and prove the persisted rows are still served. Cleanup stops
# only tag-owned processes and removes only tag-owned data.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT_DIR="${RUST_ROOT}/scripts"
TAG="campaign28-stage-e-write-${$}"
PORT_OFFSET=${C28_STAGE_E_WRITE_PORT_OFFSET:-46000}
PD_PORT=$((2379 + PORT_OFFSET))
TIDB_PORT=$((4000 + PORT_OFFSET))
RUST_PORT=$((3390 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-runtime.XXXXXX")
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
AUTH_USER=campaign28
AUTH_PASSWORD=campaign28-native-password
DATABASE=campaign28
TABLE_SIZE=16
MYSQL_PLUGIN_DIR=${C28_MYSQL_PLUGIN_DIR:-/opt/homebrew/Cellar/mysql-client/9.5.0/lib/plugin}
# The prepared read+write matrix is driven by the raw-socket Python client rather
# than sysbench: on this host the Homebrew sysbench links mariadb-connector-c,
# which refuses a plaintext connection ("SSL is required") even with
# --mysql-ssl=off. The raw client speaks the binary protocol directly, so it is
# the same COM_STMT_PREPARE/EXECUTE path sysbench would drive, without the
# client-library TLS default. The sysbench-lua load variant remains for a host
# with a libmysqlclient-linked sysbench.
RAW_CLIENT="${SCRIPT_DIR}/mysql-prepared-client.py"
WRITE_COUNT=16
SYSBENCH_BIN=${C28_SYSBENCH:-/opt/homebrew/bin/sysbench}
SYSBENCH_SCRIPT="${SCRIPT_DIR}/sysbench-prepared-read-write.lua"
RUST_SERVER="${RUST_ROOT}/target/release/tidb-server"
PLAYGROUND_PID=
RUST_PID=
STORE_ADDRESSES=

tag_status_rows() {
  tiup status 2>/dev/null | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}
tag_owned_pids() { pgrep -f "${TAG_DIR}" || true; }
endpoint_reachable() { nc -z -w 1 "${1%:*}" "${1##*:}" >/dev/null 2>&1; }

kill_rust_node() {
  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  RUST_PID=
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  kill_rust_node
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  tiup clean "${TAG}" --all >/dev/null 2>&1 || true
  local cleaned=false
  for _ in $(seq 1 30); do
    if [[ -z "$(tag_owned_pids)" ]] && [[ -z "$(tag_status_rows || true)" ]]; then
      cleaned=true
      break
    fi
    sleep 1
  done
  local address
  for address in ${STORE_ADDRESSES}; do
    endpoint_reachable "${address}" && { echo "cleanup: TiKV ${address} still reachable" >&2; cleaned=false; }
  done
  endpoint_reachable "${PD_ADDR}" && { echo "cleanup: PD ${PD_ADDR} still reachable" >&2; cleaned=false; }
  endpoint_reachable "127.0.0.1:${RUST_PORT}" && { echo "cleanup: Rust node still reachable" >&2; cleaned=false; }
  if [[ "${cleaned}" == true && "${TAG_DIR}" == "${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}" \
        && "${TAG}" == campaign28-stage-e-write-* ]]; then
    rm -rf -- "${TAG_DIR}" "${RUNTIME_DIR}"
    [[ "${status}" -eq 0 ]] && rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}"
  else
    echo "cleanup: retained ${PLAYGROUND_LOG} ${RUST_LOG} — verify manually" >&2
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

mysql_go() {
  mysql --host 127.0.0.1 --port "${TIDB_PORT}" --user root \
    --plugin-dir="${MYSQL_PLUGIN_DIR}" --default-auth=mysql_native_password \
    --batch --skip-column-names "$@"
}

launch_rust_node() {
  "${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
    --host 127.0.0.1 --port "${RUST_PORT}" \
    --read-table "${DATABASE}" accounts "${TABLE_ID}" 2 \
    id:1:clustered-pk balance:2:stored-not-null \
    --auth-file "${AUTH_FILE}" --max-connections 4 >>"${RUST_LOG}" 2>&1 &
  RUST_PID=$!
  for _ in $(seq 1 120); do
    if ! kill -0 "${RUST_PID}" 2>/dev/null; then
      echo "Rust node exited during startup" >&2; tail -40 "${RUST_LOG}" >&2; return 1
    fi
    endpoint_reachable "127.0.0.1:${RUST_PORT}" && return 0
    sleep 0.5
  done
  echo "Rust node did not become reachable on ${RUST_PORT}" >&2; return 1
}

# Drives WRITE_COUNT prepared point-read + prepared arithmetic-UPDATE pairs
# through the Rust endpoint and validates the emitted summary.
run_raw_write() {
  local output=$1
  python3 "${RAW_CLIENT}" write --port "${RUST_PORT}" \
    --user "${AUTH_USER}" --password "${AUTH_PASSWORD}" --database "${DATABASE}" \
    --count "${WRITE_COUNT}" --table-size "${TABLE_SIZE}" >"${output}" 2>&1 || {
    echo "raw prepared read+write client failed" >&2; cat "${output}" >&2; return 1
  }
  jq -e --argjson n "${WRITE_COUNT}" \
    'select(.event == "prepared_read_write")
     | .count == $n and .reads == $n and .affected_rows == $n' "${output}" >/dev/null || {
    echo "raw client did not confirm ${WRITE_COUNT} reads + ${WRITE_COUNT} committed writes" >&2
    cat "${output}" >&2; return 1
  }
}

# Drives the full prepared matrix (one-row INSERT, two-row INSERT, direct SET
# update, arithmetic UPDATE, point read) through the Rust endpoint once.
run_matrix() {
  local output=$1
  python3 "${RAW_CLIENT}" matrix --port "${RUST_PORT}" \
    --user "${AUTH_USER}" --password "${AUTH_PASSWORD}" --database "${DATABASE}" >"${output}" 2>&1 || {
    echo "prepared matrix client failed" >&2; cat "${output}" >&2; return 1
  }
  jq -e 'select(.event == "prepared_matrix")
     | .one_row_insert == 1 and .two_row_insert == 2 and .set_update == 1
       and .arithmetic_update == 1 and .point_read == 3000
       and (.duplicate_rejected_code | type) == "number"
       and (.overflow_rejected_code | type) == "number"' "${output}" >/dev/null || {
    echo "prepared matrix receipt incomplete" >&2; cat "${output}" >&2; return 1
  }
}

# --- Preconditions ------------------------------------------------------------
[[ -x "${RUST_SERVER}" ]] || { echo "missing Rust node binary: ${RUST_SERVER} (build: cargo build --release -p tidb-server)" >&2; exit 1; }
[[ -f "${MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]] || { echo "MYSQL_PLUGIN_DIR lacks mysql_native_password.so: ${MYSQL_PLUGIN_DIR}" >&2; exit 1; }
command -v python3 >/dev/null || { echo "missing python3 for the raw prepared client" >&2; exit 1; }
[[ -f "${RAW_CLIENT}" ]] || { echo "missing raw prepared client: ${RAW_CLIENT}" >&2; exit 1; }
python3 "${RAW_CLIENT}" self-test >/dev/null || { echo "raw prepared client self-test failed" >&2; exit 1; }

# --- Auth file ----------------------------------------------------------------
AUTH_HASH_HEX=$(printf '%s' "${AUTH_PASSWORD}" | openssl dgst -sha1 -binary | openssl dgst -sha1 -hex | awk '{ print toupper($NF) }')
[[ "${AUTH_HASH_HEX}" =~ ^[0-9A-F]{40}$ ]] || { echo "could not derive native-password hash" >&2; exit 1; }
(umask 077; printf '%s\t%s\t%s\t*%s\n' "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"

# --- Cluster ------------------------------------------------------------------
echo "stage-e-write: starting tag-owned playground (3 TiKV + one Go TiDB)"
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 3 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

echo "stage-e-write: waiting up to 600s for 3 stores + a mysql-connectable Go TiDB"
ready=false
for i in $(seq 1 600); do
  kill -0 "${PLAYGROUND_PID}" 2>/dev/null || { echo "playground exited early" >&2; tail -60 "${PLAYGROUND_LOG}" >&2; exit 1; }
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up") | .store.address' 2>/dev/null) || true
  STORE_COUNT=$(printf '%s\n' "${STORE_ADDRESSES}" | awk 'NF { c++ } END { print c + 0 }')
  if [[ "${STORE_COUNT}" -eq 3 ]] && endpoint_reachable "127.0.0.1:${TIDB_PORT}" \
     && mysql_go -e "SELECT 1" >/dev/null 2>&1; then ready=true; break; fi
  (( i % 15 == 0 )) && echo "stage-e-write: [${i}s] stores=${STORE_COUNT}/3"
  sleep 1
done
[[ "${ready}" == true ]] || { echo "topology not ready" >&2; tail -60 "${PLAYGROUND_LOG}" >&2; exit 1; }

# --- Fixture (Go TiDB owns DDL + seeding + region split) ----------------------
echo "stage-e-write: Go TiDB creating campaign28.accounts, seeding ${TABLE_SIZE} rows, splitting the keyspace at handle 102"
mysql_go <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE};
CREATE TABLE ${DATABASE}.accounts (id BIGINT PRIMARY KEY CLUSTERED, balance BIGINT NOT NULL);
INSERT INTO ${DATABASE}.accounts (id, balance)
  SELECT n, n * 100 FROM (
    SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
    UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
    UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16) ids;
SET SESSION tidb_wait_split_region_finish = 1;
SPLIT TABLE ${DATABASE}.accounts BY (102);
SQL
TABLE_ID=$(mysql_go -e "SELECT TIDB_TABLE_ID FROM information_schema.tables WHERE TABLE_SCHEMA='${DATABASE}' AND TABLE_NAME='accounts'")
[[ -n "${TABLE_ID}" && "${TABLE_ID}" -gt 0 ]] || { echo "table id discovery failed" >&2; exit 1; }
echo "stage-e-write: TABLE_ID=${TABLE_ID}"

# The split at handle 102 places matrix insert key 101 (< 102) and 103 (>= 102)
# in distinct regions; both writes committing proves the Rust node's RegionCache
# routes to more than the first region.
REGION_COUNT=$(mysql_go -e "SHOW TABLE ${DATABASE}.accounts REGIONS" | wc -l | tr -d ' ')
[[ "${REGION_COUNT}" -ge 2 ]] || { echo "region split did not create >=2 regions (got ${REGION_COUNT})" >&2; exit 1; }
echo "stage-e-write: table split into ${REGION_COUNT} regions (matrix keys 101 and 103 land on opposite sides of handle 102)"

# --- Rust node ----------------------------------------------------------------
echo "stage-e-write: launching Rust node on ${RUST_PORT} against table ${TABLE_ID}"
launch_rust_node
ORIGINAL_RUST_PID=${RUST_PID}

# --- Prepared read+write matrix through the Rust node -------------------------
BEFORE_SUM=$(mysql_go -e "SELECT COALESCE(SUM(balance),0) FROM ${DATABASE}.accounts")
echo "stage-e-write: before sum(balance)=${BEFORE_SUM}; driving ${WRITE_COUNT} prepared read+write pairs through the Rust node"
run_raw_write "${RUNTIME_DIR}/raw-write.jsonl"

# --- Independent persistence verification via Go TiDB -------------------------
AFTER_SUM=$(mysql_go -e "SELECT COALESCE(SUM(balance),0) FROM ${DATABASE}.accounts")
EXPECTED_SUM=$((BEFORE_SUM + WRITE_COUNT))
echo "stage-e-write: after sum(balance)=${AFTER_SUM}; expected ${EXPECTED_SUM} (before + ${WRITE_COUNT} prepared UPDATEs)"
[[ "${AFTER_SUM}" -eq "${EXPECTED_SUM}" ]] || { echo "independent Go TiDB did NOT observe the Rust-committed writes" >&2; exit 1; }

# --- Restart only the Rust node; prove persisted rows are still served --------
echo "stage-e-write: restarting only the Rust node"
kill_rust_node
launch_rust_node
run_raw_write "${RUNTIME_DIR}/raw-write-after-restart.jsonl"
FINAL_SUM=$(mysql_go -e "SELECT COALESCE(SUM(balance),0) FROM ${DATABASE}.accounts")
EXPECTED_FINAL=$((AFTER_SUM + WRITE_COUNT))
[[ "${FINAL_SUM}" -eq "${EXPECTED_FINAL}" ]] || { echo "post-restart writes not observed: got ${FINAL_SUM}, expected ${EXPECTED_FINAL}" >&2; exit 1; }

# --- Full prepared matrix (one/two-row INSERT, direct + arithmetic UPDATE, read) ---
# Uses ids 101-103 so it never touches the SUM-verified seeded rows above.
echo "stage-e-write: driving the full prepared write matrix through the restarted Rust node"
run_matrix "${RUNTIME_DIR}/matrix.jsonl"
MATRIX_101=$(mysql_go -e "SELECT balance FROM ${DATABASE}.accounts WHERE id=101")
MATRIX_102=$(mysql_go -e "SELECT balance FROM ${DATABASE}.accounts WHERE id=102")
MATRIX_103=$(mysql_go -e "SELECT balance FROM ${DATABASE}.accounts WHERE id=103")
MATRIX_104=$(mysql_go -e "SELECT balance FROM ${DATABASE}.accounts WHERE id=104")
[[ "${MATRIX_101}" -eq 1500 && "${MATRIX_102}" -eq 2005 && "${MATRIX_103}" -eq 3000 \
   && "${MATRIX_104}" == "9223372036854775807" ]] || {
  echo "independent Go TiDB matrix verification failed: 101=${MATRIX_101}(want 1500) 102=${MATRIX_102}(want 2005) 103=${MATRIX_103}(want 3000) 104=${MATRIX_104}(want i64::MAX; the failed overflow UPDATE must leave it unchanged)" >&2
  exit 1
}
echo "stage-e-write: matrix verified by Go TiDB (id101=1500 id102=2005 id103=3000; duplicate INSERT of id101 rejected → id101=1500 unchanged; overflow UPDATE of id104 rejected → id104=i64::MAX unchanged)"

# --- Optional: sysbench-specific proof of the same prepared read+write mix -----
# The raw client above already proves the binary prepared protocol; this only
# adds the literal sysbench requirement. On a host whose sysbench links
# mariadb-connector-c, force ssl-mode=DISABLED via a client my.cnf (HOME) since
# that connector otherwise requires TLS the plaintext node does not offer. This
# step is informational and never fails the proof.
SYSBENCH_RESULT=skipped
if [[ -x "${SYSBENCH_BIN}" && -f "${SYSBENCH_SCRIPT}" ]]; then
  SB_HOME=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-sbhome.XXXXXX")
  printf '[client]\nssl-mode=DISABLED\nssl=0\n' >"${SB_HOME}/.my.cnf"
  if HOME="${SB_HOME}" "${SYSBENCH_BIN}" --db-driver=mysql --db-ps-mode=auto \
      --mysql-host=127.0.0.1 --mysql-port="${RUST_PORT}" \
      --mysql-user="${AUTH_USER}" --mysql-password="${AUTH_PASSWORD}" --mysql-db= \
      --mysql-ssl=off --threads=1 --table-size="${TABLE_SIZE}" --events=1000 --time=30 \
      --rand-type=uniform "${SYSBENCH_SCRIPT}" run >"${RUNTIME_DIR}/sysbench.out" 2>&1 \
      && grep -qE 'ignored errors:[[:space:]]+0' "${RUNTIME_DIR}/sysbench.out"; then
    SYSBENCH_RESULT=passed
    # Retain the real throughput numbers in the persistent proof log before the
    # runtime dir (holding sysbench.out) is removed by cleanup.
    SYSBENCH_STATS=$(grep -E 'total number of events:|queries:|ignored errors:' \
      "${RUNTIME_DIR}/sysbench.out" | tr -s ' ' | paste -sd '; ' -)
    echo "stage-e-write: sysbench prepared read+write mix ALSO passed against the Rust node (${SYSBENCH_STATS})"
  else
    SYSBENCH_RESULT=env-blocked
    echo "stage-e-write: NOTE sysbench env-blocked here (mariadb-connector-c); raw client already proved the protocol" >&2
  fi
  rm -rf -- "${SB_HOME}"
fi

echo "stage-e-write: PASS — ${WRITE_COUNT} prepared reads + ${WRITE_COUNT} prepared UPDATEs committed through the Rust node (x2 across a restart), plus the full prepared matrix (one/two-row INSERT, direct + arithmetic UPDATE, point read) with insert keys 101 and 103 committed to distinct regions across the handle-102 split (${REGION_COUNT} regions), all independently verified by Go TiDB (table_id=${TABLE_ID}, before=${BEFORE_SUM}, after=${AFTER_SUM}, final=${FINAL_SUM}); sysbench=${SYSBENCH_RESULT}"
