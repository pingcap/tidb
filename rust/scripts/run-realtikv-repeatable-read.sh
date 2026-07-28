#!/usr/bin/env bash
#
# Live proof that the convergence node's explicit transaction holds ONE
# transaction, and that its single start_ts is what conflict detection runs on.
#
# A real Go tidb-server (the playground's own) creates the schema, the account
# and the rows. The Rust node is started in cluster-session mode and told only
# the PD address. Two independent client connections then prove, on the wire:
#
#   * session A opens BEGIN and reads a row;
#   * session B, a separate connection in autocommit, commits a new value for
#     that row;
#   * A re-reads and still sees its own BEGIN-time value -- repeatable read,
#     which is only possible because A never took a newer timestamp;
#   * A writes the row and COMMITs, and is refused with 9007, because its
#     prewrite carries the BEGIN start_ts and B's commit is newer;
#   * B's value is the durable one, and the creating Go TiDB agrees.
#
# A per-statement-timestamp session fails the third and fourth checks, which is
# exactly the divergence this proof exists to close.
#
# Usage: rust/scripts/run-realtikv-repeatable-read.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep python3; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing repeatable-read prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${REPEATABLE_READ_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "REPEATABLE_READ_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
# A MySQL 9 client dropped the built-in mysql_native_password plugin; resolve a
# plugin directory the same way the other realtikv scripts do.
MYSQL_PLUGIN_ARGS=()
MYSQL_BIN_DIR=$(cd "$(dirname "$(command -v "${MYSQL_CLIENT}")")" && pwd)
for candidate in \
  "${MYSQL_BIN_DIR}/../opt/mysql-client/lib/plugin" \
  /opt/homebrew/opt/mysql-client/lib/plugin \
  /usr/local/opt/mysql-client/lib/plugin; do
  if [[ -f "${candidate}/mysql_native_password.so" ]]; then
    MYSQL_PLUGIN_ARGS=(--plugin-dir="${candidate}")
    break
  fi
done

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="repeatable-read-${$}-$(date +%s)"
PORT_OFFSET=${REPEATABLE_READ_PORT_OFFSET:-42600}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "REPEATABLE_READ_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
RUST_SQL_PORT=$((4100 + PORT_OFFSET))

DATABASE=rr
SERVED_TABLE=accounts
ROW_ID=1
ORIGINAL_BALANCE=100
RACING_BALANCE=999
LOSING_BALANCE=500

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}.XXXXXX")
PLAYGROUND_LOG="${WORK_DIR}/playground.log"
RUST_LOG_FILE="${WORK_DIR}/rust-node.log"
CLIENT_LOG="${WORK_DIR}/client.log"
PLAYGROUND_PID=""
RUST_PID=""

cleanup() {
  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  tiup clean "${TAG}" >/dev/null 2>&1 || true
  # `tiup clean` leaves the tag's data directory behind when the playground was
  # killed rather than stopped, so remove it explicitly.
  rm -rf "${HOME}/.tiup/data/${TAG}"
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

wait_for_port() {
  local port=$1 log=$2 deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "port ${port} never opened; see ${log}" >&2
  return 1
}

go_sql() {
  "${MYSQL_CLIENT}" "${MYSQL_PLUGIN_ARGS[@]}" -h 127.0.0.1 -P "${GO_SQL_PORT}" \
    -u root --protocol=TCP "$@"
}

echo "starting playground (tag ${TAG})"
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
wait_for_port "${PD_PORT}" "${PLAYGROUND_LOG}"
wait_for_port "${GO_SQL_PORT}" "${PLAYGROUND_LOG}"

echo "the Go TiDB creates the schema, the account and the row"
go_sql <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE};
USE ${DATABASE};
CREATE TABLE ${SERVED_TABLE} (id BIGINT PRIMARY KEY CLUSTERED, balance BIGINT NOT NULL);
INSERT INTO ${SERVED_TABLE} VALUES (${ROW_ID}, ${ORIGINAL_BALANCE});
DROP USER IF EXISTS 'appuser'@'%';
CREATE USER 'appuser'@'%' IDENTIFIED WITH mysql_native_password BY 'apppw';
GRANT ALL PRIVILEGES ON ${DATABASE}.* TO 'appuser'@'%';
FLUSH PRIVILEGES;
SQL

echo "building the Rust node"
cargo build --manifest-path "${RUST_ROOT}/Cargo.toml" -p tidb-server --bin tidb-server

echo "starting the Rust node in cluster-session mode"
"${RUST_ROOT}/target/debug/tidb-server" \
  --path "127.0.0.1:${PD_PORT}" \
  --port "${RUST_SQL_PORT}" \
  --cluster-session \
  --load-privileges \
  >"${RUST_LOG_FILE}" 2>&1 &
RUST_PID=$!
wait_for_port "${RUST_SQL_PORT}" "${RUST_LOG_FILE}"
grep -F '"event":"cluster_session_node_ready"' "${RUST_LOG_FILE}" \
  || { echo "the Rust node never reported ready"; cat "${RUST_LOG_FILE}"; exit 1; }

if ! python3 "${RUST_ROOT}/scripts/cluster-repeatable-read-client.py" \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --user appuser --password apppw \
  --database "${DATABASE}" --table "${SERVED_TABLE}" \
  --row-id "${ROW_ID}" \
  --racing-balance "${RACING_BALANCE}" \
  --losing-balance "${LOSING_BALANCE}" \
  | tee "${CLIENT_LOG}"; then
  echo "the repeatable-read proof failed" >&2
  tail -200 "${RUST_LOG_FILE}" >&2
  exit 1
fi
if ! grep -q '"event":"passed","command":"repeatable-read"' "${CLIENT_LOG}"; then
  echo "the repeatable-read proof did not report a pass" >&2
  exit 1
fi

# The loop closes the other direction: the CREATING Go TiDB sees the winner's
# row and never the loser's, which is what makes the 9007 a real refusal rather
# than a client-side story.
READBACK=$(go_sql -N -B -e \
  "SELECT balance FROM ${DATABASE}.${SERVED_TABLE} WHERE id = ${ROW_ID};")
if [[ "${READBACK}" != "${RACING_BALANCE}" ]]; then
  echo "the Go TiDB sees ${READBACK}, expected the winner's ${RACING_BALANCE}" >&2
  exit 1
fi

echo "repeatable read and start_ts conflict proven: inside BEGIN the Rust node \
re-read its own BEGIN-time balance ${ORIGINAL_BALANCE} while another connection had already \
committed ${RACING_BALANCE}, its COMMIT was refused with 9007 because the prewrite carried the \
BEGIN start_ts, and the creating Go TiDB reads back ${RACING_BALANCE}"
