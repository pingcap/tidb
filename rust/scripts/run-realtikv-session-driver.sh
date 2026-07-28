#!/usr/bin/env bash
#
# Live proof that the wide-SQL session driver runs on cluster storage.
#
# A real Go tidb-server (the playground's own) creates the database, the table
# and the rows. The Rust node is told only the PD address, the schema name and
# a SQL statement; it reads the catalog out of TiKV's `m` meta namespace, binds
# every loaded table to a transactional snapshot, and then plans and executes
# the statement with the SAME driver the in-process tier uses -- scan, WHERE,
# an expression over columns, ORDER BY.
#
# What this proves that `run-realtikv-catalog-load.sh` does not: the SQL is not
# a bounded single-relation read lowered into a coprocessor request. It is the
# ordinary session driver, reading rows through `KvTable` over
# `ClusterTableStorage`.
#
# NOTE: this script has NOT been run against a live cluster yet; the playground
# slot was held by another worker. Run it before treating the live claim as
# evidence.

set -euo pipefail

for prerequisite in tiup cargo curl nc pgrep awk grep; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing session-driver prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${SESSION_DRIVER_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "SESSION_DRIVER_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="session-driver-${$}-$(date +%s)"
PORT_OFFSET=${SESSION_DRIVER_PORT_OFFSET:-46000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "SESSION_DRIVER_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}.XXXXXX")
PLAYGROUND_LOG="${WORK_DIR}/playground.log"
PLAYGROUND_PID=""

cleanup() {
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  tiup clean "${TAG}" >/dev/null 2>&1 || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

wait_for_port() {
  local port=$1 deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "port ${port} never opened; see ${PLAYGROUND_LOG}" >&2
  return 1
}

echo "starting playground (tag ${TAG})"
tiup playground \
  --tag "${TAG}" \
  --pd.port "${PD_PORT}" \
  --db.port "${GO_SQL_PORT}" \
  --kv.port "${TIKV_SEED_PORT}" \
  --db.binpath.status "${GO_STATUS_PORT}" \
  --db 1 --kv 1 --pd 1 --tiflash 0 \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
wait_for_port "${PD_PORT}"
wait_for_port "${GO_SQL_PORT}"

echo "seeding the schema and rows with the playground's own TiDB"
"${MYSQL_CLIENT}" -h 127.0.0.1 -P "${GO_SQL_PORT}" -u root --protocol=TCP <<'SQL'
DROP DATABASE IF EXISTS driverdb;
CREATE DATABASE driverdb;
USE driverdb;
CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40);
SQL

echo "building the Rust session-driver smoke"
cargo build --manifest-path "${RUST_ROOT}/Cargo.toml" -p tidb-server --bin cluster-session-smoke

OUTPUT="${WORK_DIR}/smoke.out"
"${RUST_ROOT}/target/debug/cluster-session-smoke" \
  --pd "127.0.0.1:${PD_PORT}" \
  --schema driverdb \
  --sql "SELECT id, v + id * 2 FROM t WHERE v > 10 ORDER BY id DESC" \
  | tee "${OUTPUT}"

# 4 -> 40 + 8, 3 -> 30 + 6, 2 -> 20 + 4; row 1 is filtered out by the WHERE.
expect_row() {
  local pattern=$1
  if ! grep -Fq "${pattern}" "${OUTPUT}"; then
    echo "missing expected row: ${pattern}" >&2
    exit 1
  fi
}
expect_row "Int(4)	Int(48)"
expect_row "Int(3)	Int(36)"
expect_row "Int(2)	Int(24)"
if grep -Fq "Int(1)" "${OUTPUT}"; then
  echo "the WHERE clause did not filter row 1" >&2
  exit 1
fi

echo "session driver read the Go-written rows through cluster storage"
