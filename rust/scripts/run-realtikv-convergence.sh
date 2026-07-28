#!/usr/bin/env bash
#
# The deployment-ladder capstone: a stock MySQL client connects over TCP to the
# RUST node, authenticates as an account a GO TiDB created, and runs wide SQL
# -- a join, a subquery, an aggregate with GROUP BY, a window function, and an
# explicit BEGIN/INSERT/COMMIT -- against tables a GO TiDB created, with the
# rows living in real TiKV.
#
# What each earlier script proved separately, and this one proves at once:
#
#   run-realtikv-catalog-load.sh    the catalog comes from the cluster
#   run-realtikv-session-driver.sh  the session driver reads cluster storage
#   run-live-concurrent-auth-*.sh   the MySQL wire front end authenticates
#   run-realtikv-optimistic-2pc.sh  writes publish through the 2PC
#
# The last steps close the loop the other direction: the GO TiDB reads back the
# row the RUST node committed, then uses a TABLE the Rust node created through
# its own cluster DDL path.
#
# Usage: rust/scripts/run-realtikv-convergence.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing convergence prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${CONVERGENCE_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "CONVERGENCE_MYSQL_CLIENT must name an executable stock MySQL client" >&2
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
TAG="convergence-${$}-$(date +%s)"
PORT_OFFSET=${CONVERGENCE_PORT_OFFSET:-42000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "CONVERGENCE_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
RUST_SQL_PORT=$((4100 + PORT_OFFSET))

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}.XXXXXX")
PLAYGROUND_LOG="${WORK_DIR}/playground.log"
RUST_LOG_FILE="${WORK_DIR}/rust-node.log"
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
  # killed rather than stopped, so remove it explicitly: an 80 MB cluster per
  # run otherwise accumulates silently.
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

# Every wide-SQL statement runs AS THE GO-CREATED USER against the RUST node.
rust_sql() {
  "${MYSQL_CLIENT}" "${MYSQL_PLUGIN_ARGS[@]}" -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u appuser -papppw --protocol=TCP "$@"
}

echo "starting playground (tag ${TAG})"
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
wait_for_port "${PD_PORT}" "${PLAYGROUND_LOG}"
wait_for_port "${GO_SQL_PORT}" "${PLAYGROUND_LOG}"

echo "the Go TiDB creates the schema, the account and the rows"
go_sql <<'SQL'
DROP DATABASE IF EXISTS conv;
CREATE DATABASE conv;
USE conv;
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer BIGINT NOT NULL, amount BIGINT NOT NULL);
CREATE TABLE customers (id BIGINT PRIMARY KEY, region BIGINT NOT NULL);
INSERT INTO orders VALUES (1, 10, 100), (2, 10, 250), (3, 20, 70), (4, 20, 400), (5, 30, 55);
INSERT INTO customers VALUES (10, 1), (20, 1), (30, 2);
DROP USER IF EXISTS 'appuser'@'%';
CREATE USER 'appuser'@'%' IDENTIFIED WITH mysql_native_password BY 'apppw';
GRANT ALL PRIVILEGES ON conv.* TO 'appuser'@'%';
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

expect() {
  local label=$1 expected=$2 actual=$3
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${label}: expected [${expected}], got [${actual}]" >&2
    cat "${RUST_LOG_FILE}" >&2
    exit 1
  fi
  echo "  ok  ${label}: ${actual}"
}

echo "wide SQL, as the Go-created user, on the Rust node"

# A JOIN across two Go-created tables.
JOINED=$(rust_sql -N -B -e "
  USE conv;
  SELECT o.id, c.region FROM orders o JOIN customers c ON o.customer = c.id ORDER BY o.id;
" | tr '\n' ';')
expect "join" $'1\t1;2\t1;3\t1;4\t1;5\t2;' "${JOINED}"

# An aggregate with GROUP BY over the join.
GROUPED=$(rust_sql -N -B -e "
  USE conv;
  SELECT c.region, SUM(o.amount) FROM orders o JOIN customers c ON o.customer = c.id
  GROUP BY c.region ORDER BY c.region;
" | tr '\n' ';')
expect "aggregate/group by" $'1\t820;2\t55;' "${GROUPED}"

# A subquery.
SUBQUERY=$(rust_sql -N -B -e "
  USE conv;
  SELECT id FROM orders WHERE customer IN (SELECT id FROM customers WHERE region = 2);
" | tr '\n' ';')
expect "subquery" "5;" "${SUBQUERY}"

# A window function -- the surface the bounded node could never reach.
WINDOWED=$(rust_sql -N -B -e "
  USE conv;
  SELECT id, ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC)
  FROM orders ORDER BY id;
" | tr '\n' ';')
expect "window function" $'1\t2;2\t1;3\t2;4\t1;5\t1;' "${WINDOWED}"

# An explicit transaction: staged across statements, published once at COMMIT.
rust_sql -e "
  USE conv;
  BEGIN;
  INSERT INTO orders VALUES (6, 30, 999);
  COMMIT;
"

# The Go TiDB reads back what the Rust node committed. This is the loop closing.
READBACK=$(go_sql -N -B -e "SELECT id, customer, amount FROM conv.orders WHERE id = 6;" | tr '\n' ';')
expect "Go TiDB reads the Rust-committed row" $'6\t30\t999;' "${READBACK}"

echo "DDL through the Rust node: the client creates a table, the Go TiDB uses it"

# The client CREATEs a table through the RUST node. The catalog change goes
# through the same 2PC the rows do, so this is a real cluster catalog write.
rust_sql -e "USE conv; CREATE TABLE rust_made (id BIGINT PRIMARY KEY, note VARCHAR(32) NOT NULL);"

# The SAME connection uses the table it just created: the connection's tables
# are rebuilt because the node reloaded its catalog inline after the DDL.
SAME_CONN=$(rust_sql -N -B -e "
  USE conv;
  CREATE TABLE rust_made_two (id BIGINT PRIMARY KEY, v BIGINT);
  INSERT INTO rust_made_two VALUES (1, 11), (2, 22);
  SELECT id, v FROM rust_made_two ORDER BY id;
" | tr '\n' ';')
expect "same connection uses the table it created" $'1\t11;2\t22;' "${SAME_CONN}"

# The GO TiDB sees the Rust-created table AND writes it -- which it can only do
# if the stored TableInfo is one a real TiDB accepts.
GO_SEES=$(go_sql -N -B -e "SHOW TABLES IN conv LIKE 'rust_made';" | tr '\n' ';')
expect "Go TiDB sees the Rust-created table" "rust_made;" "${GO_SEES}"
go_sql -e "INSERT INTO conv.rust_made VALUES (1, 'written by go');"

# And the Rust node reads back the row the Go TiDB wrote into that table.
RUST_READS=$(rust_sql -N -B -e "SELECT id, note FROM conv.rust_made;" | tr '\n' ';')
expect "Rust node reads the Go-written row" $'1\twritten by go;' "${RUST_READS}"

# DROP through the Rust node, and the Go TiDB stops seeing the table.
rust_sql -e "DROP TABLE conv.rust_made_two;"
DROPPED=$(go_sql -N -B -e "SHOW TABLES IN conv LIKE 'rust_made_two';" | tr '\n' ';')
expect "Go TiDB no longer sees the dropped table" "" "${DROPPED}"

# What this mode still refuses, on purpose: a stored-schema change the cluster
# DDL path cannot express, and every account change.
if rust_sql -e "USE conv; ALTER TABLE rust_made ADD COLUMN extra BIGINT;" \
  >"${WORK_DIR}/ddl.out" 2>&1; then
  echo "ALTER was accepted, but this mode must refuse it" >&2
  exit 1
fi
grep -Fq "CREATE TABLE, DROP TABLE" "${WORK_DIR}/ddl.out" \
  || { echo "ALTER failed for the wrong reason:"; cat "${WORK_DIR}/ddl.out"; exit 1; }
echo "  ok  ALTER refused by name: $(tail -1 "${WORK_DIR}/ddl.out")"

if rust_sql -e "CREATE USER 'refused'@'%';" >"${WORK_DIR}/account.out" 2>&1; then
  echo "CREATE USER was accepted, but this mode must refuse it" >&2
  exit 1
fi
grep -Fq "cannot write them" "${WORK_DIR}/account.out" \
  || { echo "CREATE USER failed for the wrong reason:"; cat "${WORK_DIR}/account.out"; exit 1; }
echo "  ok  account change refused by name: $(tail -1 "${WORK_DIR}/account.out")"

echo "convergence proven: wide SQL, cluster storage, cluster accounts, one node"
