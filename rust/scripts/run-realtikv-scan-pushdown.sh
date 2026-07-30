#!/usr/bin/env bash
#
# The scan-pushdown differential: for one schema and one data set, a real GO
# TiDB and this RUST node are asked the SAME queries, and the ROWS THEY RETURN
# are compared byte for byte.
#
# Why rows and not plans. A pushed predicate is evaluated by TiKV, and the rows
# it rejects never reach this node -- the local filter that runs afterwards can
# only narrow the answer, never widen it. So a predicate lowered WRONG is a
# silently short answer, not a slow query, and a DAG-shape assertion would pass
# for a request that answers wrongly. Every case below therefore compares the
# actual result sets of the two nodes on the same cluster, where the tables,
# the rows and the statistics are literally the same bytes and the only
# variable is the engine.
#
# What is being proved. The scan lowering accepts, and TiKV therefore
# evaluates:
#
#   * a comparison between an integer column and an integer constant, over the
#     whole integer family -- BIGINT signed and unsigned, INT, SMALLINT,
#     TINYINT -- in either operand order;
#   * `IS NULL` and `IS NOT NULL`;
#   * `IN` and `NOT IN` over integer constants;
#   * `OR` and `NOT` composed over any of those.
#
# and it refuses, on purpose, two shapes Go itself does not send as written: a
# non-positive constant against an UNSIGNED column (Go's
# `refineArgsByUnsignedFlag` rewrites it) and a non-integer constant (Go's
# `RefineComparedConstant`). Those cases are in the table below too, because a
# refusal must also return the right rows -- it just returns them after the
# whole table has crossed the network.
#
# The wire receipt. Rows returned alone cannot show that anything was pushed:
# a node that ignored the predicate entirely and filtered locally returns the
# same rows. So each case is also run through `cluster-session-smoke --cop`,
# which prints the DAG's executor list and the number of rows the coprocessor
# actually sent to the node. That count, against the table's own row count, is
# the wire saving; a case that claims a Selection and still receives every row
# would be visible here and nowhere else.
#
# NOTE on the smoke driver: it is a second, separate Rust process against the
# same PD, because a served `--cluster-session` node has no surface that
# reports its coprocessor counters. The rows-returned comparison above uses the
# served node over the MySQL protocol; the wire receipt uses the driver. Both
# read the same cluster and run the same statement.
#
# Usage: rust/scripts/run-realtikv-scan-pushdown.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep awk; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing scan-pushdown-differential prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${SCAN_PUSHDOWN_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "SCAN_PUSHDOWN_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
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
TAG="scanpushdown-${$}-$(date +%s)"
PORT_OFFSET=${SCAN_PUSHDOWN_PORT_OFFSET:-43700}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "SCAN_PUSHDOWN_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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
FAILURES=0
ROW_DIVERGENCES=0

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

# The Rust node refuses a handshake that names an initial database, so every
# statement below selects the schema with `USE` instead of `-D`.
rust_sql() {
  "${MYSQL_CLIENT}" "${MYSQL_PLUGIN_ARGS[@]}" -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u root --protocol=TCP "$@"
}

check() {
  local label=$1
  shift
  if "$@"; then
    echo "  PASS  ${label}"
  else
    echo "  FAIL  ${label}" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

echo "starting playground (tag ${TAG})"
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
wait_for_port "${PD_PORT}" "${PLAYGROUND_LOG}"
wait_for_port "${GO_SQL_PORT}" "${PLAYGROUND_LOG}"

# One table, no secondary index. The absence is deliberate: with no index to
# range over, both engines read the whole relation, so the only thing that can
# reduce the rows crossing the wire is the pushed Selection -- which is exactly
# what this differential measures. Every integer width the lowering claims has
# a column, and each nullable column carries real NULLs so the three-valued
# cases are not vacuous.
echo "the Go TiDB creates the fixture, with auto-analyze off"
go_sql -e "SET GLOBAL tidb_enable_auto_analyze = OFF"
go_sql <<'SQL'
DROP DATABASE IF EXISTS pushdiff;
CREATE DATABASE pushdiff;
USE pushdiff;
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  sbig BIGINT,
  ubig BIGINT UNSIGNED,
  sint INT,
  tiny TINYINT,
  small SMALLINT,
  note VARCHAR(32)
);
SQL

echo "seeding 2000 rows into t (every 7th row NULL in the nullable columns)"
{
  echo "USE pushdiff;"
  echo "INSERT INTO t VALUES"
  for i in $(seq 1 2000); do
    sep=","
    [[ "${i}" -eq 2000 ]] && sep=";"
    if ((i % 7 == 0)); then
      # A NULL in every nullable integer column, so IS NULL / IS NOT NULL /
      # NOT IN over NULL have something to be UNKNOWN about.
      echo "(${i}, NULL, NULL, NULL, NULL, NULL, 'n${i}')${sep}"
    else
      echo "(${i}, $((i - 1000)), ${i}, $((i % 1000)), $((i % 100 - 50)), $((i % 3000 - 1500)), 'n${i}')${sep}"
    fi
  done
} | go_sql

TABLE_ROWS=$(go_sql -Nse "USE pushdiff; SELECT COUNT(*) FROM t")
echo "the fixture holds ${TABLE_ROWS} rows"

echo "building and starting the Rust node"
cargo build --manifest-path "${RUST_ROOT}/Cargo.toml" -p tidb-server \
  --bin tidb-server --bin cluster-session-smoke
"${RUST_ROOT}/target/debug/tidb-server" \
  --path "127.0.0.1:${PD_PORT}" \
  --port "${RUST_SQL_PORT}" \
  --cluster-session \
  --load-privileges \
  >"${RUST_LOG_FILE}" 2>&1 &
RUST_PID=$!
wait_for_port "${RUST_SQL_PORT}" "${RUST_LOG_FILE}"
deadline=$((SECONDS + 120))
until grep -F '"event":"cluster_session_node_ready"' "${RUST_LOG_FILE}" >/dev/null 2>&1; do
  if ((SECONDS >= deadline)); then
    echo "the Rust node never reported itself ready; see ${RUST_LOG_FILE}" >&2
    exit 1
  fi
  sleep 1
done
echo "the Rust node is ready on port ${RUST_SQL_PORT}"

# `rows <node_fn> <query>`: the query's whole result set, one row per line,
# tab-separated, in the query's own ORDER BY. Compared as text, so a differing
# VALUE and not merely a differing COUNT is a failure.
go_rows() {
  go_sql -N -B -e "USE pushdiff; $1"
}
rust_rows() {
  rust_sql -N -B -e "USE pushdiff; $1"
}

# `wire <query>`: the number of rows the coprocessor sent to the node for this
# statement, and whether the DAG carried a Selection. Read off the driver's own
# receipt lines rather than claimed.
WIRE_ROWS=""
WIRE_SHAPE=""
wire() {
  local out
  out=$("${RUST_ROOT}/target/debug/cluster-session-smoke" \
    --pd "127.0.0.1:${PD_PORT}" --schema pushdiff --cop --sql "$1" 2>&1) || {
    WIRE_ROWS="error"
    WIRE_SHAPE="error"
    printf '%s\n' "${out}" | tail -3 >&2
    return 0
  }
  WIRE_ROWS=$(printf '%s\n' "${out}" \
    | awk '/rows across the wire/ { print $NF }' | tail -1)
  WIRE_SHAPE=$(printf '%s\n' "${out}" \
    | awk -F 'coprocessor request: ' '/coprocessor request:/ { print $2; exit }')
  : "${WIRE_ROWS:=<none>}"
  : "${WIRE_SHAPE:=<no request>}"
}

has_selection() {
  case "${WIRE_SHAPE}" in
  *Selection*) return 0 ;;
  *) return 1 ;;
  esac
}
no_selection() {
  ! has_selection
}

# `compare <label> <query> <expect_selection>`: the headline check.
#
# Reads the DAG shape the last `wire` call recorded.
#
# `expect_selection` names which of three outcomes is intended, and all three
# are asserted, so a widening that silently stopped pushing and a refusal that
# silently started pushing are each a failure rather than an unnoticed change:
#
#   pushed    the whole predicate lowered: a Selection travels, and the
#             coprocessor sends exactly the rows the predicate selects;
#   refused   the scan is served remotely but the lowering refused this
#             predicate, so no Selection travels and the whole relation does;
#   noscan    the scan is not served remotely at all, because the PROJECTION
#             gate refused a column type (see `cop_scan`'s module doc) -- a
#             separate gap from the predicate lowering, named here so it cannot
#             be mistaken for one.
#
# A fourth argument, `expect_wire`, overrides the expected wire row count for a
# `pushed` case whose statement the smoke driver plans differently from the
# served node (a `LIMIT` the driver does not offer to the source). The rows the
# two nodes return are compared regardless, and that comparison -- not the
# counter -- is what proves the cap invariant.
compare() {
  local label=$1 query=$2 expect_selection=$3 expect_wire=${4:-}
  local go_out rust_out go_count
  go_out=$(go_rows "${query}")
  rust_out=$(rust_rows "${query}")
  go_count=$(printf '%s' "${go_out}" | grep -c . || true)
  wire "${query}"
  echo
  echo "--- ${label}"
  echo "    ${query}"
  printf '      rows: GO %s / RUST %s\n' \
    "${go_count}" "$(printf '%s' "${rust_out}" | grep -c . || true)"
  printf '      wire: %s rows of %s   dag: %s\n' \
    "${WIRE_ROWS}" "${TABLE_ROWS}" "${WIRE_SHAPE}"
  if [[ "${go_out}" != "${rust_out}" ]]; then
    echo "  FINDING  ${label}: the two nodes returned DIFFERENT ROWS" >&2
    diff <(printf '%s\n' "${go_out}") <(printf '%s\n' "${rust_out}") \
      | head -10 >&2
    ROW_DIVERGENCES=$((ROW_DIVERGENCES + 1))
    return
  fi
  check "${label}: both nodes returned the same rows, value for value" true
  case "${expect_selection}" in
    pushed)
      check "${label}: the DAG carried a Selection" has_selection
      # The predicate travelled, so fewer rows than the table holds crossed
      # the wire. This is the saving, and it is read off the counter.
      check "${label}: fewer rows crossed the wire than the table holds" \
        test "${WIRE_ROWS}" -lt "${TABLE_ROWS}"
      # And the coprocessor sent exactly the rows the predicate selects: the
      # remote filter is neither weaker (more rows) nor stronger (fewer).
      check "${label}: the coprocessor sent exactly the qualifying rows" \
        test "${WIRE_ROWS}" -eq "${expect_wire:-${go_count}}"
      ;;
    refused)
      check "${label}: the DAG carried NO Selection, as the refusal intends" \
        no_selection
      check "${label}: so the whole relation crossed the wire" \
        test "${WIRE_ROWS}" -eq "${TABLE_ROWS}"
      ;;
    noscan)
      check "${label}: no remote scan at all, as the projection gate intends" \
        test "${WIRE_SHAPE}" = "<no request>"
      ;;
    *)
      echo "  FAIL  ${label}: bad expect_selection ${expect_selection}" >&2
      FAILURES=$((FAILURES + 1))
      ;;
  esac
}

echo
echo "=== the comparison operators, over every integer width the lowering claims"
compare "signed BIGINT, column on the left" \
  "SELECT id, sbig FROM t WHERE sbig > 900 ORDER BY id" pushed
compare "signed BIGINT, literal on the left" \
  "SELECT id, sbig FROM t WHERE 900 < sbig ORDER BY id" pushed
compare "signed BIGINT, negative constant" \
  "SELECT id, sbig FROM t WHERE sbig <= -900 ORDER BY id" pushed
compare "unsigned BIGINT, positive constant" \
  "SELECT id, ubig FROM t WHERE ubig > 1900 ORDER BY id" pushed
compare "INT" \
  "SELECT id, sint FROM t WHERE sint < 20 ORDER BY id" pushed
compare "TINYINT, negative range" \
  "SELECT id, tiny FROM t WHERE tiny < -45 ORDER BY id" pushed
compare "SMALLINT" \
  "SELECT id, small FROM t WHERE small >= 1400 ORDER BY id" pushed
compare "equality and inequality" \
  "SELECT id FROM t WHERE sbig = 500 ORDER BY id" pushed
compare "not-equal keeps the NULL rows out" \
  "SELECT id FROM t WHERE sbig <> 500 ORDER BY id" pushed

echo
echo "=== IS NULL, IS NOT NULL, IN, NOT IN"
compare "IS NULL" \
  "SELECT id FROM t WHERE sbig IS NULL ORDER BY id" pushed
compare "IS NOT NULL" \
  "SELECT id FROM t WHERE ubig IS NOT NULL ORDER BY id" pushed
compare "IN over a constant list" \
  "SELECT id FROM t WHERE sbig IN (-999, 0, 500, 999) ORDER BY id" pushed
compare "IN with a duplicated constant" \
  "SELECT id FROM t WHERE sbig IN (500, 500, 501) ORDER BY id" pushed
compare "NOT IN is UNKNOWN for a NULL column, so those rows are absent" \
  "SELECT id FROM t WHERE sbig NOT IN (-999, 0, 500) ORDER BY id" pushed

echo
echo "=== OR and NOT composition"
compare "a two-branch OR" \
  "SELECT id FROM t WHERE sbig = 500 OR sbig = 501 ORDER BY id" pushed
compare "a long OR chain folds left and still answers the same rows" \
  "SELECT id FROM t WHERE sbig = 1 OR sbig = 2 OR sbig = 3 OR sbig = 4 ORDER BY id" pushed
compare "OR mixing a comparison with IS NULL: TRUE beats UNKNOWN" \
  "SELECT id FROM t WHERE sbig > 998 OR sbig IS NULL ORDER BY id" pushed
compare "OR over two different columns" \
  "SELECT id FROM t WHERE sbig < -998 OR small > 1498 ORDER BY id" pushed
compare "NOT over a comparison" \
  "SELECT id FROM t WHERE NOT sbig > -995 ORDER BY id" pushed
compare "NOT over an IN" \
  "SELECT id FROM t WHERE NOT sbig IN (-999, -998) ORDER BY id" pushed
compare "NOT over an IS NULL is IS NOT NULL" \
  "SELECT id FROM t WHERE NOT sbig IS NULL ORDER BY id" pushed
compare "a conjunction of two pushed predicates" \
  "SELECT id FROM t WHERE sbig > 900 AND small < 1000 ORDER BY id" pushed
compare "a pushed disjunction beside a pushed comparison" \
  "SELECT id FROM t WHERE (sbig = 500 OR sbig = 900) AND ubig > 100 ORDER BY id" pushed

echo
echo "=== the deliberate refusals: right rows, no Selection"
# Go's `refineArgsByUnsignedFlag` rewrites a non-positive constant against an
# UNSIGNED column into a comparison whose truth value it already knows. This
# lowering does not implement that rewrite, so it refuses -- and must still
# answer correctly.
compare "unsigned column against a negative constant" \
  "SELECT id FROM t WHERE ubig > -1 ORDER BY id" refused
compare "unsigned column against zero" \
  "SELECT id FROM t WHERE ubig >= 0 ORDER BY id" refused
# Go's `RefineComparedConstant` rewrites a non-integer constant, so the form
# written here is not the form Go sends.
compare "integer column against a string constant" \
  "SELECT id FROM t WHERE sbig = '500' ORDER BY id" refused
compare "integer column against a fractional constant" \
  "SELECT id FROM t WHERE sbig > 900.5 ORDER BY id" refused
# A non-integer column is outside the ETInt family this lowering speaks.
compare "a VARCHAR column with its collation" \
  "SELECT id FROM t WHERE note = 'n500' ORDER BY id" noscan
# An expression over a column is not a predicate shape at all, so it never
# even reaches the lowering: it stays in the Selection above the scan.
compare "an expression over a column" \
  "SELECT id FROM t WHERE sbig + 1 > 999 ORDER BY id" refused
# A column-to-column comparison: no constant operand, so no description.
compare "a column-to-column comparison" \
  "SELECT id FROM t WHERE sbig > small ORDER BY id" refused

echo
echo "=== the cap-and-predicate invariant, on rows"
# A `LIMIT` may only travel with a predicate that travelled WHOLE. When part
# of it stayed behind, TiKV would count its cap against a weaker filter and the
# local pass would remove some of those rows -- a silently short answer. Both
# spellings must return the same rows the Go node returns.
compare "LIMIT over a fully pushed predicate" \
  "SELECT id FROM t WHERE sbig > 900 ORDER BY id LIMIT 5" pushed 86
compare "LIMIT over a predicate only half of which lowers" \
  "SELECT id FROM t WHERE sbig > 900 AND sbig = '950' ORDER BY id LIMIT 5" pushed 86

echo
echo "=== what is NOT pushed, and why (not a failure, a scope statement)"
cat <<'NOTE'
  Every one of the 55 expressions in Go's `TestExprPushDownToTiKV` pushed
  table is still refused, and none of them is reachable by widening the
  predicate set: every row of that table is a BUILTIN FUNCTION CALL
  (sin(i), date_format(d, s), conv(s, i, i), ...), not a comparison, a
  logical connective, IS NULL or IN. Reaching them needs a
  name-to-ScalarFuncSig resolution catalog and the cast-inserting type
  inference Go's `getFunction` performs; `ScalarFunction` in this tree
  carries no resolved signature at all. That gap is tracked by the
  `#[ignore]`d `tikv_pushes_what_go_pushes` and pinned by the running
  `every_go_pushable_expression_is_still_refused_here`, both in
  `tidb_executor::scan_pushdown`.

  Also still refused, each for a stated reason rather than an omission:
  string, decimal and temporal comparisons (their constants need the cast
  and collation resolution Go applies before conversion), and LIKE.
NOTE

echo
if [[ "${ROW_DIVERGENCES}" -gt 0 ]]; then
  echo "${ROW_DIVERGENCES} case(s) where the two nodes returned DIFFERENT ROWS -- see the FINDING lines above" >&2
fi
if [[ "${FAILURES}" -eq 0 && "${ROW_DIVERGENCES}" -eq 0 ]]; then
  echo "the scan-pushdown differential passed"
else
  echo "the scan-pushdown differential had ${FAILURES} failure(s) and ${ROW_DIVERGENCES} row divergence(s)" >&2
  exit 1
fi
