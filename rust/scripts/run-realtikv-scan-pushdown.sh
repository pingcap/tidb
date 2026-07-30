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
#   * `OR` and `NOT` composed over any of those;
#   * a BUILTIN CALL of the math family -- `MOD`, `ROUND`, `SIN`, `ASIN`,
#     `COS`, `ACOS`, `ATAN`, `ATAN2`, `COT`, `POW`, `POWER`, `PI` -- resolved
#     to Go's own `tipb.ScalarFuncSig` by `tidb_expr::pushdown_catalog`, with
#     the implicit `CastIntAsReal` wrappers Go's `newBaseBuiltinFuncWithTp`
#     inserts. This is the family whose signature depends on the ARGUMENT
#     TYPES, so a signature resolved from the wrong type is the failure mode
#     the wire-count equality below exists to catch;
#   * a BUILTIN CALL of the STRING family -- `CHAR_LENGTH`, `UPPER`, `LOWER`,
#     `SUBSTR`/`SUBSTRING`/`MID` and `CONV` -- whose signature Go chooses by
#     `types.IsBinaryStr` on the first argument, and whose TiPB leaves carry
#     the column's own collation id. The fixture holds the SAME multibyte
#     bytes as a `VARCHAR` and as a `VARBINARY`, and the two answer DIFFERENT
#     `CHAR_LENGTH`s, so resolving the wrong spelling changes the rows the
#     coprocessor sends -- caught by the wire-count equality and by nothing
#     else. String COMPARISON is deliberately still refused; see that
#     section's comment for the `deriveCollation` function this tier cannot
#     follow.
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
  # `KEEP_LOGS=1` preserves the two node logs, which are the only evidence of a
  # startup failure -- without it a node that never opened its port leaves
  # nothing behind to read.
  if [[ -n "${KEEP_LOGS:-}" ]]; then
    echo "logs kept in ${WORK_DIR}" >&2
  else
    rm -rf "${WORK_DIR}"
  fi
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
  note VARCHAR(32),
  -- The string family's two spellings, over columns that differ in NOTHING
  -- but their collation, exactly as Go's own `stringColumn` and
  -- `binaryStringColumn` do.
  bnote VARBINARY(32),
  -- The same MULTIBYTE text in both spellings. This pair is the whole point:
  -- `CHAR_LENGTH` counts CHARACTERS over `mnote` and BYTES over `bmnote`, so
  -- a signature resolved from the wrong collation returns a different answer
  -- for the same bytes -- silently, and with no plan difference to see.
  mnote VARCHAR(16),
  bmnote VARBINARY(64),
  -- A case-INSENSITIVE collation, which is a third collator again and the one
  -- a `binary`-versus-`utf8mb4_bin` guess would silently replace.
  cinote VARCHAR(32) COLLATE utf8mb4_general_ci,
  -- Hex digits, so `CONV` has something to convert.
  hexnote VARCHAR(8),
  -- A family the projection gate still refuses, so the difference between a
  -- refused PREDICATE and a refused SCAN stays observable now that VARCHAR has
  -- moved to the served side.
  amount DECIMAL(10, 2)
);
SQL

echo "seeding 2000 rows into t (every 7th row NULL in the nullable columns)"
{
  echo "USE pushdiff;"
  echo "INSERT INTO t VALUES"
  for i in $(seq 1 2000); do
    sep=","
    [[ "${i}" -eq 2000 ]] && sep=";"
    # One to five TWO-byte characters, plus zero or one ASCII character.
    # Character count is `c + a` and byte count is `2c + a`, which differ in
    # PARITY for every odd `c` -- a three-byte character would have kept the
    # two counts in step (3c and c share a parity) and made every
    # characters-versus-bytes case below vacuously equal, which is exactly what
    # the paired check after them exists to catch.
    multi=""
    for _ in $(seq 1 $((i % 5 + 1))); do multi="${multi}é"; done
    ((i % 2 == 0)) && multi="${multi}z"
    strings="'n${i}', 'n${i}', '${multi}', '${multi}', 'MiXeD${i}', '$(printf '%X' $((i % 4096)))', ${i}.25"
    if ((i % 7 == 0)); then
      # A NULL in every nullable integer column, so IS NULL / IS NOT NULL /
      # NOT IN over NULL have something to be UNKNOWN about.
      echo "(${i}, NULL, NULL, NULL, NULL, NULL, ${strings})${sep}"
    else
      echo "(${i}, $((i - 1000)), ${i}, $((i % 1000)), $((i % 100 - 50)), $((i % 3000 - 1500)), ${strings})${sep}"
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

# `local_eval_divergence <label> <predicate_query> <projection_query>`: a
# statement whose two nodes return different rows because THIS NODE'S LOCAL
# BUILTIN disagrees with TiDB -- not because the push-down lowered anything
# wrongly.
#
# The distinction is not a matter of opinion, and this function proves it with
# two independent facts:
#
#   1. The coprocessor sent EXACTLY the rows the GO node returns. TiKV
#      evaluated the pushed signature and answered TiDB's answer, so the
#      signature that travelled was the right one. If the lowering had chosen
#      the wrong spelling, this count would be the WRONG one instead.
#   2. The same builtin written as a PROJECTION -- no predicate, nothing
#      pushed, the local evaluator and nothing else -- diverges the same way.
#
# Together those say: the wire is right, the local re-application is wrong, and
# the bug is upstream of push-down in `tidb_expr`'s builtin dispatch, which
# threads no collation to the string signatures and so evaluates every
# `SUBSTRING` with UTF-8 CHARACTER semantics even for a `binary` argument
# (Go has separate `builtinSubstring2ArgsSig`/`3ArgsSig` that slice BYTES).
# Recorded here rather than asserted away, because this differential is the
# only place it is visible.
local_eval_divergence() {
  local label=$1 predicate_query=$2 projection_query=$3
  local go_out rust_out go_count go_proj rust_proj
  go_out=$(go_rows "${predicate_query}")
  rust_out=$(rust_rows "${predicate_query}")
  go_count=$(printf '%s' "${go_out}" | grep -c . || true)
  go_proj=$(go_rows "${projection_query}")
  rust_proj=$(rust_rows "${projection_query}")
  wire "${predicate_query}"
  echo
  echo "--- ${label}  (KNOWN LOCAL-EVALUATOR DIVERGENCE)"
  echo "    ${predicate_query}"
  printf '      rows: GO %s / RUST %s\n' \
    "${go_count}" "$(printf '%s' "${rust_out}" | grep -c . || true)"
  printf '      wire: %s rows of %s   dag: %s\n' \
    "${WIRE_ROWS}" "${TABLE_ROWS}" "${WIRE_SHAPE}"
  printf '      projection control: GO %s / RUST %s\n' \
    "$(printf '%s' "${go_proj}" | head -1)" \
    "$(printf '%s' "${rust_proj}" | head -1)"
  check "${label}: the DAG carried a Selection" has_selection
  # THE PUSH IS FAITHFUL: the coprocessor sent TiDB's own answer.
  check "${label}: the coprocessor sent exactly the rows TiDB selects, \
so the signature that travelled is the right one" \
    test "${WIRE_ROWS}" -eq "${go_count}"
  # AND THE LOCAL BUILTIN IS NOT: the same expression with nothing pushed
  # diverges identically, which is what makes this not a push-down defect.
  check "${label}: the same builtin as a PROJECTION diverges too, \
so the gap is the local evaluator and not the lowering" \
    test "${go_proj}" != "${rust_proj}"
}

# `error_case <label> <query> <known_rust_code>`: a statement that FAILS.
#
# A pushed builtin that errors locally but not remotely, or the reverse, is a
# divergence a rows comparison cannot see, because a statement that errors
# returns no rows on either side. `COT(0)` is the case in the math family: MySQL
# and TiDB raise ER_DATA_OUT_OF_RANGE (1690) rather than returning NULL.
#
# TWO checks, deliberately separate, because they are different facts:
#
#   1. BOTH nodes fail. This is the safety property: whichever side evaluates
#      the expression, the statement does not quietly succeed with a different
#      row set. A pushed predicate is evaluated by TiKV, so this is also the
#      only place TiKV's own error is observed.
#   2. This node's error NUMBER is the one currently known. TiDB says 1690;
#      this node does not, and the `known_rust_code` argument records what it
#      does say so the gap is pinned rather than asserted away. The gap is NOT
#      introduced by push-down -- the projection control below shows the same
#      number with no scan involved -- and its two halves are separately owned:
#      the builtin's own error mapping (`Eval(FloatOverflow)` where TiDB raises
#      ER_DATA_OUT_OF_RANGE), and `cop_scan`'s handling of a coprocessor error
#      response (`Unsupported("table bytes failed to decode")` where TiKV did
#      report the error). Both are outside the push-down catalog.
error_case() {
  local label=$1 query=$2 known_rust_code=$3
  local go_out rust_out go_code rust_code
  go_out=$(go_sql -N -B -e "USE pushdiff; ${query}" 2>&1) || true
  rust_out=$(rust_sql -N -B -e "USE pushdiff; ${query}" 2>&1) || true
  go_code=$(printf '%s\n' "${go_out}" | sed -n 's/^ERROR \([0-9]*\).*/\1/p' | head -1)
  rust_code=$(printf '%s\n' "${rust_out}" | sed -n 's/^ERROR \([0-9]*\).*/\1/p' | head -1)
  echo
  echo "--- ${label}"
  echo "    ${query}"
  printf '      GO   error %s: %s\n' "${go_code:-<none>}" \
    "$(printf '%s' "${go_out}" | head -1)"
  printf '      RUST error %s: %s\n' "${rust_code:-<none>}" \
    "$(printf '%s' "${rust_out}" | head -1)"
  check "${label}: the Go node raises an error" test -n "${go_code}"
  check "${label}: and so does this node, rather than answering rows" \
    test -n "${rust_code}"
  check "${label}: this node's error number is the known ${known_rust_code}, \
not yet TiDB's ${go_code}" \
    test "${rust_code}" = "${known_rust_code}"
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
echo "=== the math builtin family, newly pushed through the push-down catalog"
# Each of these is a BUILTIN CALL evaluated by TiKV as a Selection condition,
# resolved to Go's own tipb.ScalarFuncSig by `tidb_expr::pushdown_catalog`. The
# `pushed` expectation asserts `wire == qualifying rows`, so a remote filter
# that is weaker OR stronger than the local one fails here -- which is the only
# way a wrongly-resolved signature can be caught, because rows returned alone
# cannot distinguish "TiKV filtered correctly" from "TiKV filtered wrongly and
# the local pass repaired it".
#
# MOD over signed and unsigned columns, which is four different signatures on
# Go's side (ModIntSignedSigned / ModIntUnsignedSigned) selected by the
# UNSIGNED flag alone -- the same argument value, a different function.
compare "MOD over a signed BIGINT column" \
  "SELECT id FROM t WHERE mod(sbig, 100) ORDER BY id" pushed
compare "MOD over an UNSIGNED BIGINT column picks the other signature" \
  "SELECT id FROM t WHERE mod(ubig, 100) ORDER BY id" pushed
compare "MOD between two columns" \
  "SELECT id FROM t WHERE mod(sbig, sint) ORDER BY id" pushed
# ROUND over an integer column keeps the integer domain (RoundInt), and over a
# narrow one too.
compare "ROUND over a signed BIGINT column" \
  "SELECT id FROM t WHERE round(sbig) ORDER BY id" pushed
compare "ROUND over a SMALLINT column" \
  "SELECT id FROM t WHERE round(small) ORDER BY id" pushed
# The trigonometric family, whose arguments Go wraps in CastIntAsReal. ACOS is
# the NULL-domain case: it is NULL for every argument outside [-1, 1] and
# exactly 0 (false) at 1, so only `tiny IN (-1, 0)` qualifies -- if TiKV and
# this engine disagreed about the out-of-domain result being NULL rather than an
# error or a number, the wire count would not equal the row count.
compare "ACOS over a TINYINT column, NULL outside its domain" \
  "SELECT id, tiny FROM t WHERE acos(tiny) ORDER BY id" pushed
compare "ASIN over a TINYINT column" \
  "SELECT id, tiny FROM t WHERE asin(tiny) ORDER BY id" pushed
compare "SIN over a TINYINT column, zero only at zero" \
  "SELECT id, tiny FROM t WHERE sin(tiny) ORDER BY id" pushed
compare "COS over a TINYINT column, never exactly zero" \
  "SELECT id, tiny FROM t WHERE cos(tiny) ORDER BY id" pushed
compare "ATAN with one argument" \
  "SELECT id, tiny FROM t WHERE atan(tiny) ORDER BY id" pushed
# `PI()` takes no argument, so it is truthy for every row: a Selection travels
# and rejects nothing, and there is no row saving to assert. The `pushed` case
# above would fail its wire-saving check for a reason that is not a defect, so
# this one is spelled out: the same rows, and a Selection in the DAG.
PI_QUERY="SELECT id FROM t WHERE pi() ORDER BY id"
wire "${PI_QUERY}"
echo
echo "--- PI(), a constant predicate that travels and rejects nothing"
echo "    ${PI_QUERY}"
printf '      wire: %s rows of %s   dag: %s\n' \
  "${WIRE_ROWS}" "${TABLE_ROWS}" "${WIRE_SHAPE}"
check "PI(): both nodes returned the same rows, value for value" \
  test "$(go_rows "${PI_QUERY}")" = "$(rust_rows "${PI_QUERY}")"
check "PI(): the DAG carried a Selection" has_selection
check "PI(): which rejects nothing, so the whole relation crosses the wire" \
  test "${WIRE_ROWS}" -eq "${TABLE_ROWS}"
compare "ATAN2 over two columns" \
  "SELECT id FROM t WHERE atan2(tiny, small) ORDER BY id" pushed
# The base is the UNSIGNED column, which is never zero here, and the exponent
# the TINYINT one: `POW(0, <negative>)` and a large exponent are both out of
# DOUBLE range, which TiDB raises as an error rather than returning -- that is
# the `COT(0)` case again and it has its own section below.
compare "POW over two integer columns, one of them UNSIGNED" \
  "SELECT id, tiny FROM t WHERE pow(ubig, tiny) ORDER BY id" pushed
# ... but POW with an integer CONSTANT exponent refuses, on purpose: the Real
# slot needs a cast, and Go folds `CAST(2 AS REAL)` at plan time and sends a
# Float64 literal, which this tier does not encode. Sending
# `CastIntAsReal(Int64(2))` instead would be a different expression tree from
# the one Go sends, so the catalog refuses and the conjunct is applied locally.
compare "POW with an integer constant exponent, refused by the constant rule" \
  "SELECT id, tiny FROM t WHERE pow(tiny, 2) ORDER BY id" refused
# The composition rules apply to a pushed builtin exactly as to a comparison.
compare "NOT over a pushed builtin" \
  "SELECT id FROM t WHERE NOT mod(sbig, 100) ORDER BY id" pushed
compare "OR mixing a pushed builtin with a comparison" \
  "SELECT id FROM t WHERE mod(sbig, 1000) = 0 OR sbig = 500 ORDER BY id" refused
compare "a pushed builtin beside a pushed comparison" \
  "SELECT id FROM t WHERE acos(tiny) AND sbig > -900 ORDER BY id" pushed
# And the cap-and-predicate invariant with a builtin in the pushed half. The
# smoke driver does not offer the source a LIMIT, so the wire count is the
# UNLIMITED qualifying count; it is read off the Go node rather than guessed,
# and the rows the two nodes return -- not this counter -- are what prove the
# cap invariant.
ACOS_ROWS=$(go_sql -Nse "USE pushdiff; SELECT COUNT(*) FROM t WHERE acos(tiny)")
echo "  (acos(tiny) qualifies ${ACOS_ROWS} of ${TABLE_ROWS} rows)"
compare "LIMIT over a fully pushed builtin predicate" \
  "SELECT id FROM t WHERE acos(tiny) ORDER BY id LIMIT 3" pushed "${ACOS_ROWS}"
compare "LIMIT over a builtin whose sibling conjunct did not lower" \
  "SELECT id FROM t WHERE acos(tiny) AND sbig = '-950' ORDER BY id LIMIT 3" pushed "${ACOS_ROWS}"

echo
echo "=== the string family, whose signature is chosen by COLLATION"
# Every case below wraps the string builtin in `MOD(..., 2)` so the predicate
# is SELECTIVE: a truth test on the string itself is false for every row here
# and would prove only that a Selection travelled, not that it filtered the
# rows the query selects. With `MOD` on top, `wire == qualifying rows` is a
# real equality over a real split of the table.
#
# THE TRAP THIS SECTION EXISTS FOR. `CHAR_LENGTH` over `mnote` counts
# CHARACTERS and over `bmnote` counts BYTES, and the two columns hold THE SAME
# BYTES. If the lowering resolved `CharLengthUTF8` for a binary column (or the
# reverse), TiKV would answer a different length, `MOD(..., 2)` would flip for
# most rows, and the coprocessor would send the wrong rows -- which no local
# pass can repair, because the rows it dropped never crossed the network. The
# `pushed` expectation asserts `wire == the Go node's own qualifying count`,
# so that is exactly what fails.
compare "CHAR_LENGTH over a utf8mb4 column (CharLengthUTF8)" \
  "SELECT id FROM t WHERE mod(char_length(mnote), 2) ORDER BY id" pushed
compare "CHAR_LENGTH over the SAME BYTES as VARBINARY (CharLength)" \
  "SELECT id FROM t WHERE mod(char_length(bmnote), 2) ORDER BY id" pushed
# And the two really do disagree, so the pair above is not two names for one
# answer. If this check ever passed by equality, the trap cases would be
# vacuous and every collation guess would look correct.
#
# The comparison is between the two ROW SETS and not their counts: over this
# fixture the two spellings happen to select the same NUMBER of rows while
# selecting different ones, so a count comparison would report the trap as
# vacuous when it is not. What has to be true is that swapping the spelling
# changes WHICH rows come back.
CHARS_SET=$(go_rows "SELECT id FROM t WHERE mod(char_length(mnote), 2) ORDER BY id")
BYTES_SET=$(go_rows "SELECT id FROM t WHERE mod(char_length(bmnote), 2) ORDER BY id")
echo
echo "--- the two CHAR_LENGTH spellings over identical bytes"
printf '      characters: %s rows    bytes: %s rows    differing ids: %s\n' \
  "$(printf '%s' "${CHARS_SET}" | grep -c . || true)" \
  "$(printf '%s' "${BYTES_SET}" | grep -c . || true)" \
  "$(comm -3 <(printf '%s\n' "${CHARS_SET}") <(printf '%s\n' "${BYTES_SET}") \
     | grep -c . || true)"
check "the UTF-8 and binary CHAR_LENGTH select DIFFERENT rows, \
so resolving the wrong one is observable" \
  test "${CHARS_SET}" != "${BYTES_SET}"

compare "CHAR_LENGTH over an ASCII utf8mb4 column" \
  "SELECT id FROM t WHERE mod(char_length(note), 2) ORDER BY id" pushed
compare "CHAR_LENGTH over a VARBINARY column" \
  "SELECT id FROM t WHERE mod(char_length(bnote), 2) ORDER BY id" pushed
compare "CHAR_LENGTH over a case-insensitive collation" \
  "SELECT id FROM t WHERE mod(char_length(cinote), 2) ORDER BY id" pushed
# UPPER / LOWER, whose result carries the ARGUMENT's collation -- so the
# CHAR_LENGTH on top of them must still resolve the same spelling it would
# have resolved on the column itself.
compare "UPPER, whose result keeps its argument's collation" \
  "SELECT id FROM t WHERE mod(char_length(upper(cinote)), 2) ORDER BY id" pushed
compare "LOWER over a utf8mb4 column" \
  "SELECT id FROM t WHERE mod(char_length(lower(mnote)), 2) ORDER BY id" pushed
compare "LOWER over a VARBINARY column, the binary spelling" \
  "SELECT id FROM t WHERE mod(char_length(lower(bmnote)), 2) ORDER BY id" pushed
# SUBSTR in both arities and under all three of its registered names, with the
# UTF-8 spelling counting characters -- so `SUBSTR(mnote, 2)` drops one CJK
# CHARACTER and not one byte.
compare "SUBSTR with three arguments over multibyte text" \
  "SELECT id FROM t WHERE mod(char_length(substr(mnote, 1, 2)), 2) ORDER BY id" pushed
compare "SUBSTRING with two arguments" \
  "SELECT id FROM t WHERE mod(char_length(substring(mnote, 2)), 2) ORDER BY id" pushed
# SUBSTRING over a BINARY argument is the one row where the two nodes differ,
# and the difference is NOT in the push: TiKV answered TiDB's own answer.
# `tidb_expr`'s builtin dispatch threads no collation to `substring`, so it
# evaluates `builtinSubstring3ArgsUTF8Sig` for every argument -- slicing
# CHARACTERS where Go's `builtinSubstring3ArgsSig` slices BYTES.
local_eval_divergence "MID over a VARBINARY column, the binary spelling" \
  "SELECT id FROM t WHERE mod(char_length(mid(bmnote, 2, 5)), 2) ORDER BY id" \
  "SELECT char_length(mid(bmnote, 2, 5)) FROM t WHERE id = 3"
# CONV, the one collation-blind row: one signature for either spelling, and a
# result in the CONNECTION charset rather than the argument's.
#
# CONV returns a STRING, so `MOD(CONV(...), 2)` is not the spelling used for
# the families above: no `MOD` row takes an `ETString` argument -- so the
# catalog would refuse the conjunct -- and this node's local `MOD` raises
# `Unsupported("string operand")` for one, so that form would fail rather than
# measure anything. `CHAR_LENGTH` on top keeps the whole expression inside the
# catalog and stays selective, because a base-2 conversion's length varies with
# the value.
compare "CONV from base 16 to base 2, under CHAR_LENGTH" \
  "SELECT id FROM t WHERE mod(char_length(conv(hexnote, 16, 2)), 2) ORDER BY id" pushed
compare "CONV over a VARBINARY column resolves the same single signature" \
  "SELECT id FROM t WHERE mod(char_length(conv(bnote, 36, 2)), 2) ORDER BY id" pushed
# The composition rules apply to a string builtin exactly as to a math one.
compare "NOT over a pushed string builtin" \
  "SELECT id FROM t WHERE NOT mod(char_length(mnote), 2) ORDER BY id" pushed
compare "a string builtin beside a pushed integer comparison" \
  "SELECT id FROM t WHERE mod(char_length(mnote), 2) AND sbig > 0 ORDER BY id" pushed

echo
echo "=== the string family's deliberate refusals"
# A NON-string argument in a string slot needs Go's WrapWithCastAsString,
# whose target FieldType takes its flen from a per-source-type table. The
# catalog has no row for it, so the conjunct stays above the scan.
compare "CHAR_LENGTH over an integer column, the cast this tier will not build" \
  "SELECT id FROM t WHERE mod(char_length(sbig), 2) ORDER BY id" refused
compare "UPPER over an integer column" \
  "SELECT id FROM t WHERE mod(char_length(upper(sbig)), 2) ORDER BY id" refused
# A string CONSTANT argument is not describable either: the catalog encodes
# only integer literals, so a string literal never reaches the wire.
compare "a string constant in the string slot" \
  "SELECT id FROM t WHERE mod(char_length(substr('abcdef', sbig)), 2) ORDER BY id" refused
# ... whereas an integer COLUMN in the integer slot of the same call needs no
# cast at all and does push, which is what makes the refusal above specific to
# the string leaf rather than to nested calls in general.
compare "an integer column as SUBSTR's position argument" \
  "SELECT id FROM t WHERE mod(char_length(substr(mnote, tiny)), 2) ORDER BY id" pushed
# String COMPARISON is refused on purpose, and this is the loudest refusal in
# the change. Go picks the comparison's collation with `deriveCollation`'s
# ast.EQ case -> CheckAndDeriveCollationFromExprs -> inferCollation, which
# AGGREGATES coercibility and repertoire across BOTH operands and can pick a
# collation belonging to NEITHER of them. `PbScalar` carries no coercibility at
# all, so this tier cannot follow that function, and a comparison sent with a
# guessed collation returns wrong rows for every case-insensitive column.
compare "a VARCHAR column against a string constant, refused for its collation" \
  "SELECT id FROM t WHERE note = 'n500' ORDER BY id" refused
compare "a case-INSENSITIVE column against a string constant" \
  "SELECT id FROM t WHERE cinote = 'mixed500' ORDER BY id" refused
compare "two string columns compared to each other" \
  "SELECT id FROM t WHERE note = bnote ORDER BY id" refused

echo
echo "=== the error case of the math family, on both nodes"
# `COT(0)` is out of DOUBLE range, which TiDB reports as an ERROR and not as
# NULL. The predicate is pushed, so the expression is evaluated by TiKV -- and
# the error must still reach the client with the same number.
error_case "COT(0) is an error, not NULL, and the pushed form still says so" \
  "SELECT id FROM t WHERE cot(tiny) ORDER BY id" 1105
# The same expression outside any pushed predicate, as the control: the error
# number gap is the builtin's own and not something push-down introduced.
error_case "COT(0) written as a projection, the control" \
  "SELECT cot(0)" 1105

echo
echo "=== builtins the catalog deliberately does not hold: right rows, no Selection"
# TAN is absent from Go's own TiKV whitelist (the source comment cites Rust's
# LLVM math precision differing from cmath), so pushing it would be a bug and
# not merely a widening -- this case is live coverage for that.
compare "TAN, which Go itself refuses to push to TiKV" \
  "SELECT id FROM t WHERE tan(sbig) ORDER BY id" refused
# ABS is on Go's whitelist but has no catalog row yet, so it refuses: a gap,
# named, that still answers correctly.
compare "ABS, on Go's whitelist but not yet in the catalog" \
  "SELECT id FROM t WHERE abs(sbig) ORDER BY id" refused
# ROUND with a frac argument is one of the signatures the TiKV switch excludes.
compare "ROUND with a frac argument, which the TiKV switch excludes" \
  "SELECT id FROM t WHERE round(sbig, 1) ORDER BY id" refused
# An argument that is not a column, an integer constant, or a nested catalog
# call is not describable, so the whole conjunct stays above the scan.
compare "a builtin over an arithmetic argument" \
  "SELECT id FROM t WHERE sin(sbig + 1) ORDER BY id" refused
compare "a builtin over a non-integer constant argument" \
  "SELECT id FROM t WHERE mod(sbig, 2.5) ORDER BY id" refused

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
# A DECIMAL column is still outside the projection gate, so a scan that must
# emit one is not served remotely at all -- a different gap from the predicate
# lowering, and named here so it cannot be mistaken for one.
compare "a DECIMAL column in the projection, refused by the projection gate" \
  "SELECT id, amount FROM t WHERE sbig > 900 ORDER BY id" noscan
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
  Nineteen of the 50 expressions in Go's `TestExprPushDownToTiKV` pushed table
  now push here:

    * the math family -- sin, asin, cos, acos, atan, cot, atan2, pi, round,
      mod, pow, power;
    * the string family -- conv, substr/substring/mid, char_length, upper,
      lower -- whose signature Go chooses by `types.IsBinaryStr` on the first
      argument and whose leaves carry the column's own collation id.

  The signature each resolves to is Go's own, chosen by
  `tidb_expr::pushdown_catalog` from the argument types exactly as Go's
  per-family `getFunction` does, and the rows-versus-wire cases above are the
  live proof that TiKV's evaluation of them agrees with this engine's.

  The other 31 are still refused, each for a reason and not an omission:

    * the date family (date_format, hour, minute, second, month, microsecond,
      date, week, datediff) needs the temporal cast wrappers
      `WrapWithCastAsTime`/`WrapWithCastAsDuration` insert, whose target
      FieldType carries an FSP computed from the SOURCE type rather than the
      fixed shape an ETReal slot has -- and a MysqlTime constant additionally
      needs `codec.EncodeMySQLTime` against a session time zone this scan path
      does not put in the DAG request. That is a separate seam from the
      collation one the string family needed, not more of the same work.
    * the date_add/date_sub/adddate/subdate family additionally sends the
      INTERVAL unit as a third string argument and picks among more than
      twenty signatures by unit and argument type.
    * the JSON family needs the ETJson TiPB field type and the implicit
      CAST(... AS JSON) wrappers.
    * from_unixtime, unix_timestamp and timestampdiff need the session time
      zone in the DAG request, which this scan path does not yet send.

  That split is tracked by `GO_PUSHES_HERE_TOO` (asserted running) and
  `GO_PUSHES_NOT_HERE_YET` (the `#[ignore]`d `tikv_pushes_what_go_pushes`,
  pinned as refused by
  `every_not_yet_pushable_expression_is_still_refused_here`), both in
  `tidb_executor::scan_pushdown`.

  Also still refused, each for a stated reason rather than an omission:

    * string COMPARISON -- `col = 'x'`, `col = other_col`. Go picks the
      comparison's collation with `deriveCollation`'s ast.EQ case ->
      CheckAndDeriveCollationFromExprs -> inferCollation, which AGGREGATES
      coercibility and repertoire across BOTH operands and can land on a
      collation belonging to neither. The push-down description carries no
      coercibility at all, so this tier cannot follow that function, and a
      guess would silently return the wrong rows for every case-insensitive
      column. Refusing loudly is the only safe answer until the coercibility
      seam exists.
    * decimal and temporal comparisons (their constants need the cast and
      collation resolution Go applies before conversion), and LIKE.
    * a scan projecting a DECIMAL, temporal or JSON column, which `cop_scan`'s
      own PROJECTION gate refuses outright -- a separate gap from the
      predicate lowering, and the `noscan` case above is its live coverage.
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
