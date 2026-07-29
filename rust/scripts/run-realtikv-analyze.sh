#!/usr/bin/env bash
#
# The statistics differential: a RUST node runs `ANALYZE TABLE` and a real GO
# TiDB has to accept and use what it wrote.
#
# Three checks, in the order that makes each one mean something:
#
#   1. the Go server's `SHOW STATS_HISTOGRAMS` / `SHOW STATS_BUCKETS` /
#      `SHOW STATS_TOPN` RENDER the rows -- so the encoding is the one its own
#      reader expects, bucket-count deltas and blob bounds and all;
#   2. the Go server's `EXPLAIN` on a filtered query shows a NON-PSEUDO
#      estRows derived from those rows -- so the numbers are not merely
#      readable but usable;
#   3. the Rust node's own loader reads them back, so the round trip closes.
#
# Then the same table, with the same rows and the same knobs, is analyzed by
# the GO node, and the two sets of statistics are compared. Sampling is
# randomised, so the comparison is structural and bounded, never exact:
# bucket count, monotone bounds, counts summing to the row count, NDV within
# tolerance. What could not be compared is printed, not hidden.
#
# Usage: rust/scripts/run-realtikv-analyze.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep awk; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing analyze-differential prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${ANALYZE_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "ANALYZE_MYSQL_CLIENT must name an executable stock MySQL client" >&2
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
TAG="analyze-${$}-$(date +%s)"
PORT_OFFSET=${ANALYZE_PORT_OFFSET:-43000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "ANALYZE_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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

# Two tables with IDENTICAL contents: `rust_side` is analyzed by the Rust
# node, `go_side` by the Go node, so the two sets of statistics describe the
# same distribution and any difference is the analyzer's, not the data's.
#
# The distribution is deliberately skewed and wider than the default TopN, so
# that both a TopN and buckets exist to compare. 600 rows: `grade` takes 300
# distinct values with a heavy tail on the small ones, `bucketable` is 600
# distinct values so it can only be described by buckets.
echo "the Go TiDB creates two identical tables"
go_sql <<'SQL'
DROP DATABASE IF EXISTS statdiff;
CREATE DATABASE statdiff;
USE statdiff;
CREATE TABLE rust_side (
  id BIGINT PRIMARY KEY,
  grade BIGINT NOT NULL,
  bucketable BIGINT NOT NULL,
  KEY idx_grade (grade)
);
CREATE TABLE go_side (
  id BIGINT PRIMARY KEY,
  grade BIGINT NOT NULL,
  bucketable BIGINT NOT NULL,
  KEY idx_grade (grade)
);
SQL

echo "seeding 600 rows of a known skewed distribution"
{
  echo "USE statdiff;"
  echo "INSERT INTO rust_side VALUES"
  for i in $(seq 1 600); do
    grade=$(( i <= 200 ? 7 : (i <= 300 ? 13 : i) ))
    sep=","
    [[ "${i}" -eq 600 ]] && sep=";"
    echo "(${i}, ${grade}, ${i})${sep}"
  done
  echo "INSERT INTO go_side SELECT * FROM rust_side;"
} >"${WORK_DIR}/seed.sql"
go_sql <"${WORK_DIR}/seed.sql"

seeded=$(go_sql -Nse "SELECT COUNT(*) FROM statdiff.rust_side")
[[ "${seeded}" == 600 ]] || { echo "the fixture did not seed 600 rows, got ${seeded}" >&2; exit 1; }

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
grep -F '"event":"cluster_session_node_ready"' "${RUST_LOG_FILE}" >/dev/null \
  || { echo "the Rust node never reported ready"; cat "${RUST_LOG_FILE}"; exit 1; }

# ---------------------------------------------------------------- the write
echo
echo "=== the RUST node analyzes statdiff.rust_side ==="
rust_sql -e "ANALYZE TABLE statdiff.rust_side WITH 32 BUCKETS, 20 TOPN;"
analyzed=$(grep -F '"event":"cluster_table_analyzed"' "${RUST_LOG_FILE}" | tail -1)
[[ -n "${analyzed}" ]] || { echo "the Rust node logged no analysis"; cat "${RUST_LOG_FILE}"; exit 1; }
echo "  ${analyzed}"

echo "=== the GO node analyzes statdiff.go_side with the same knobs ==="
go_sql -e "ANALYZE TABLE statdiff.go_side WITH 32 BUCKETS, 20 TOPN;"
# The Go server loads a histogram into its domain lazily: until something
# asks for it, `SHOW STATS_BUCKETS` reports the column as `allEvicted` and
# returns nothing. A synchronous-load query on each table is what makes the
# checks below read the statistics rather than the absence of them.
echo "warming the Go server's statistics cache"
for table in rust_side go_side; do
  for _ in $(seq 1 30); do
    # Every compared column must appear in a predicate: TiDB loads a column's
    # histogram only when a statement needs it, and an unloaded column
    # renders as `allEvicted` with no buckets at all -- which looks exactly
    # like a histogram that was never written.
    go_sql -Nse "SET SESSION tidb_stats_load_sync_wait = 10000;
                 SELECT COUNT(*) FROM statdiff.${table} WHERE grade = 7 OR bucketable = 7;
                 EXPLAIN SELECT * FROM statdiff.${table} WHERE grade = 7;
                 EXPLAIN SELECT * FROM statdiff.${table} WHERE bucketable = 7" >/dev/null 2>&1 || true
    loaded=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='${table}' AND column_name='bucketable' AND is_index=0" | wc -l | tr -d ' ')
    [[ "${loaded}" -gt 0 ]] && break
    sleep 1
  done
done

# ------------------------------------------------- check 1: Go renders them
echo
echo "=== CHECK 1: the Go server renders the Rust node's rows ==="

hist_rows=$(go_sql -Nse "SHOW STATS_HISTOGRAMS WHERE db_name='statdiff' AND table_name='rust_side'" | wc -l | tr -d ' ')
check "SHOW STATS_HISTOGRAMS returns rows for rust_side (${hist_rows})" \
  test "${hist_rows}" -ge 3

bucket_rows=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='rust_side'" | wc -l | tr -d ' ')
check "SHOW STATS_BUCKETS returns rows for rust_side (${bucket_rows})" \
  test "${bucket_rows}" -ge 1

topn_rows=$(go_sql -Nse "SHOW STATS_TOPN WHERE db_name='statdiff' AND table_name='rust_side'" | wc -l | tr -d ' ')
check "SHOW STATS_TOPN returns rows for rust_side (${topn_rows})" \
  test "${topn_rows}" -ge 1

meta_count=$(go_sql -Nse "SELECT count FROM mysql.stats_meta WHERE table_id = (SELECT tidb_table_id FROM information_schema.tables WHERE table_schema='statdiff' AND table_name='rust_side')")
check "mysql.stats_meta.count is the seeded row count (${meta_count})" \
  test "${meta_count}" = 600

echo
echo "--- the Go server's own rendering of what the Rust node wrote ---"
go_sql -e "SHOW STATS_HISTOGRAMS WHERE db_name='statdiff' AND table_name='rust_side'"
go_sql -e "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='rust_side'" | head -20
go_sql -e "SHOW STATS_TOPN WHERE db_name='statdiff' AND table_name='rust_side'" | head -10

# A bound the Go server could not decode comes back empty or as a raw blob;
# the reference table's own bounds are the shape to match.
empty_bounds=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='rust_side'" \
  | awk -F '\t' '$9 == "" || $10 == "" { n++ } END { print n + 0 }')
check "every rendered bucket has both bounds (${empty_bounds} empty)" \
  test "${empty_bounds}" -eq 0

# ------------------------------------------- check 2: Go ESTIMATES from them
echo
echo "=== CHECK 2: the Go server's EXPLAIN estimates from the Rust node's rows ==="

# `grade = 7` is 200 of the 600 rows and is the single most common value, so a
# planner reading real statistics must estimate close to 200 -- and a planner
# on pseudo statistics cannot: with no statistics at all TiDB estimates a
# fixed fraction of a fixed pseudo row count, never 200 out of 600.
est_rust=$(go_sql -Nse "EXPLAIN FORMAT='brief' SELECT * FROM statdiff.rust_side WHERE grade = 7" \
  | awk -F '\t' 'NR == 1 { print $2 }')
est_go=$(go_sql -Nse "EXPLAIN FORMAT='brief' SELECT * FROM statdiff.go_side WHERE grade = 7" \
  | awk -F '\t' 'NR == 1 { print $2 }')
echo "  estRows over the RUST-analyzed table: ${est_rust}"
echo "  estRows over the GO-analyzed table:   ${est_go}"

pseudo_warning=$(go_sql -Nse "EXPLAIN SELECT * FROM statdiff.rust_side WHERE grade = 7; SHOW WARNINGS" \
  | grep -ci pseudo || true)
check "the Go planner reports no pseudo-statistics warning" \
  test "${pseudo_warning}" -eq 0

check "estRows over the Rust-analyzed table is within 5% of the true 200" \
  awk -v e="${est_rust}" 'BEGIN { exit !(e >= 190 && e <= 210) }'

check "the two nodes' estimates agree within 5%" \
  awk -v a="${est_rust}" -v b="${est_go}" \
  'BEGIN { d = a - b; if (d < 0) d = -d; exit !(b > 0 && d / b <= 0.05) }'

# ------------------------------------ check 3: the Rust loader round-trips
echo
echo "=== CHECK 3: the Rust node reads its own statistics back ==="
reloaded=$(grep -F '"event":"stats_reloaded_after_analyze"' "${RUST_LOG_FILE}" | tail -1)
check "the node republished its own statistics after the commit" \
  test -n "${reloaded}"
echo "  ${reloaded:-<none>}"
loaded_tables=$(printf '%s\n' "${reloaded}" | sed -n 's/.*"loaded":\([0-9]*\).*/\1/p')
check "the reload found at least one table with real statistics (${loaded_tables:-0})" \
  test "${loaded_tables:-0}" -ge 1

# The Rust node reading its own `mysql.stats_*` back through the same loader
# the planner uses: if a bound could not be decoded, the load fails loudly and
# the table falls back to pseudo, which the count above would show.
rust_sql -e "SELECT COUNT(*) FROM statdiff.rust_side WHERE grade = 7" >/dev/null

# ------------------------------------------------ the structural comparison
echo
echo "=== the two analyzers, side by side ==="

stats_of() {
  local table=$1 field=$2
  go_sql -Nse "SHOW STATS_HISTOGRAMS WHERE db_name='statdiff' AND table_name='${table}'" \
    | awk -F '\t' -v col="${field}" '$4 == col && $5 == 0 { print $7 "\t" $8 }'
}

for column in grade bucketable; do
  read -r rust_ndv rust_null <<<"$(stats_of rust_side "${column}")"
  read -r go_ndv go_null <<<"$(stats_of go_side "${column}")"
  echo "  ${column}: distinct_count rust=${rust_ndv} go=${go_ndv}, null_count rust=${rust_null} go=${go_null}"
  check "${column}: the two NDVs agree within 10%" \
    awk -v a="${rust_ndv}" -v b="${go_ndv}" \
    'BEGIN { d = a - b; if (d < 0) d = -d; exit !(b > 0 && d / b <= 0.10) }'
  check "${column}: neither analyzer invented a NULL" \
    test "${rust_null}" = "0" -a "${go_null}" = "0"
done

bucket_count_of() {
  go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='$1' AND column_name='$2' AND is_index=0" \
    | wc -l | tr -d ' '
}
for column in grade bucketable; do
  rust_buckets=$(bucket_count_of rust_side "${column}")
  go_buckets=$(bucket_count_of go_side "${column}")
  echo "  ${column}: buckets rust=${rust_buckets} go=${go_buckets}"
  check "${column}: neither analyzer exceeded the 32 buckets asked for" \
    test "${rust_buckets}" -le 32 -a "${go_buckets}" -le 32
done

# Monotone bounds and a total that adds up: the two invariants a histogram
# must satisfy whoever built it. `SHOW STATS_BUCKETS` prints the CUMULATIVE
# count, so the last bucket's count plus the TopN's is the column's rows.
for table in rust_side go_side; do
  for column in grade bucketable; do
    monotone=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='${table}' AND column_name='${column}' AND is_index=0" \
      | awk -F '\t' 'NR > 1 && $7 <= prev { bad++ } { prev = $7 } END { print bad + 0 }')
    check "${table}.${column}: cumulative bucket counts increase" \
      test "${monotone}" -eq 0
    total=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='statdiff' AND table_name='${table}' AND column_name='${column}' AND is_index=0" \
      | awk -F '\t' 'END { print $7 + 0 }')
    topn_total=$(go_sql -Nse "SHOW STATS_TOPN WHERE db_name='statdiff' AND table_name='${table}' AND column_name='${column}' AND is_index=0" \
      | awk -F '\t' '{ s += $7 } END { print s + 0 }')
    echo "  ${table}.${column}: histogram ${total} + topn ${topn_total} = $((total + topn_total)) of 600"
    check "${table}.${column}: the histogram and the TopN account for every row" \
      awk -v t="$((total + topn_total))" 'BEGIN { d = t - 600; if (d < 0) d = -d; exit !(d <= 6) }'
  done
done

echo
echo "=== what was NOT compared ==="
cat <<'NOTE'
  * Bucket bounds are not compared value for value. Both analyzers sampled
    the same 600 rows at rate 1.0, but the bucket a value lands in depends on
    the running count, and Go's sample arrives merged from per-region
    coprocessor collectors while the Rust node's arrives from one scan; the
    two orderings can differ where values tie.
  * TopN membership is compared only in aggregate. Both keep the commonest
    values, but which singleton survives `processTopNValue`'s heap depends on
    insertion order, which differs for the same reason.
  * `mysql.stats_fm_sketch` and `mysql.column_stats_usage` are written by Go
    and not by this node; neither is read by the estimator.
  * Auto-analyze is out of scope entirely: nothing here exercises the
    background trigger, only the explicit statement.
NOTE

echo
if [[ "${FAILURES}" -eq 0 ]]; then
  echo "the analyze differential passed"
else
  echo "the analyze differential had ${FAILURES} failure(s)" >&2
  exit 1
fi
