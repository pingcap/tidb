#!/usr/bin/env bash
#
# The access-path differential: for one schema and one data set, a real GO
# TiDB and this RUST node are asked the SAME queries, and their EXPLAIN plans
# are compared -- which path each chose, and what estRows each printed.
#
# The comparison is against GO, never against a previous Rust output. Both
# nodes read the same cluster, so the tables, the rows and the statistics are
# literally the same bytes; the only variable is the planner.
#
# The cases, each chosen because it can distinguish two planners. The first
# five are about COSTING; the four after them are about PRUNING -- Go's
# `skylinePruning` drops a candidate before the cost formula sees it, and a
# planner that only costs cannot reproduce that.
#
#   1. a filter on a LEADING index column                     -- the easy case;
#   2. a filter where the SECOND-declared index is far more selective than the
#      first -- the case a "first index that fits" rule gets wrong;
#   3. a filter selective enough to prefer an index over a full scan;
#   4. a filter broad enough to prefer the full scan;
#   5. a COVERING index, where there is no double read to pay for;
#
#   6. STRICT SUPERSET of access columns: `bucket = 1 AND rare = 7` ranges
#      idx_bucket(bucket), idx_rare(rare) AND idx_cover(bucket, rare). Every
#      one of them estimates the same handful of rows, so COST CANNOT SEPARATE
#      THEM -- and cost alone in fact picks idx_rare, whose smaller index row
#      makes it the cheapest. Go picks idx_cover because its access conditions
#      strictly contain the others'. This is simultaneously the "one path's
#      access columns strictly contain another's" case and the "the pruned
#      candidate would have won on cost" case, which is why it is the headline
#      receipt for this unit.
#   7. COVERING vs NON-COVERING with IDENTICAL access conditions:
#      `SELECT bucket, rare FROM t WHERE bucket = 1` ranges idx_bucket and
#      idx_cover on the same single column, so `accessResult` is 0 and only
#      `compareIndexBack` can separate them -- idx_cover needs no row lookup.
#   8. NO candidate dominates: `SELECT * FROM u WHERE a = 1 AND b = 2` over
#      idx_a(a) and idx_b(b). The access-column sets are the SAME SIZE and
#      DIFFERENT, which is exactly Go's "not comparable", so both survive
#      pruning and the cost formula makes the call.
#   9. an EMPTY range: `bucket = 1 AND bucket = 2` is contradictory, and Go
#      returns that one candidate alone rather than letting a full scan
#      survive beside it. Judged on the rows the query READS, because the two
#      nodes name the resulting plan differently (see the shape note below).
#
#  10. every one of the above BEFORE and AFTER `ANALYZE TABLE`, so the
#      pseudo -> real transition is visible on both nodes at once.
#
#  11. the DOUBLE READ'S BATCHING FACTOR, over a 50000-row table, at three
#      points on the selectivity curve: one row, 1/50th, and a quarter. Go's
#      cost model prices a double read at `rows/IndexLookupSize*32` table-side
#      requests because Go's `IndexLookUpExecutor` batches handles that way;
#      this tier's `IndexRangeSourceExec` issues one kvrpc Get per index entry.
#      These cases report the REQUESTS TIKV ACTUALLY SERVED for each node, read
#      off TiKV's own grpc counters -- so the receipt shows requests issued,
#      not costs claimed.
#
# estRows is compared with a tolerance, and every case where the two nodes
# CHOSE DIFFERENTLY is reported as a finding rather than tuned away.
#
# Usage: rust/scripts/run-realtikv-access-path.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep awk curl; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing access-path-differential prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${ACCESS_PATH_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "ACCESS_PATH_MYSQL_CLIENT must name an executable stock MySQL client" >&2
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
TAG="accesspath-${$}-$(date +%s)"
PORT_OFFSET=${ACCESS_PATH_PORT_OFFSET:-43500}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "ACCESS_PATH_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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
DIFFERENT_PATHS=0

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
# statement below selects the schema with `USE` instead of `-D`. That is a
# node-side gap outside this unit, worked around here rather than left to look
# like a planner difference.
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

# One table with TWO secondary indexes, declared worst-first on purpose:
# `idx_bucket` is declared BEFORE `idx_rare`, and `bucket` takes only 4
# distinct values while `rare` is nearly unique. A rule that takes the first
# index whose leading column the WHERE constrains reads through `idx_bucket`;
# a cost-based one reads through `idx_rare`.
# Auto-analyze would fire on the freshly loaded table within a minute and take
# the pre-ANALYZE half of this differential away. That half is not "a table
# with no statistics at all": it is a table whose ROW COUNT is real (the
# insert's delta tracking writes a `mysql.stats_meta` row) but whose
# DISTRIBUTION is not, which is Go's `HistColl.Pseudo` -- a real `estRows` on
# the scan AND `stats:pseudo` in the same row. The explicit ANALYZE below is
# what ends that state.
echo "the Go TiDB creates the fixture, with auto-analyze off"
go_sql -e "SET GLOBAL tidb_enable_auto_analyze = OFF"
go_sql <<'SQL'
DROP DATABASE IF EXISTS pathdiff;
CREATE DATABASE pathdiff;
USE pathdiff;
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  bucket BIGINT NOT NULL,
  rare BIGINT NOT NULL,
  payload VARCHAR(64) NOT NULL,
  KEY idx_bucket (bucket),
  KEY idx_rare (rare),
  KEY idx_cover (bucket, rare)
);
-- The second table exists for ONE pruning case: two single-column indexes on
-- DIFFERENT columns. Their access-column sets are the same size and neither
-- contains the other, which is exactly the "not comparable" answer from
-- `util.CompareCol2Len` -- so nothing is pruned and cost decides. `t` cannot
-- express that, because idx_cover contains every other index's columns.
CREATE TABLE u (
  id BIGINT PRIMARY KEY,
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  payload VARCHAR(64) NOT NULL,
  KEY idx_a (a),
  KEY idx_b (b)
);
-- The third table is the one where the DOUBLE READ'S BATCHING FACTOR decides
-- the plan. It is an order of magnitude larger than the other two on purpose:
-- the cost of a double read grows with the rows looked up, the cost of a full
-- scan grows with the table, and only a table big enough to separate those two
-- slopes can show where the crossover really is.
--
-- Three selectivities on one table, so one fixture covers the whole curve:
--   rare  -- unique, so `rare = k` looks up ONE row;
--   mid   -- 50 values, so `mid = k` looks up ~1/50th of the table;
--   bucket-- 4 values, so `bucket = k` looks up a quarter of it.
CREATE TABLE big (
  id BIGINT PRIMARY KEY,
  bucket BIGINT NOT NULL,
  mid BIGINT NOT NULL,
  rare BIGINT NOT NULL,
  payload VARCHAR(64) NOT NULL,
  KEY idx_big_bucket (bucket),
  KEY idx_big_mid (mid),
  KEY idx_big_rare (rare)
);
SQL

echo "seeding 2000 rows into each table"
{
  echo "USE pathdiff;"
  echo "INSERT INTO t VALUES"
  for i in $(seq 1 2000); do
    bucket=$((i % 4))
    rare=${i}
    sep=","
    [[ "${i}" -eq 2000 ]] && sep=";"
    echo "(${i}, ${bucket}, ${rare}, 'p${i}')${sep}"
  done
  # `a` takes 8 values and `b` takes 500, so once the histograms exist the two
  # incomparable candidates are far apart on cost and the case is decisive
  # rather than a tie-break.
  echo "INSERT INTO u VALUES"
  for i in $(seq 1 2000); do
    a=$((i % 8))
    b=$((i % 500))
    sep=","
    [[ "${i}" -eq 2000 ]] && sep=";"
    echo "(${i}, ${a}, ${b}, 'q${i}')${sep}"
  done
} >"${WORK_DIR}/seed.sql"
go_sql <"${WORK_DIR}/seed.sql"

BIG_ROWS=${ACCESS_PATH_BIG_ROWS:-50000}
echo "seeding ${BIG_ROWS} rows into pathdiff.big, in chunks"
{
  echo "USE pathdiff;"
  for i in $(seq 1 "${BIG_ROWS}"); do
    if (( i % 1000 == 1 )); then
      echo "INSERT INTO big VALUES"
    fi
    sep=","
    if (( i % 1000 == 0 || i == BIG_ROWS )); then sep=";"; fi
    echo "(${i}, $((i % 4)), $((i % 50)), ${i}, 'r${i}')${sep}"
  done
} >"${WORK_DIR}/seed-big.sql"
go_sql <"${WORK_DIR}/seed-big.sql"
seeded=$(go_sql -Nse "SELECT COUNT(*) FROM pathdiff.big")
[[ "${seeded}" == "${BIG_ROWS}" ]] \
  || { echo "pathdiff.big did not seed ${BIG_ROWS} rows, got ${seeded}" >&2; exit 1; }

for fixture in t u; do
  seeded=$(go_sql -Nse "SELECT COUNT(*) FROM pathdiff.${fixture}")
  [[ "${seeded}" == 2000 ]] || { echo "pathdiff.${fixture} did not seed 2000 rows, got ${seeded}" >&2; exit 1; }
done

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

# The chosen access path, as one word, and the scan's estRows. Go wraps its
# scan in a TableReader/IndexReader/IndexLookUp; this tier prints neither (see
# `tidb_executor::explain`'s divergence 1), so both sides are reduced to the
# SCAN row -- the operator that names the path -- which both do print.
go_path() {
  go_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' 'match($1, /TableDual|Batch_Point_Get|Point_Get|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan/) { print substr($1, RSTART, RLENGTH); exit }'
}
go_est() {
  go_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /(TableDual|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan|Point_Get|Batch_Point_Get)/ { print $2; exit }'
}
go_index() {
  go_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /IndexRangeScan/ { print $4; exit }'
}
rust_path() {
  rust_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' 'match($1, /TableDual|Batch_Point_Get|Point_Get|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan/) { print substr($1, RSTART, RLENGTH); exit }'
}
rust_est() {
  rust_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /(TableDual|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan|Point_Get|Batch_Point_Get)/ { print $2; exit }'
}
rust_index() {
  rust_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /IndexRangeScan/ { print $4; exit }'
}

# One case: print both plans' path and estRows side by side, then judge.
#
# A different PATH is a finding and is reported as one -- it is never turned
# into a pass by loosening a tolerance. estRows is compared within
# ACCESS_PATH_EST_TOLERANCE (a ratio) only when the two chose the SAME path,
# because two different paths legitimately estimate different row counts.
EST_TOLERANCE=${ACCESS_PATH_EST_TOLERANCE:-0.05}

# The known-divergence escape hatch for PRUNING is gone. The one this
# differential used to carry -- Go picking idx_cover where costing alone picked
# idx_rare -- is what `tidb_executor::skyline` now reproduces, so every case
# below must choose the same path, and a divergence is a failure.
#
# Two cases carry a NON-pruning divergence instead, each named and counted
# separately so it cannot hide a path divergence:
#
#   "estimator"  the two nodes agree on the path but not on estRows. That is
#                `get_index_row_count_for_stats_v2` against Go's own
#                `GetRowCountByIndexRanges`, a different unit from this one.
#   "shape"      Go renders a provably empty scan as a `TableDual` and this
#                tier renders it as an index scan over zero ranges. Both read
#                no rows -- the case asserts that directly -- so this is the
#                plan TEXT, not the plan. Pruning itself agrees: Go's
#                `if len(path.Ranges) == 0 { return one candidate }` is ported
#                and does return that one candidate.
ESTIMATOR_NOTES=0
SHAPE_NOTES=0

# `compare <label> <query> [estimator]`: passing `estimator` reports an estRows
# mismatch as a note instead of a failure. The PATH is judged either way.
compare() {
  local label=$1 query=$2 tolerate=${3:-}
  local gp ge gi rp re ri
  gp=$(go_path "${query}")
  ge=$(go_est "${query}")
  gi=$(go_index "${query}")
  rp=$(rust_path "${query}")
  re=$(rust_est "${query}")
  ri=$(rust_index "${query}")
  echo
  echo "--- ${label}"
  echo "    ${query}"
  printf '      GO    %-16s %-12s %s\n' "${gp:-<none>}" "${ge:-<none>}" "${gi:-}"
  printf '      RUST  %-16s %-12s %s\n' "${rp:-<none>}" "${re:-<none>}" "${ri:-}"
  local divergence=""
  if [[ "${gp}" != "${rp}" ]]; then
    divergence="the two planners chose DIFFERENT paths"
  elif [[ -n "${gi}" && "${gi}" != "${ri}" ]]; then
    divergence="same operator, DIFFERENT index (${gi} vs ${ri})"
  fi
  if [[ -n "${divergence}" ]]; then
    echo "      FINDING: ${divergence}" >&2
    DIFFERENT_PATHS=$((DIFFERENT_PATHS + 1))
    return
  fi
  if [[ "${tolerate}" == "estimator" ]] \
    && ! awk -v a="${ge}" -v b="${re}" -v tol="${EST_TOLERANCE}" \
      'BEGIN { if (a == 0 && b == 0) exit 0; d = a - b; if (d < 0) d = -d; exit !(a > 0 && d / a <= tol) }'; then
    echo "      ESTIMATOR NOTE: same path, estRows ${ge} vs ${re} -- index row-count"
    echo "                      estimation, not path choice; see this script's header"
    ESTIMATOR_NOTES=$((ESTIMATOR_NOTES + 1))
    return
  fi
  check "${label}: same path, and estRows within ${EST_TOLERANCE}" \
    awk -v a="${ge}" -v b="${re}" -v tol="${EST_TOLERANCE}" \
    'BEGIN { if (a == 0 && b == 0) exit 0; d = a - b; if (d < 0) d = -d; exit !(a > 0 && d / a <= tol) }'
}

# The contradictory filter, judged on what it READS rather than on what it is
# called. Go plans a `TableDual`; this tier plans an index scan over zero
# ranges. Both must return no row, and the plan-text difference is counted as
# a shape note.
compare_empty_range() {
  local query=$1
  local gp rp gr rr
  gp=$(go_path "${query}")
  rp=$(rust_path "${query}")
  gr=$(go_sql -Nse "USE pathdiff; SELECT COUNT(*) FROM (${query}) AS c")
  rr=$(rust_sql -Nse "USE pathdiff; SELECT COUNT(*) FROM (${query}) AS c")
  echo
  echo "--- a contradictory filter reads nothing on both nodes"
  echo "    ${query}"
  printf '      GO    %-16s rows read %s\n' "${gp:-<none>}" "${gr}"
  printf '      RUST  %-16s rows read %s\n' "${rp:-<none>}" "${rr}"
  check "contradictory filter: both nodes read zero rows" \
    test "${gr}" = "0" -a "${rr}" = "0"
  if [[ "${gp}" != "${rp}" ]]; then
    echo "      SHAPE NOTE: ${gp} vs ${rp} -- the plan text, not the plan; see the header"
    SHAPE_NOTES=$((SHAPE_NOTES + 1))
  fi
}

Q_LEADING="SELECT * FROM t WHERE bucket = 1"
Q_SECOND="SELECT * FROM t WHERE bucket = 1 AND rare = 7"
Q_SELECTIVE="SELECT * FROM t WHERE rare = 7"
Q_BROAD="SELECT * FROM t WHERE rare > 0"
Q_COVERING="SELECT bucket, rare FROM t WHERE bucket = 1"
# The pruning cases. Q_SECOND above is one of them too -- it is both the
# strict-superset case and the "the pruned candidate would have won on cost"
# case -- so it is not repeated here.
Q_SUPERSET_RANGE="SELECT * FROM t WHERE bucket = 1 AND rare > 1990"
Q_INCOMPARABLE="SELECT * FROM u WHERE a = 1 AND b = 2"
Q_EMPTY_RANGE="SELECT * FROM t WHERE bucket = 1 AND bucket = 2"

# Every case, in one list, so the two phases below cannot drift apart.
CASES=(
  "leading index column|${Q_LEADING}"
  "strict superset of access columns, and cost alone would pick the other|${Q_SECOND}"
  "selective enough for an index|${Q_SELECTIVE}"
  "broad enough for the full scan|${Q_BROAD}"
  "covering beats non-covering on identical access conditions|${Q_COVERING}"
  "strict superset where the extra column is a RANGE, not an equality|${Q_SUPERSET_RANGE}|estimator"
  "no candidate dominates, so cost decides|${Q_INCOMPARABLE}"
)

echo
echo "==================== BEFORE ANALYZE (pseudo statistics) ===================="
for pair in "${CASES[@]}"; do
  IFS='|' read -r case_label case_query case_tolerate <<<"${pair}"
  compare "${case_label}" "${case_query}" "${case_tolerate}"
done
compare_empty_range "${Q_EMPTY_RANGE}"

# `stats:pseudo` must be on both sides here: this is the pre-ANALYZE state,
# and a node that printed a real-looking estimate would be inventing one.
go_pseudo=$(go_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
rust_pseudo=$(rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
echo
check "before ANALYZE both nodes print stats:pseudo (go=${go_pseudo} rust=${rust_pseudo})" \
  test "${go_pseudo}" -ge 1 -a "${rust_pseudo}" -ge 1

echo
echo "==================== ANALYZE, on the GO node ===================="
go_sql -e "ANALYZE TABLE pathdiff.t"
go_sql -e "ANALYZE TABLE pathdiff.u"
go_sql -e "ANALYZE TABLE pathdiff.big"
# The Go server loads a histogram lazily; a synchronous-load query on each
# compared column is what makes its own EXPLAIN read the statistics rather
# than the absence of them.
for _ in $(seq 1 30); do
  go_sql -Nse "SET SESSION tidb_stats_load_sync_wait = 10000;
               USE pathdiff;
               SELECT COUNT(*) FROM t WHERE bucket = 1 OR rare = 7;
               SELECT COUNT(*) FROM u WHERE a = 1 OR b = 2" >/dev/null 2>&1 || true
  loaded=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='pathdiff' AND table_name IN ('t','u') AND column_name IN ('rare','b') AND is_index=0" | wc -l | tr -d ' ')
  [[ "${loaded}" -ge 2 ]] && break
  sleep 1
done

# The Rust node re-reads mysql.stats_* on its own ticker (there is no etcd
# notification for a stats change; see `tidb_exec::stats_watch`), so it has to
# be given a tick or two before its planner can see the ANALYZE. Both tables
# are waited on: pruning compares candidates within ONE table, but a case that
# read `u` under pseudo statistics while `t` was analyzed would be measuring
# the reload race instead of the planner.
echo "waiting for the Rust node's stats reload to pick the ANALYZE up"
for _ in $(seq 1 60); do
  if ! rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -q "stats:pseudo" \
    && ! rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_INCOMPARABLE}" | grep -q "stats:pseudo" \
    && ! rust_sql -Nse "USE pathdiff; EXPLAIN SELECT * FROM big WHERE mid = 7" \
      | grep -q "stats:pseudo"; then
    break
  fi
  sleep 2
done

echo
echo "==================== AFTER ANALYZE (real statistics) ===================="
for pair in "${CASES[@]}"; do
  IFS='|' read -r case_label case_query case_tolerate <<<"${pair}"
  compare "${case_label}" "${case_query}" "${case_tolerate}"
done
compare_empty_range "${Q_EMPTY_RANGE}"

go_pseudo=$(go_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
rust_pseudo=$(rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
echo
check "after ANALYZE neither node prints stats:pseudo (go=${go_pseudo} rust=${rust_pseudo})" \
  test "${go_pseudo}" -eq 0 -a "${rust_pseudo}" -eq 0

echo
echo "==================== THE DOUBLE READ, PRICED AND MEASURED ===================="
cat <<'NOTE'
  Go's cost model prices a double read at `rows/IndexLookupSize*32` table-side
  requests -- 0.0016 per index row -- and Go's `IndexLookUpExecutor` earns it:
  `fetchHandles` gathers `IndexLookupSize` handles and `buildTableReader`
  issues cop tasks for the whole batch (pkg/executor/distsql.go).

  This tier's `IndexRangeSourceExec` calls `get_row_by_handle` once per index
  entry, which is one kvrpc Get per row. The three cases below sit at three
  points on the selectivity curve of one 50000-row table, and each one reports
  the REQUESTS TIKV ACTUALLY SERVED for each node -- read straight off TiKV's
  own grpc counters, so it is a measurement and not a restatement of the cost
  formula. A different path is a FINDING, not something to tune away.
NOTE

TIKV_STATUS_PORT=$((20180 + PORT_OFFSET))

# One TiKV grpc message type's served count, cluster-wide. `kv_get` is a point
# read, `coprocessor` is a pushed-down scan: the two shapes a double read can
# take, and the whole difference between the batched reader and ours.
tikv_msg_count() {
  local type=$1
  curl -sf "http://127.0.0.1:${TIKV_STATUS_PORT}/metrics" 2>/dev/null \
    | awk -v t="type=\"${type}\"" '
        $0 ~ /^tikv_grpc_msg_duration_seconds_count/ && index($0, t) { total += $NF }
        END { printf "%d\n", total }'
}

# Runs `query` on one node and reports how many kvrpc Gets and coprocessor
# requests TiKV served while it ran. Idle traffic (heartbeats, stats) does not
# touch these two counters, so the delta is the statement's own.
measure_requests() {
  local node=$1 query=$2
  local before_get before_cop after_get after_cop
  before_get=$(tikv_msg_count kv_get)
  before_cop=$(tikv_msg_count coprocessor)
  if [[ "${node}" == "go" ]]; then
    go_sql -Nse "USE pathdiff; ${query}" >/dev/null
  else
    rust_sql -Nse "USE pathdiff; ${query}" >/dev/null
  fi
  after_get=$(tikv_msg_count kv_get)
  after_cop=$(tikv_msg_count coprocessor)
  echo "$((after_get - before_get)) $((after_cop - before_cop))"
}

# `compare_double_read <label> <expected rows> <query>`: the plan each node
# chose, and the requests each one really issued to get there.
compare_double_read() {
  local label=$1 expected=$2 query=$3
  local gp rp gm rm gg gc rg rc grows rrows
  gp=$(go_path "${query}")
  rp=$(rust_path "${query}")
  # SELECT COUNT(*) would let the planner cover the query with the index and
  # skip the lookup entirely; the row projection is what forces the double
  # read this section is about.
  grows=$(go_sql -Nse "USE pathdiff; SELECT COUNT(*) FROM (${query}) AS c")
  rrows=$(rust_sql -Nse "USE pathdiff; SELECT COUNT(*) FROM (${query}) AS c")
  gm=$(measure_requests go "${query}")
  rm=$(measure_requests rust "${query}")
  read -r gg gc <<<"${gm}"
  read -r rg rc <<<"${rm}"
  echo
  echo "--- ${label}"
  echo "    ${query}"
  printf '      %-6s %-16s %-10s %-12s %-12s %s\n' \
    "" "path" "rows" "kv_get" "coprocessor" "rows read"
  printf '      %-6s %-16s %-10s %-12s %-12s %s\n' \
    "GO" "${gp:-<none>}" "${expected}" "${gg}" "${gc}" "${grows}"
  printf '      %-6s %-16s %-10s %-12s %-12s %s\n' \
    "RUST" "${rp:-<none>}" "${expected}" "${rg}" "${rc}" "${rrows}"
  check "${label}: both nodes returned the same rows" test "${grows}" = "${rrows}"
  if [[ "${gp}" != "${rp}" ]]; then
    echo "      FINDING: the two planners chose DIFFERENT paths (${gp} vs ${rp})" >&2
    DIFFERENT_PATHS=$((DIFFERENT_PATHS + 1))
  fi
  # The point of the section: when THIS node reads through an index, does it
  # issue one request per row?
  if [[ "${rp}" == "IndexRangeScan" ]] && (( rg >= expected )); then
    echo "      UNBATCHED: ${rg} kvrpc Gets for ${expected} index rows -- one round"
    echo "                 trip per row, which is what access_cost.rs now prices"
  fi
}

compare_double_read "one row: a unique index over a large table" 1 \
  "SELECT * FROM big WHERE rare = 7"
compare_double_read "mid selectivity: 1/50th of a large table, near the crossover" 1000 \
  "SELECT * FROM big WHERE mid = 7"
compare_double_read "a quarter of a large table, where the scan must win" 12500 \
  "SELECT * FROM big WHERE bucket = 1"

echo
echo "=== the two planners' full plans, side by side, after ANALYZE ==="
for query in "${Q_SECOND}" "${Q_BROAD}" "${Q_INCOMPARABLE}"; do
  echo
  echo "  ${query}"
  echo "  --- GO"
  go_sql -e "USE pathdiff; EXPLAIN ${query}"
  echo "  --- RUST"
  rust_sql -e "USE pathdiff; EXPLAIN ${query}"
done

echo
echo "=== what was NOT compared ==="
cat <<'NOTE'
  * The task column and the reader row. Go prints a TableReader /
    IndexReader / IndexLookUpReader over a cop[tikv] task; this tier has no
    coprocessor task in its plan text, so only the SCAN row -- the operator
    that names the path -- is compared. That divergence is documented in
    `tidb_executor::explain`.
  * Index merge, multi-valued-index and partition paths. They are excluded
    from enumeration by name (see `tidb_executor::access_cost`), so a query
    Go answers with one is out of scope here rather than silently mis-costed.
  * Join order and join cardinality. This unit costs ONE table's access
    path; a multi-table plan's estRows is a separate estimator.
  * The exact cost numbers. Go does not print them without
    `EXPLAIN FORMAT='verbose'`/`true_card_cost`, and a cost is only ever a
    means to a choice -- the choice and the estRows are what is compared.
  * The pruning dimensions this tier holds at zero: the required physical
    property (`matchProperty`), global-index preference, and the RISK RATIO
    (`compareRiskRatio` over `MaxCountAfterAccess`). The first two are exact
    for the call this tier makes; the risk ratio is a real exclusion that can
    prune a candidate Go keeps, and `tidb_executor::skyline`'s module doc
    states its direction. No case below can catch it, because this rewrite
    does not compute the risk estimate that would make it fire.
  * A covering index with NO access conditions. Go keeps it
    (`keepIndex := ... || path.IsSingleScan`) and it then prunes the table
    path, so `SELECT bucket FROM t` is an `IndexFullScan` there and a
    `TableFullScan` here. That is an ENUMERATION gap, named in
    `tidb_executor::access_cost`, not a pruning one.
NOTE

echo
if [[ "${ESTIMATOR_NOTES}" -gt 0 || "${SHAPE_NOTES}" -gt 0 ]]; then
  echo "${ESTIMATOR_NOTES} estimator note(s) and ${SHAPE_NOTES} shape note(s), both non-pruning -- see the header"
fi
if [[ "${DIFFERENT_PATHS}" -gt 0 ]]; then
  echo "${DIFFERENT_PATHS} NEW case(s) where the two planners chose differently -- see the FINDING lines above" >&2
fi
if [[ "${FAILURES}" -eq 0 && "${DIFFERENT_PATHS}" -eq 0 ]]; then
  echo "the access-path differential passed"
else
  echo "the access-path differential had ${FAILURES} failure(s) and ${DIFFERENT_PATHS} divergent choice(s)" >&2
  exit 1
fi
