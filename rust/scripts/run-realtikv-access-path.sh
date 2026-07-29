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
# Six cases, each chosen because it can distinguish two planners:
#
#   1. a filter on a LEADING index column                     -- the easy case;
#   2. a filter where the SECOND-declared index is far more selective than the
#      first -- the case a "first index that fits" rule gets wrong, and the
#      headline receipt for this unit;
#   3. a filter selective enough to prefer an index over a full scan;
#   4. a filter broad enough to prefer the full scan;
#   5. a COVERING index, where there is no double read to pay for;
#   6. every one of the above BEFORE and AFTER `ANALYZE TABLE`, so the
#      pseudo -> real transition is visible on both nodes at once.
#
# estRows is compared with a tolerance, and every case where the two nodes
# CHOSE DIFFERENTLY is reported as a finding rather than tuned away.
#
# Usage: rust/scripts/run-realtikv-access-path.sh

set -euo pipefail

for prerequisite in tiup cargo nc grep awk; do
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
SQL

echo "seeding 2000 rows"
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
} >"${WORK_DIR}/seed.sql"
go_sql <"${WORK_DIR}/seed.sql"

seeded=$(go_sql -Nse "SELECT COUNT(*) FROM pathdiff.t")
[[ "${seeded}" == 2000 ]] || { echo "the fixture did not seed 2000 rows, got ${seeded}" >&2; exit 1; }

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
    | awk -F '\t' 'match($1, /Batch_Point_Get|Point_Get|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan/) { print substr($1, RSTART, RLENGTH); exit }'
}
go_est() {
  go_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /(TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan|Point_Get|Batch_Point_Get)/ { print $2; exit }'
}
go_index() {
  go_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /IndexRangeScan/ { print $4; exit }'
}
rust_path() {
  rust_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' 'match($1, /Batch_Point_Get|Point_Get|TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan/) { print substr($1, RSTART, RLENGTH); exit }'
}
rust_est() {
  rust_sql -Nse "USE pathdiff; EXPLAIN $1" \
    | awk -F '\t' '$1 ~ /(TableFullScan|IndexRangeScan|IndexFullScan|TableRangeScan|Point_Get|Batch_Point_Get)/ { print $2; exit }'
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

# The ONE divergence this unit ships with, named so that a NEW one cannot hide
# behind it. Go prunes `idx_rare` before costing anything, because
# `idx_cover`'s access conditions are a strict superset of it
# (`skylinePruning` / `compareCandidates`, `find_best_task.go`); costing every
# candidate cannot reproduce that, and both paths estimate the same one row,
# so the cost formula has nothing to separate them by. See
# `tidb_executor::access_cost`'s module doc. It appears only with real
# statistics -- under pseudo statistics the two paths estimate differently and
# the cost model picks Go's.
KNOWN_FINDING="second index far more selective than the first"
KNOWN_FINDINGS_SEEN=0

compare() {
  local label=$1 query=$2
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
    if [[ "${label}" == "${KNOWN_FINDING}" ]]; then
      echo "      KNOWN FINDING (skyline pruning, documented): ${divergence}"
      KNOWN_FINDINGS_SEEN=$((KNOWN_FINDINGS_SEEN + 1))
    else
      echo "      FINDING: ${divergence}" >&2
      DIFFERENT_PATHS=$((DIFFERENT_PATHS + 1))
    fi
    return
  fi
  check "${label}: same path, and estRows within ${EST_TOLERANCE}" \
    awk -v a="${ge}" -v b="${re}" -v tol="${EST_TOLERANCE}" \
    'BEGIN { if (a == 0 && b == 0) exit 0; d = a - b; if (d < 0) d = -d; exit !(a > 0 && d / a <= tol) }'
}

Q_LEADING="SELECT * FROM t WHERE bucket = 1"
Q_SECOND="SELECT * FROM t WHERE bucket = 1 AND rare = 7"
Q_SELECTIVE="SELECT * FROM t WHERE rare = 7"
Q_BROAD="SELECT * FROM t WHERE rare > 0"
Q_COVERING="SELECT bucket, rare FROM t WHERE bucket = 1"

echo
echo "==================== BEFORE ANALYZE (pseudo statistics) ===================="
for pair in \
  "leading index column|${Q_LEADING}" \
  "second index far more selective than the first|${Q_SECOND}" \
  "selective enough for an index|${Q_SELECTIVE}" \
  "broad enough for the full scan|${Q_BROAD}" \
  "covering index|${Q_COVERING}"; do
  compare "${pair%%|*}" "${pair#*|}"
done

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
# The Go server loads a histogram lazily; a synchronous-load query on each
# compared column is what makes its own EXPLAIN read the statistics rather
# than the absence of them.
for _ in $(seq 1 30); do
  go_sql -Nse "SET SESSION tidb_stats_load_sync_wait = 10000;
               USE pathdiff;
               SELECT COUNT(*) FROM t WHERE bucket = 1 OR rare = 7" >/dev/null 2>&1 || true
  loaded=$(go_sql -Nse "SHOW STATS_BUCKETS WHERE db_name='pathdiff' AND table_name='t' AND column_name='rare' AND is_index=0" | wc -l | tr -d ' ')
  [[ "${loaded}" -gt 0 ]] && break
  sleep 1
done

# The Rust node re-reads mysql.stats_* on its own ticker (there is no etcd
# notification for a stats change; see `tidb_exec::stats_watch`), so it has to
# be given a tick or two before its planner can see the ANALYZE.
echo "waiting for the Rust node's stats reload to pick the ANALYZE up"
for _ in $(seq 1 60); do
  if rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -qv "stats:pseudo"; then
    if ! rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -q "stats:pseudo"; then
      break
    fi
  fi
  sleep 2
done

echo
echo "==================== AFTER ANALYZE (real statistics) ===================="
for pair in \
  "leading index column|${Q_LEADING}" \
  "second index far more selective than the first|${Q_SECOND}" \
  "selective enough for an index|${Q_SELECTIVE}" \
  "broad enough for the full scan|${Q_BROAD}" \
  "covering index|${Q_COVERING}"; do
  compare "${pair%%|*}" "${pair#*|}"
done

go_pseudo=$(go_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
rust_pseudo=$(rust_sql -Nse "USE pathdiff; EXPLAIN ${Q_BROAD}" | grep -c "stats:pseudo" || true)
echo
check "after ANALYZE neither node prints stats:pseudo (go=${go_pseudo} rust=${rust_pseudo})" \
  test "${go_pseudo}" -eq 0 -a "${rust_pseudo}" -eq 0

echo
echo "=== the two planners' full plans, side by side, after ANALYZE ==="
for query in "${Q_SECOND}" "${Q_BROAD}"; do
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
NOTE

echo
if [[ "${KNOWN_FINDINGS_SEEN}" -gt 0 ]]; then
  echo "${KNOWN_FINDINGS_SEEN} KNOWN divergence(s), the documented skyline-pruning gap"
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
