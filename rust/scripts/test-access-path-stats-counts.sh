#!/usr/bin/env bash
# Exercise the runner's real pre-comparison barrier with independently delayed caches.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUST_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
BIG_ROWS=50000
round=0
scenario=delayed
barrier=$(awk '/^wait_for_cluster_session_ready / { capture=1; next } capture && /^# The chosen access path/ { exit } capture { print }' "${SCRIPT_DIR}/run-realtikv-access-path.sh")

stats_rows() {
  local ready=$1 table count
  for table in big risky t u; do
    count=2000
    [[ "$table" != big ]] || count=$BIG_ROWS
    ((ready)) || count=0
    printf 'pathdiff\t%s\t\t2026-01-01 00:00:00\t0\t%s\tNULL\n' "$table" "$count"
  done
}
go_sql() { stats_rows "$((round >= 2))"; }
rust_sql() {
  [[ "$scenario" != sql_error ]] || return 1
  if [[ "$scenario" == delayed ]] && ((round >= 3)); then
    stats_rows 1
  else
    stats_rows 0
  fi
}
sleep() {
  round=$((round + 1))
  if [[ "$scenario" == delayed ]]; then
    SECONDS=$((SECONDS + 1))
  else
    SECONDS=$((SECONDS + 180))
  fi
}
eval "$barrier"
if ((round < 3)); then
  echo 'FAIL: comparisons started before both statistics counts caught up' >&2
  exit 1
fi
echo 'PASS: independently delayed statistics counts converge before comparisons'
for scenario in stuck sql_error; do
  if ( eval "$barrier" ) >/dev/null 2>&1; then
    echo "FAIL: ${scenario} statistics accepted" >&2
    exit 1
  fi
done
echo 'PASS: stale statistics and SQL errors cannot pass the barrier'
