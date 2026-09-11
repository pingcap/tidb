#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
printf '0\n' >"${WORK_DIR}/calls"
rust_root_sql() {
  local calls
  calls=$(<"${WORK_DIR}/calls")
  calls=$((calls + 1))
  printf '%s\n' "$calls" >"${WORK_DIR}/calls"
  if ((calls == 1)); then
    printf '%s\n' "GRANT SELECT,INSERT ON *.* TO 'rustmade'@'%'" \
      'GRANT SELECT,UPDATE ON `conv`.`orders` TO '\''rustmade'\''@'\''%'\'
  else
    printf '%s\n' "GRANT SELECT,INSERT,UPDATE ON *.* TO 'rustmade'@'%'"
  fi
}
sleep() { :; }
check=$(awk '/^wait_for_rust_grant\(\)/ { capture=1 } capture { print } capture && /^wait_for_rust_grant / { exit }' \
  "${SCRIPT_DIR}/run-realtikv-convergence.sh")
[[ -n "$check" ]]
eval "$check"
if [[ $(<"${WORK_DIR}/calls") != 2 ]]; then
  echo 'FAIL: table UPDATE was mistaken for the new global UPDATE' >&2
  exit 1
fi
echo 'PASS: global privilege observation waits beyond an existing table grant'
