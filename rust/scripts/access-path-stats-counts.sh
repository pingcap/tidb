#!/usr/bin/env bash
# SHOW STATS_META reads each node's cache, not only the durable stats_meta row.
access_path_stats_counts_match() {
  awk -F '\t' -v big_rows="${BIG_ROWS}" '
    BEGIN { expected["t"]=2000; expected["u"]=2000; expected["risky"]=2000; expected["big"]=big_rows }
    $1 == "pathdiff" && $2 in expected {
      if (seen[$2]++ || $3 != "" || $6 != expected[$2]) invalid=1
      count++
    }
    END { exit !(count == 4 && !invalid) }
  '
}

wait_for_access_path_stats_counts() {
  local deadline=$((SECONDS + 180)) go_meta rust_meta
  while true; do
    go_meta=$(go_sql -Nse "SHOW STATS_META WHERE Db_name = 'pathdiff'") || return 1
    rust_meta=$(rust_sql -Nse "SHOW STATS_META WHERE Db_name = 'pathdiff'") || return 1
    if access_path_stats_counts_match <<<"${go_meta}" \
      && access_path_stats_counts_match <<<"${rust_meta}"; then
      echo 'both statistics caches contain the seeded fixture row counts'
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo 'fixture row counts did not reach both statistics caches within 180 seconds' >&2
      printf 'Go SHOW STATS_META:\n%s\nRust SHOW STATS_META:\n%s\n' "${go_meta}" "${rust_meta}" >&2
      return 1
    fi
    sleep 1
  done
}
