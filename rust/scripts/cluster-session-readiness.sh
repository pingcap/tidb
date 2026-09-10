#!/usr/bin/env bash
# Shared by cluster-session integration runners after the TCP listener opens.
wait_for_cluster_session_ready() {
  local node_pid=$1 node_log=$2 ready_deadline=$((SECONDS + 180))
  # bind() precedes memory-runner and signal initialization. Like Go's server
  # health flag, application readiness is later than listener availability.
  while ! grep -F '"event":"cluster_session_node_ready"' "${node_log}" >/dev/null; do
    if ! kill -0 "${node_pid}" 2>/dev/null; then
      echo "the Rust node exited before reporting ready" >&2
      cat "${node_log}" >&2
      return 1
    fi
    if ((SECONDS >= ready_deadline)); then
      echo "the Rust node never reported ready within 180 seconds" >&2
      cat "${node_log}" >&2
      return 1
    fi
    sleep 1
  done
}
