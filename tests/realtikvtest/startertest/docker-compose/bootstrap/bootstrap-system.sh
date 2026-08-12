#!/bin/sh
set -eu

log_file=/var/log/tidb/bootstrap-system.log
stdout_file=/var/log/tidb/bootstrap-system.stdout.log
tidb_pid=

terminate_tidb() {
    if [ -z "${tidb_pid}" ] || ! kill -0 "${tidb_pid}" 2>/dev/null; then
        return 0
    fi

    kill -TERM "${tidb_pid}"
    attempt=1
    while [ "${attempt}" -le 30 ]; do
        if ! kill -0 "${tidb_pid}" 2>/dev/null; then
            wait "${tidb_pid}" || true
            tidb_pid=
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    echo "SYSTEM bootstrap TiDB did not exit within 30 seconds after TERM; sending KILL" >&2
    kill -KILL "${tidb_pid}"
    wait "${tidb_pid}" || true
    tidb_pid=
    return 1
}

on_exit() {
    exit_code=$?
    cleanup_code=0
    terminate_tidb || cleanup_code=$?
    if [ "${exit_code}" -ne 0 ]; then
        exit "${exit_code}"
    fi
    exit "${cleanup_code}"
}
on_signal() {
    trap - EXIT INT TERM
    terminate_tidb
    exit 130
}
trap on_exit EXIT
trap on_signal INT TERM

mkdir -p /var/log/tidb
/opt/tidb/bin/tidb-server \
    -P 4000 \
    -status 10080 \
    -host 0.0.0.0 \
    -advertise-address bootstrap-tidb \
    -status-host 0.0.0.0 \
    -store tikv \
    -path pd:2379 \
    -config /etc/tidb/starter.toml \
    -config-strict \
    -keyspace-name SYSTEM \
    -tidb-service-scope dxf_service \
    -log-file "${log_file}" \
    >"${stdout_file}" 2>&1 &
tidb_pid=$!

attempt=1
while [ "${attempt}" -le 120 ]; do
    if curl -fsS --max-time 2 http://127.0.0.1:10080/status >/dev/null 2>&1; then
        terminate_tidb
        exit 0
    fi
    if ! kill -0 "${tidb_pid}" 2>/dev/null; then
        wait "${tidb_pid}" || exit_code=$?
        echo "SYSTEM bootstrap TiDB exited before metadata bootstrap completed (exit ${exit_code:-0})" >&2
        cat "${stdout_file}" >&2 || true
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 1
done

echo "SYSTEM bootstrap TiDB did not become ready within 120 seconds" >&2
cat "${stdout_file}" >&2 || true
exit 1
