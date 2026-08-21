#! /usr/bin/env bash
#
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

readonly pd_client_urls="http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384"

function clear_bootstrap_logs() {
    : > pd0.log
    : > pd1.log
    : > pd2.log
    : > tikv0.log
    : > tikv1.log
    : > tikv2.log
    : > tikv-worker.log
}

function wait_for_pd_leader() {
    for _ in {1..60}; do
        if grep -q "PD leader is ready to serve" pd0.log pd1.log pd2.log 2>/dev/null; then
            echo "PD leader is ready"
            return 0
        fi
        sleep 1
    done
    echo "PD leader did not become ready" >&2
    tail -n 100 pd0.log pd1.log pd2.log >&2 || true
    return 1
}

function wait_for_tikv_cluster() {
    for _ in {1..60}; do
        if curl -fsS http://127.0.0.1:20180/status >/dev/null 2>&1 && \
            curl -fsS http://127.0.0.1:20181/status >/dev/null 2>&1 && \
            curl -fsS http://127.0.0.1:20182/status >/dev/null 2>&1 && \
            curl -fsS http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null |
                grep -Eq '"count"[[:space:]]*:[[:space:]]*3([[:space:]]*[,}])'; then
            echo "TiKV cluster is ready"
            return 0
        fi
        sleep 1
    done
    echo "TiKV cluster did not become ready" >&2
    tail -n 100 tikv0.log tikv1.log tikv2.log >&2 || true
    return 1
}

function stop_tikv_cluster() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        killall -9 -q tikv-server || true
    else
        killall -9 -r -q tikv-server || true
    fi
}

function start_tikv_cluster() {
    local config_dir="$1"
    local data_base_dir="$2"

    for attempt in {1..3}; do
        echo "Starting TiKV cluster (attempt ${attempt}/3)"
        bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-0/data --addr=127.0.0.1:20160 --advertise-addr=127.0.0.1:20160 --status-addr=127.0.0.1:20180 --pd=${pd_client_urls} --log-file=tikv0.log &
        bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-1/data --addr=127.0.0.1:20161 --advertise-addr=127.0.0.1:20161 --status-addr=127.0.0.1:20181 --pd=${pd_client_urls} --log-file=tikv1.log &
        bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-2/data --addr=127.0.0.1:20162 --advertise-addr=127.0.0.1:20162 --status-addr=127.0.0.1:20182 --pd=${pd_client_urls} --log-file=tikv2.log &
        if wait_for_tikv_cluster; then
            return 0
        fi
        stop_tikv_cluster
        sleep 2
    done
    return 1
}

function wait_for_tikv_worker() {
    for _ in {1..60}; do
        if (echo > /dev/tcp/127.0.0.1/19000) >/dev/null 2>&1; then
            echo "TiKV worker is ready"
            return 0
        fi
        sleep 1
    done
    echo "TiKV worker did not become ready" >&2
    tail -n 100 tikv-worker.log >&2 || true
    return 1
}
