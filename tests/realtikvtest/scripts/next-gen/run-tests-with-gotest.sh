#! /usr/bin/env bash

# It need TCP ports:
# - pd: 2379, 2380, 2381, 2383, 2384
# - tikv: 20160, 20161, 20162, 20180, 20181, 20182
# - tikv-worker: 20001
function main() {
    local test_suite="$1"
    local suite_timeout="${2:-40m}"
    local data_base_dir=$(mktemp -d)
    mkdir -pv ${data_base_dir}/pd-{0,1,2}/data
    mkdir -pv ${data_base_dir}/tikv-{0,1,2}/data
    mkdir -pv ${data_base_dir}/tikv-worker/data

    local config_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../../configs/next-gen")"
    if [[ ! -d "${config_dir}" ]]; then
        echo "Error: config_dir '${config_dir}' does not exist." >&2
        exit 1
    fi

    # start the servers.
    bin/pd-server --name=pd-0 --config=pd.toml --data-dir=${data_base_dir}/pd-0/data --peer-urls=http://127.0.0.1:2380 --advertise-peer-urls=http://127.0.0.1:2380 --client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd0.log &
    bin/pd-server --name=pd-1 --config=pd.toml --data-dir=${data_base_dir}/pd-1/data --peer-urls=http://127.0.0.1:2381 --advertise-peer-urls=http://127.0.0.1:2381 --client-urls=http://127.0.0.1:2382 --advertise-client-urls=http://127.0.0.1:2382 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd1.log &
    bin/pd-server --name=pd-2 --config=pd.toml --data-dir=${data_base_dir}/pd-2/data --peer-urls=http://127.0.0.1:2383 --advertise-peer-urls=http://127.0.0.1:2383 --client-urls=http://127.0.0.1:2384 --advertise-client-urls=http://127.0.0.1:2384 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd2.log &
    bin/tikv-server --config=tikv.toml --data-dir=${data_base_dir}tikv-0/data --addr=127.0.0.1:20160 --advertise-addr=127.0.0.1:20160 --status-addr=127.0.0.1:20180 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv0.log &
    bin/tikv-server --config=tikv.toml --data-dir=${data_base_dir}tikv-1/data --addr=127.0.0.1:20161 --advertise-addr=127.0.0.1:20161 --status-addr=127.0.0.1:20181 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv1.log &
    bin/tikv-server --config=tikv.toml --data-dir=${data_base_dir}tikv-2/data --addr=127.0.0.1:20162 --advertise-addr=127.0.0.1:20162 --status-addr=127.0.0.1:20182 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv2.log &
    bin/tikv-worker --config=tikv-worker.toml --data-dir=${data_base_dir}/tikv-worker/data --addr=127.0.0.1:20001 --pd-endpoints=http://127.0.0.1:2379

    sleep 10
    NEXT_GEN=1 go test ./tests/realtikvtest/${test_suite} -v --tags=intest -with-real-tikv -timeout ${suite_timeout}
}

function cleanup() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS: no -r option
        killall -9 -q tikv-worker || true
        killall -9 -q tikv-server || true
        killall -9 -q pd-server || true
    else
        # Linux: supports -r for regex
        killall -9 -r -q tikv-worker || true
        killall -9 -r -q tikv-server || true
        killall -9 -r -q pd-server || true
    fi
}

exit_code=0
{ # try block
    main "$@"
} || { # catch block
   exit_code="$?"  # exit code of last command which is 44
}
# finally block:
cleanup

if [[ "$exit_code" != '0' ]]; then
   exit ${exit_code}
fi
