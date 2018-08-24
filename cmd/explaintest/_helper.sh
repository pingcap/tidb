#!/bin/bash
# set -ueax
# when you want to debug, please uncomment the above line and comment the following line.
set -uea

OLD_GOPATH=$GOPATH
PINGCAP_PATH="$(dirname $PWD)"
while [ "$(basename $PINGCAP_PATH)" != "pingcap" ]; do
    PINGCAP_PATH="$(dirname $PINGCAP_PATH)"
done
TIDB_VENDOR_PATH=$PINGCAP_PATH/tidb/vendor

function prepare_env {
    export GOPATH=$GOPATH:$TIDB_VENDOR_PATH
}

function recover_env {
    export GOPATH=$OLD_GOPATH
}

function kill_proc_by_port() {
    if [ ! -z $1 ]; then
        kill $(lsof -t -i:$1)
    else 
        echo "please specify port number"
    fi
}

set +uea
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT

