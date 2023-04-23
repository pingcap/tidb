#!/bin/bash

set -euxo pipefail

# Prepare pd-server & tikv-server
mkdir -p bin
tiup install pd:nightly tikv:nightly
cp ~/.tiup/components/pd/$(ls ~/.tiup/components/pd | tail -1)/pd-server bin/
cp ~/.tiup/components/tikv/$(ls ~/.tiup/components/pd | tail -1)/tikv-server bin

TIDB_PATH=$(builtin cd ../..; pwd)

docker build -t globalkilltest .
docker run --name globalkilltest -it --rm -v $TIDB_PATH:/tidb globalkilltest /bin/bash -c 'cd /tidb/tests/globalkilltest && make && ./run-tests.sh'
