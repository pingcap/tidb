#!/usr/bin/env bash
# Copyright 2023 PingCAP, Inc.
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


set -euxo pipefail

# Prepare pd-server & tikv-server
mkdir -p bin
tiup install pd:nightly tikv:nightly
cp ~/.tiup/components/pd/$(ls ~/.tiup/components/pd | tail -1)/pd-server bin/
cp ~/.tiup/components/tikv/$(ls ~/.tiup/components/tikv | tail -1)/tikv-server bin/

cd ../..
TIDB_PATH=$(pwd)

docker build -t globalkilltest -f tests/globalkilltest/Dockerfile .

# To see the logs, mount an additional volume to /tmp. E.g. -v $TIDB_PATH/tmp:/tmp
docker run --name globalkilltest -it --rm \
  -v $TIDB_PATH:/tidb \
  globalkilltest \
  /bin/bash -c \
  'git config --global --add safe.directory /tidb && cd /tidb/tests/globalkilltest && make && ./run-tests.sh'
