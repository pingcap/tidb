#!/bin/bash
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

set -euo pipefail

function start_tidb() {
  export VERSION_SOURCE="nightly"

  if [ ! -f "../../bin/tidb-server" ]; then
    cd ../../ || exit 1
    echo "building tidb-server..."
    make
    echo "build successfully"
    cd - || exit 1
  fi

  echo "Starting TiUP Playground in the background..."
  if [ -f "../../bin/tikv-server" ] && [ -f "../../bin/pd-server" ] && [ -f "../../bin/tiflash" ]; then
    tiup playground nightly --mode=tidb \
    --db.binpath=../../bin/tidb-server \
    --db.config=./config.toml \
    --kv.binpath=../../bin/tikv-server \
    --pd.binpath=../../bin/pd-server \
    --tiflash.binpath=../../bin/tiflash &
  else
    tiup playground nightly --db=1 --kv=1 --tiflash=1 --db.binpath=../../bin/tidb-server --db.config=./config.toml &
  fi
}

function check_and_prepare_datasets() {
  if [ -f "./fashion-mnist-784-euclidean.hdf5" ] && [ -f "./mnist-784-euclidean.hdf5" ]; then
    echo "Datasets already exist, skip"
    return
  fi

  if [ -d "${ASSETS_DIR}" ]; then
    if [ -f "${ASSETS_DIR}/fashion-mnist-784-euclidean.hdf5" ]; then
      echo "Moving fashion-mnist dataset from ASSETS_DIR..."
      mv "${ASSETS_DIR}/fashion-mnist-784-euclidean.hdf5" .
    else
      echo "Downloading fashion-mnist dataset..."
      wget https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5
    fi

    if [ -f "${ASSETS_DIR}/mnist-784-euclidean.hdf5" ]; then
      echo "Moving mnist dataset from ASSETS_DIR..."
      mv "${ASSETS_DIR}/mnist-784-euclidean.hdf5" .
    else
      echo "Downloading mnist dataset..."
      wget https://ann-benchmarks.com/mnist-784-euclidean.hdf5
    fi
  else
    echo "Downloading fashion-mnist dataset..."
    wget https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5

    echo "Downloading mnist dataset..."
    wget https://ann-benchmarks.com/mnist-784-euclidean.hdf5

  fi
}

function start_tidb_fixed_version() {
  export VERSION_SOURCE="v8.5.1"

  echo "Starting TiUP Playground in the background..."
  tiup playground v8.5.1 --db=1 --kv=1 --tiflash=1 --db.config=./config.toml &
}

function build_mysql_tester() {
  echo "+ Installing mysql-tester"
  GOBIN=$PWD go install github.com/pingcap/mysql-tester/src@f2d90ea9522d30c9a8e8d70cc31c7f016ca2801f
  mv src mysql-tester
}

function wait_for_tidb() {
  echo
  echo "+ Waiting TiDB start up"

  for i in {1..30}; do
    if mysql -e 'show databases' -u root -h 127.0.0.1 --port 4000; then
      echo "  - TiDB startup successfully"
      return
    fi
    sleep 3
  done
  echo "* Fail to start TiDB cluster in 900s"
  exit 1
}

function wait_for_tiflash() {
  echo
  echo "+ Waiting TiFlash start up (30s)"
  sleep 30
}

function stop_tiup() {
  echo "+ Stopping TiUP"
  TIUP_PID=$(pgrep -f "tiup-playground")
  if [ -n "$TIUP_PID" ]; then
    echo "  - Sending SIGTERM to PID=$TIUP_PID"
    kill $TIUP_PID
  fi

  for i in {1..60}; do
    if ! pgrep -f "tiup-playground" > /dev/null; then
      echo "  - TiUP stopped successfully"
      return
    fi
    sleep 1
  done

  echo "* Fail to stop TiUP in 60s"
  exit 1
}

function print_versions() {
  # Print versions
  if [ "$VERSION_SOURCE" = "nightly" ]; then
    echo "+ TiDB Version"
    ../../bin/tidb-server -V
    echo
    if [ -f "../../bin/tikv-server" ] && [ -f "../../bin/pd-server" ] && [ -f "../../bin/tiflash" ]; then
      echo "+ TiKV Version"
      ../../bin/tikv-server --version
      echo
      echo "+ TiFlash Version"
      ../../bin/tiflash --version
      echo
    else
      echo "+ TiKV Version"
      tiup tikv:nightly --version
      echo
      echo "+ TiFlash Version"
      tiup tiflash:nightly --version
      echo
    fi
  else
    echo "+ TiDB Version"
    tiup tidb:v8.5.1 -V
    echo
    echo "+ TiKV Version"
    tiup tikv:v8.5.1 --version
    echo
    echo "+ TiFlash Version"
    tiup tiflash:v8.5.1 --version
    echo
  fi

  echo "+ TiUP Version"
  ~/.tiup/bin/tiup playground -v
  
}