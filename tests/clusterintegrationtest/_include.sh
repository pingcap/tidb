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
  local playground_ver="${TIUP_PLAYGROUND_VERSION:-nightly}"
  export VERSION_SOURCE="$playground_ver"

  local tidb_binpath="${TIDB_BINPATH:-../../bin/tidb-server}"
  if [ -z "${TIDB_BINPATH:-}" ] && [ ! -f "$tidb_binpath" ]; then
    cd ../../ || exit 1
    echo "building tidb-server..."
    make
    echo "build successfully"
    cd - || exit 1
  fi
  if [ ! -f "$tidb_binpath" ]; then
    echo "* tidb-server binary not found: $tidb_binpath"
    exit 1
  fi

  echo "Starting TiUP Playground in the background..."
  local args=(playground "$playground_ver" --db=1 --kv=1 --tiflash=1 --db.binpath="$tidb_binpath" --db.config=./config.toml)
  if [ -n "${TIKV_BINPATH:-}" ]; then
    if [ ! -f "${TIKV_BINPATH}" ]; then
      echo "* tikv-server binary not found: ${TIKV_BINPATH}"
      exit 1
    fi
    args+=(--kv.binpath="${TIKV_BINPATH}")
  elif [ -f "../../bin/tikv-server" ]; then
    args+=(--kv.binpath=../../bin/tikv-server)
  fi
  if [ -n "${TIFLASH_BINPATH:-}" ]; then
    if [ ! -f "${TIFLASH_BINPATH}" ]; then
      echo "* tiflash binary not found: ${TIFLASH_BINPATH}"
      exit 1
    fi
    args+=(--tiflash.binpath="${TIFLASH_BINPATH}")
  elif [ -f "../../bin/tiflash" ]; then
    args+=(--tiflash.binpath=../../bin/tiflash)
  fi
  if [ -f "../../bin/pd-server" ]; then
    args+=(--pd.binpath=../../bin/pd-server)
  fi

  tiup "${args[@]}" &
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
  local tidb_binpath="${TIDB_BINPATH:-../../bin/tidb-server}"

  # Print versions
  echo "+ TiDB Version"
  if [ -f "$tidb_binpath" ]; then
    "$tidb_binpath" -V
  else
    tiup tidb:"$VERSION_SOURCE" -V
  fi
  echo

  echo "+ TiKV Version"
  if [ -n "${TIKV_BINPATH:-}" ]; then
    "${TIKV_BINPATH}" --version
  elif [ -f "../../bin/tikv-server" ]; then
    ../../bin/tikv-server --version
  else
    tiup tikv:"$VERSION_SOURCE" --version
  fi
  echo

  echo "+ TiFlash Version"
  if [ -n "${TIFLASH_BINPATH:-}" ]; then
    "${TIFLASH_BINPATH}" --version
  elif [ -f "../../bin/tiflash" ]; then
    ../../bin/tiflash --version
  else
    tiup tiflash:"$VERSION_SOURCE" --version
  fi
  echo

  echo "+ TiUP Version"
  ~/.tiup/bin/tiup playground -v
  
}
