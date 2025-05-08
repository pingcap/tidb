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
  cd ../../ || exit 1
  echo "building tidb-server..."
  make
  echo "build successfully"

  cd - || exit 1

  echo "Starting TiUP Playground in the background..."
  tiup playground nightly --db=1 --kv=1 --tiflash=1 --db.binpath=../../bin/tidb-server &

  sleep 20
}

function start_tidb_latest() {
  echo "Starting TiUP Playground in the background..."
  tiup playground nightly --db=1 --kv=1 --tiflash=1 &

  sleep 20
}

function build_mysql_tester() {
  echo "+ Installing mysql-tester"
  go install github.com/pingcap/mysql-tester/src@314107b26aa8fce86beb0dd48e75827fb269b365
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

  for i in {1..30}; do
    if ! pgrep -f "tiup-playground" > /dev/null; then
      echo "  - TiUP stopped successfully"
      return
    fi
    sleep 1
  done

  echo "* Fail to stop TiUP in 30s"
  exit 1
}

function print_versions() {
  # Print versions
  echo "+ TiDB Version"
  ../../bin/tidb-server -V
  echo
  echo "+ TiKV Version"
  ~/.tiup/components/tikv/v9.0.0-beta.1.pre-nightly/tikv-server --version
  echo
  echo "+ TiFlash Version"
  ~/.tiup/components/tiflash/v9.0.0-beta.1.pre-nightly/tiflash/tiflash version
  echo
  echo "+ TiUP Version"
  ~/.tiup/bin/tiup playground -v
}