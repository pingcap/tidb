#!/usr/bin/env bash

# Copyright 2026 PingCAP, Inc.
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

build=1
mysql_tester="./mysql_tester"
tidb_server=""
mysql_tester_log="./loadablefunc-test.out"
tests=""
record=0
record_case=""

set -eu
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS 2>/dev/null || true' EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

function help_message() {
  cat <<'USAGE'
Usage: ./run-tests.sh [options]

  -h: Print this help message.

  -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                         Example: "./run-tests.sh -s ../../bin/tidb-server"

  -b <y|Y|n|N>: "y" or "Y" for building test binaries [default "y" if omitted].
                "n" or "N" for not building.
                The building of tidb-server will be skipped if "-s <tidb-server-path>" is provided.

  -r <test-name>|all: Run tests in file "t/<test-name>.test" and record result to file "r/<test-name>.result".
                      "all" records all tests.

  -t <test-name>: Run tests in file "t/<test-name>.test".
                  Ignored if "-r" is provided.
                  Run all tests if not provided.
USAGE
}

function find_available_port() {
  local port=$1
  while :; do
    if [ "$port" -ge 65536 ]; then
      echo "Error: No available ports found below 65536." >&2
      exit 1
    fi
    if ! lsof -i :"$port" &>/dev/null; then
      echo $port
      return 0
    fi
    ((port++))
  done
}

function find_multiple_available_ports() {
  local start_port=$1
  local count=$2
  local ports=()

  while [ ${#ports[@]} -lt $count ]; do
    local available_port
    available_port=$(find_available_port $start_port)
    ports+=($available_port)
    ((start_port = available_port + 1))
  done
  echo "${ports[@]}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIDB_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
UDF_SRC_DIR="${SCRIPT_DIR}/funcs"
UDF_LIB_DIR="${SCRIPT_DIR}/lib"

function build_tidb_server() {
  tidb_server="./loadablefunc_tidb-server"
  echo "building tidb-server binary: ${tidb_server}"
  rm -rf "${tidb_server}"
  CGO_ENABLED=1 GO111MODULE=on go build -o "${tidb_server}" github.com/pingcap/tidb/cmd/tidb-server
}

function build_mysql_tester() {
  echo "building mysql-tester binary: ${mysql_tester}"
  rm -rf "${mysql_tester}"

  local pkg="github.com/pingcap/mysql-tester/src@95f51dcb0494e877ed16bfceb4a82ec0c241c84b"
  if ! GOBIN=$PWD go install "${pkg}"; then
    echo "go install mysql-tester failed with current GOPROXY=$(go env GOPROXY); retry with https://proxy.golang.org,direct"
    if ! GOPROXY="https://proxy.golang.org,direct" GOBIN=$PWD go install "${pkg}"; then
      echo "go install mysql-tester failed again; retry with GOSUMDB=off"
      GOPROXY="https://proxy.golang.org,direct" GOSUMDB="off" GOBIN=$PWD go install "${pkg}"
    fi
  fi

  mv src mysql_tester
}

function build_udfs() {
  echo "building loadable functions into: ${UDF_LIB_DIR}"
  rm -rf "${UDF_LIB_DIR}"
  mkdir -p "${UDF_LIB_DIR}"

  local inc_dir="${TIDB_ROOT}/pkg/expression/loadable_function/include/mysql"

  shopt -s nullglob
  local c_sources=("${UDF_SRC_DIR}"/*.c)
  local cpp_sources=("${UDF_SRC_DIR}"/*.cc "${UDF_SRC_DIR}"/*.cpp "${UDF_SRC_DIR}"/*.cxx)
  shopt -u nullglob

  if [ ${#c_sources[@]} -eq 0 ] && [ ${#cpp_sources[@]} -eq 0 ]; then
    echo "no UDF sources found under: ${UDF_SRC_DIR}" >&2
    exit 1
  fi

  local cc="${CC:-}"
  if [ -n "${cc}" ]; then
    if ! command -v "${cc}" >/dev/null 2>&1; then
      echo "CC=${cc} not found in PATH" >&2
      exit 1
    fi
  else
    if command -v cc >/dev/null 2>&1; then
      cc="cc"
    elif command -v gcc >/dev/null 2>&1; then
      cc="gcc"
    elif command -v clang >/dev/null 2>&1; then
      cc="clang"
    else
      echo "no C compiler found (need one of: cc/gcc/clang), please install it or set CC" >&2
      exit 1
    fi
  fi
  echo "using C compiler: ${cc}"

  local cxx=""
  if [ ${#cpp_sources[@]} -gt 0 ]; then
    if [ -n "${CXX:-}" ]; then
      cxx="${CXX}"
      if ! command -v "${cxx}" >/dev/null 2>&1; then
        echo "CXX=${cxx} not found in PATH" >&2
        exit 1
      fi
    else
      if command -v g++ >/dev/null 2>&1; then
        cxx="g++"
      elif command -v clang++ >/dev/null 2>&1; then
        cxx="clang++"
      elif command -v c++ >/dev/null 2>&1; then
        cxx="c++"
      else
        echo "no C++ compiler found (need one of: g++/clang++/c++), please install it or set CXX" >&2
        exit 1
      fi
    fi
    echo "using C++ compiler: ${cxx}"
  fi

  if [ ${#c_sources[@]} -gt 0 ]; then
    for src in "${c_sources[@]}"; do
      local base
      base="$(basename "${src}")"
      local out="${UDF_LIB_DIR}/${base%.c}.so"
      echo "  ${base} -> $(basename "${out}")"
      "${cc}" -std=c11 -O2 -fPIC -shared -I"${inc_dir}" -o "${out}" "${src}"
    done
  fi

  if [ ${#cpp_sources[@]} -gt 0 ]; then
    for src in "${cpp_sources[@]}"; do
      local base
      base="$(basename "${src}")"
      local out="${UDF_LIB_DIR}/${base%.*}.so"
      echo "  ${base} -> $(basename "${out}")"
      "${cxx}" -std=c++17 -O2 -fPIC -shared -I"${inc_dir}" -o "${out}" "${src}"
    done
  fi
}

while getopts "t:s:r:b:h" opt; do
  case $opt in
  t)
    tests="$OPTARG"
    ;;
  s)
    tidb_server="$OPTARG"
    ;;
  r)
    record=1
    record_case="$OPTARG"
    ;;
  b)
    case $OPTARG in
    y | Y)
      build=1
      ;;
    n | N)
      build=0
      ;;
    *)
      help_message 1>&2
      exit 1
      ;;
    esac
    ;;
  h)
    help_message
    exit 0
    ;;
  *)
    help_message 1>&2
    exit 1
    ;;
  esac
done

build_udfs

if [ $build -eq 1 ]; then
  if [ -z "$tidb_server" ]; then
    build_tidb_server
  else
    echo "skip building tidb-server, using existing binary: $tidb_server"
  fi
  build_mysql_tester
else
  if [ -z "$tidb_server" ]; then
    tidb_server="./loadablefunc_tidb-server"
    if [[ ! -f "$tidb_server" ]]; then
      build_tidb_server
    else
      echo "skip building tidb-server, using existing binary: $tidb_server"
    fi
  fi
  if [ -z "$mysql_tester" ]; then
    mysql_tester="./mysql_tester"
    if [[ ! -f "$mysql_tester" ]]; then
      build_mysql_tester
    else
      echo "skip building mysql-tester, using existing binary: $mysql_tester"
    fi
  fi
fi

rm -rf "${mysql_tester_log}"

ports=($(find_multiple_available_ports 4400 2))
port=${ports[0]}
status=${ports[1]}

function start_tidb_server() {
  local plugin_dir
  plugin_dir="$(cd "${UDF_LIB_DIR}" && pwd)"

  echo "start tidb-server, log file: ${mysql_tester_log}"
  "${tidb_server}" \
    -P "${port}" \
    -status "${status}" \
    -config "${SCRIPT_DIR}/config.toml" \
    -store unistore \
    -path "" \
    --plugin-dir "${plugin_dir}" >"${mysql_tester_log}" 2>&1 &
  SERVER_PID=$!
  echo "tidb-server(PID: $SERVER_PID) started, plugin-dir: ${plugin_dir}"
}

function run_mysql_tester() {
  if [ $record -eq 1 ]; then
    if [ "$record_case" = 'all' ]; then
      echo "record all cases"
      "${mysql_tester}" -port "${port}" --check-error=true --record
    else
      echo "record result for case: \"${record_case}\""
      "${mysql_tester}" -port "${port}" --check-error=true --record "${record_case}"
    fi
  else
    if [ -z "${tests}" ]; then
      echo "run all loadablefunc test cases"
    else
      echo "run loadablefunc test cases: ${tests}"
    fi
    "${mysql_tester}" -port "${port}" --check-error=true ${tests}
  fi
}

start_tidb_server
run_mysql_tester
kill -15 "${SERVER_PID}" 2>/dev/null || true
while ps -p "${SERVER_PID}" >/dev/null; do
  sleep 1
done

echo "loadablefunc integration test passed!"
