#!/usr/bin/env bash
# Copyright 2020 PingCAP, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT

function help_message()
{
    echo "Usage: $0 [options]"
    echo '
    -h: Print this help message.

    -L <info|warn|error>: Log level of testing. Defaults to "info".

    --server_log_level <info|warn|error>: Log level of TiDB server. Defaults to "info".
    
    --tmp <temporary path>: Temporary files path. Defaults to "/tmp/tidb_globalkilltest".

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           Defaults to "bin/globalkilltest_tidb-server".

    -p <pd-server-path>: Use pd-server in <pd-server-path> for testing.
                         Defaults to "bin/pd-server".

    -k <tikv-server-path>: Use tikv-server in <tikv-server-path> for testing.
                           Defaults to "bin/tikv-server".

    --tidb_start_port <port>: First TiDB server listening port. port ~ port+2 will be used.
                              Defaults to "5000".

    --tidb_status_port <port>: First TiDB server status listening port. port ~ port+2 will be used.
                               Defaults to "8000".

    --conn_lost <timeout in seconds>: Lost connection to PD timeout,
                                      should be the same as TiDB ldflag <ldflagLostConnectionToPDTimeout>.
                                      See tidb/Makefile for detail.
                                      Defaults to "5".

    --conn_restored <timeout in seconds>: Time to check PD connection restored,
                                          should be the same as TiDB ldflag 
                                          <ldflagServerIDTimeToCheckPDConnectionRestored>.
                                          See tidb/Makefile for detail.
                                          Defaults to "1".
'
}

function clean_cluster()
{
    set +e
    pkill -9 -f tidb-server
    pkill -9 -f tikv-server
    pkill -9 -f pd-server
    set -e
}

function go_tests()
{
    go test -args $*
}

while getopts "h" opt; do
    case $opt in
        h)
            help_message
            exit 0
            ;;
    esac
done

clean_cluster

go_tests

clean_cluster

echo "globalkilltest end"
