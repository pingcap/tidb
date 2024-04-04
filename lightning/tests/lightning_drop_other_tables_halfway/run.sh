#!/bin/sh
#
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

set -eu

run_sql 'DROP DATABASE IF EXISTS lntest'
run_sql 'CREATE DATABASE lntest'
run_sql 'CREATE TABLE lntest.tbl02 (id BIGINT PRIMARY KEY AUTO_INCREMENT, val VARCHAR(255));'

GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/getTableStructuresByFileMeta_BeforeFetchRemoteTableModels=return(6000)" run_lightning &
pid=$!

sleep 4
run_sql 'DROP TABLE lntest.tbl02;' && echo "table dropped"
wait "${pid}"
