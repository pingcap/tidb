#!/bin/bash
#
# Copyright 2022 PingCAP, Inc.
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

set -eux

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/server/EnableTestAPI=return"
export GO_FAILPOINTS="${GO_FAILPOINTS};github.com/pingcap/tidb/pkg/lightning/backend/local/ReadyForImportEngine=sleep(10000)"

run_lightning --backend='local' &
shpid="$!"
pid=

ensure_lightning_is_started() {
  for _ in {0..60}; do
    pid=$(pstree -p "$shpid" | grep -Eo "tidb-lightning\.\([0-9]*\)" | grep -Eo "[0-9]*") || true
    [ -n "$pid" ] && break
    sleep 1
  done
  if [ -z "$pid" ]; then
    echo "lightning doesn't start successfully, please check the log" >&2
    exit 1
  fi
  echo "lightning is started, pid is $pid"
}

ready_for_import_engine() {
  for _ in {0..60}; do
    grep -Fq "start import engine" "$TEST_DIR"/lightning.log && return
    sleep 1
  done
  echo "lightning doesn't start import engine, please check the log" >&2
  exit 1
}

ensure_lightning_is_started
ready_for_import_engine

run_curl "https://${PD_ADDR}/pd/api/v1/config/cluster-version"

length=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | jq '[ .[] | select(.rule_type == "key-range" and .labels[0].key == "schedule") ] | length')
if [ "$length" != "1" ]; then
  echo "region-label key-range schedule rules should be 1, but got $length" >&2
  exit 1
fi

wait "$shpid"

length=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | jq '[ .[] | select(.rule_type == "key-range" and .labels[0].key == "schedule") ] | length')
if [ -n "$length" ] && [ "$length" -ne 0 ]; then
  echo "region-label key-range schedule rules should be 0, but got $length" >&2
  exit 1
fi
