#!/bin/bash
#
# Copyright 2019 PingCAP, Inc.
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

# currently the script is WIP and not used in the CI.

set -eu
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export UTILS_DIR="$CUR/../../tests/_utils"
export PATH="$PATH:$CUR/../../bin:$UTILS_DIR"
export TEST_DIR=/tmp/lightning_test
export COV_DIR="/tmp/group_cover"
mkdir -p $COV_DIR || true
export TIKV_CONFIG="$CUR/config/tikv-no-tls.toml"
export PD_CONFIG="$CUR/config/pd-no-tls.toml"
export TESTS_ROOT="$CUR"
source $UTILS_DIR/run_services

export TIKV_COUNT=1
export PD_HTTP_PROTO="http"

# Create COV_DIR if not exists
if [ -d "$COV_DIR" ]; then
  mkdir -p $COV_DIR
fi

# Reset TEST_DIR
rm -rf $TEST_DIR && mkdir -p $TEST_DIR

trap stop_services EXIT
start_services $@ --no-tiflash --no-tidb

rm /tmp/*.sst || true

# change to project root
cd $CUR/../..
go test ./pkg/lightning/tikv -tikv-write-test -test.v -test.run TestIntegrationTest
cp /tmp/lightning_test/tikv1/import/*_write_*.sst /tmp/tikv-write-cf.sst

for prefix in go tikv; do
  bin/tikv-ctl sst_dump --file=/tmp/$prefix-write-cf.sst --command=scan --output_hex --show_properties \
    > /tmp/$prefix-write-cf-scan.txt
  awk "
    /from \[\] to \[\]/ { f1 = 1; next }
    /Table Properties:/ { f1 = 0; f2 = 1; next }
    f1 { print > \"/tmp/$prefix-write-cf-data.txt\" }
    f2 { print > \"/tmp/$prefix-write-cf-properties.txt\" }
  " /tmp/$prefix-write-cf-scan.txt
  # filter some properties that are not deterministic by logical content
  grep -v -F -e "data block size" -e "index block size" -e "entries for filter" -e "(estimated) table size" \
    -e "DB identity" -e "DB session identity" -e "DB host id" -e "original file number" -e "unique ID" \
    /tmp/$prefix-write-cf-properties.txt > /tmp/$prefix-write-cf-properties.txt.filtered
done

diff /tmp/tikv-write-cf-data.txt /tmp/go-write-cf-data.txt
diff /tmp/tikv-write-cf-properties.txt.filtered /tmp/go-write-cf-properties.txt.filtered

# clean tikv-ctl temporary files
rm -rf ctl-engine-info-log || true
