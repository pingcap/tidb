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

cp "$TEST_DIR/certs/lightning.pem" "$TEST_DIR/certs/lightning-valid.pem"
trap 'mv "$TEST_DIR/certs/lightning-valid.pem" "$TEST_DIR/certs/lightning.pem"' EXIT

# shellcheck disable=SC2089
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/SetCertExpiredSoon=return(\"$TEST_DIR/certs/ca.key\")"
export GO_FAILPOINTS="${GO_FAILPOINTS};github.com/pingcap/tidb/br/pkg/lightning/restore/SlowDownWriteRows=sleep(15000)"

# 1. After 10s, the certificate will be expired and import should report connection error.
run_lightning --backend='local' &
shpid="$!"
sleep 15
ok=0
for _ in {0..60}; do
  if grep -Fq "connection closed before server preface received" "$TEST_DIR"/lightning.log; then
    ok=1
    break
  fi
  sleep 1
done
# Lightning process is wrapped by a shell process, use pstree to extract it out.
pid=$(pstree -p "$shpid" | grep -Eo "tidb-lightning\.\([0-9]*\)" | grep -Eo "[0-9]*")
if [ -n "$pid" ]; then
  kill -9 "$pid" &>/dev/null || true
fi
if [ "$ok" = "0" ]; then
  echo "lightning should report connection error due to certificate expired, but no error is reported"
  exit 1
fi
# Do some cleanup.
cp "$TEST_DIR/certs/lightning-valid.pem" "$TEST_DIR/certs/lightning.pem"
rm -rf "$TEST_DIR/lightning_reload_cert.sorted" "$TEST_DIR"/lightning.log

# 2. Replace the certificate with a valid certificate before it is expired. Lightning should import successfully.
sleep 10 && cp "$TEST_DIR/certs/lightning-valid.pem" "$TEST_DIR/certs/lightning.pem" &
run_lightning --backend='local'
