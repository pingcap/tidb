#!/bin/sh
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

cat > $TEST_DIR/tiflash-learner.toml <<EOF
[rocksdb]
wal-dir = ""

[security]
ca-path = "$TEST_DIR/certs/ca.pem"
cert-path = "$TEST_DIR/certs/tiflash.pem"
key-path = "$TEST_DIR/certs/tiflash.key"

[server]
addr = "0.0.0.0:20170"
advertise-addr = "127.0.0.1:20170"
status-addr = "127.0.0.1:20292"
engine-addr = "127.0.0.1:3930"

[storage]
data-dir = "$TEST_DIR/tiflash/data"
reserve-space = "1KB"
EOF

cat > $TEST_DIR/tiflash.toml <<EOF
listen_host = "0.0.0.0"
path = "$TEST_DIR/tiflash/data"
tcp_port_secure = 9002
tmp_path = "$TEST_DIR/tiflash/tmp"
capacity = "10737418240"

[application]
runAsDaemon = true

[flash]
service_addr = "127.0.0.1:3930"
tidb_status_addr = "127.0.0.1:10080"

[flash.proxy]
config = "$TEST_DIR/tiflash-learner.toml"
log-file = "$TEST_DIR/tiflash-proxy.log"

[logger]
count = 20
level = "debug"
log = "$TEST_DIR/tiflash-stdout.log"
errorlog = "$TEST_DIR/tiflash-stderr.log"
size = "1000M"

[raft]
pd_addr = "$PD_ADDR"

[profiles]
[profiles.default]
max_memory_usage = 10000000000

[security]
ca_path = "$TEST_DIR/certs/ca.pem"
cert_path = "$TEST_DIR/certs/tiflash.pem"
key_path = "$TEST_DIR/certs/tiflash.key"
EOF
