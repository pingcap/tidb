// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source test for Go `pkg/config/config_test.go::TestRemovedVariableCheck`
//! (the full TiDB 6.0-era config file). Comment lines from the Go heredoc are
//! dropped; every live option line is kept verbatim so the removed-item set is
//! identical to the Go test's.

use tidb_config::config_tree::new_config;

/// The exact error message expected by Go `TestRemovedVariableCheck` for the
/// 6.0 config file (all removed items, comma-joined in sorted order).
const EXPECTED_6_0_ERROR: &str = "The following configuration options are no longer supported \
in this version of TiDB. Check the release notes for more information: check-mb4-value-in-utf8, \
enable-batch-dml, instance.tidb_memory_usage_alarm_ratio, log.enable-slow-log, \
log.expensive-threshold, log.query-log-max-len, log.record-plan-in-slow-log, log.slow-threshold, \
lower-case-table-names, mem-quota-query, oom-action, performance.committer-concurrency, \
performance.feedback-probability, performance.force-priority, performance.memory-usage-alarm-ratio, \
performance.query-feedback-limit, performance.run-auto-analyze, prepared-plan-cache.capacity, \
prepared-plan-cache.enabled, prepared-plan-cache.memory-guard-ratio";

const TIDB_6_0_CONFIG: &str = r#"
host = "0.0.0.0"
advertise-address = ""
port = 4000
store = "unistore"
path = "/tmp/tidb"
socket = "/tmp/tidb-{Port}.sock"
lease = "45s"
split-table = true
token-limit = 1000
mem-quota-query = 1073741824
tmp-storage-quota = -1
oom-action = "cancel"
enable-batch-dml = false
lower-case-table-names = 2
compatible-kill-query = false
graceful-wait-before-shutdown = 0
check-mb4-value-in-utf8 = true
treat-old-version-utf8-as-utf8mb4 = true
max-index-length = 3072
index-limit = 64
enable-table-lock = false
delay-clean-table-lock = 0
split-region-max-num = 1000
alter-primary-key = false
server-version = ""
repair-mode = false
repair-table-list = []
new_collations_enabled_on_first_bootstrap = true
skip-register-to-dashboard = false
enable-telemetry = true
deprecate-integer-display-length = true
enable-enum-length-limit = true
[instance]
tidb_memory_usage_alarm_ratio = 0.7
max_connections = 0
tidb_enable_ddl = true
[log]
level = "info"
format = "text"
enable-slow-log = true
slow-query-file = "tidb-slow.log"
slow-threshold = 300
record-plan-in-slow-log = 1
expensive-threshold = 10000
query-log-max-len = 4096
[log.file]
filename = ""
max-size = 300
max-days = 0
max-backups = 0
[security]
ssl-ca = ""
ssl-cert = ""
ssl-key = ""
cluster-ssl-ca = ""
cluster-ssl-cert = ""
cluster-ssl-key = ""
spilled-file-encryption-method = "plaintext"
enable-sem = false
auto-tls = true
tls-version = ""
rsa-key-size = 4096
[status]
report-status = true
status-host = "0.0.0.0"
status-port = 10080
metrics-addr = ""
metrics-interval = 15
record-db-qps = false
record-db-label = false
[performance]
max-procs = 0
server-memory-quota = 0
memory-usage-alarm-ratio = 0.7
stmt-count-limit = 5000
tcp-keep-alive = true
cross-join = true
stats-lease = "3s"
run-auto-analyze = true
feedback-probability = 0.0
query-feedback-limit = 512
pseudo-estimate-ratio = 0.8
force-priority = "NO_PRIORITY"
bind-info-lease = "3s"
distinct-agg-push-down = false
txn-total-size-limit = 104857600
txn-entry-size-limit = 6291456
committer-concurrency = 128
max-txn-ttl = 3600000
gogc = 100
[proxy-protocol]
networks = ""
header-timeout = 5
[prepared-plan-cache]
enabled = false
capacity = 1000
memory-guard-ratio = 0.1
[opentracing]
enable = false
rpc-metrics = false
[opentracing.sampler]
type = "const"
param = 1.0
sampling-server-url = ""
max-operations = 0
sampling-refresh-interval = 0
[opentracing.reporter]
queue-size = 0
buffer-flush-interval = 0
log-spans = false
local-agent-host-port = ""
[pd-client]
pd-server-timeout = 3
[tikv-client]
grpc-connection-count = 4
grpc-keepalive-time = 10
grpc-keepalive-timeout = 3
grpc-compression-type = "none"
commit-timeout = "41s"
max-batch-size = 128
overload-threshold = 200
max-batch-wait-time = 0
batch-wait-size = 8
enable-chunk-rpc = true
region-cache-ttl = 600
store-limit = 0
store-liveness-timeout = "1s"
ttl-refreshed-txn-size = 33554432
resolve-lock-lite-threshold = 16
[tikv-client.copr-cache]
capacity-mb = 1000.0
ignore-error = false
binlog-socket = ""
[pessimistic-txn]
max-retry-count = 256
deadlock-history-capacity = 10
deadlock-history-collect-retryable = false
pessimistic-auto-commit = false
[experimental]
allow-expression-index = false
[isolation-read]
engines = ["tikv", "tiflash", "tidb"]
"#;

// Go `TestRemovedVariableCheck`, first table case: an unknown option is not a
// removed option, so `RemovedVariableCheck` itself reports no error.
#[test]
fn removed_variable_check_ignores_unrecognized_options() {
    let conf = new_config();
    conf.removed_variable_check("unrecognized-option-test = true\n")
        .unwrap();
}

// Go `TestRemovedVariableCheck`, second table case: the TiDB 6.0 config file
// produces exactly the sorted removed-items message.
#[test]
fn removed_variable_check_full_6_0_config_matches_source() {
    let conf = new_config();
    let err = conf
        .removed_variable_check(TIDB_6_0_CONFIG)
        .unwrap_err();
    assert_eq!(err, EXPECTED_6_0_ERROR);
}

// Go `TestRemovedVariableCheck`, tail check: the shipped example config must
// not contain any removed items ("bad user experience" guard).
#[test]
fn removed_variable_check_passes_on_example_config() {
    let conf = new_config();
    conf.removed_variable_check(include_str!(
        "../../../../pkg/config/config.toml.example"
    ))
    .unwrap();
}
