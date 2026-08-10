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

//! Source tests for the end-to-end load/default path from Go
//! `pkg/config/config_test.go::TestConfig`.

use std::collections::HashMap;

use tidb_config::config_tree::{is_all_removed_config_items, new_config, AtomicBool, LoadError};

fn example_config_text() -> &'static str {
    if tidb_config::kerneltype::is_next_gen() {
        include_str!("../../../../pkg/config/config.toml.nextgen.example")
    } else {
        include_str!("../../../../pkg/config/config.toml.example")
    }
}

#[test]
fn config_load_rejects_unknown_and_removed_items() {
    // Go `TestConfig`: the first file load rejects unknown keys.
    let mut conf = new_config();
    let err = conf
        .load_str("config.toml", "unrecognized-option-test = true\n")
        .unwrap_err();
    match err {
        LoadError::ValidationFailed {
            undecoded_items, ..
        } => {
            assert_eq!(undecoded_items, vec!["unrecognized-option-test".to_owned()]);
        }
        other => panic!("expected ValidationFailed, got {other:?}"),
    }

    // Go `TestConfig`: removed items are reported as a validation failure
    // and every undecoded item belongs to the removed-item set.
    let text = r#"
[log.file]
log-rotate = true
[performance]
mem-profile-interval = "1m"
[stmt-summary]
enable = false
enable-internal-query = true
max-stmt-count = 1000
max-sql-length = 1024
refresh-interval = 100
history-size = 100
"#;
    let err = conf.load_str("config.toml", text).unwrap_err();
    match err {
        LoadError::ValidationFailed {
            undecoded_items, ..
        } => assert!(is_all_removed_config_items(&undecoded_items)),
        other => panic!("expected ValidationFailed, got {other:?}"),
    }
}

#[test]
fn config_load_applies_source_overrides() {
    // Go `TestConfig`: the bulk config file load overwrites defaults.
    let text = r#"
token-limit = 0
enable-table-lock = true
alter-primary-key = true
delay-clean-table-lock = 5
split-region-max-num = 10000
server-version = "test_version"
repair-mode = true
max-index-length = 3080
index-limit = 70
table-column-count-limit = 4000
skip-register-to-dashboard = true
deprecate-integer-display-length = true
enable-enum-length-limit = false
stores-refresh-interval = 30
enable-forwarding = true
enable-global-kill = true
tidb-max-reuse-chunk = 10
tidb-max-reuse-column = 20
tidb-enable-exit-check = false
[performance]
txn-total-size-limit = 2000
tcp-no-delay = false
enable-load-fmsketch = true
plan-replayer-dump-worker-concurrency = 1
skip-init-stats = false
lite-init-stats = true
force-init-stats = false
[tikv-client]
commit-timeout = "41s"
max-batch-size = 128
region-cache-ttl = 6000
store-limit = 0
ttl-refreshed-txn-size = 8192
resolve-lock-lite-threshold = 16
copr-req-timeout = 120000000000
grpc-keepalive-timeout = 0.2
[tikv-client.async-commit]
keys-limit = 123
total-key-size-limit = 1024
[experimental]
allow-expression-index = true
[isolation-read]
engines = ["tiflash"]
[labels]
foo = "bar"
group = "abc"
zone = "dc-1"
[security]
spilled-file-encryption-method = "plaintext"
[pessimistic-txn]
deadlock-history-capacity = 123
deadlock-history-collect-retryable = true
pessimistic-auto-commit = true
[top-sql]
receiver-address = "127.0.0.1:10100"
[status]
grpc-keepalive-time = 20
grpc-keepalive-timeout = 10
grpc-concurrent-streams = 2048
grpc-initial-window-size = 10240
grpc-max-send-msg-size = 40960
[instance]
max_connections = 200
"#;

    let mut conf = new_config();
    conf.load_str("config.toml", text).unwrap();
    conf.valid().unwrap();

    assert_eq!(conf.token_limit, 1000);
    assert!(conf.enable_table_lock);
    assert!(conf.alter_primary_key);
    assert_eq!(conf.delay_clean_table_lock, 5);
    assert_eq!(conf.split_region_max_num, 10_000);
    assert_eq!(conf.server_version, "test_version");
    assert!(conf.repair_mode);
    assert_eq!(conf.max_index_length, 3080);
    assert_eq!(conf.index_limit, 70);
    assert_eq!(conf.table_column_count_limit, 4000);
    assert!(conf.skip_register_to_dashboard);
    assert!(conf.deprecate_integer_display_width);
    assert!(!conf.enable_enum_length_limit);
    assert_eq!(conf.stores_refresh_interval, 30);
    assert!(conf.enable_forwarding);
    assert!(conf.enable_global_kill);
    assert_eq!(conf.tidb_max_reuse_chunk, 10);
    assert_eq!(conf.tidb_max_reuse_column, 20);
    assert!(!conf.tidb_enable_exit_check);

    assert_eq!(conf.performance.txn_total_size_limit, 2000);
    assert!(!conf.performance.tcp_no_delay);
    assert!(conf.performance.enable_load_fmsketch);
    assert_eq!(conf.performance.plan_replayer_dump_worker_concurrency, 1);
    assert!(!conf.performance.skip_init_stats);
    assert!(conf.performance.lite_init_stats);
    assert!(!conf.performance.force_init_stats);

    assert_eq!(conf.tikv_client.commit_timeout, "41s");
    assert_eq!(conf.tikv_client.max_batch_size, 128);
    assert_eq!(conf.tikv_client.region_cache_ttl, 6000);
    assert_eq!(conf.tikv_client.store_limit, 0);
    assert_eq!(conf.tikv_client.ttl_refreshed_txn_size, 8192);
    assert_eq!(conf.tikv_client.resolve_lock_lite_threshold, 16);
    assert_eq!(conf.tikv_client.copr_req_timeout, 120 * 1_000_000_000);
    assert_eq!(
        conf.tikv_client.grpc_keep_alive_timeout_nanos(),
        200_000_000
    );
    assert_eq!(conf.tikv_client.async_commit.keys_limit, 123);
    assert_eq!(conf.tikv_client.async_commit.total_key_size_limit, 1024);

    assert!(conf.experimental.allows_expression_index);
    assert_eq!(conf.isolation_read.engines, vec!["tiflash".to_owned()]);
    assert_eq!(
        conf.labels,
        HashMap::from([
            ("foo".to_owned(), "bar".to_owned()),
            ("group".to_owned(), "abc".to_owned()),
            ("zone".to_owned(), "dc-1".to_owned()),
        ])
    );
    assert_eq!(conf.security.spilled_file_encryption_method, "plaintext");
    assert_eq!(conf.pessimistic_txn.deadlock_history_capacity, 123);
    assert!(conf.pessimistic_txn.deadlock_history_collect_retryable);
    assert!(conf.pessimistic_txn.pessimistic_auto_commit.load());
    assert_eq!(conf.top_sql.receiver_address, "127.0.0.1:10100");
    assert_eq!(conf.status.grpc_keep_alive_time, 20);
    assert_eq!(conf.status.grpc_keep_alive_timeout, 10);
    assert_eq!(conf.status.grpc_concurrent_streams, 2048);
    assert_eq!(conf.status.grpc_initial_window_size, 10_240);
    assert_eq!(conf.status.grpc_max_send_msg_size, 40_960);
    assert_eq!(conf.instance.max_connections, 200);
}

#[test]
fn config_example_matches_global_config_except_auto_tls() {
    let mut conf = new_config();
    conf.load_str("config.toml", example_config_text()).unwrap();

    conf.security.auto_tls = false;
    assert_eq!(conf.ru_v2.ru_scale, 2.01);
    let mut expected = new_config();
    expected.security.auto_tls = false;
    if cfg!(feature = "nextgen") {
        expected.keyspace_name = "SYSTEM".to_owned();
        expected.pessimistic_txn.pessimistic_auto_commit = AtomicBool::new(true);
    }
    assert_eq!(conf, expected);
}
