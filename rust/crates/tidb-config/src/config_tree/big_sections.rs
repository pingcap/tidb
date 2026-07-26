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

//! The larger sub-section structs of Go `pkg/config/config.go`'s `Config`:
//! `Security`, `Status`, and `Performance`, with their `defaultConf`
//! values.

use serde::{Deserialize, Serialize};

/// "plaintext" disables spilled-file encryption (Go
/// `SpilledFileEncryptionMethodPlaintext`).
pub const SPILLED_FILE_ENCRYPTION_METHOD_PLAINTEXT: &str = "plaintext";
/// AES-128-CTR spilled-file encryption (Go
/// `SpilledFileEncryptionMethodAES128CTR`).
pub const SPILLED_FILE_ENCRYPTION_METHOD_AES128_CTR: &str = "aes128-ctr";

/// Security section of the config (Go `Security`).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Security {
    /// Skip the grant table (needs root).
    #[serde(rename = "skip-grant-table")]
    pub skip_grant_table: bool,
    /// SQL TLS CA path.
    #[serde(rename = "ssl-ca")]
    pub ssl_ca: String,
    /// SQL TLS cert path.
    #[serde(rename = "ssl-cert")]
    pub ssl_cert: String,
    /// SQL TLS key path.
    #[serde(rename = "ssl-key")]
    pub ssl_key: String,
    /// Cluster TLS CA path.
    #[serde(rename = "cluster-ssl-ca")]
    pub cluster_ssl_ca: String,
    /// Cluster TLS cert path.
    #[serde(rename = "cluster-ssl-cert")]
    pub cluster_ssl_cert: String,
    /// Cluster TLS key path.
    #[serde(rename = "cluster-ssl-key")]
    pub cluster_ssl_key: String,
    /// Cluster verified common names.
    #[serde(rename = "cluster-verify-cn")]
    pub cluster_verify_cn: Vec<String>,
    /// Session-token signing cert (for `tidb_session_token`).
    #[serde(rename = "session-token-signing-cert")]
    pub session_token_signing_cert: String,
    /// Session-token signing key.
    #[serde(rename = "session-token-signing-key")]
    pub session_token_signing_key: String,
    /// Spilled-file encryption method (`plaintext` disables).
    #[serde(rename = "spilled-file-encryption-method")]
    pub spilled_file_encryption_method: String,
    /// Whether Security Enhanced Mode is enabled.
    #[serde(rename = "enable-sem")]
    pub enable_sem: bool,
    /// Path to the SEM configuration file.
    #[serde(rename = "sem-config")]
    pub sem_config: String,
    /// Allow automatic TLS certificate generation.
    #[serde(rename = "auto-tls")]
    pub auto_tls: bool,
    /// Minimum TLS version.
    #[serde(rename = "tls-version")]
    pub min_tls_version: String,
    /// RSA key size for auto-TLS.
    #[serde(rename = "rsa-key-size")]
    pub rsa_key_size: i64,
    /// Whether bootstrap is secure.
    #[serde(rename = "secure-bootstrap")]
    pub secure_bootstrap: bool,
    /// JWKS path for `tidb_auth_token`.
    #[serde(rename = "auth-token-jwks")]
    pub auth_token_jwks: String,
    /// JWKS refresh interval (a Go-duration string).
    #[serde(rename = "auth-token-refresh-interval")]
    pub auth_token_refresh_interval: String,
    /// Disconnect directly when the password is expired.
    #[serde(rename = "disconnect-on-expired-password")]
    pub disconnect_on_expired_password: bool,
}

impl Default for Security {
    // From Go `defaultConf.Security` (fields not listed default to zero).
    fn default() -> Self {
        Security {
            skip_grant_table: false,
            ssl_ca: String::new(),
            ssl_cert: String::new(),
            ssl_key: String::new(),
            cluster_ssl_ca: String::new(),
            cluster_ssl_cert: String::new(),
            cluster_ssl_key: String::new(),
            cluster_verify_cn: Vec::new(),
            session_token_signing_cert: String::new(),
            session_token_signing_key: String::new(),
            spilled_file_encryption_method: SPILLED_FILE_ENCRYPTION_METHOD_PLAINTEXT.into(),
            enable_sem: false,
            sem_config: String::new(),
            auto_tls: false,
            min_tls_version: String::new(),
            rsa_key_size: 4096,
            secure_bootstrap: false,
            auth_token_jwks: String::new(),
            // Go: DefAuthTokenRefreshInterval.String() == "1h0m0s".
            auth_token_refresh_interval: "1h0m0s".into(),
            disconnect_on_expired_password: true,
        }
    }
}

/// Status section of the config (Go `Status`).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Status {
    /// Status server host.
    #[serde(rename = "status-host")]
    pub status_host: String,
    /// Metrics push address.
    #[serde(rename = "metrics-addr")]
    pub metrics_addr: String,
    /// Status server port.
    #[serde(rename = "status-port")]
    pub status_port: u32,
    /// Metrics push interval, seconds.
    #[serde(rename = "metrics-interval")]
    pub metrics_interval: u32,
    /// Whether the status server reports.
    #[serde(rename = "report-status")]
    pub report_status: bool,
    /// Whether to record QPS by DB.
    #[serde(rename = "record-db-qps")]
    pub record_qps_by_db: bool,
    /// Whether to record the DB label.
    #[serde(rename = "record-db-label")]
    pub record_db_label: bool,
    /// gRPC keepalive ping interval, seconds.
    #[serde(rename = "grpc-keepalive-time")]
    pub grpc_keep_alive_time: u32,
    /// gRPC keepalive timeout, seconds.
    #[serde(rename = "grpc-keepalive-timeout")]
    pub grpc_keep_alive_timeout: u32,
    /// Max concurrent streams per client connection.
    #[serde(rename = "grpc-concurrent-streams")]
    pub grpc_concurrent_streams: u32,
    /// Initial stream window size.
    #[serde(rename = "grpc-initial-window-size")]
    pub grpc_initial_window_size: i64,
    /// Max gRPC send message size (`-1` unlimited).
    #[serde(rename = "grpc-max-send-msg-size")]
    pub grpc_max_send_msg_size: i64,
}

impl Default for Status {
    // From Go `defaultConf.Status`.
    fn default() -> Self {
        Status {
            status_host: "0.0.0.0".into(),
            metrics_addr: String::new(),
            status_port: 10080,
            metrics_interval: 15,
            report_status: true,
            record_qps_by_db: false,
            record_db_label: false,
            grpc_keep_alive_time: 10,
            grpc_keep_alive_timeout: 3,
            grpc_concurrent_streams: 1024,
            grpc_initial_window_size: 2 * 1024 * 1024,
            grpc_max_send_msg_size: i32::MAX as i64,
        }
    }
}

/// Performance section of the config (Go `Performance`).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Performance {
    /// Max OS threads.
    #[serde(rename = "max-procs")]
    pub max_procs: u32,
    /// Deprecated: use `server-memory-quota`.
    #[serde(rename = "max-memory")]
    pub max_memory: u64,
    /// Server memory quota.
    #[serde(rename = "server-memory-quota")]
    pub server_memory_quota: u64,
    /// Stats lease (Go-duration string).
    #[serde(rename = "stats-lease")]
    pub stats_lease: String,
    /// Deprecated statement count limit.
    #[serde(rename = "stmt-count-limit")]
    pub stmt_count_limit: u32,
    /// Pseudo estimate ratio.
    #[serde(rename = "pseudo-estimate-ratio")]
    pub pseudo_estimate_ratio: f64,
    /// Bind-info lease (Go-duration string).
    #[serde(rename = "bind-info-lease")]
    pub bind_info_lease: String,
    /// Per-entry txn size limit.
    #[serde(rename = "txn-entry-size-limit")]
    pub txn_entry_size_limit: u64,
    /// Total txn size limit.
    #[serde(rename = "txn-total-size-limit")]
    pub txn_total_size_limit: u64,
    /// TCP keepalive.
    #[serde(rename = "tcp-keep-alive")]
    pub tcp_keep_alive: bool,
    /// TCP no-delay.
    #[serde(rename = "tcp-no-delay")]
    pub tcp_no_delay: bool,
    /// Cross join allowed.
    #[serde(rename = "cross-join")]
    pub cross_join: bool,
    /// Distinct-agg push down.
    #[serde(rename = "distinct-agg-push-down")]
    pub distinct_agg_push_down: bool,
    /// Max txn TTL.
    #[serde(rename = "max-txn-ttl")]
    pub max_txn_ttl: u64,
    /// Deprecated mem-profile interval (runtime-only, `toml:"-"`).
    #[serde(skip)]
    pub mem_profile_interval: String,
    /// Deprecated index-usage sync lease.
    #[serde(rename = "index-usage-sync-lease")]
    pub index_usage_sync_lease: String,
    /// Plan-replayer GC lease (Go-duration string).
    #[serde(rename = "plan-replayer-gc-lease")]
    pub plan_replayer_gc_lease: String,
    /// GOGC percentage.
    #[serde(rename = "gogc")]
    pub gogc: i64,
    /// Enforce MPP.
    #[serde(rename = "enforce-mpp")]
    pub enforce_mpp: bool,
    /// Stats-load concurrency (0 = auto).
    #[serde(rename = "stats-load-concurrency")]
    pub stats_load_concurrency: i64,
    /// Stats-load request queue size.
    #[serde(rename = "stats-load-queue-size")]
    pub stats_load_queue_size: u32,
    /// Deprecated analyze-partition concurrency quota.
    #[serde(rename = "analyze-partition-concurrency-quota")]
    pub analyze_partition_concurrency_quota: u32,
    /// Plan-replayer dump-worker concurrency.
    #[serde(rename = "plan-replayer-dump-worker-concurrency")]
    pub plan_replayer_dump_worker_concurrency: u32,
    /// Enable stats-cache mem quota.
    #[serde(rename = "enable-stats-cache-mem-quota")]
    pub enable_stats_cache_mem_quota: bool,
    /// Deprecated committer concurrency.
    #[serde(rename = "committer-concurrency")]
    pub committer_concurrency: i64,
    /// Deprecated run-auto-analyze.
    #[serde(rename = "run-auto-analyze")]
    pub run_auto_analyze: bool,
    /// Deprecated force priority.
    #[serde(rename = "force-priority")]
    pub force_priority: String,
    /// Deprecated memory-usage alarm ratio.
    #[serde(rename = "memory-usage-alarm-ratio")]
    pub memory_usage_alarm_ratio: f64,
    /// Deprecated enable-load-fmsketch.
    #[serde(rename = "enable-load-fmsketch")]
    pub enable_load_fmsketch: bool,
    /// Skip init stats.
    #[serde(rename = "skip-init-stats")]
    pub skip_init_stats: bool,
    /// Lite init stats.
    #[serde(rename = "lite-init-stats")]
    pub lite_init_stats: bool,
    /// Force init stats before serving.
    #[serde(rename = "force-init-stats")]
    pub force_init_stats: bool,
    /// Deprecated concurrently-init-stats.
    #[serde(rename = "concurrently-init-stats")]
    pub concurrently_init_stats: bool,
    /// Deprecated projection push-down.
    #[serde(rename = "projection-push-down")]
    pub projection_push_down: bool,
    /// Use async API for batch-get.
    #[serde(rename = "enable-async-batch-get")]
    pub enable_async_batch_get: bool,
}

impl Default for Performance {
    // From Go `defaultConf.Performance`.
    fn default() -> Self {
        Performance {
            max_procs: 0,
            max_memory: 0,
            server_memory_quota: 0,
            stats_lease: "3s".into(),
            stmt_count_limit: 5000,
            pseudo_estimate_ratio: 0.8,
            bind_info_lease: "3s".into(),
            txn_entry_size_limit: 6 * 1024 * 1024,
            txn_total_size_limit: 100 * 1024 * 1024,
            tcp_keep_alive: true,
            tcp_no_delay: true,
            cross_join: true,
            distinct_agg_push_down: false,
            max_txn_ttl: 60 * 60 * 1000, // defTiKVCfg.MaxTxnTTL (1 hour)
            mem_profile_interval: String::new(),
            index_usage_sync_lease: String::new(),
            plan_replayer_gc_lease: "10m".into(),
            gogc: 100,
            enforce_mpp: false,
            stats_load_concurrency: 0,
            stats_load_queue_size: 1000,
            analyze_partition_concurrency_quota: 16,
            plan_replayer_dump_worker_concurrency: 1,
            enable_stats_cache_mem_quota: true,
            committer_concurrency: 128, // defTiKVCfg.CommitterConcurrency
            run_auto_analyze: true,
            force_priority: "NO_PRIORITY".into(),
            memory_usage_alarm_ratio: 0.8, // DefMemoryUsageAlarmRatio
            enable_load_fmsketch: false,
            skip_init_stats: false,
            lite_init_stats: true,
            force_init_stats: true,
            concurrently_init_stats: true,
            projection_push_down: true,
            enable_async_batch_get: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let s = Security::default();
        assert_eq!(s.spilled_file_encryption_method, "plaintext");
        assert_eq!(s.rsa_key_size, 4096);
        assert_eq!(s.auth_token_refresh_interval, "1h0m0s");
        assert!(s.disconnect_on_expired_password);

        let st = Status::default();
        assert_eq!(st.status_port, 10080);
        assert_eq!(st.grpc_max_send_msg_size, i32::MAX as i64);
        assert!(st.report_status);

        let p = Performance::default();
        assert!(p.tcp_no_delay); // TestTcpNoDelay
        assert_eq!(p.txn_total_size_limit, 100 * 1024 * 1024);
        assert_eq!(p.stats_load_queue_size, 1000);
        assert_eq!(p.committer_concurrency, 128);
        assert_eq!(p.stats_lease, "3s");
        assert!(p.lite_init_stats && p.force_init_stats);
    }

    #[test]
    fn security_toml_roundtrip() {
        let toml = r#"
spilled-file-encryption-method = "aes128-ctr"
rsa-key-size = 2048
"#;
        let s: Security = toml::from_str(toml).unwrap();
        assert_eq!(s.spilled_file_encryption_method, "aes128-ctr");
        assert_eq!(s.rsa_key_size, 2048);
        // Unspecified keeps default.
        assert!(s.disconnect_on_expired_password);
    }
}
