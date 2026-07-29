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

//! Transcreation of the `github.com/tikv/client-go/v2/config` struct shapes
//! (config.go + client.go + security.go).
//!
//! These are the config-file-facing data types TiDB's `pkg/config.Config`
//! embeds (the `[tikv-client]`, `[pd-client]`, `[txn-local-latches]`,
//! `[security]` sections), carried here for TOML/JSON fidelity. The actual
//! TiKV client is `tikv/client-rust`; this module intentionally holds only
//! the data + `Valid()` validation + defaults, with no client, gRPC, or
//! TLS-opening logic (Go's `Security.ToTLSConfig` and the global-config
//! singleton belong to the client/runtime layers, not the config shapes).

use serde::{Deserialize, Serialize};

/// Default stores-refresh interval in seconds (Go `DefStoresRefreshInterval`).
pub const DEF_STORES_REFRESH_INTERVAL: u64 = 60;
/// Default gRPC initial window size, 128 MiB (Go `DefGrpcInitialWindowSize`).
pub const DEF_GRPC_INITIAL_WINDOW_SIZE: i32 = 1 << 27;
/// Default gRPC initial connection window size (Go
/// `DefGrpcInitialConnWindowSize`).
pub const DEF_GRPC_INITIAL_CONN_WINDOW_SIZE: i32 = 1 << 27;
/// Default max concurrency request limit (Go `DefMaxConcurrencyRequestLimit`).
pub const DEF_MAX_CONCURRENCY_REQUEST_LIMIT: i64 = i64::MAX;
/// Default store-liveness timeout (Go `DefStoreLivenessTimeout`).
pub const DEF_STORE_LIVENESS_TIMEOUT: &str = "1s";

/// Batch policy consistent with pre-v8.3.0 (Go `BatchPolicyBasic`).
pub const BATCH_POLICY_BASIC: &str = "basic";
/// Dynamic batching by request arrival intervals (Go `BatchPolicyStandard`).
pub const BATCH_POLICY_STANDARD: &str = "standard";
/// Always additionally batch (Go `BatchPolicyPositive`).
pub const BATCH_POLICY_POSITIVE: &str = "positive";
/// Custom internal batch options (Go `BatchPolicyCustom`).
pub const BATCH_POLICY_CUSTOM: &str = "custom";
/// Default batch policy (Go `DefBatchPolicy`).
pub const DEF_BATCH_POLICY: &str = BATCH_POLICY_STANDARD;

/// The global txn scope (Go `oracle.GlobalTxnScope`).
pub const GLOBAL_TXN_SCOPE: &str = "global";

/// Security section (Go `tikvcfg.Security`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Security {
    /// Cluster CA path.
    #[serde(rename = "cluster-ssl-ca")]
    pub cluster_ssl_ca: String,
    /// Cluster cert path.
    #[serde(rename = "cluster-ssl-cert")]
    pub cluster_ssl_cert: String,
    /// Cluster key path.
    #[serde(rename = "cluster-ssl-key")]
    pub cluster_ssl_key: String,
    /// Verified common names.
    #[serde(rename = "cluster-verify-cn")]
    pub cluster_verify_cn: Vec<String>,
}

impl Security {
    /// Go `NewSecurity`.
    pub fn new(
        ssl_ca: String,
        ssl_cert: String,
        ssl_key: String,
        verify_cn: Vec<String>,
    ) -> Security {
        Security {
            cluster_ssl_ca: ssl_ca,
            cluster_ssl_cert: ssl_cert,
            cluster_ssl_key: ssl_key,
            cluster_verify_cn: verify_cn,
        }
    }
}

/// PD client config (Go `PDClient`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PdClient {
    /// Max time PD client waits for the PD server, in seconds.
    #[serde(rename = "pd-server-timeout")]
    pub pd_server_timeout: u32,
}

impl Default for PdClient {
    fn default() -> Self {
        PdClient {
            pd_server_timeout: 3,
        }
    }
}

impl PdClient {
    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        if self.pd_server_timeout == 0 {
            return Err("pd-server-timeout can not be 0".into());
        }
        Ok(())
    }
}

/// Transaction-local latches (Go `TxnLocalLatches`; `toml:"-"` so both
/// fields are runtime-only, not config-file loaded).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct TxnLocalLatches {
    /// Whether local latches are enabled.
    pub enabled: bool,
    /// Latch capacity.
    pub capacity: u32,
}

impl TxnLocalLatches {
    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        if self.enabled && self.capacity == 0 {
            return Err("txn-local-latches.capacity can not be 0".into());
        }
        Ok(())
    }
}

/// Pessimistic transaction config (Go `tikvcfg.PessimisticTxn`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PessimisticTxn {
    /// Max retry count for a single statement.
    #[serde(rename = "max-retry-count")]
    pub max_retry_count: u32,
}

/// Async-commit config (Go `AsyncCommit`; durations are nanoseconds).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AsyncCommit {
    /// Use async commit only under this key count.
    #[serde(rename = "keys-limit")]
    pub keys_limit: u32,
    /// Use async commit only under this total key size.
    #[serde(rename = "total-key-size-limit")]
    pub total_key_size_limit: u64,
    /// Safe window for old-schema commit (nanoseconds).
    #[serde(rename = "safe-window")]
    pub safe_window: i64,
    /// Additional clock-drift allowance (nanoseconds).
    #[serde(rename = "allowed-clock-drift")]
    pub allowed_clock_drift: i64,
}

impl Default for AsyncCommit {
    fn default() -> Self {
        AsyncCommit {
            keys_limit: 256,
            total_key_size_limit: 4 * 1024,
            safe_window: 2 * 1_000_000_000,
            allowed_clock_drift: 500 * 1_000_000,
        }
    }
}

/// Coprocessor cache config (Go `CoprocessorCache`).
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CoprocessorCache {
    /// Cache capacity in MB; 0 disables the cache.
    #[serde(rename = "capacity-mb")]
    pub capacity_mb: f64,
    /// Only cache requests with few ranges (hidden from json).
    #[serde(rename = "admission-max-ranges")]
    pub admission_max_ranges: u64,
    /// Only cache small result sets (hidden from json).
    #[serde(rename = "admission-max-result-mb")]
    pub admission_max_result_mb: f64,
    /// Only cache requests taking notable time (hidden from json).
    #[serde(rename = "admission-min-process-ms")]
    pub admission_min_process_ms: u64,
}

impl Default for CoprocessorCache {
    fn default() -> Self {
        CoprocessorCache {
            capacity_mb: 1000.0,
            admission_max_ranges: 500,
            admission_max_result_mb: 10.0,
            admission_min_process_ms: 5,
        }
    }
}

/// RU v2 TiKV-side weights (Go `RUV2TiKVConfig`).
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RuV2TiKVConfig {
    /// Scale factor from RU v2 floats to scaled integer values.
    #[serde(rename = "ru-scale")]
    pub ru_scale: f64,
    /// KV-engine cache-miss weight.
    #[serde(rename = "tikv-kv-engine-cache-miss")]
    pub tikv_kv_engine_cache_miss: f64,
    /// Resource-manager write-count weight.
    #[serde(rename = "resource-manager-write-cnt-tikv")]
    pub resource_manager_write_cnt_tikv: f64,
    /// Executor-inputs weight.
    #[serde(rename = "executor-inputs")]
    pub executor_inputs: f64,
    /// Coprocessor executor-iterations weight.
    #[serde(rename = "tikv-coprocessor-executor-iterations")]
    pub tikv_coprocessor_executor_iterations: f64,
    /// Coprocessor response-bytes weight.
    #[serde(rename = "tikv-coprocessor-response-bytes")]
    pub tikv_coprocessor_response_bytes: f64,
    /// Raftstore write-trigger write-batch weight.
    #[serde(rename = "tikv-raftstore-store-write-trigger-wb-bytes")]
    pub tikv_raftstore_store_write_trigger_wb: f64,
    /// Storage processed-keys (batch get) weight.
    #[serde(rename = "tikv-storage-processed-keys-batch-get")]
    pub tikv_storage_processed_keys_batch_get: f64,
    /// Storage processed-keys (get) weight.
    #[serde(rename = "tikv-storage-processed-keys-get")]
    pub tikv_storage_processed_keys_get: f64,
}

impl Default for RuV2TiKVConfig {
    fn default() -> Self {
        RuV2TiKVConfig {
            ru_scale: 2.10,
            tikv_kv_engine_cache_miss: 0.45975389,
            resource_manager_write_cnt_tikv: 0.09642181,
            executor_inputs: 0.00003150,
            tikv_coprocessor_executor_iterations: 0.05775369,
            tikv_coprocessor_response_bytes: 0.00000087,
            tikv_raftstore_store_write_trigger_wb: 0.00006100,
            tikv_storage_processed_keys_batch_get: 0.00266791,
            tikv_storage_processed_keys_get: 0.01416829,
        }
    }
}

/// TiKV client config (Go `TiKVClient`; durations are nanoseconds).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TiKVClient {
    /// Max gRPC connections per tikv-server.
    #[serde(rename = "grpc-connection-count")]
    pub grpc_connection_count: u32,
    /// gRPC keepalive ping interval, seconds.
    #[serde(rename = "grpc-keepalive-time")]
    pub grpc_keep_alive_time: u32,
    /// gRPC keepalive timeout, seconds (float).
    #[serde(rename = "grpc-keepalive-timeout")]
    pub grpc_keep_alive_timeout: f64,
    /// gRPC channel compression: `none` or `gzip`.
    #[serde(rename = "grpc-compression-type")]
    pub grpc_compression_type: String,
    /// Share the gRPC buffer pool across TiKV clients.
    #[serde(rename = "grpc-shared-buffer-pool")]
    pub grpc_shared_buffer_pool: bool,
    /// Initial per-stream window size.
    #[serde(rename = "grpc-initial-window-size")]
    pub grpc_initial_window_size: i32,
    /// Initial per-connection window size.
    #[serde(rename = "grpc-initial-conn-window-size")]
    pub grpc_initial_conn_window_size: i32,
    /// Max time `commit` waits.
    #[serde(rename = "commit-timeout")]
    pub commit_timeout: String,
    /// Async-commit sub-config.
    #[serde(rename = "async-commit")]
    pub async_commit: AsyncCommit,
    /// Batch policy for requests.
    #[serde(rename = "batch-policy")]
    pub batch_policy: String,
    /// Max batch size for batch-commands.
    #[serde(rename = "max-batch-size")]
    pub max_batch_size: u32,
    /// TiKV-load threshold above which TiDB waits to avoid little batch.
    #[serde(rename = "overload-threshold")]
    pub overload_threshold: u32,
    /// Max wait time for batch (nanoseconds).
    #[serde(rename = "max-batch-wait-time")]
    pub max_batch_wait_time: i64,
    /// Max wait size for batch.
    #[serde(rename = "batch-wait-size")]
    pub batch_wait_size: u32,
    /// Chunk-format encoding for coprocessor requests.
    #[serde(rename = "enable-chunk-rpc")]
    pub enable_chunk_rpc: bool,
    /// Region reload interval on inactivity, seconds.
    #[serde(rename = "region-cache-ttl")]
    pub region_cache_ttl: u32,
    /// Per-store dispatch token limit.
    #[serde(rename = "store-limit")]
    pub store_limit: i64,
    /// Store liveness-check timeout.
    #[serde(rename = "store-liveness-timeout")]
    pub store_liveness_timeout: String,
    /// Coprocessor cache sub-config.
    #[serde(rename = "copr-cache")]
    pub copr_cache: CoprocessorCache,
    /// Single coprocessor-request timeout (nanoseconds).
    #[serde(rename = "copr-req-timeout")]
    pub copr_req_timeout: i64,
    /// Whether a txn updates its TTL based on size.
    #[serde(rename = "ttl-refreshed-txn-size")]
    pub ttl_refreshed_txn_size: i64,
    /// Resolve-lock-lite threshold.
    #[serde(rename = "resolve-lock-lite-threshold")]
    pub resolve_lock_lite_threshold: u64,
    /// Max in-flight requests to a tikv; 0 = auto.
    #[serde(rename = "max-concurrency-request-limit")]
    pub max_concurrency_request_limit: i64,
    /// Deprecated replica-selector-v2 toggle.
    #[serde(rename = "enable-replica-selector-v2")]
    pub enable_replica_selector_v2: bool,
    /// RU v2 TiKV-side weights.
    #[serde(rename = "ru-v2")]
    pub ru_v2: RuV2TiKVConfig,
}

impl Default for TiKVClient {
    fn default() -> Self {
        TiKVClient {
            grpc_connection_count: 4,
            grpc_keep_alive_time: 10,
            grpc_keep_alive_timeout: 3.0,
            grpc_compression_type: "none".into(),
            grpc_shared_buffer_pool: false,
            grpc_initial_window_size: DEF_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEF_GRPC_INITIAL_CONN_WINDOW_SIZE,
            commit_timeout: "41s".into(),
            async_commit: AsyncCommit::default(),
            batch_policy: DEF_BATCH_POLICY.into(),
            max_batch_size: 128,
            overload_threshold: 200,
            max_batch_wait_time: 0,
            batch_wait_size: 8,
            enable_chunk_rpc: true,
            region_cache_ttl: 600,
            store_limit: 0,
            store_liveness_timeout: DEF_STORE_LIVENESS_TIMEOUT.into(),
            copr_cache: CoprocessorCache::default(),
            copr_req_timeout: 60 * 1_000_000_000,
            ttl_refreshed_txn_size: 32 * 1024 * 1024,
            resolve_lock_lite_threshold: 512,
            max_concurrency_request_limit: DEF_MAX_CONCURRENCY_REQUEST_LIMIT,
            enable_replica_selector_v2: true,
            ru_v2: RuV2TiKVConfig::default(),
        }
    }
}

impl TiKVClient {
    /// Go `GetGrpcKeepAliveTimeout`: the timeout as nanoseconds.
    pub fn grpc_keep_alive_timeout_nanos(&self) -> i64 {
        (self.grpc_keep_alive_timeout * 1_000_000_000.0) as i64
    }

    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        if self.grpc_connection_count == 0 {
            return Err("grpc-connection-count should be greater than 0".into());
        }
        if self.grpc_compression_type != "none" && self.grpc_compression_type != "gzip" {
            return Err(format!(
                "grpc-compression-type should be none or gzip, but got {}",
                self.grpc_compression_type
            ));
        }
        if self.grpc_keep_alive_timeout_nanos() < 50 * 1_000_000 {
            return Err(format!(
                "grpc-keepalive-timeout should be at least 0.05, but got {:.6}",
                self.grpc_keep_alive_timeout
            ));
        }
        Ok(())
    }
}

/// The full client-go config (Go `tikvcfg.Config`). TiDB's `config.Config`
/// projects into this via `GetTiKVConfig`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Committer concurrency.
    pub committer_concurrency: i64,
    /// Max txn TTL (ms).
    pub max_txn_ttl: u64,
    /// TiKV client config.
    pub tikv_client: TiKVClient,
    /// Security config.
    pub security: Security,
    /// PD client config.
    pub pd_client: PdClient,
    /// Pessimistic-txn config.
    pub pessimistic_txn: PessimisticTxn,
    /// Transaction-local latches (Go `toml:"-" json:"-"`: runtime-only).
    #[serde(skip)]
    pub txn_local_latches: TxnLocalLatches,
    /// Stores-refresh interval, seconds.
    pub stores_refresh_interval: u64,
    /// Whether OpenTracing is enabled.
    pub open_tracing_enable: bool,
    /// Store path.
    pub path: String,
    /// Whether request forwarding is enabled.
    pub enable_forwarding: bool,
    /// Transaction scope (zone label; deprecating).
    pub txn_scope: String,
    /// Whether async commit is enabled.
    pub enable_async_commit: bool,
    /// Whether 1PC is enabled.
    pub enable_1pc: bool,
    /// Regions-refresh interval, seconds (0 = disabled).
    pub regions_refresh_interval: u64,
    /// Whether to preload region info at client init.
    pub enable_preload: bool,
    /// Whether to use the async batch-get API.
    pub enable_async_batch_get: bool,
    /// The instance's zone label.
    pub zone_label: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            committer_concurrency: 128,
            max_txn_ttl: 60 * 60 * 1000,
            tikv_client: TiKVClient::default(),
            security: Security::default(),
            pd_client: PdClient::default(),
            pessimistic_txn: PessimisticTxn::default(),
            txn_local_latches: TxnLocalLatches::default(),
            stores_refresh_interval: DEF_STORES_REFRESH_INTERVAL,
            open_tracing_enable: false,
            path: String::new(),
            enable_forwarding: false,
            txn_scope: String::new(),
            enable_async_commit: false,
            enable_1pc: false,
            regions_refresh_interval: 0,
            enable_preload: false,
            enable_async_batch_get: false,
            zone_label: String::new(),
        }
    }
}

/// Extracts the effective txn scope (Go `GetTxnScopeFromConfig`, without the
/// failpoint injection). Empty scope resolves to [`GLOBAL_TXN_SCOPE`].
pub fn txn_scope_or_global(txn_scope: &str) -> String {
    if !txn_scope.is_empty() {
        txn_scope.to_string()
    } else {
        GLOBAL_TXN_SCOPE.to_string()
    }
}

/// Parses a `tikv://` store path (Go `ParsePath`): returns etcd addresses,
/// the disable-GC flag, and the keyspace name.
pub fn parse_path(path: &str) -> Result<(Vec<String>, bool, String), String> {
    // Path: tikv://host1,host2?disableGC=false&keyspaceName=NAME
    let (scheme, rest) = path
        .split_once("://")
        .ok_or_else(|| format!("Uri scheme expected [tikv] but found [{path}]"))?;
    if scheme.to_lowercase() != "tikv" {
        return Err(format!("Uri scheme expected [tikv] but found [{scheme}]"));
    }
    let (host, query) = match rest.split_once('?') {
        Some((h, q)) => (h, q),
        None => (rest, ""),
    };
    let mut disable_gc = false;
    let mut keyspace_name = String::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        match k {
            "keyspaceName" => keyspace_name = v.to_string(),
            "disableGC" => match v.to_lowercase().as_str() {
                "true" => disable_gc = true,
                "false" | "" => {}
                _ => return Err("disableGC flag should be true/false".into()),
            },
            _ => {}
        }
    }
    let etcd_addrs = host.split(',').map(|s| s.to_string()).collect();
    Ok((etcd_addrs, disable_gc, keyspace_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestParsePath.
    #[test]
    fn test_parse_path() {
        let (addrs, disable_gc, ks) = parse_path("tikv://node1:2379,node2:2379").unwrap();
        assert_eq!(addrs, vec!["node1:2379", "node2:2379"]);
        assert!(!disable_gc);
        assert!(ks.is_empty());

        assert!(parse_path("tikv://node1:2379").is_ok());

        let (_addrs, disable_gc, ks) =
            parse_path("tikv://node1:2379?disableGC=true&keyspaceName=DEFAULT").unwrap();
        assert!(disable_gc);
        assert_eq!(ks, "DEFAULT");

        assert!(parse_path("http://node1:2379").is_err());
        assert!(parse_path("tikv://node1:2379?disableGC=bogus").is_err());
    }

    // Go TestTxnScopeValue (without failpoint injection).
    #[test]
    fn test_txn_scope() {
        assert_eq!(txn_scope_or_global("bj"), "bj");
        assert_eq!(txn_scope_or_global(""), "global");
        assert_eq!(txn_scope_or_global("global"), "global");
    }

    // Go TestValidateGRPCKeepAliveTimeout.
    #[test]
    fn test_validate_grpc_keepalive_timeout() {
        let mut cfg = TiKVClient::default();
        assert!(cfg.valid().is_ok());
        assert_eq!(cfg.grpc_keep_alive_timeout_nanos(), 3 * 1_000_000_000);
        cfg.grpc_keep_alive_timeout = 0.05;
        assert!(cfg.valid().is_ok());
        assert_eq!(cfg.grpc_keep_alive_timeout_nanos(), 50 * 1_000_000);
        cfg.grpc_keep_alive_timeout = 0.04;
        assert_eq!(
            cfg.valid().unwrap_err(),
            "grpc-keepalive-timeout should be at least 0.05, but got 0.040000"
        );
    }

    // The `[security]` cluster TLS keys deserialize from their TiDB config
    // names (`cluster-ssl-ca` / `cluster-ssl-cert` / `cluster-ssl-key` /
    // `cluster-verify-cn`), and an absent section stays plaintext.
    #[test]
    fn security_cluster_tls_keys_parse() {
        let security: Security = toml::from_str(
            r#"
cluster-ssl-ca = "/etc/tls/ca.pem"
cluster-ssl-cert = "/etc/tls/cert.pem"
cluster-ssl-key = "/etc/tls/key.pem"
cluster-verify-cn = ["tidb", "tikv"]
"#,
        )
        .unwrap();
        assert_eq!(security.cluster_ssl_ca, "/etc/tls/ca.pem");
        assert_eq!(security.cluster_ssl_cert, "/etc/tls/cert.pem");
        assert_eq!(security.cluster_ssl_key, "/etc/tls/key.pem");
        assert_eq!(security.cluster_verify_cn, ["tidb", "tikv"]);

        // No keys: the backward-compatible plaintext default.
        let empty: Security = toml::from_str("").unwrap();
        assert_eq!(empty, Security::default());
        assert!(empty.cluster_ssl_ca.is_empty());
    }

    #[test]
    fn defaults_and_valid() {
        let c = Config::default();
        assert_eq!(c.committer_concurrency, 128);
        assert_eq!(c.tikv_client.grpc_connection_count, 4);
        assert_eq!(c.pd_client.pd_server_timeout, 3);
        c.pd_client.valid().unwrap();
        c.tikv_client.valid().unwrap();
        c.txn_local_latches.valid().unwrap();

        assert_eq!(
            PdClient {
                pd_server_timeout: 0
            }
            .valid()
            .unwrap_err(),
            "pd-server-timeout can not be 0"
        );
        assert_eq!(
            TxnLocalLatches {
                enabled: true,
                capacity: 0
            }
            .valid()
            .unwrap_err(),
            "txn-local-latches.capacity can not be 0"
        );

        let bad = TiKVClient {
            grpc_compression_type: "snappy".into(),
            ..Default::default()
        };
        assert!(bad.valid().unwrap_err().contains("should be none or gzip"));
    }
}
