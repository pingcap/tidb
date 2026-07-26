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

//! The bounded sub-section structs of Go `pkg/config/config.go`'s `Config`:
//! the RU-v2 weights, tracing, proxy-protocol, pessimistic-txn, plugin,
//! top-sql, isolation-read, experimental, standby, and starter sections,
//! with their `Default*` values.

use serde::{Deserialize, Serialize};

use super::marshal::AtomicBool;
use crate::configtypes::ByteSize;

/// RU v2 weight calculation config (Go `RUV2Config`).
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RuV2Config {
    /// Scale factor from RU v2 floats to scaled integer values.
    #[serde(rename = "ru-scale")]
    pub ru_scale: f64,
    /// Weight for cells materialized into result chunks.
    #[serde(rename = "result-chunk-cells")]
    pub result_chunk_cells: f64,
    /// Weight for fast-path cell-scaling executors.
    #[serde(rename = "executor-l1")]
    pub executor_l1: f64,
    /// Weight for general executors.
    #[serde(rename = "executor-l2")]
    pub executor_l2: f64,
    /// Weight for heavier operators (Sort, StreamAgg).
    #[serde(rename = "executor-l3")]
    pub executor_l3: f64,
    /// Weight for insert rows × inserted column count.
    #[serde(rename = "executor-l5-insert-rows")]
    pub executor_l5_insert_rows: f64,
    /// Plan count weight.
    #[serde(rename = "plan-cnt")]
    pub plan_cnt: f64,
    /// Plan derive-stats paths weight.
    #[serde(rename = "plan-derive-stats-paths")]
    pub plan_derive_stats_paths: f64,
    /// Resource-manager read-count weight.
    #[serde(rename = "resource-manager-read-cnt")]
    pub resource_manager_read_cnt: f64,
    /// Resource-manager write-count weight.
    #[serde(rename = "resource-manager-write-cnt")]
    pub resource_manager_write_cnt: f64,
    /// Write-keys weight.
    #[serde(rename = "write-keys")]
    pub write_keys: f64,
    /// Session parser total weight.
    #[serde(rename = "session-parser-total")]
    pub session_parser_total: f64,
    /// Transaction-count weight.
    #[serde(rename = "txn-cnt")]
    pub txn_cnt: f64,
}

impl Default for RuV2Config {
    // Go `DefaultRUV2Config`.
    fn default() -> Self {
        RuV2Config {
            ru_scale: 2.01,
            result_chunk_cells: 0.00010000,
            executor_l1: 0.00013278,
            executor_l2: 0.00000383,
            executor_l3: 0.00141739,
            executor_l5_insert_rows: 0.00472572,
            plan_cnt: 0.15392217,
            plan_derive_stats_paths: 0.24968182,
            resource_manager_read_cnt: 0.02072003,
            resource_manager_write_cnt: 0.07179779,
            write_keys: 0.330760861554226,
            session_parser_total: 0.19230499,
            txn_cnt: 0.03013709,
        }
    }
}

/// Pessimistic transaction config (Go `PessimisticTxn`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PessimisticTxn {
    /// Max retry count for a single statement.
    #[serde(rename = "max-retry-count")]
    pub max_retry_count: u32,
    /// Max deadlock events recorded in information_schema.deadlocks.
    #[serde(rename = "deadlock-history-capacity")]
    pub deadlock_history_capacity: u32,
    /// Whether retryable (in-statement) deadlocks are collected.
    #[serde(rename = "deadlock-history-collect-retryable")]
    pub deadlock_history_collect_retryable: bool,
    /// Whether auto-commit transactions run in pessimistic mode.
    #[serde(rename = "pessimistic-auto-commit")]
    pub pessimistic_auto_commit: AtomicBool,
    /// Default for `tidb_constraint_check_in_place_pessimistic`.
    #[serde(rename = "constraint-check-in-place-pessimistic")]
    pub constraint_check_in_place_pessimistic: bool,
}

impl PessimisticTxn {
    /// Go `DefaultPessimisticTxn` (Classic kernel: auto-commit off).
    pub fn default_config() -> PessimisticTxn {
        PessimisticTxn {
            max_retry_count: 256,
            deadlock_history_capacity: 10,
            deadlock_history_collect_retryable: false,
            pessimistic_auto_commit: AtomicBool::new(crate::kerneltype::is_next_gen()),
            constraint_check_in_place_pessimistic: true,
        }
    }
}

impl Default for PessimisticTxn {
    fn default() -> Self {
        PessimisticTxn::default_config()
    }
}

/// Plugin config (Go `Plugin`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Plugin {
    /// Plugin directory.
    #[serde(rename = "dir")]
    pub dir: String,
    /// Plugins to load.
    #[serde(rename = "load")]
    pub load: String,
}

/// TopSQL config (Go `TopSQL`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TopSql {
    /// The TopSQL data receiver address.
    #[serde(rename = "receiver-address")]
    pub receiver_address: String,
}

/// Isolation-read config (Go `IsolationRead`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IsolationRead {
    /// Engines that filter tidb-server access paths.
    #[serde(rename = "engines")]
    pub engines: Vec<String>,
}

/// Experimental features config (Go `Experimental`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Experimental {
    /// Whether expression indexes may be created.
    #[serde(rename = "allow-expression-index")]
    pub allows_expression_index: bool,
    /// Whether the charset feature is enabled (json-hidden in Go).
    #[serde(rename = "enable-new-charset", skip_serializing)]
    pub enable_new_charset: bool,
}

/// Standby-mode config (Go `Standby`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Standby {
    /// Whether standby mode is enabled.
    #[serde(rename = "standby-mode")]
    pub standby_mode: bool,
    /// Max idle time (seconds) before exit.
    #[serde(rename = "max-idle-seconds")]
    pub max_idle_seconds: u32,
    /// Max time (seconds) to activate from standby.
    #[serde(rename = "activation-timeout")]
    pub activation_timeout: u32,
    /// Whether the idle watcher ignores session migration.
    #[serde(rename = "enable-zero-backend")]
    pub enable_zero_backend: bool,
}

/// Starter-only extension params (Go `StarterParams`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StarterParams {
    /// Export identifier from standby activation.
    #[serde(rename = "export-id", skip_serializing_if = "String::is_empty")]
    pub export_id: String,
    /// Whether Starter graceful shutdown notifies the TiDB manager.
    #[serde(rename = "enable-manager-notifier", skip_serializing_if = "is_false")]
    pub enable_manager_notifier: bool,
    /// TiDB manager address for the shutdown notifier.
    #[serde(rename = "manager-addr", skip_serializing_if = "String::is_empty")]
    pub manager_addr: String,
    /// Max total real source data size for IMPORT INTO (0 = unlimited).
    #[serde(rename = "max-import-data-size")]
    pub max_import_data_size: ByteSize,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Plan cache config (Go `PlanCache`; currently unused in defaults).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanCache {
    /// Whether enabled.
    #[serde(rename = "enabled")]
    pub enabled: bool,
    /// Capacity.
    #[serde(rename = "capacity")]
    pub capacity: u32,
    /// Shards.
    #[serde(rename = "shards")]
    pub shards: u32,
}

/// Prepared plan cache config (Go `PreparedPlanCache`).
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PreparedPlanCache {
    /// Whether enabled.
    #[serde(rename = "enabled")]
    pub enabled: bool,
    /// Capacity.
    #[serde(rename = "capacity")]
    pub capacity: u32,
    /// Memory guard ratio.
    #[serde(rename = "memory-guard-ratio")]
    pub memory_guard_ratio: f64,
}

impl Default for PreparedPlanCache {
    // From Go `defaultConf.PreparedPlanCache`.
    fn default() -> Self {
        PreparedPlanCache {
            enabled: true,
            capacity: 100,
            memory_guard_ratio: 0.1,
        }
    }
}

/// OpenTracing sampler config (Go `OpenTracingSampler`).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenTracingSampler {
    /// Sampler type.
    #[serde(rename = "type")]
    pub sampler_type: String,
    /// Sampler param.
    #[serde(rename = "param")]
    pub param: f64,
    /// Sampling server URL.
    #[serde(rename = "sampling-server-url")]
    pub sampling_server_url: String,
    /// Max operations.
    #[serde(rename = "max-operations")]
    pub max_operations: i64,
    /// Sampling refresh interval (nanoseconds).
    #[serde(rename = "sampling-refresh-interval")]
    pub sampling_refresh_interval: i64,
}

impl Default for OpenTracingSampler {
    fn default() -> Self {
        OpenTracingSampler {
            sampler_type: String::new(),
            param: 0.0,
            sampling_server_url: String::new(),
            max_operations: 0,
            sampling_refresh_interval: 0,
        }
    }
}

/// OpenTracing reporter config (Go `OpenTracingReporter`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenTracingReporter {
    /// Queue size.
    #[serde(rename = "queue-size")]
    pub queue_size: i64,
    /// Buffer flush interval (nanoseconds).
    #[serde(rename = "buffer-flush-interval")]
    pub buffer_flush_interval: i64,
    /// Whether to log spans.
    #[serde(rename = "log-spans")]
    pub log_spans: bool,
    /// Local agent host:port.
    #[serde(rename = "local-agent-host-port")]
    pub local_agent_host_port: String,
}

/// OpenTracing config (Go `OpenTracing`).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenTracing {
    /// Whether enabled.
    #[serde(rename = "enable")]
    pub enable: bool,
    /// Whether RPC metrics are enabled.
    #[serde(rename = "rpc-metrics")]
    pub rpc_metrics: bool,
    /// Sampler config.
    #[serde(rename = "sampler")]
    pub sampler: OpenTracingSampler,
    /// Reporter config.
    #[serde(rename = "reporter")]
    pub reporter: OpenTracingReporter,
}

impl Default for OpenTracing {
    // From Go `defaultConf.OpenTracing`.
    fn default() -> Self {
        OpenTracing {
            enable: false,
            rpc_metrics: false,
            sampler: OpenTracingSampler {
                sampler_type: "const".into(),
                param: 1.0,
                ..Default::default()
            },
            reporter: OpenTracingReporter::default(),
        }
    }
}

/// PROXY-protocol config (Go `ProxyProtocol`).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ProxyProtocol {
    /// Acceptable client networks (empty disables, `*` all).
    #[serde(rename = "networks")]
    pub networks: String,
    /// Header read timeout, seconds.
    #[serde(rename = "header-timeout")]
    pub header_timeout: u32,
    /// Whether the header is process-fallback-able.
    #[serde(rename = "fallbackable")]
    pub fallbackable: bool,
}

impl Default for ProxyProtocol {
    // From Go `defaultConf.ProxyProtocol`.
    fn default() -> Self {
        ProxyProtocol {
            networks: String::new(),
            header_timeout: 5,
            fallbackable: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Defaults from Go's DefaultRUV2Config / defaultConf sub-sections.
    #[test]
    fn defaults() {
        let ru = RuV2Config::default();
        assert_eq!(ru.ru_scale, 2.01);
        assert_eq!(ru.write_keys, 0.330760861554226);
        assert_eq!(ru.txn_cnt, 0.03013709);

        let ppc = PreparedPlanCache::default();
        assert!(ppc.enabled);
        assert_eq!(ppc.capacity, 100);
        assert_eq!(ppc.memory_guard_ratio, 0.1);

        let ot = OpenTracing::default();
        assert!(!ot.enable);
        assert_eq!(ot.sampler.sampler_type, "const");
        assert_eq!(ot.sampler.param, 1.0);

        let pp = ProxyProtocol::default();
        assert_eq!(pp.header_timeout, 5);
        assert!(pp.fallbackable);

        let pt = PessimisticTxn::default();
        assert_eq!(pt.max_retry_count, 256);
        assert_eq!(pt.deadlock_history_capacity, 10);
        assert!(pt.constraint_check_in_place_pessimistic);
        // Classic kernel: auto-commit defaults off.
        assert_eq!(
            pt.pessimistic_auto_commit.load(),
            crate::kerneltype::is_next_gen()
        );
    }

    // Sub-sections round-trip through TOML with their Go field names.
    #[test]
    fn toml_field_names() {
        let toml = r#"
ru-scale = 3.0
write-keys = 1.5
"#;
        let ru: RuV2Config = toml::from_str(toml).unwrap();
        assert_eq!(ru.ru_scale, 3.0);
        assert_eq!(ru.write_keys, 1.5);
        // Unspecified fields keep defaults.
        assert_eq!(ru.txn_cnt, RuV2Config::default().txn_cnt);
    }
}
