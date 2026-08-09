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

//! The top-level `Config` struct of Go `pkg/config/config.go`: the field
//! tree, `DefaultConfig`/`NewConfig`, and `Valid`.
//!
//! Not ported here (runtime/CLI machinery, not the config data model):
//! `InitializeConfig` (flag parsing + `os.Exit`), the global-config atomic
//! singleton, and `Load`'s TOML-metadata undecoded/instance-section
//! migration (which depends on the toml library's metadata API — a
//! following tranche). `Valid`'s skip-grant-table check delegates to the
//! process euid and is noted where it lands.

use std::sync::{
    atomic::{AtomicBool as StdAtomicBool, Ordering},
    OnceLock, RwLock,
};

use serde::{Deserialize, Serialize};

use super::big_sections::{
    Performance, Security, Status, SPILLED_FILE_ENCRYPTION_METHOD_AES128_CTR,
    SPILLED_FILE_ENCRYPTION_METHOD_PLAINTEXT,
};
use super::helpers::{
    prepare_error_message_extensions, valid_max_allowed_packet, Cse, ErrorMessageExtension,
    TrxSummary, DEF_MAX_ALLOWED_PACKET,
};
use super::log_instance::{Instance, Log};
use super::marshal::AtomicBool;
use super::sections::{
    Experimental, IsolationRead, OpenTracing, PessimisticTxn, Plugin, PreparedPlanCache,
    ProxyProtocol, RuV2Config, Standby, StarterParams, TopSql,
};
use crate::deploymode::Mode;
use crate::external_workload::ExternalWorkload;
use crate::keyspace_observability::{
    KeyspaceObservability, KeyspaceObservabilityLogField, KeyspaceObservabilityValues,
};
use crate::store::StoreType;
use crate::tiflash::is_valid_auto_scaler_config;
use crate::tikvcfg;

// Config number limitations (Go `pkg/config` consts).
const MAX_LOG_FILE_SIZE: i64 = 4096;
const MAX_PLUGIN_AUDIT_LOG_BUFFER_SIZE: i64 = 100 * 1024 * 1024;
const MAX_PLUGIN_AUDIT_LOG_FLUSH_INTERVAL: i64 = 3600;
const DEF_MAX_INDEX_LENGTH: i64 = 3072;
const DEF_MAX_OF_MAX_INDEX_LENGTH: i64 = 3072 * 4;
const DEF_INDEX_LIMIT: i64 = 64;
const DEF_MAX_OF_INDEX_LIMIT: i64 = 64 * 8;
const DEF_TABLE_COLUMN_COUNT_LIMIT: u32 = 1017;
const DEF_MAX_OF_TABLE_COLUMN_COUNT_LIMIT: u32 = 4096;
const DEF_STATS_LOAD_CONCURRENCY_LIMIT: i64 = 0;
const DEF_MAX_OF_STATS_LOAD_CONCURRENCY_LIMIT: i64 = 128;
const DEF_STATS_LOAD_QUEUE_SIZE_LIMIT: u32 = 1;
const DEF_MAX_OF_STATS_LOAD_QUEUE_SIZE_LIMIT: u32 = 100000;
const DEF_DXF_RESOURCE_LIMIT: i64 = 100;
const MIN_DXF_RESOURCE_LIMIT: i64 = 10;
const MAX_DXF_RESOURCE_LIMIT: i64 = 100;
const DEF_PORT: u32 = 4000;
const DEF_HOST: &str = "0.0.0.0";
const DEF_TEMP_DIR: &str = "/tmp/tidb";
const MAX_KEYSPACE_NAME_LENGTH: usize = 20;

fn valid_keyspace_name(name: &str) -> bool {
    name.len() <= MAX_KEYSPACE_NAME_LENGTH
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Azure credentials parsed from a metering storage URI.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AzureMeteringConfig {
    /// Azure storage account name.
    pub account_name: String,
    /// Azure storage account key.
    pub account_key: String,
}

/// Storage target parsed from Go metering SDK's `NewFromURI` contract.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MeteringConfig {
    /// Storage type (`s3` or `azure`).
    pub storage_type: String,
    /// Bucket or Azure container.
    pub bucket: String,
    /// Object prefix without the leading slash.
    pub prefix: String,
    /// S3 region from `region-id`.
    pub region: String,
    /// Azure-specific settings.
    pub azure: Option<AzureMeteringConfig>,
}

impl MeteringConfig {
    /// Go metering SDK `config.NewFromURI`, for the storage schemes consumed
    /// by TiDB's metering configuration.
    pub fn from_uri(uri: &str) -> Result<MeteringConfig, String> {
        let (storage_type, rest) = uri
            .split_once("://")
            .ok_or_else(|| "metering storage URI must contain a scheme".to_owned())?;
        if !matches!(storage_type, "s3" | "azure") {
            return Err(format!(
                "unsupported metering storage URI scheme {storage_type:?}"
            ));
        }
        let (location, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
        if bucket.is_empty() {
            return Err("metering storage URI bucket must not be empty".to_owned());
        }

        let mut region = String::new();
        let mut account_name = String::new();
        let mut account_key = String::new();
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "region-id" => region = value.to_owned(),
                "account-name" => account_name = value.to_owned(),
                "account-key" => account_key = value.to_owned(),
                _ => {}
            }
        }

        let azure = (storage_type == "azure").then_some(AzureMeteringConfig {
            account_name,
            account_key,
        });
        Ok(MeteringConfig {
            storage_type: storage_type.to_owned(),
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            region,
            azure,
        })
    }
}

/// Configuration options (Go `Config`). Deprecated/upgrade-only fields are
/// carried for TOML round-trip fidelity, as in the source.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
#[allow(missing_docs)] // field docs live on the Go source; tags are the contract
pub struct Config {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "advertise-address")]
    pub advertise_address: String,
    #[serde(rename = "port")]
    pub port: u32,
    #[serde(rename = "cors")]
    pub cors: String,
    #[serde(rename = "store")]
    pub store: StoreType,
    #[serde(rename = "path")]
    pub path: String,
    #[serde(rename = "socket")]
    pub socket: String,
    #[serde(rename = "lease")]
    pub lease: String,
    #[serde(rename = "split-table")]
    pub split_table: bool,
    #[serde(rename = "token-limit")]
    pub token_limit: u32,
    #[serde(rename = "max-allowed-packet")]
    pub max_allowed_packet: u64,
    #[serde(rename = "temp-dir")]
    pub temp_dir: String,
    #[serde(rename = "tmp-storage-path")]
    pub temp_storage_path: String,
    #[serde(rename = "tmp-storage-quota")]
    pub temp_storage_quota: i64,
    #[serde(skip)]
    pub txn_local_latches: tikvcfg::TxnLocalLatches,
    #[serde(rename = "server-version")]
    pub server_version: String,
    #[serde(rename = "version-comment")]
    pub version_comment: String,
    #[serde(rename = "tidb-edition")]
    pub tidb_edition: String,
    #[serde(rename = "tidb-release-version")]
    pub tidb_release_version: String,
    #[serde(rename = "deploy-mode")]
    pub deploy_mode: Mode,
    #[serde(rename = "dxf-resource-limit")]
    pub dxf_resource_limit: i64,
    #[serde(rename = "keyspace-name")]
    pub keyspace_name: String,
    #[serde(rename = "tikv-worker-url")]
    pub tikv_worker_url: String,
    #[serde(rename = "log", alias = "Log")]
    pub log: Log,
    #[serde(rename = "instance")]
    pub instance: Instance,
    #[serde(rename = "security")]
    pub security: Security,
    #[serde(rename = "status")]
    pub status: Status,
    #[serde(rename = "performance")]
    pub performance: Performance,
    #[serde(rename = "prepared-plan-cache")]
    pub prepared_plan_cache: PreparedPlanCache,
    #[serde(rename = "opentracing")]
    pub open_tracing: OpenTracing,
    #[serde(rename = "proxy-protocol")]
    pub proxy_protocol: ProxyProtocol,
    #[serde(rename = "pd-client")]
    pub pd_client: tikvcfg::PdClient,
    #[serde(rename = "tikv-client")]
    pub tikv_client: tikvcfg::TiKVClient,
    #[serde(rename = "ru-v2")]
    pub ru_v2: RuV2Config,
    #[serde(rename = "compatible-kill-query")]
    pub compatible_kill_query: bool,
    #[serde(rename = "pessimistic-txn")]
    pub pessimistic_txn: PessimisticTxn,
    #[serde(rename = "max-index-length")]
    pub max_index_length: i64,
    #[serde(rename = "index-limit")]
    pub index_limit: i64,
    #[serde(rename = "table-column-count-limit")]
    pub table_column_count_limit: u32,
    #[serde(rename = "graceful-wait-before-shutdown")]
    pub graceful_wait_before_shutdown: i64,
    #[serde(rename = "alter-primary-key")]
    pub alter_primary_key: bool,
    #[serde(rename = "treat-old-version-utf8-as-utf8mb4")]
    pub treat_old_version_utf8_as_utf8mb4: bool,
    #[serde(rename = "enable-table-lock")]
    pub enable_table_lock: bool,
    #[serde(rename = "delay-clean-table-lock")]
    pub delay_clean_table_lock: u64,
    #[serde(rename = "split-region-max-num")]
    pub split_region_max_num: u64,
    #[serde(rename = "top-sql")]
    pub top_sql: TopSql,
    #[serde(rename = "repair-mode")]
    pub repair_mode: bool,
    #[serde(rename = "repair-table-list")]
    pub repair_table_list: Vec<String>,
    #[serde(rename = "isolation-read")]
    pub isolation_read: IsolationRead,
    #[serde(rename = "new_collations_enabled_on_first_bootstrap")]
    pub new_collations_enabled_on_first_bootstrap: bool,
    #[serde(rename = "experimental")]
    pub experimental: Experimental,
    #[serde(rename = "skip-register-to-dashboard")]
    pub skip_register_to_dashboard: bool,
    #[serde(rename = "enable-telemetry")]
    pub enable_telemetry: bool,
    #[serde(rename = "labels")]
    pub labels: std::collections::HashMap<String, String>,
    #[serde(rename = "error-msg-extension")]
    pub error_message_extensions: Vec<ErrorMessageExtension>,
    #[serde(rename = "keyspace-observability")]
    pub keyspace_observability: KeyspaceObservability,
    #[serde(skip)]
    pub keyspace_observability_values: KeyspaceObservabilityValues,
    #[serde(rename = "enable-global-index")]
    pub enable_global_index: bool,
    #[serde(rename = "deprecate-integer-display-length")]
    pub deprecate_integer_display_width: bool,
    #[serde(rename = "enable-enum-length-limit")]
    pub enable_enum_length_limit: bool,
    #[serde(rename = "stores-refresh-interval")]
    pub stores_refresh_interval: u64,
    #[serde(rename = "enable-tcp4-only")]
    pub enable_tcp4_only: bool,
    #[serde(rename = "enable-forwarding")]
    pub enable_forwarding: bool,
    #[serde(rename = "max-ballast-object-size")]
    pub max_ballast_object_size: i64,
    #[serde(rename = "ballast-object-size")]
    pub ballast_object_size: i64,
    #[serde(rename = "transaction-summary")]
    pub trx_summary: TrxSummary,
    #[serde(rename = "enable-global-kill")]
    pub enable_global_kill: bool,
    #[serde(rename = "enable-32bits-connection-id")]
    pub enable_32bits_connection_id: bool,
    #[serde(rename = "initialize-sql-file")]
    pub initialize_sql_file: String,
    #[serde(rename = "keyspace-activate")]
    pub keyspace_activate_mode: bool,
    #[serde(rename = "standby")]
    pub standby: Standby,
    #[serde(rename = "starter-params")]
    pub starter_params: StarterParams,
    #[serde(rename = "external-workload")]
    pub external_workload: ExternalWorkload,
    // Deprecated (upgrade-only) fields.
    #[serde(rename = "enable-batch-dml")]
    pub enable_batch_dml: bool,
    #[serde(rename = "mem-quota-query")]
    pub mem_quota_query: i64,
    #[serde(rename = "oom-action")]
    pub oom_action: String,
    #[serde(rename = "oom-use-tmp-storage")]
    pub oom_use_tmp_storage: bool,
    #[serde(rename = "check-mb4-value-in-utf8")]
    pub check_mb4_value_in_utf8: AtomicBool,
    #[serde(rename = "enable-collect-execution-info")]
    pub enable_collect_execution_info: bool,
    #[serde(rename = "plugin")]
    pub plugin: Plugin,
    #[serde(rename = "max-server-connections")]
    pub max_server_connections: u32,
    #[serde(rename = "run-ddl")]
    pub run_ddl: bool,
    #[serde(rename = "disaggregated-tiflash")]
    pub disaggregated_tiflash: bool,
    #[serde(rename = "autoscaler-type")]
    pub tiflash_compute_auto_scaler_type: String,
    #[serde(rename = "autoscaler-addr")]
    pub tiflash_compute_auto_scaler_addr: String,
    #[serde(rename = "is-tiflashcompute-fixed-pool")]
    pub is_tiflash_compute_fixed_pool: bool,
    #[serde(rename = "autoscaler-cluster-id")]
    pub auto_scaler_cluster_id: String,
    #[serde(rename = "use-autoscaler")]
    pub use_auto_scaler: bool,
    #[serde(rename = "tidb-max-reuse-chunk")]
    pub tidb_max_reuse_chunk: u32,
    #[serde(rename = "tidb-max-reuse-column")]
    pub tidb_max_reuse_column: u32,
    #[serde(rename = "tidb-enable-exit-check")]
    pub tidb_enable_exit_check: bool,
    #[serde(rename = "in-mem-slow-query-topn-num")]
    pub in_mem_slow_query_topn_num: i64,
    #[serde(rename = "in-mem-slow-query-recent-num")]
    pub in_mem_slow_query_recent_num: i64,
    #[serde(rename = "metering-storage-uri")]
    pub metering_storage_uri: String,
    #[serde(rename = "cse")]
    pub cse: Cse,
}

impl Default for Config {
    // Go `DefaultConfig` / `defaultConf`.
    fn default() -> Self {
        Config {
            host: DEF_HOST.into(),
            advertise_address: String::new(),
            port: DEF_PORT,
            cors: String::new(),
            store: StoreType(crate::store::STORE_TYPE_UNISTORE.into()),
            path: "/tmp/tidb".into(),
            socket: "/tmp/tidb-{Port}.sock".into(),
            lease: "45s".into(), // DefSchemaLease.String()
            split_table: true,
            token_limit: 1000,
            max_allowed_packet: DEF_MAX_ALLOWED_PACKET,
            temp_dir: DEF_TEMP_DIR.into(),
            temp_storage_path: String::new(),
            temp_storage_quota: -1,
            txn_local_latches: tikvcfg::TxnLocalLatches::default(),
            server_version: String::new(),
            version_comment: String::new(),
            tidb_edition: String::new(),
            tidb_release_version: String::new(),
            deploy_mode: Mode::Premium,
            dxf_resource_limit: DEF_DXF_RESOURCE_LIMIT,
            keyspace_name: String::new(),
            tikv_worker_url: String::new(),
            log: Log::default(),
            instance: Instance::default(),
            security: Security::default(),
            status: Status::default(),
            performance: Performance::default(),
            prepared_plan_cache: PreparedPlanCache::default(),
            open_tracing: OpenTracing::default(),
            proxy_protocol: ProxyProtocol::default(),
            pd_client: tikvcfg::PdClient::default(),
            tikv_client: tikvcfg::TiKVClient::default(),
            ru_v2: RuV2Config::default(),
            compatible_kill_query: false,
            pessimistic_txn: PessimisticTxn::default_config(),
            max_index_length: 3072,
            index_limit: 64,
            table_column_count_limit: 1017,
            graceful_wait_before_shutdown: 0,
            alter_primary_key: false,
            treat_old_version_utf8_as_utf8mb4: true,
            enable_table_lock: false,
            delay_clean_table_lock: 0,
            split_region_max_num: 1000,
            top_sql: TopSql::default(),
            repair_mode: false,
            repair_table_list: Vec::new(),
            isolation_read: IsolationRead {
                engines: vec!["tikv".into(), "tiflash".into(), "tidb".into()],
            },
            new_collations_enabled_on_first_bootstrap: true,
            experimental: Experimental::default(),
            skip_register_to_dashboard: false,
            enable_telemetry: false,
            labels: std::collections::HashMap::new(),
            error_message_extensions: Vec::new(),
            keyspace_observability: KeyspaceObservability::default(),
            keyspace_observability_values: KeyspaceObservabilityValues::default(),
            enable_global_index: false,
            deprecate_integer_display_width: true,
            enable_enum_length_limit: true,
            stores_refresh_interval: tikvcfg::DEF_STORES_REFRESH_INTERVAL,
            enable_tcp4_only: false,
            enable_forwarding: false,
            max_ballast_object_size: 0,
            ballast_object_size: 0,
            trx_summary: TrxSummary::default(),
            enable_global_kill: true,
            enable_32bits_connection_id: true,
            initialize_sql_file: String::new(),
            keyspace_activate_mode: false,
            standby: Standby::default(),
            starter_params: StarterParams::default(),
            external_workload: ExternalWorkload::default(),
            enable_batch_dml: false,
            mem_quota_query: 1 << 30,
            oom_action: "cancel".into(),
            oom_use_tmp_storage: true,
            check_mb4_value_in_utf8: AtomicBool::new(true),
            enable_collect_execution_info: true,
            plugin: Plugin::default(),
            max_server_connections: 0,
            run_ddl: true,
            disaggregated_tiflash: false,
            tiflash_compute_auto_scaler_type: crate::tiflash::DEF_AS_STR.into(),
            tiflash_compute_auto_scaler_addr: crate::tiflash::DEF_AWS_AUTO_SCALER_ADDR.into(),
            is_tiflash_compute_fixed_pool: false,
            auto_scaler_cluster_id: String::new(),
            use_auto_scaler: false,
            tidb_max_reuse_chunk: 64,
            tidb_max_reuse_column: 256,
            tidb_enable_exit_check: false,
            in_mem_slow_query_topn_num: 30,
            in_mem_slow_query_recent_num: 500,
            metering_storage_uri: String::new(),
            cse: Cse::default_config(),
        }
    }
}

/// Go `NewConfig`.
pub fn new_config() -> Config {
    Config::default()
}

static GLOBAL_CONFIG: OnceLock<RwLock<Config>> = OnceLock::new();
static PREPARED_ERROR_MESSAGE_EXTENSIONS: OnceLock<RwLock<Vec<ErrorMessageExtension>>> =
    OnceLock::new();
static CHECK_TABLE_BEFORE_DROP: StdAtomicBool = StdAtomicBool::new(false);

fn global_config() -> &'static RwLock<Config> {
    GLOBAL_CONFIG.get_or_init(|| RwLock::new(new_config()))
}

fn prepared_error_message_extensions() -> &'static RwLock<Vec<ErrorMessageExtension>> {
    PREPARED_ERROR_MESSAGE_EXTENSIONS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Go `GetGlobalConfig`. Rust returns an owned snapshot so readers never
/// retain a lock while using the configuration.
pub fn get_global_config() -> Config {
    global_config()
        .read()
        .expect("global config lock poisoned")
        .clone()
}

/// Go `GetErrorMessageExtensions`.
pub fn get_error_message_extensions() -> Vec<ErrorMessageExtension> {
    prepared_error_message_extensions()
        .read()
        .expect("prepared error message extensions lock poisoned")
        .clone()
}

/// Go `StoreGlobalConfig`.
pub fn store_global_config(config: Config) {
    let (extensions, _) = prepare_error_message_extensions(&config.error_message_extensions, true)
        .expect("ignore-invalid preparation cannot fail");
    let mut global = global_config()
        .write()
        .expect("global config lock poisoned");
    let mut prepared = prepared_error_message_extensions()
        .write()
        .expect("prepared error message extensions lock poisoned");
    *global = config;
    *prepared = extensions;
}

/// Go `CheckTableBeforeDrop`.
pub fn check_table_before_drop() -> bool {
    CHECK_TABLE_BEFORE_DROP.load(Ordering::Relaxed)
}

/// Go `initByLDFlags`.
pub fn init_by_ld_flags(_edition: &str, check_before_drop_ld_flag: &str) {
    store_global_config(new_config());
    if check_before_drop_ld_flag == "1" {
        CHECK_TABLE_BEFORE_DROP.store(true, Ordering::Relaxed);
    }
}

/// Go `UpdateGlobal`.
pub fn update_global(update: impl FnOnce(&mut Config)) {
    let mut config = global_config()
        .write()
        .expect("global config lock poisoned");
    update(&mut config);
    let (extensions, _) = prepare_error_message_extensions(&config.error_message_extensions, true)
        .expect("ignore-invalid preparation cannot fail");
    *prepared_error_message_extensions()
        .write()
        .expect("prepared error message extensions lock poisoned") = extensions;
}

/// Go `GetGlobalKeyspaceName`.
pub fn get_global_keyspace_name() -> String {
    global_config()
        .read()
        .expect("global config lock poisoned")
        .keyspace_name
        .clone()
}

// Go `hasRootPrivilege`. Reads the process effective uid; deferred here
// (returns false) because std has no geteuid and skip-grant-table defaults
// off, so the only effect is that skip-grant-table=true is conservatively
// rejected (matching Go's "need root privilege" outcome absent root).
fn has_root_privilege() -> bool {
    false
}

impl Config {
    /// Go `Config.ResolveKeyspaceObservability`.
    pub fn resolve_keyspace_observability(
        &mut self,
        values: &std::collections::HashMap<String, String>,
    ) -> Result<(), String> {
        self.keyspace_observability_values = self.keyspace_observability.resolve(values)?;
        Ok(())
    }

    /// Go `Config.GetKeyspaceObservabilityMetricLabels`.
    pub fn get_keyspace_observability_metric_labels(
        &self,
    ) -> &std::collections::HashMap<String, String> {
        &self.keyspace_observability_values.metric_labels
    }

    /// Go `Config.GetKeyspaceObservabilitySlowLogFields`.
    pub fn get_keyspace_observability_slow_log_fields(&self) -> &[KeyspaceObservabilityLogField] {
        &self.keyspace_observability_values.slow_log_fields
    }

    /// Go `Config.GetKeyspaceObservabilityStmtLogFields`.
    pub fn get_keyspace_observability_stmt_log_fields(
        &self,
    ) -> &std::collections::HashMap<String, String> {
        &self.keyspace_observability_values.stmt_log_fields
    }

    /// Go `Config.GetTiKVConfig`.
    pub fn get_tikv_config(&self) -> tikvcfg::Config {
        let zone_label = self.labels.get("zone").cloned().unwrap_or_default();
        tikvcfg::Config {
            committer_concurrency: self.performance.committer_concurrency,
            max_txn_ttl: self.performance.max_txn_ttl,
            tikv_client: self.tikv_client.clone(),
            security: tikvcfg::Security::new(
                self.security.cluster_ssl_ca.clone(),
                self.security.cluster_ssl_cert.clone(),
                self.security.cluster_ssl_key.clone(),
                self.security.cluster_verify_cn.clone(),
            ),
            pd_client: self.pd_client,
            pessimistic_txn: tikvcfg::PessimisticTxn {
                max_retry_count: self.pessimistic_txn.max_retry_count,
            },
            txn_local_latches: self.txn_local_latches,
            stores_refresh_interval: self.stores_refresh_interval,
            open_tracing_enable: self.open_tracing.enable,
            path: self.path.clone(),
            enable_forwarding: self.enable_forwarding,
            txn_scope: zone_label.clone(),
            zone_label,
            enable_async_batch_get: self.performance.enable_async_batch_get,
            ..tikvcfg::Config::default()
        }
    }

    /// Go `Config.Valid`.
    pub fn valid(&mut self) -> Result<(), String> {
        if !valid_keyspace_name(&self.keyspace_name) {
            return Err(format!(
                "invalid keyspace name: the value '{}' is invalid. It must be {} characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)",
                self.keyspace_name, MAX_KEYSPACE_NAME_LENGTH
            ));
        }
        if self.log.enable_error_stack == self.log.disable_error_stack
            && self.log.enable_error_stack != crate::config_tree::NB_UNSET
        {
            self.log.disable_error_stack = crate::config_tree::NB_UNSET;
        }
        if self.log.enable_timestamp == self.log.disable_timestamp
            && self.log.enable_timestamp != crate::config_tree::NB_UNSET
        {
            self.log.disable_timestamp = crate::config_tree::NB_UNSET;
        }

        if self.security.skip_grant_table && !has_root_privilege() {
            return Err("TiDB run with skip-grant-table need root privilege".into());
        }
        if !self.error_message_extensions.is_empty() && self.deploy_mode != Mode::Starter {
            return Err(
                "error-msg-extension can only be configured when deploy-mode is starter".into(),
            );
        }
        prepare_error_message_extensions(&self.error_message_extensions, false)?;
        if !self.store.valid() {
            return Err(format!(
                "invalid store={}, valid storages={:?}",
                self.store,
                crate::store::store_type_list()
            ));
        }
        if !self.deploy_mode.valid() {
            return Err(format!("invalid deploy-mode={}", self.deploy_mode));
        }
        if !crate::kerneltype::is_next_gen() && self.deploy_mode != Mode::Premium {
            return Err("deploy-mode can only be configured for nextgen TiDB".into());
        }
        if self.standby.standby_mode && self.keyspace_activate_mode {
            return Err("can't set standby and keyspace-activate mode at the same time".into());
        }
        if self.keyspace_activate_mode && self.deploy_mode != Mode::Starter {
            return Err("keyspace-activate can only be configured for starter deploy mode".into());
        }
        if self.starter_params.enable_manager_notifier && self.deploy_mode != Mode::Starter {
            return Err(
                "starter-params.enable-manager-notifier can only be configured for starter deploy mode"
                    .into(),
            );
        }
        if self.starter_params.max_import_data_size.0 > 0 && self.deploy_mode != Mode::Starter {
            return Err(
                "starter-params.max-import-data-size can only be configured for starter deploy mode"
                    .into(),
            );
        }
        if !self.keyspace_observability.fields.is_empty() && self.deploy_mode != Mode::Starter {
            return Err(
                "keyspace-observability.fields can only be configured when deploy-mode is starter"
                    .into(),
            );
        }
        if self.dxf_resource_limit < MIN_DXF_RESOURCE_LIMIT
            || self.dxf_resource_limit > MAX_DXF_RESOURCE_LIMIT
        {
            return Err(format!(
                "dxf-resource-limit should be between {MIN_DXF_RESOURCE_LIMIT} and {MAX_DXF_RESOURCE_LIMIT}"
            ));
        }
        if self.dxf_resource_limit != DEF_DXF_RESOURCE_LIMIT
            && self.deploy_mode != Mode::PremiumReserved
        {
            return Err(
                "dxf-resource-limit can only be configured when deploy-mode is premium_reserved"
                    .into(),
            );
        }
        if self.deploy_mode == Mode::Starter && !valid_max_allowed_packet(self.max_allowed_packet) {
            return Err(
                "max-allowed-packet should be [1024, 1073741824] and a multiple of 1024".into(),
            );
        }
        self.keyspace_observability.valid()?;
        if self.store.0 == crate::store::STORE_TYPE_MOCKTIKV
            && !self.instance.tidb_enable_ddl.load()
        {
            return Err("can't disable DDL on mocktikv".into());
        }
        if self.max_index_length < DEF_MAX_INDEX_LENGTH
            || self.max_index_length > DEF_MAX_OF_MAX_INDEX_LENGTH
        {
            return Err(format!(
                "max-index-length should be [{DEF_MAX_INDEX_LENGTH}, {DEF_MAX_OF_MAX_INDEX_LENGTH}]"
            ));
        }
        if self.index_limit < DEF_INDEX_LIMIT || self.index_limit > DEF_MAX_OF_INDEX_LIMIT {
            return Err(format!(
                "index-limit should be [{DEF_INDEX_LIMIT}, {DEF_MAX_OF_INDEX_LIMIT}]"
            ));
        }
        if self.log.file.max_size > MAX_LOG_FILE_SIZE {
            return Err(format!(
                "invalid max log file size={} which is larger than max={MAX_LOG_FILE_SIZE}",
                self.log.file.max_size
            ));
        }
        if self.table_column_count_limit < DEF_TABLE_COLUMN_COUNT_LIMIT
            || self.table_column_count_limit > DEF_MAX_OF_TABLE_COLUMN_COUNT_LIMIT
        {
            return Err(format!(
                "table-column-limit should be [{DEF_TABLE_COLUMN_COUNT_LIMIT}, {DEF_MAX_OF_TABLE_COLUMN_COUNT_LIMIT}]"
            ));
        }
        if self.instance.plugin_audit_log_buffer_size < 0
            || self.instance.plugin_audit_log_buffer_size > MAX_PLUGIN_AUDIT_LOG_BUFFER_SIZE
        {
            return Err(format!(
                "plugin-audit-log-buffer-size should be [0, {MAX_PLUGIN_AUDIT_LOG_BUFFER_SIZE}]"
            ));
        }
        if self.instance.plugin_audit_log_flush_interval <= 0
            || self.instance.plugin_audit_log_flush_interval > MAX_PLUGIN_AUDIT_LOG_FLUSH_INTERVAL
        {
            return Err(format!(
                "plugin-audit-log-flush-interval should be [1, {MAX_PLUGIN_AUDIT_LOG_FLUSH_INTERVAL}]"
            ));
        }
        // txn-local-latches / pd-client / tikv-client / trx-summary.
        self.tikv_client.valid()?;
        self.pd_client.valid()?;
        self.trx_summary.valid()?;
        if self.deploy_mode != Mode::Starter {
            if self.external_workload.is_configured() {
                return Err(
                    "external-workload can only be configured when deploy-mode is starter".into(),
                );
            }
        } else {
            self.external_workload.valid()?;
        }
        if self.performance.txn_total_size_limit > 1 << 40 {
            return Err(format!(
                "txn-total-size-limit should be less than {}",
                1u64 << 40
            ));
        }
        if self.instance.memory_usage_alarm_ratio > 1.0
            || self.instance.memory_usage_alarm_ratio < 0.0
        {
            return Err(
                "tidb_memory_usage_alarm_ratio in [Instance] must be greater than or equal to 0 and less than or equal to 1"
                    .into(),
            );
        }
        if self.isolation_read.engines.is_empty() {
            return Err(
                "the number of [isolation-read]engines for isolation read should be at least 1"
                    .into(),
            );
        }
        for engine in &self.isolation_read.engines {
            if engine != "tidb" && engine != "tikv" && engine != "tiflash" {
                return Err(format!(
                    "type of [isolation-read]engines can't be {engine} should be one of tidb or tikv or tiflash"
                ));
            }
        }
        // Security: spilled-file encryption method (lowercased).
        let method = self.security.spilled_file_encryption_method.to_lowercase();
        if method != SPILLED_FILE_ENCRYPTION_METHOD_PLAINTEXT
            && method != SPILLED_FILE_ENCRYPTION_METHOD_AES128_CTR
        {
            return Err(format!(
                "unsupported [security]spilled-file-encryption-method {method}, TiDB only supports [{SPILLED_FILE_ENCRYPTION_METHOD_PLAINTEXT}, {SPILLED_FILE_ENCRYPTION_METHOD_AES128_CTR}]"
            ));
        }
        if self.performance.stats_load_concurrency < DEF_STATS_LOAD_CONCURRENCY_LIMIT
            || self.performance.stats_load_concurrency > DEF_MAX_OF_STATS_LOAD_CONCURRENCY_LIMIT
        {
            return Err(format!(
                "stats-load-concurrency should be [{DEF_STATS_LOAD_CONCURRENCY_LIMIT}, {DEF_MAX_OF_STATS_LOAD_CONCURRENCY_LIMIT}]"
            ));
        }
        if self.performance.stats_load_queue_size < DEF_STATS_LOAD_QUEUE_SIZE_LIMIT
            || self.performance.stats_load_queue_size > DEF_MAX_OF_STATS_LOAD_QUEUE_SIZE_LIMIT
        {
            return Err(format!(
                "stats-load-queue-size should be [{DEF_STATS_LOAD_QUEUE_SIZE_LIMIT}, {DEF_MAX_OF_STATS_LOAD_QUEUE_SIZE_LIMIT}]"
            ));
        }
        if self.disaggregated_tiflash && self.use_auto_scaler {
            if !is_valid_auto_scaler_config(&self.tiflash_compute_auto_scaler_type) {
                return Err("invalid AutoScaler type".into());
            }
            if self.tiflash_compute_auto_scaler_addr.is_empty() {
                return Err(
                    "autoscaler-addr cannot be empty when disaggregated-tiflash mode is true"
                        .into(),
                );
            }
        }
        if !self.cse.valid() {
            return Err(format!(
                "invalid columnar-store-type={}, valid types=[\"tiflash\", \"columnar\", \"both\"]",
                self.cse.columnar_store_type
            ));
        }
        // Log level parse (Go's final check).
        crate::config_tree::parse_log_level(&self.log.level)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static GLOBAL_CONFIG_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // Go TestCloneConf.
    #[test]
    fn test_clone_conf() {
        let mut first = new_config();
        let second = first.clone();
        assert_eq!(first, second);

        first.host = "example.invalid".to_owned();
        first.port = 2333;
        first.instance.enable_slow_log = AtomicBool::new(!first.instance.enable_slow_log.load());
        first.repair_table_list.push("test.t".to_owned());

        assert_ne!(first.host, second.host);
        assert_ne!(first.port, second.port);
        assert_ne!(
            first.instance.enable_slow_log,
            second.instance.enable_slow_log
        );
        assert_ne!(first.repair_table_list, second.repair_table_list);
    }

    // Go TestGetGlobalKeyspaceName.
    #[test]
    fn test_get_global_keyspace_name() {
        let _guard = GLOBAL_CONFIG_TEST_LOCK.lock().unwrap();
        let config = new_config();
        assert!(config.keyspace_name.is_empty());

        update_global(|config| config.keyspace_name = "test".to_owned());
        assert_eq!(get_global_keyspace_name(), "test");

        update_global(|config| config.keyspace_name.clear());
    }

    // Go TestGetGlobalTiKVWorkerURL.
    #[test]
    fn test_get_global_tikv_worker_url() {
        let _guard = GLOBAL_CONFIG_TEST_LOCK.lock().unwrap();
        let config = new_config();
        assert!(config.tikv_worker_url.is_empty());

        update_global(|config| config.tikv_worker_url = "tikv-worker-0:10080".to_owned());
        assert_eq!(get_global_config().tikv_worker_url, "tikv-worker-0:10080");

        update_global(|config| config.tikv_worker_url.clear());
    }

    // Go TestAutoScalerConfig.
    #[test]
    fn test_auto_scaler_config() {
        let _guard = GLOBAL_CONFIG_TEST_LOCK.lock().unwrap();
        let config = new_config();
        assert!(!config.use_auto_scaler);
        assert!(!get_global_config().use_auto_scaler);

        update_global(|config| config.use_auto_scaler = true);
        assert!(get_global_config().use_auto_scaler);

        update_global(|config| config.use_auto_scaler = false);
    }

    // Go TestModifyThroughLDFlags.
    #[test]
    fn test_modify_through_ld_flags() {
        let _guard = GLOBAL_CONFIG_TEST_LOCK.lock().unwrap();
        let original_check_table_before_drop = check_table_before_drop();
        let original_global_config = get_global_config();

        for (edition, flag, enable_telemetry, expected_check_before_drop) in [
            ("Community", "None", false, false),
            ("Community", "1", false, true),
            ("Enterprise", "None", false, false),
            ("Enterprise", "1", false, true),
        ] {
            CHECK_TABLE_BEFORE_DROP.store(false, Ordering::Relaxed);
            init_by_ld_flags(edition, flag);

            assert_eq!(get_global_config().enable_telemetry, enable_telemetry);
            assert_eq!(new_config().enable_telemetry, enable_telemetry);
            assert_eq!(
                check_table_before_drop(),
                expected_check_before_drop,
                "edition={edition}, flag={flag}"
            );
        }

        CHECK_TABLE_BEFORE_DROP.store(original_check_table_before_drop, Ordering::Relaxed);
        store_global_config(original_global_config);
    }

    // Go TestMetering (the source test runs under the `nextgen` build tag).
    #[cfg(feature = "nextgen")]
    #[test]
    fn test_metering() {
        let mut config = new_config();
        config.metering_storage_uri =
            "s3://test-bucket/test-prefix?region-id=test-region".to_owned();
        config.valid().unwrap();
        let metering = MeteringConfig::from_uri(&config.metering_storage_uri).unwrap();
        assert_eq!(metering.storage_type, "s3");
        assert_eq!(metering.bucket, "test-bucket");
        assert_eq!(metering.prefix, "test-prefix");
        assert_eq!(metering.region, "test-region");

        let mut config = new_config();
        config.metering_storage_uri =
            "azure://metering-data/test-prefix?account-name=test-account&account-key=test-key"
                .to_owned();
        config.valid().unwrap();
        let metering = MeteringConfig::from_uri(&config.metering_storage_uri).unwrap();
        assert_eq!(metering.storage_type, "azure");
        assert_eq!(metering.bucket, "metering-data");
        assert_eq!(metering.prefix, "test-prefix");
        let azure = metering.azure.unwrap();
        assert_eq!(azure.account_name, "test-account");
        assert_eq!(azure.account_key, "test-key");
    }

    // Go TestKeyspaceObservability.
    #[test]
    fn test_keyspace_observability() {
        let content = r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"
slow-log-field = "Keyspace_meta_slow_a"
stmt-log-field = "stmt_meta_a"
required = true

[[keyspace-observability.fields]]
source = "meta_b"
metric-label = "keyspace_meta_label_b"
slow-log-field = "Keyspace_meta_slow_b"
"#;
        let mut config: Config = toml::from_str(content).unwrap();
        config.keyspace_observability.valid().unwrap();
        config
            .resolve_keyspace_observability(&std::collections::HashMap::from([
                ("meta_a".to_owned(), "value_a".to_owned()),
                ("meta_b".to_owned(), "value_b".to_owned()),
            ]))
            .unwrap();
        assert_eq!(
            config.get_keyspace_observability_metric_labels(),
            &std::collections::HashMap::from([
                ("keyspace_meta_label_a".to_owned(), "value_a".to_owned()),
                ("keyspace_meta_label_b".to_owned(), "value_b".to_owned()),
            ])
        );
        assert_eq!(
            config.get_keyspace_observability_slow_log_fields(),
            [
                KeyspaceObservabilityLogField {
                    name: "Keyspace_meta_slow_a".to_owned(),
                    value: "value_a".to_owned(),
                },
                KeyspaceObservabilityLogField {
                    name: "Keyspace_meta_slow_b".to_owned(),
                    value: "value_b".to_owned(),
                },
            ]
        );
        assert_eq!(
            config.get_keyspace_observability_stmt_log_fields(),
            &std::collections::HashMap::from([("stmt_meta_a".to_owned(), "value_a".to_owned())])
        );

        assert!(config
            .resolve_keyspace_observability(&std::collections::HashMap::from([(
                "meta_b".to_owned(),
                "value_b".to_owned()
            )]))
            .unwrap_err()
            .contains("missing required keyspace metadata entry \"meta_a\""));
    }

    // Go TestKeyspaceObservabilityInvalid deploy-mode check.
    #[test]
    fn test_keyspace_observability_invalid_deploy_mode() {
        let mut config: Config = toml::from_str(
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"
"#,
        )
        .unwrap();
        assert!(config.valid().unwrap_err().contains(
            "keyspace-observability.fields can only be configured when deploy-mode is starter"
        ));
    }

    // Go TestExternalWorkloadValid.
    #[test]
    fn test_external_workload_valid() {
        let mut config = new_config();
        config.valid().unwrap();

        config.external_workload.enable = true;
        assert!(config
            .valid()
            .unwrap_err()
            .contains("external-workload can only be configured when deploy-mode is starter"));

        let mut config = new_config();
        assert!(config
            .load_str("tidb.toml", "[external-workload]\nenable = false\n")
            .unwrap_err()
            .to_string()
            .contains("external-workload can only be configured when deploy-mode is starter"));

        if !crate::kerneltype::is_next_gen() {
            return;
        }

        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config.external_workload.enable = true;
        assert!(config
            .valid()
            .unwrap_err()
            .contains("external-workload controller-addr must not be empty"));

        config.external_workload.controller_addr = "http://127.0.0.1:1234".to_owned();
        config.external_workload.tidb_pool.clear();
        assert!(config
            .valid()
            .unwrap_err()
            .contains("external-workload tidb-pool must not be empty"));

        config.external_workload.tidb_pool = "pool-a".to_owned();
        config.external_workload.role.0 = "unknown".to_owned();
        assert!(config
            .valid()
            .unwrap_err()
            .contains("invalid external-workload role \"unknown\""));

        config.external_workload.role.0 = " GCV2 ".to_owned();
        config.valid().unwrap();
        assert_eq!(
            config.external_workload.role.0,
            crate::external_workload::ROLE_GCV2_WORKER
        );
    }

    // The global-copy portion of Go TestErrorMessageExtensionConfig.
    #[test]
    fn error_message_extension_config_global_copy() {
        let _guard = GLOBAL_CONFIG_TEST_LOCK.lock().unwrap();
        let original = get_global_config();
        let mut config = new_config();
        config.error_message_extensions = vec![ErrorMessageExtension {
            pattern: "^Access denied$".to_owned(),
            suffix: "see documentation".to_owned(),
        }];
        store_global_config(config);

        let mut prepared = get_error_message_extensions();
        assert!(!prepared.is_empty());
        prepared[0].suffix.clear();
        assert!(!get_error_message_extensions()[0].suffix.is_empty());

        store_global_config(original);
    }

    // Go TestErrorMessageExtensionInvalidRegexp.
    #[test]
    fn test_error_message_extension_invalid_regexp() {
        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config.error_message_extensions = vec![ErrorMessageExtension {
            pattern: "[".to_owned(),
            suffix: "invalid regexp".to_owned(),
        }];
        assert!(config
            .valid()
            .unwrap_err()
            .contains("invalid error-msg-extension regexp"));

        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config.error_message_extensions = vec![ErrorMessageExtension {
            pattern: " \t".to_owned(),
            suffix: "missing pattern".to_owned(),
        }];
        assert!(config
            .valid()
            .unwrap_err()
            .contains("empty error-msg-extension pattern"));

        let mut config = new_config();
        config.error_message_extensions = vec![ErrorMessageExtension {
            pattern: ".*".to_owned(),
            suffix: "not allowed".to_owned(),
        }];
        assert!(config
            .valid()
            .unwrap_err()
            .contains("error-msg-extension can only be configured when deploy-mode is starter"));

        let mut config = new_config();
        assert!(config
            .load_str(
                "config.toml",
                "error-msg-extension = [\n  { pattern = \".*\", suffix = \"not allowed\" },\n]\n"
            )
            .unwrap_err()
            .to_string()
            .contains("error-msg-extension can only be configured when deploy-mode is starter"));

        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config
            .load_str(
                "config.toml",
                "error-msg-extension = [\n  { suffix = \"missing pattern\" },\n]\n",
            )
            .unwrap();
        assert!(config
            .valid()
            .unwrap_err()
            .contains("empty error-msg-extension pattern"));

        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config
            .load_str(
                "config.toml",
                "error-msg-extension = [\n  { pattern = \"\", suffix = \"empty pattern\" },\n]\n",
            )
            .unwrap();
        assert!(config
            .valid()
            .unwrap_err()
            .contains("empty error-msg-extension pattern"));
    }

    // Go TestKeyspaceActivateModeConfig (the source test runs under the
    // `nextgen` build tag).
    #[cfg(feature = "nextgen")]
    #[test]
    fn test_keyspace_activate_mode_config() {
        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config.keyspace_activate_mode = true;
        config.valid().unwrap();

        config.standby.standby_mode = true;
        assert!(config
            .valid()
            .unwrap_err()
            .contains("can't set standby and keyspace-activate mode at the same time"));

        config.standby.standby_mode = false;
        config.deploy_mode = Mode::Premium;
        assert!(config
            .valid()
            .unwrap_err()
            .contains("keyspace-activate can only be configured for starter deploy mode"));
    }

    // Go TestKeyspaceName.
    #[test]
    fn test_keyspace_name() {
        let mut config = new_config();
        config.keyspace_name = "#!".to_owned();
        assert!(config.valid().unwrap_err().contains("is invalid"));

        config.keyspace_name = "abc".to_owned();
        config.valid().unwrap();

        config.keyspace_name = "18446744073709551615".to_owned();
        config.valid().unwrap();

        config.keyspace_name = "a18446744073709551615".to_owned();
        assert!(config
            .valid()
            .unwrap_err()
            .contains("invalid keyspace name"));
    }

    // Go TestGetTiKVConfigKeepsZeroRUV2RUScale.
    #[test]
    fn test_get_tikv_config_keeps_zero_ru_v2_ru_scale() {
        let mut config = new_config();
        config.ru_v2.ru_scale = 123.0;
        config.tikv_client.ru_v2.ru_scale = 0.0;

        let tikv_config = config.get_tikv_config();
        assert_eq!(tikv_config.tikv_client.ru_v2.ru_scale, 0.0);
    }

    // Go TestLogConfig.
    #[test]
    fn test_log_config() {
        use crate::config_tree::{NB_FALSE, NB_TRUE, NB_UNSET};

        for (
            text,
            expected_enable_error_stack,
            expected_disable_error_stack,
            expected_enable_timestamp,
            expected_disable_timestamp,
            resulting_disable_timestamp,
            resulting_disable_error_stack,
        ) in [
            (
                "[Log]\n", NB_UNSET, NB_UNSET, NB_UNSET, NB_UNSET, false, true,
            ),
            (
                "[Log]\nenable-timestamp = false\n",
                NB_UNSET,
                NB_UNSET,
                NB_FALSE,
                NB_UNSET,
                true,
                true,
            ),
            (
                "[Log]\nenable-timestamp = true\ndisable-timestamp = false\n",
                NB_UNSET,
                NB_UNSET,
                NB_TRUE,
                NB_FALSE,
                false,
                true,
            ),
            (
                "[Log]\nenable-timestamp = false\ndisable-timestamp = true\n",
                NB_UNSET,
                NB_UNSET,
                NB_FALSE,
                NB_TRUE,
                true,
                true,
            ),
            (
                "[Log]\nenable-timestamp = true\ndisable-timestamp = true\n",
                NB_UNSET,
                NB_UNSET,
                NB_TRUE,
                NB_UNSET,
                false,
                true,
            ),
            (
                "[Log]\nenable-error-stack = false\ndisable-error-stack = false\n",
                NB_FALSE,
                NB_UNSET,
                NB_UNSET,
                NB_UNSET,
                false,
                true,
            ),
        ] {
            let mut config = new_config();
            config.load_str("log_config.toml", text).unwrap();
            config.valid().unwrap();

            assert_eq!(config.log.enable_error_stack, expected_enable_error_stack);
            assert_eq!(config.log.disable_error_stack, expected_disable_error_stack);
            assert_eq!(config.log.enable_timestamp, expected_enable_timestamp);
            assert_eq!(config.log.disable_timestamp, expected_disable_timestamp);
            assert_eq!(
                config.log.get_disable_timestamp(),
                resulting_disable_timestamp
            );
            assert_eq!(
                config.log.get_disable_error_stack(),
                resulting_disable_error_stack
            );
        }
    }

    // Go TestMaxIndexLength.
    #[test]
    fn max_index_length() {
        let mut c = new_config();
        for (v, ok) in [
            (DEF_MAX_INDEX_LENGTH, true),
            (DEF_MAX_INDEX_LENGTH - 1, false),
            (DEF_MAX_OF_MAX_INDEX_LENGTH, true),
            (DEF_MAX_OF_MAX_INDEX_LENGTH + 1, false),
        ] {
            c.max_index_length = v;
            assert_eq!(c.valid().is_ok(), ok, "max_index_length={v}");
        }
    }

    // Go TestIndexLimit.
    #[test]
    fn index_limit() {
        let mut c = new_config();
        for (v, ok) in [
            (DEF_INDEX_LIMIT, true),
            (DEF_INDEX_LIMIT - 1, false),
            (DEF_MAX_OF_INDEX_LIMIT, true),
            (DEF_MAX_OF_INDEX_LIMIT + 1, false),
        ] {
            c.index_limit = v;
            assert_eq!(c.valid().is_ok(), ok, "index_limit={v}");
        }
    }

    // Go TestTableColumnCountLimit.
    #[test]
    fn table_column_count_limit() {
        let mut c = new_config();
        for (v, ok) in [
            (DEF_TABLE_COLUMN_COUNT_LIMIT, true),
            (DEF_TABLE_COLUMN_COUNT_LIMIT - 1, false),
            (DEF_MAX_OF_TABLE_COLUMN_COUNT_LIMIT, true),
            (DEF_MAX_OF_TABLE_COLUMN_COUNT_LIMIT + 1, false),
        ] {
            c.table_column_count_limit = v;
            assert_eq!(c.valid().is_ok(), ok, "col_limit={v}");
        }
    }

    // Go TestTxnTotalSizeLimitValid.
    #[test]
    fn txn_total_size_limit() {
        let mut c = new_config();
        for (v, ok) in [
            (4u64 << 10, true),
            (10 << 30, true),
            ((10 << 30) + 1, true),
            (1 << 40, true),
            ((1u64 << 40) + 1, false),
        ] {
            c.performance.txn_total_size_limit = v;
            assert_eq!(c.valid().is_ok(), ok, "txn_total={v}");
        }
    }

    // Go TestSecurityValid.
    #[test]
    fn security_valid() {
        let mut c = new_config();
        for (m, ok) in [
            ("", false),
            ("Plaintext", true),
            ("plaintext123", false),
            ("aes256-ctr", false),
            ("aes128-ctr", true),
        ] {
            c.security.spilled_file_encryption_method = m.into();
            assert_eq!(c.valid().is_ok(), ok, "method={m:?}");
        }
    }

    // Go TestStatsLoadLimit.
    #[test]
    fn stats_load_limit() {
        let mut c = new_config();
        for (v, ok) in [
            (DEF_STATS_LOAD_CONCURRENCY_LIMIT, true),
            (DEF_STATS_LOAD_CONCURRENCY_LIMIT - 1, false),
            (DEF_MAX_OF_STATS_LOAD_CONCURRENCY_LIMIT, true),
            (DEF_MAX_OF_STATS_LOAD_CONCURRENCY_LIMIT + 1, false),
        ] {
            c.performance.stats_load_concurrency = v;
            assert_eq!(c.valid().is_ok(), ok, "concurrency={v}");
        }
        let mut c = new_config();
        for (v, ok) in [
            (DEF_STATS_LOAD_QUEUE_SIZE_LIMIT, true),
            (DEF_STATS_LOAD_QUEUE_SIZE_LIMIT - 1, false),
            (DEF_MAX_OF_STATS_LOAD_QUEUE_SIZE_LIMIT, true),
            (DEF_MAX_OF_STATS_LOAD_QUEUE_SIZE_LIMIT + 1, false),
        ] {
            c.performance.stats_load_queue_size = v;
            assert_eq!(c.valid().is_ok(), ok, "queue_size={v}");
        }
    }

    // Go TestPluginAuditLog.
    #[test]
    fn plugin_audit_log() {
        let mut c = new_config();
        for (v, ok) in [
            (-1, false),
            (MAX_PLUGIN_AUDIT_LOG_BUFFER_SIZE, true),
            (MAX_PLUGIN_AUDIT_LOG_BUFFER_SIZE + 1, false),
        ] {
            c.instance.plugin_audit_log_buffer_size = v;
            assert_eq!(c.valid().is_ok(), ok, "buffer={v}");
        }
        let mut c = new_config();
        for (v, ok) in [
            (-1, false),
            (MAX_PLUGIN_AUDIT_LOG_FLUSH_INTERVAL, true),
            (MAX_PLUGIN_AUDIT_LOG_FLUSH_INTERVAL + 1, false),
        ] {
            c.instance.plugin_audit_log_flush_interval = v;
            assert_eq!(c.valid().is_ok(), ok, "flush={v}");
        }
    }

    // The default config is valid, and TestTcpNoDelay's default.
    #[test]
    fn default_config_valid() {
        let mut c = new_config();
        c.valid().unwrap();
        assert!(c.performance.tcp_no_delay);
        assert_eq!(c.token_limit, 1000);
        assert_eq!(c.max_allowed_packet, DEF_MAX_ALLOWED_PACKET);
    }

    // Full-config TOML round-trip keeps defaults for unspecified keys.
    #[test]
    fn toml_partial_load() {
        let mut c: Config =
            toml::from_str("port = 5000\n[performance]\ncross-join = false\n").unwrap();
        assert_eq!(c.port, 5000);
        assert!(!c.performance.cross_join);
        assert_eq!(c.host, "0.0.0.0"); // default retained
        c.valid().unwrap();
    }
}
