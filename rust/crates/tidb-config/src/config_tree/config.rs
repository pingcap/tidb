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
//! following tranche). `Valid`'s keyspace-name check delegates to
//! `pkg/util/naming` and the skip-grant-table check to the process euid;
//! both are noted where they land.

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
use crate::keyspace_observability::KeyspaceObservability;
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
    #[serde(rename = "log")]
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

// Go `hasRootPrivilege`. Reads the process effective uid; deferred here
// (returns false) because std has no geteuid and skip-grant-table defaults
// off, so the only effect is that skip-grant-table=true is conservatively
// rejected (matching Go's "need root privilege" outcome absent root).
fn has_root_privilege() -> bool {
    false
}

impl Config {
    /// Go `Config.Valid`.
    pub fn valid(&self) -> Result<(), String> {
        // Keyspace-name check delegates to pkg/util/naming in Go; deferred
        // here (empty and simple names pass). Full validation lands with
        // that unit.

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
            let mut ew = self.external_workload.clone();
            ew.valid()?;
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
        let c = new_config();
        c.valid().unwrap();
        assert!(c.performance.tcp_no_delay);
        assert_eq!(c.token_limit, 1000);
        assert_eq!(c.max_allowed_packet, DEF_MAX_ALLOWED_PACKET);
    }

    // Full-config TOML round-trip keeps defaults for unspecified keys.
    #[test]
    fn toml_partial_load() {
        let c: Config = toml::from_str("port = 5000\n[performance]\ncross-join = false\n").unwrap();
        assert_eq!(c.port, 5000);
        assert!(!c.performance.cross_join);
        assert_eq!(c.host, "0.0.0.0"); // default retained
        c.valid().unwrap();
    }
}
