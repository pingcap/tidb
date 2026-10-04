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

//! MySQL connection dispatch and SQL node lifecycle.
//!
//! [`mysql_connection`] decodes protocol commands and dispatches queries,
//! transaction control and prepared statements through the shared session.
//! [`SqlNode`] owns the listener and connection lifecycle. Authentication
//! passes through [`ConfiguredUserStore`] and the shared privilege registry.
//!
//! [`resolve_server_tls`] uses canonical node configuration for configured
//! certificates or generated auto-TLS material. The connection advertises
//! `CLIENT_SSL` only when material is available, upgrades an `SSLRequest`,
//! then reads the handshake response from the encrypted stream.

// Tests of process-global sysvar hooks must not share those hooks with other
// simulated nodes. Run the original test body in its own harness process;
// threads created inside the body still exercise the real publication races.
#[cfg(test)]
pub(crate) fn isolate_process_globals() -> bool {
    const MARKER: &str = "TIDB_SERVER_ISOLATED_GLOBALS_TEST";
    let thread = std::thread::current();
    let name = thread.name().expect("named Rust test thread");
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name, "--nocapture"])
        .env(MARKER, name)
        .output()
        .expect("start isolated global-state test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "isolated test {name} failed or was not selected: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

mod aggregate_result_set;
mod auth_exchange;
mod auth_identity;
mod auth_plugin_registry;
pub mod bootstrap_publish;
pub mod cluster_account_seam;
pub mod cluster_analyze_seam;
pub mod cluster_auto_id_seam;
pub mod cluster_binding_seam;
mod cluster_privileges;
pub mod cluster_session;
pub mod cluster_session_node;
pub mod cluster_stats_lock_seam;
pub mod cluster_sysvar_seam;
mod configured_user_store;
pub mod connection_resultset;
mod connection_writers;
mod cursor_state;
mod distinct_result_set;
mod global_config_sync;
pub mod handshake;
mod handshake_response;
pub mod http_status;
pub mod http_settings;
mod http_request;
pub mod main_flags;
mod mysql_connection;
mod mysql_tls;
mod native_password;
mod node_config;
mod pipeline_session;
mod query_metrics;
mod real_tikv_node;
pub mod resultset_source;
pub mod resultset_writer;
mod secure_transport;
pub mod server_metrics;
pub mod serverinfo_etcd;
mod session_transaction;
mod set_global_vars;
mod shutdown_signal;
pub mod signal_exit;
mod sorting_result_set;
mod sql_node;
mod unistore_node;
pub mod wire_status;
pub use aggregate_result_set::AggregateResultSetSource;
pub use auth_exchange::{
    decode_client_packet, AuthClientResponse, AuthExchangeError, AuthMoreData, AuthSwitchRequest,
    AUTH_MORE_DATA_PREFIX, AUTH_SWITCH_REQUEST,
};
pub use auth_identity::{
    AuthPluginHandoff, AuthPluginHandoffError, IdentityCatalog, IdentityLookupPolicy,
    IdentityLookupRequest, IdentityLookupResult, MatchedIdentity, PrivilegeRowAdmission,
    DEFAULT_AUTH_PLUGIN,
};
pub use auth_plugin_registry::{
    AuthPluginAdmission, AuthPluginDescriptor, AuthPluginRegistry, AuthPluginRegistryError,
    ClientPluginSelection, ClientPluginSelectionRequest, DEFAULT_AUTH_PLUGINS,
};
pub use cluster_privileges::{registry_from_cluster, LoadedRegistry, SkippedGrant};
use cluster_session_node::run_cluster_session_node_with_spill;
pub use cluster_session_node::{
    run_cluster_session_node, ClusterServerSession, ClusterSessionFactory,
};
pub use configured_user_store::{
    AuthenticatedIdentity, AuthenticationFailure, ConfiguredUserStore, ConfiguredUserStoreError,
};
pub use distinct_result_set::DistinctResultSetSource;
pub use handshake::{
    negotiate_capabilities, parse_response, parse_response_body,
    parse_response_body_into_with_attrs_state, parse_response_body_with_attrs_state,
    parse_response_header, parse_response_header_into, parse_response_with_attrs_state,
    parse_response_with_global_sysvars, AuthHandshake, AuthHandshakePacket, AuthHandshakePhase,
    AuthHandshakeRequest, AuthPluginAction, ConnectionAttrsState, HandshakeError,
    HandshakeResponseHeader, InitialHandshake, DEFAULT_CONNECT_ATTRS_SIZE,
};
pub use handshake_response::{HandshakeResponse41, WireString};
pub use mysql_connection::{
    serve_mysql_connection, serve_mysql_connection_with_tls, ConnectionCommandCounts,
    ConnectionExit, ConnectionReport, MysqlConnectionError,
};
pub use mysql_tls::{resolve_server_tls, ClientStream, MysqlServerTls, MysqlTlsError};
pub use native_password::{
    generate_handshake_salt, verify_candidate, NativePasswordHash, NativePasswordHashError,
    HANDSHAKE_SALT_LEN, NATIVE_PASSWORD_HASH_LEN,
};
pub use node_config::{NodeConfig, NodeConfigError};
pub use pipeline_session::{
    MaterializedResultSetSource, PipelineServerSession, PipelineSessionFactory,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

pub use real_tikv_node::{run_with_process_shutdown, ProcessReadAuthority, RunConfiguredNodeError};
pub use resultset_source::ResultSetSource;
pub use secure_transport::{
    SecureTransportError, SecureTransportPolicy, TransportDecision, TransportKind,
};
pub use session_transaction::SessionTransaction;
pub use sorting_result_set::SortingResultSetSource;
pub use sql_node::{
    ActiveQueryCancellation, BoxedResultSetSource, ConcurrentSqlNode, ConnectionCancellation,
    ConnectionTracker, GeneralExecuteOutcome, PreparedGeneral, PreparedPointRead,
    PreparedStatement, PreparedWrite, QueryCancellationLease, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, ShutdownHandle, SqlNodeError, SqlQueryError, WriteOutcome,
    RESULT_UNDETERMINED_MESSAGE,
};
pub use wire_status::{
    WireStatus, SERVER_MORE_RESULTS_EXISTS, SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_CURSOR_EXISTS,
    SERVER_STATUS_IN_TRANS, SERVER_STATUS_LAST_ROW_SEND,
};

/// Starts the shared SQL session/catalog lifecycle over the selected storage engine.
pub fn run_configured_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    tidb_util::traceevent::register_with_client_go();
    config.install_process_globals();
    tidb_util::cgmon::start_cgroup_monitor();
    let _cgroup_monitor_cleanup = CgroupMonitorCleanup;
    initialize_temp_dir(&config)?;
    let _temp_dir_cleanup = TempDirCleanup;
    {
        let global_config = tidb_config::config_tree::config::get_global_config();
        tidb_domain::domainutil::REPAIR_INFO.set_repair_mode(global_config.repair_mode);
        tidb_domain::domainutil::REPAIR_INFO
            .set_repair_table_list(global_config.repair_table_list.clone());
    }
    tidb_util::printer::print_tidb_info();
    start_system_time_monitor();
    let spill_storage = open_spill_storage(&config)?;
    let memory_arbitrator = MemoryArbitratorAuthority::open(&config)?;
    tidb_resourcemanager::instance_resource_manager().start();
    let _resource_manager_cleanup = ResourceManagerCleanup;
    server_metrics::init();
    install_server_boot_gauges();
    // Go's per-package metric inits run from pkg/metrics' RegisterMetrics and
    // each subsystem's startup; the dashboard surface they produce is one
    // boot-time call per Rust crate that owns the families.
    tidb_planner::metrics::init_dashboard_series();
    tidb_executor::metrics::init_dashboard_series();
    tidb_session::metrics::init_dashboard_series();
    tidb_distsql::metrics::init_dashboard_series();
    tidb_ddl_session::metrics::init_dashboard_series();
    tidb_domain::metrics::init_dashboard_series();
    tidb_owner::metrics::init_dashboard_series();
    tidb_meta::metrics::init_dashboard_series();
    tidb_util::memory_metrics::init_dashboard_series();
    tidb_stats_handle_metrics::init_dashboard_series();
    tidb_dxf::metrics::init_dashboard_series();
    tidb_stmtsummary::metrics::init_dashboard_series();
    tidb_util::topsql_reporter::metrics::init_metrics_vars();
    let _statement_observation_cleanup = start_statement_observation();
    tidb_txnkv::client_go_metrics::init_dashboard_series();
    if config.store_kind == node_config::StoreKind::Unistore {
        // Go: `session.RegisterStore("unistore", mockstore.EmbedUnistoreDriver{})`
        // -- the same node code over the embedded store, no PD dialed.
        return unistore_node::run_unistore_cluster_session(
            config,
            spill_storage,
            memory_arbitrator.arbitrator(),
        );
    }
    run_cluster_session_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

// The process, rather than an individual session or store, owns these workers.
// This guard outlives both store runners and flushes after connections close.
struct StatementObservationCleanup;

fn start_statement_observation() -> StatementObservationCleanup {
    let config = tidb_config::config_tree::config::get_global_config();
    tidb_exec::txn_summary::RECORDER.resize(config.trx_summary.transaction_summary_capacity);
    tidb_exec::txn_summary::RECORDER.set_min_duration(std::time::Duration::from_millis(
        config.trx_summary.transaction_id_digest_min_duration as u64,
    ));
    let instance = &config.instance;
    if instance.stmt_summary_enable_persistent {
        let summary_config = tidb_stmtsummary::v2::stmtsummary::Config {
            filename: instance.stmt_summary_filename.clone(),
            file_max_size: instance.stmt_summary_file_max_size,
            file_max_days: instance.stmt_summary_file_max_days,
            file_max_backups: instance.stmt_summary_file_max_backups,
        };
        if let Err(error) = tidb_stmtsummary::v2::stmtsummary::setup(&summary_config) {
            eprintln!("{error}");
        }
    }
    tidb_util::topsql_stmtstats::setup_aggregator();
    StatementObservationCleanup
}

impl Drop for StatementObservationCleanup {
    fn drop(&mut self) {
        tidb_util::topsql_stmtstats::close_aggregator();
        tidb_stmtsummary::v2::stmtsummary::close();
    }
}

struct TempDirCleanup;

struct CgroupMonitorCleanup;

struct ResourceManagerCleanup;

impl Drop for CgroupMonitorCleanup {
    fn drop(&mut self) {
        tidb_util::cgmon::stop_cgroup_monitor();
    }
}

impl Drop for ResourceManagerCleanup {
    fn drop(&mut self) {
        tidb_resourcemanager::instance_resource_manager().stop();
    }
}

impl Drop for TempDirCleanup {
    fn drop(&mut self) {
        tidb_util::disk::clean_up();
    }
}

fn initialize_temp_dir(config: &NodeConfig) -> Result<(), RunConfiguredNodeError> {
    tidb_util::disk::initialize_temp_dir().map_err(|source| {
        RunConfiguredNodeError::Spill(tidb_util::spill_storage::SpillStorageOpenError::Io {
            operation: "initialize temporary storage directory",
            path: config.spill_storage.path.clone(),
            source,
        })
    })
}

static SYSTEM_TIME_JUMP_BACKWARD_COUNT: AtomicU64 = AtomicU64::new(0);

fn start_system_time_monitor() {
    let _ = std::thread::spawn(|| {
        tidb_util::systimemon::start_monitor(SystemTime::now, || {
            SYSTEM_TIME_JUMP_BACKWARD_COUNT.fetch_add(1, Ordering::Relaxed);
            server_metrics::TIME_JUMP_BACK_TOTAL.inc();
        });
    });
}

/// Go `pkg/server/server.go` startup metric writes: `ConnGauge`'s family
/// starts exporting, `ConfigStatus` mirrors the two configuration values Go
/// publishes (`server.go:494-495`), `MaxProcs` reports the worker
/// parallelism standing in for `GOMAXPROCS` (`cgmon.go` keeps the same gauge
/// current in Go), `GOGC` reports the `GOGC` environment value Go's GC runs
/// with, `MemoryLimit` mirrors `cgmon.go:156`, and `ServerInfo` stamps the
/// start timestamp under the running version/hash (`conn.go`'s
/// `addConnMetrics` counterpart in `server.go:507`).
fn install_server_boot_gauges() {
    let parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    server_metrics::MAXPROCS.set(parallelism as i64);
    let gogc = std::env::var("GOGC")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100);
    server_metrics::GOGC.set(gogc);
    let config = tidb_config::config_tree::config::get_global_config();
    server_metrics::CONFIG_STATUS
        .with_label_values(&["token-limit"])
        .set(config.token_limit as i64);
    server_metrics::CONFIG_STATUS
        .with_label_values(&["max_connections"])
        .set(i64::from(config.max_server_connections));
    let start_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    server_metrics::SERVER_INFO
        .with_label_values(&[
            &tidb_mysql::runtime_versions().server_version,
            tidb_util::versioninfo::TIDB_GIT_HASH,
        ])
        .set(start_seconds as i64);
}

fn open_spill_storage(
    config: &NodeConfig,
) -> Result<Arc<tidb_util::spill_storage::SpillStorage>, RunConfiguredNodeError> {
    tidb_exec::configure_deadlock_history(
        config.deadlock_history_capacity,
        config.deadlock_history_collect_retryable,
    );
    tidb_session::sysvar::install_sem_v2_sysvar_registry();
    if config.sem_enabled && !config.sem_config.is_empty() {
        tidb_util::sem::disable();
        tidb_util::sem_v2::enable(&config.sem_config)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    } else if config.sem_enabled {
        tidb_util::sem_v2::disable();
        tidb_util::sem::enable();
    } else {
        tidb_util::sem_v2::disable();
        tidb_util::sem::disable();
    }
    tidb_util::spill_storage::SpillStorage::open(config.spill_storage.clone())
        .map(Arc::new)
        .map_err(RunConfiguredNodeError::Spill)
}

fn open_memory_arbitrator(
    config: &NodeConfig,
) -> Result<Option<Arc<tidb_util::memory::MemArbitrator>>, RunConfiguredNodeError> {
    let mode = tidb_util::memory::parse_work_mode_text(&config.memory_arbitrator.mode);
    let limit =
        tidb_util::memory::parse_server_memory_limit(&config.memory_arbitrator.server_memory_limit)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let (soft_bytes, soft_ratio, soft_mode) =
        tidb_util::memory::parse_soft_limit_text(&config.memory_arbitrator.soft_limit);
    let state_dir = config.spill_storage.path.join("mem-arbitrator");
    let arbitrator = tidb_util::memory::MemArbitrator::new(
        i64::try_from(limit).expect("validated server memory limit fits i64"),
        tidb_util::memory::DEF_POOL_STATUS_SHARDS,
        tidb_util::memory::DEF_POOL_QUOTA_SHARDS,
        64 << 10,
        Box::new(tidb_util::memory::RuntimeMemStateRecorder::new(&state_dir)),
    );
    arbitrator.set_soft_limit(soft_bytes, soft_ratio, soft_mode);
    if !arbitrator.auto_run(
        tidb_util::memory::MemArbitratorActions::default(),
        tidb_util::memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        tidb_util::memory::DEF_AWAIT_FREE_POOL_SHARD_NUM,
        tidb_util::memory::DEF_TASK_TICK_DUR,
    ) {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "failed to start global memory arbitrator".to_owned(),
        )));
    }
    arbitrator.set_work_mode(mode);
    Ok(Some(arbitrator))
}

pub(crate) struct MemoryArbitratorAuthority {
    arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    registration: Option<tidb_util::memory::ProcessArbitratorRegistration>,
}

impl MemoryArbitratorAuthority {
    pub(crate) fn open(config: &NodeConfig) -> Result<Self, RunConfiguredNodeError> {
        let arbitrator = open_memory_arbitrator(config)?;
        let registration = arbitrator
            .as_ref()
            .map(tidb_util::memory::install_process_arbitrator);
        Ok(Self {
            arbitrator,
            registration,
        })
    }

    pub(crate) fn arbitrator(&self) -> Option<Arc<tidb_util::memory::MemArbitrator>> {
        self.arbitrator.as_ref().map(Arc::clone)
    }
}

impl Drop for MemoryArbitratorAuthority {
    fn drop(&mut self) {
        if let Some(arbitrator) = self.arbitrator.as_ref() {
            let _ = arbitrator.stop();
        }
        self.registration.take();
    }
}

#[cfg(test)]
mod skip_grant_startup_tests {
    use super::*;

    #[test]
    fn enabled_startup_memory_policy_builds_one_running_process_arbitrator() {
        let mut config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ])
        .unwrap();
        config.memory_arbitrator.server_memory_limit = "1GiB".to_owned();
        config.memory_arbitrator.mode = "priority".to_owned();
        config.memory_arbitrator.soft_limit = "0.75".to_owned();

        let authority = MemoryArbitratorAuthority::open(&config).unwrap();
        let arbitrator = authority
            .arbitrator()
            .expect("an enabled policy starts a process controller");
        assert_eq!(
            arbitrator.work_mode(),
            tidb_util::memory::ArbitratorWorkMode::Priority
        );
        assert_eq!(arbitrator.limit_u64(), 1 << 30);
        assert_eq!(arbitrator.soft_limit(), 3 << 28);
        drop(authority);
        assert!(!arbitrator.stop());
    }
}

mod cluster_topology;
