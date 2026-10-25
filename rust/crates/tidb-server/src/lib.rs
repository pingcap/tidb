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


mod auth_exchange;
pub(crate) use tidb_session::domain::auth_identity;
mod auth_plugin_registry;
pub use tidb_ddl::bootstrap_publish;
pub use tidb_session::domain::cluster_account_seam;
pub use tidb_session::domain::cluster_analyze_seam;
pub use tidb_session::domain::cluster_auto_id_seam;
pub use tidb_session::domain::cluster_binding_seam;
pub(crate) use tidb_session::domain::cluster_privileges;
pub use tidb_session::domain::cluster_session;
pub use tidb_session::domain::cluster_session_node;
pub use tidb_session::domain::cluster_stats_lock_seam;
pub use tidb_session::domain::cluster_sysvar_seam;
pub(crate) use tidb_session::domain::configured_user_store;
pub mod connection_resultset;
mod connection_writers;
mod cursor_state;
pub(crate) use tidb_session::domain::global_config_sync;
pub mod handshake;
mod handshake_response;
pub mod http_settings;
pub mod http_status;
pub use tidb_session::domain::main_flags;
mod mysql_connection;
mod mysql_tls;
pub(crate) use tidb_session::domain::native_password;
pub(crate) use tidb_session::domain::node_config;
pub mod peer_rpc;
pub(crate) use tidb_session::domain::pipeline_session;
mod query_metrics;
pub(crate) use tidb_session::domain::real_tikv_node;
pub use tidb_session::domain::resultset_source;
pub mod resultset_writer;
pub(crate) use tidb_session::domain::secure_transport;
pub mod server_metrics;
pub use tidb_session::domain::serverinfo_etcd;
pub(crate) use tidb_session::domain::session_transaction;
pub(crate) use tidb_session::domain::set_global_vars;
mod shutdown_signal;
pub mod signal_exit;
pub(crate) use tidb_session::domain::session_api;
mod cluster_boot;
pub use cluster_boot::run_cluster_session_node;
mod unistore_run;
mod node_status;
mod domain_wire_tests;
#[cfg(test)]
mod domain_test_support;
#[cfg(test)]
mod production_replay;
mod sql_node;
mod status_transport;
pub(crate) use tidb_session::domain::unistore_node;
pub use tidb_session::domain::wire_status;
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
use cluster_boot::run_cluster_session_node_with_spill;
pub use cluster_session_node::{
    ClusterServerSession, ClusterSessionFactory,
};
pub use configured_user_store::{
    AuthenticatedIdentity, AuthenticationFailure, ConfiguredUserStore, ConfiguredUserStoreError,
};
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
pub use pipeline_session::MaterializedResultSetSource;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

pub use real_tikv_node::{run_with_process_shutdown, ProcessReadAuthority, RunConfiguredNodeError};
pub use resultset_source::ResultSetSource;
pub use secure_transport::{
    SecureTransportError, SecureTransportPolicy, TransportDecision, TransportKind,
};
pub use sql_node::{
    ActiveQueryCancellation, BoxedResultSetSource, ConcurrentSqlNode, ConnectionCancellation,
    ConnectionTracker, GeneralExecuteOutcome, PreparedGeneral,
    QueryCancellationLease, QueryResult, QuerySession,
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
    setup_log()?;
    let result = run_node_with_log(config);
    // Go `main`: `syncLog()` after the server exited; a failed sync turns
    // the exit into a generic failure.
    if let Err(error) = tidb_util::logutil::sync() {
        eprintln!("sync log err: {error}");
        if result.is_ok() {
            return Err(RunConfiguredNodeError::Log(error.to_string()));
        }
    }
    result
}

/// Go `cmd/tidb-server/main.go` `setupLog`:
/// `logutil.InitLogger(cfg.Log.ToLogConfig(), keyspace.WrapZapcoreWithKeyspace())`.
/// The log level, file and slow-query file already carry the `-L`,
/// `--log-file` and `--log-slow-query` overrides (`overrideConfig`).
fn setup_log() -> Result<(), RunConfiguredNodeError> {
    let log = tidb_config::config_tree::config::get_global_config()
        .log
        .to_log_config();
    let file = tidb_util::logutil::FileLogConfig {
        filename: log.file.filename,
        max_size: log.file.max_size,
        max_days: log.file.max_days,
        max_backups: log.file.max_backups,
        compression: log.file.compression,
        is_buffered: log.file.is_buffered,
        buffer_size: log.file.buffer_size,
        buffer_flush_interval: log.file.buffer_flush_interval,
    };
    let mut log_config = tidb_util::logutil::new_log_config(
        &log.level,
        &log.format,
        &log.slow_query_file,
        &log.general_log_file,
        file,
        log.disable_timestamp,
    );
    log_config.config.disable_error_verbose = log.disable_error_verbose;
    log_config.config.timeout = log.timeout;
    // `keyspace.WrapZapcoreWithKeyspace`: entries carry `keyspaceName` only
    // when a keyspace is configured.
    let keyspace_name = tidb_util::keyspace::get_keyspace_name_by_settings();
    let core_fields = if tidb_util::keyspace::is_keyspace_name_empty(&keyspace_name) {
        Vec::new()
    } else {
        vec![tidb_log::Field::new(
            "keyspaceName",
            tidb_log::Value::Str(keyspace_name),
        )]
    };
    tidb_util::logutil::init_logger_with_core_fields(&log_config, &core_fields)
        .map_err(RunConfiguredNodeError::Log)
}








/// Go `printInfo`: the TiDB info is always printed, whatever the level.
fn print_info() {
    let level = tidb_log::get_level();
    tidb_log::set_level(tidb_log::Level::Info);
    tidb_util::printer::print_tidb_info();
    tidb_log::set_level(level);
}

fn run_node_with_log(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    initialize_compute_topology(&config.global_config)?;
    tidb_txnkv::global_mpp_failed_store_prober().run();
    let _mpp_prober_cleanup = MppProberCleanup;
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
    print_info();
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
        return unistore_run::run_unistore_cluster_session(
            config,
            spill_storage,
            memory_arbitrator.arbitrator(),
        );
    }
    run_cluster_session_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

struct MppProberCleanup;
impl Drop for MppProberCleanup {
    fn drop(&mut self) {
        tidb_txnkv::global_mpp_failed_store_prober().stop();
    }
}

fn initialize_compute_topology(
    config: &tidb_config::config_tree::config::Config,
) -> Result<(), RunConfiguredNodeError> {
    if config.disaggregated_tiflash && config.use_auto_scaler {
        tidb_exec::tiflash_compute::init_global_topo_fetcher(
            &config.tiflash_compute_auto_scaler_type,
            &config.tiflash_compute_auto_scaler_addr,
            &config.auto_scaler_cluster_id,
            config.is_tiflash_compute_fixed_pool,
        )
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    }
    Ok(())
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
            tidb_util::logutil::bg_logger()
                .error("failed to setup statements summary", &[zap_error(error)]);
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

pub(crate) use tidb_session::domain::cluster_topology;

#[cfg(test)]
mod compute_topology_batch_tests {
    #[test]
    fn compute_topology_batch_startup_gates_and_errors() {
        let mut config = tidb_config::config_tree::config::Config::default();
        config.tiflash_compute_auto_scaler_type = "gcp".into();
        config.auto_scaler_cluster_id = "cluster".into();
        config.tiflash_compute_auto_scaler_addr = "unused:1234".into();
        config.disaggregated_tiflash = false;
        config.use_auto_scaler = true;
        assert!(super::initialize_compute_topology(&config).is_ok());
        config.disaggregated_tiflash = true;
        assert!(super::initialize_compute_topology(&config)
            .unwrap_err()
            .to_string()
            .contains("not implemented yet(gcp)"));
        config.use_auto_scaler = false;
        assert!(super::initialize_compute_topology(&config).is_ok());
        config.use_auto_scaler = true;
        config.tiflash_compute_auto_scaler_type = "aws".into();
        config.auto_scaler_cluster_id.clear();
        assert!(super::initialize_compute_topology(&config)
            .unwrap_err()
            .to_string()
            .contains("addr is empty"));
    }
}

#[cfg(test)]
mod setup_log_tests {
    use std::path::{Path, PathBuf};

    // `setupLog` reads and replaces process globals (the global config and
    // the loggers); the Go binary runs it once, so these tests run one at a
    // time.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tidb_rust_setup_log_{name}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn boot_logging(dir: &Path, config: &str, flags: &[&str]) {
        let config_path = dir.join("tidb.toml");
        std::fs::write(&config_path, config).unwrap();
        let config_path = config_path.to_string_lossy().into_owned();
        let mut arguments = vec![
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--store",
            "tikv",
            "--config",
            &config_path,
        ];
        arguments.extend_from_slice(flags);
        let config = crate::NodeConfig::parse(arguments).expect("node config");
        config.install_process_globals();
        super::setup_log().expect("setupLog");
    }

    /// Go `overrideConfig`: `--log-file`, `-L` and `--log-slow-query` win
    /// over `log.file.filename`, `log.level` and `log.slow-query-file`.
    #[test]
    fn log_flags_override_the_config_file() {
        let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
        let dir = scratch("flags");
        let from_config = dir.join("from-config.log");
        let from_flag = dir.join("from-flag.log");
        let slow = dir.join("slow.log");
        boot_logging(
            &dir,
            &format!(
                "[log]\nlevel = \"error\"\nslow-query-file = {:?}\n[log.file]\nfilename = {:?}\n",
                dir.join("config-slow.log"),
                from_config
            ),
            &[
                "--log-file",
                &from_flag.to_string_lossy(),
                "-L",
                "info",
                "--log-slow-query",
                &slow.to_string_lossy(),
            ],
        );
        tidb_util::logutil::bg_logger().info("info passes the flag's level", &[]);
        tidb_log::info("the pingcap/log global writes the same file", &[]);
        tidb_util::logutil::slow_query_logger().info("slow entry", &[]);
        let content = std::fs::read_to_string(&from_flag).unwrap();
        assert!(content.contains("[INFO] [lib.rs:"), "{content}");
        assert!(
            content.contains("[\"info passes the flag's level\"]"),
            "{content}"
        );
        assert!(
            content.contains("[\"the pingcap/log global writes the same file\"]"),
            "{content}"
        );
        assert!(!from_config.exists());
        let slow_content = std::fs::read_to_string(&slow).unwrap();
        assert!(slow_content.starts_with("# Time: "), "{slow_content}");
        assert!(slow_content.ends_with("\nslow entry\n"), "{slow_content}");
        assert!(!dir.join("config-slow.log").exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Without the flags, the config file names the log file and level.
    #[test]
    fn config_file_names_the_log_file_and_level() {
        let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
        let dir = scratch("config");
        let from_config = dir.join("from-config.log");
        boot_logging(
            &dir,
            &format!(
                "[log]\nlevel = \"warn\"\nslow-query-file = {:?}\n[log.file]\nfilename = {:?}\n",
                dir.join("slow.log"),
                from_config
            ),
            &[],
        );
        tidb_util::logutil::bg_logger().info("filtered by the config level", &[]);
        tidb_util::logutil::bg_logger().warn("warn passes", &[]);
        let content = std::fs::read_to_string(&from_config).unwrap();
        assert!(!content.contains("filtered"), "{content}");
        assert!(content.contains("[WARN] [lib.rs:"), "{content}");
        assert_eq!(tidb_log::get_level(), tidb_log::Level::Warn);
        let _ = std::fs::remove_dir_all(dir);
    }
}

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
pub(crate) use tidb_session::domain::{
    join_host_port, log_mysql_listening, log_recovered_panic, log_status_listen,
    log_status_listen_failed, open_spill_storage, zap_error, zap_stack, MemoryArbitratorAuthority,
};
