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

//! Source-shaped connection dispatch for the standalone Rust SQL node.
//!
//! This is the first server-layer consumer of `tidb-protocol`'s command
//! decoder and `tidb-exec`'s shared session. Dispatched today, each with a
//! real arm in [`mysql_connection`]: `COM_QUERY`, `COM_PING`, `COM_QUIT`,
//! `COM_INIT_DB` (which really selects a schema), and the binary
//! prepared-statement family `COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`/`RESET`/
//! `COM_STMT_FETCH`. A prepare claims transaction control first -- `BEGIN`,
//! `COMMIT`, `ROLLBACK` and the savepoint statements are applied through
//! `control_transaction` at EXECUTE, exactly as the text arm applies them, so
//! a prepared `BEGIN` opens the connection's transaction rather than being
//! run as an ordinary statement -- and otherwise falls through three tiers,
//! point read, then write, then general. It is no longer the single
//! signed-BIGINT point-read path this paragraph used to describe. Also owned:
//! the bounded
//! table-less automatic result-metadata path, source-shaped handshake
//! primitives, negotiated compressed command I/O, and TCP listener lifecycle.
//!
//! Inbound TLS on the MySQL port is now served, not refused: with server
//! certificate material present (`--ssl-cert`/`--ssl-key`, or the self-signed
//! pair `--auto-tls` generates) the node advertises `CLIENT_SSL`, upgrades the
//! socket in place on an `SSLRequest`, and reads the real
//! `HandshakeResponse41` off the encrypted stream. Without material the bit
//! stays clear, because advertising it without performing the upgrade hangs
//! every client that asks.
//!
//! STILL EXPLICIT BOUNDARIES, refused rather than faked: client-certificate
//! authentication (`--ssl-ca` and `REQUIRE X509`), `COM_FIELD_LIST`,
//! `COM_SET_OPTION`, `COM_RESET_CONNECTION`, and every unknown command.
//!
//! This paragraph claimed "database selection" and "general prepared
//! statements" were boundaries after both had landed. It is the third module
//! doc in this tree found asserting behaviour that was no longer true, so:
//! when a unit changes what this crate accepts, correct this list in the
//! same commit.

mod aggregate_result_set;
mod auth_exchange;
mod auth_identity;
mod auth_plugin_registry;
mod auth_session;
mod auth_token;
mod bootstrap;
pub mod cluster_account_seam;
pub mod cluster_analyze_seam;
pub mod cluster_auto_id_seam;
mod cluster_privileges;
pub mod cluster_session;
pub mod cluster_session_node;
pub mod cluster_sysvar_seam;
mod configured_user_store;
pub mod connection_resultset;
mod connection_writers;
mod cursor_state;
mod distinct_result_set;
pub mod handshake;
mod handshake_response;
mod listener;
mod mysql_connection;
mod mysql_tls;
mod native_password;
mod node_config;
mod pipeline_session;
mod real_tikv_multi_node;
mod real_tikv_node;
pub mod resultset_source;
pub mod resultset_writer;
mod secure_transport;
mod session_transaction;
pub mod signal_exit;
mod sorting_result_set;
mod sql_node;
mod transaction_overlay_result_set;
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
pub use auth_session::{
    AuthChallenge, AuthRejectionReason, AuthSessionAttempt, AuthSessionError, AuthSessionState,
    AUTH_SOCKET_PLUGIN,
};
pub use auth_token::{
    AuthTokenAttempt, AuthTokenCheck, AuthTokenCheckAction, AuthTokenCheckError,
    AuthTokenJwksState, AuthTokenRetryState, JwtCompactShape, AUTH_TOKEN_INVALID_JWT,
    AUTH_TOKEN_NO_VALID_JWKS, AUTH_TOKEN_RETRY_EXHAUSTED,
};
pub use bootstrap::{
    decide_start_mode, start_mode, BootstrapDecisionError, BootstrapFeatureGates, BootstrapMode,
    BootstrapPhase, BOOTSTRAP_PHASE_ORDER, NOT_BOOTSTRAPPED,
};
pub use cluster_privileges::{registry_from_cluster, LoadedRegistry, SkippedGrant};
use cluster_session_node::run_cluster_session_node_with_spill;
pub use cluster_session_node::{
    run_cluster_session_node, ClusterServerSession, ClusterSessionFactory,
};
pub use configured_user_store::{
    AuthenticatedIdentity, ConfiguredUserStore, ConfiguredUserStoreError,
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
pub use listener::{ListenerConfig, ListenerError, ListenerLifecycle, ListenerState};
pub use mysql_connection::{
    serve_mysql_connection, serve_mysql_connection_with_tls, ConnectionCommandCounts,
    ConnectionExit, ConnectionReport, MysqlConnectionError,
};
pub use mysql_tls::{resolve_server_tls, ClientStream, MysqlServerTls, MysqlTlsError};
pub use native_password::{
    generate_handshake_salt, verify_candidate, NativePasswordHash, NativePasswordHashError,
    HANDSHAKE_SALT_LEN, NATIVE_PASSWORD_HASH_LEN,
};
pub use node_config::{
    ConfiguredReadColumn, ConfiguredReadColumnKind, ConfiguredReadTable, NodeConfig,
    NodeConfigError,
};
pub use pipeline_session::{
    MaterializedResultSetSource, PipelineServerSession, PipelineSessionFactory,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use real_tikv_multi_node::{run_bound_multi_node, run_configured_multi_node_with_spill};
pub use real_tikv_multi_node::{
    run_configured_multi_node, RealTiKvMultiServerSession, RealTiKvMultiSessionFactory,
};
use real_tikv_node::{
    connect_loaded_catalog_authority, node_accounts, run_bound_node,
    run_configured_node_with_spill, LoadedCatalogAuthority,
};
pub use real_tikv_node::{
    run_configured_node as run_single_configured_node, run_with_process_shutdown,
    ProcessReadAuthority, RealTiKvServerSession, RealTiKvSessionFactory, RunConfiguredNodeError,
};
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

/// Starts the one configured SQL-node authority for its admitted table shape.
///
/// One servable table keeps the established single-reader path. Exactly two
/// servable tables use the connected same-snapshot join path; no fallback can
/// silently execute a multi-table query against an in-memory or single-table
/// authority. A command-line-only shape (`--read-table` with no
/// `--load-table`) still routes purely from its local table count, with no PD
/// side effect needed to make that decision. A `--load-table` shape cannot be
/// routed until its schema is read from the cluster's own catalog, so it
/// connects once and then serves whichever of the single-reader or
/// connected-join surfaces its servable table count reaches.
///
/// `--cluster-session` leaves that family entirely: it names no table and
/// serves the cluster's whole loaded catalog through the wide-SQL session
/// driver ([`cluster_session_node`]), so it is routed first.
pub fn run_configured_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    tidb_util::printer::print_tidb_info(&config.version_info, &config.startup_config_json());
    if config.store_kind == node_config::StoreKind::Unistore {
        // Go: `session.RegisterStore("unistore", mockstore.EmbedUnistoreDriver{})`
        // -- the same node code over the embedded store, no PD dialed.
        let _system_time_monitor = start_system_time_monitor();
        let spill_storage = open_spill_storage(&config)?;
        let memory_arbitrator = MemoryArbitratorAuthority::open(&config)?;
        return unistore_node::run_unistore_node(
            config,
            spill_storage,
            memory_arbitrator.arbitrator(),
        );
    }
    let _system_time_monitor = start_system_time_monitor();
    let spill_storage = open_spill_storage(&config)?;
    let memory_arbitrator = MemoryArbitratorAuthority::open(&config)?;
    if config.cluster_session {
        return run_cluster_session_node_with_spill(
            config,
            spill_storage,
            memory_arbitrator.arbitrator(),
        );
    }
    if !config.load_tables.is_empty() {
        return match connect_loaded_catalog_authority(&config)
            .map_err(RunConfiguredNodeError::Engine)?
        {
            LoadedCatalogAuthority::Single(factory, authority) => {
                let (users, privilege_reloader) = node_accounts(&config, &authority)?;
                run_bound_node(
                    config,
                    *factory,
                    authority,
                    users,
                    Arc::clone(&spill_storage),
                    memory_arbitrator.arbitrator(),
                    privilege_reloader,
                )
            }
            LoadedCatalogAuthority::Multi(factory, authority) => {
                let (users, privilege_reloader) = node_accounts(&config, &authority)?;
                run_bound_multi_node(
                    config,
                    *factory,
                    authority,
                    users,
                    Arc::clone(&spill_storage),
                    memory_arbitrator.arbitrator(),
                    privilege_reloader,
                )
            }
        };
    }
    if command_line_privilege_source_requires_cluster(&config) {
        // The account load rides the same authority a `--load-table` node
        // connects; a command-line-only node never connects one, so there is
        // nothing to read `mysql.*` through.
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "--load-privileges requires at least one --load-table, which is what connects this \
             node to the cluster whose mysql.* holds the accounts"
                .to_owned(),
        )));
    }
    match config.read_tables.len() {
        1 => run_configured_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator()),
        2 => run_configured_multi_node_with_spill(
            config,
            spill_storage,
            memory_arbitrator.arbitrator(),
        ),
        count => Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            format!("configured SQL node requires one or two tables, got {count}"),
        ))),
    }
}

fn command_line_privilege_source_requires_cluster(config: &NodeConfig) -> bool {
    config.load_privileges && !config.skip_grant_table
}

static SYSTEM_TIME_JUMP_BACKWARD_COUNT: AtomicU64 = AtomicU64::new(0);

fn start_system_time_monitor() -> tidb_util::systimemon::SystemTimeMonitor {
    tidb_util::systimemon::SystemTimeMonitor::start(SystemTime::now, || {
        SYSTEM_TIME_JUMP_BACKWARD_COUNT.fetch_add(1, Ordering::Relaxed);
    })
}

fn open_spill_storage(
    config: &NodeConfig,
) -> Result<Arc<tidb_util::disk::SpillStorage>, RunConfiguredNodeError> {
    tidb_executor::deadlock_history::configure_global_deadlock_history(
        config.deadlock_history_capacity,
        config.deadlock_history_collect_retryable,
    );
    if config.sem_enabled {
        tidb_util::sem::enable();
    } else {
        tidb_util::sem::disable();
    }
    tidb_util::disk::SpillStorage::open(config.spill_storage.clone())
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
    sampler_running: Arc<AtomicBool>,
    sampler: Option<JoinHandle<()>>,
}

impl MemoryArbitratorAuthority {
    pub(crate) fn open(config: &NodeConfig) -> Result<Self, RunConfiguredNodeError> {
        let arbitrator = open_memory_arbitrator(config)?;
        let sampler_running = Arc::new(AtomicBool::new(true));
        let sampler = arbitrator.as_ref().map(|arbitrator| {
            let arbitrator = Arc::downgrade(arbitrator);
            let running = Arc::clone(&sampler_running);
            std::thread::spawn(move || {
                while running.load(Ordering::Acquire) {
                    if let Some(arbitrator) = arbitrator.upgrade() {
                        if let Ok(bytes) = tidb_util::cgroup::current_process_memory_usage() {
                            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
                            arbitrator.handle_runtime_stats(tidb_util::memory::MemStats {
                                heap_alloc: bytes,
                                heap_inuse: bytes,
                                ..Default::default()
                            });
                        }
                    } else {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
        });
        let registration = arbitrator
            .as_ref()
            .map(tidb_util::memory::install_process_arbitrator);
        Ok(Self {
            arbitrator,
            registration,
            sampler_running,
            sampler,
        })
    }

    pub(crate) fn arbitrator(&self) -> Option<Arc<tidb_util::memory::MemArbitrator>> {
        self.arbitrator.as_ref().map(Arc::clone)
    }
}

impl Drop for MemoryArbitratorAuthority {
    fn drop(&mut self) {
        self.sampler_running.store(false, Ordering::Release);
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
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
            "--load-table",
            "test.rows",
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

    #[test]
    fn skip_grant_table_ignores_a_command_line_privilege_source_without_cluster_tables() {
        let mut config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--read-table",
            "test",
            "rows",
            "42",
            "1",
            "id:1:clustered-pk",
            "--load-privileges",
        ])
        .expect("the command line shape parses");
        assert!(command_line_privilege_source_requires_cluster(&config));
        config.skip_grant_table = true;
        assert!(
            !command_line_privilege_source_requires_cluster(&config),
            "recovery mode does not consult the configured privilege source"
        );
    }
}
