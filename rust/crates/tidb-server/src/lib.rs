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
//! decoder and `tidb-exec`'s shared session. It owns the currently executable
//! `COM_QUERY`, `COM_PING`, and `COM_QUIT` lifecycle, plus one authenticated
//! configured signed-BIGINT `COM_STMT_PREPARE`/`EXECUTE`/`CLOSE` point-read
//! path, the bounded table-less automatic result-metadata path, source-shaped
//! handshake primitives, negotiated compressed command I/O, and TCP listener
//! lifecycle. TLS, database selection, general prepared statements, broad
//! catalog-backed schema binding, and every unsupported command remain
//! explicit boundaries instead of becoming fake success paths.

mod aggregate_result_set;
mod auth_exchange;
mod auth_identity;
mod auth_plugin_registry;
mod auth_session;
mod auth_token;
mod bootstrap;
pub mod cluster_account_seam;
pub mod cluster_analyze_seam;
mod cluster_privileges;
pub mod cluster_session;
pub mod cluster_session_node;
pub mod cluster_sysvar_seam;
mod compressed_command_io;
mod configured_user_store;
pub mod connection_resultset;
mod distinct_result_set;
pub mod handshake;
mod handshake_response;
mod listener;
mod mysql_connection;
mod native_password;
mod node_config;
mod pipeline_session;
mod real_tikv_multi_node;
mod real_tikv_node;
pub mod resultset_source;
pub mod resultset_writer;
mod secure_transport;
mod session_transaction;
mod sorting_result_set;
mod sql_node;
mod transaction_overlay_result_set;
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
pub use cluster_session_node::{
    run_cluster_session_node, ClusterServerSession, ClusterSessionFactory,
};
pub use compressed_command_io::{
    CommandIoError, CommandIoOutcome, CompressedCommandIo, NegotiatedCompression, CLIENT_COMPRESS,
};
pub use configured_user_store::{
    AuthenticatedIdentity, ConfiguredUserStore, ConfiguredUserStoreError,
};
pub use distinct_result_set::DistinctResultSetSource;
pub use handshake::{
    negotiate_capabilities, parse_response, parse_response_body, parse_response_header,
    AuthHandshake, AuthHandshakePacket, AuthHandshakePhase, AuthHandshakeRequest, AuthPluginAction,
    HandshakeError, HandshakeResponseHeader, InitialHandshake,
};
pub use handshake_response::HandshakeResponse41;
pub use listener::{ListenerConfig, ListenerError, ListenerLifecycle, ListenerState};
pub use mysql_connection::{
    serve_mysql_connection, ConnectionCommandCounts, ConnectionExit, ConnectionReport,
    MysqlConnectionError,
};
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
use real_tikv_multi_node::run_bound_multi_node;
pub use real_tikv_multi_node::{
    run_configured_multi_node, RealTiKvMultiServerSession, RealTiKvMultiSessionFactory,
};
use real_tikv_node::{
    connect_loaded_catalog_authority, node_accounts, run_bound_node, LoadedCatalogAuthority,
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
    ConnectionTracker, PreparedPointRead, PreparedStatement, PreparedWrite, QueryCancellationLease,
    QueryResult, QuerySession, QuerySessionFactory, SessionContext, ShutdownHandle, SqlNodeError,
    SqlQueryError, WriteOutcome,
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
    if config.cluster_session {
        return run_cluster_session_node(config);
    }
    if !config.load_tables.is_empty() {
        return match connect_loaded_catalog_authority(&config)
            .map_err(RunConfiguredNodeError::Engine)?
        {
            LoadedCatalogAuthority::Single(factory, authority) => {
                let (users, privilege_reloader) = node_accounts(&config, &authority)?;
                run_bound_node(config, *factory, authority, users, privilege_reloader)
            }
            LoadedCatalogAuthority::Multi(factory, authority) => {
                let (users, privilege_reloader) = node_accounts(&config, &authority)?;
                run_bound_multi_node(config, *factory, authority, users, privilege_reloader)
            }
        };
    }
    if config.load_privileges {
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
        1 => run_single_configured_node(config),
        2 => run_configured_multi_node(config),
        count => Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            format!("configured SQL node requires one or two tables, got {count}"),
        ))),
    }
}
