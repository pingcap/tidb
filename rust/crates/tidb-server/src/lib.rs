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
//! `COM_QUERY`, `COM_PING`, and `COM_QUIT` lifecycle, plus the bounded
//! table-less automatic result-metadata path, source-shaped handshake
//! primitives, negotiated compressed command I/O, and TCP listener lifecycle. Authentication, TLS,
//! database selection, prepared statements, catalog-backed schema binding,
//! and every unsupported command remain explicit boundaries instead of
//! becoming fake success paths.

mod accept_loop;
mod auth_exchange;
mod auth_identity;
mod auth_plugin_registry;
mod auth_session;
mod auth_token;
mod bootstrap;
mod compressed_command_io;
pub mod connection_resultset;
mod error_response;
pub mod handshake;
mod listener;
pub mod resultset_source;
pub mod resultset_writer;
mod secure_transport;

use std::io::Cursor;

use tidb_datatype::Collation;
use tidb_distsql::DistSqlContext;
use tidb_exec::{Cluster, ExecError, Outcome, Session};
use tidb_protocol::{
    decode_command, encode_ok_packet, ColumnInfo, Command, OkPacket, PacketReader, PacketWriter,
    ResultSetOptions,
};

pub use accept_loop::{
    AcceptListener, AcceptLoop, AcceptLoopError, AcceptLoopExit, ShutdownHandle,
};
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
pub use compressed_command_io::{
    CommandIoError, CommandIoOutcome, CompressedCommandIo, NegotiatedCompression, CLIENT_COMPRESS,
};
pub use error_response::{frame_execution_error_response, frame_rendered_error_response};
pub use handshake::{
    negotiate_capabilities, parse_response, parse_response_body, parse_response_header,
    AuthHandshake, AuthHandshakePacket, AuthHandshakePhase, AuthHandshakeRequest, AuthPluginAction,
    HandshakeError, HandshakeResponse, HandshakeResponseHeader, InitialHandshake,
};
pub use listener::{ListenerConfig, ListenerError, ListenerLifecycle, ListenerState};
pub use secure_transport::{
    SecureTransportError, SecureTransportPolicy, TransportDecision, TransportKind,
};

/// A response from the currently supported connection dispatch envelope.
#[derive(Debug, PartialEq)]
pub enum DispatchResponse {
    /// The peer requested that the connection close.
    Quit,
    /// A command completed without a result set.
    Ok,
    /// A text query completed and retained its request metadata.
    Query {
        /// The query outcome from the shared executor.
        outcome: Outcome,
        /// The exact UTF-8 SQL text copied to the DistSQL request context.
        original_sql: String,
    },
}

/// A response from the framed command boundary.
///
/// `Packets` contains complete uncompressed MySQL packet frames. `Quit` is a
/// deliberate no-response close signal matching Go's `COM_QUIT` dispatch: the
/// caller closes the transport after observing it.
#[derive(Debug, PartialEq)]
pub enum FramedResponse {
    /// Complete response packet frames, beginning at server sequence one.
    Packets(Vec<u8>),
    /// Close the connection without writing a response packet.
    Quit,
}

/// Errors returned before a command can produce a response.
#[derive(Debug, PartialEq)]
pub enum DispatchError {
    /// The connection received a command after `COM_QUIT`.
    ConnectionClosed,
    /// The query payload is not valid UTF-8 for the current Rust server
    /// boundary. Go's connection charset decoder is a later owner.
    InvalidQueryUtf8,
    /// The command is decoded correctly but not executable in this server
    /// milestone.
    UnsupportedCommand(u8),
    /// The shared session rejected or failed the SQL statement.
    Execution(ExecError),
    /// The command packet did not contain a command byte.
    MalformedCommand(String),
    /// A response packet sequence could not be reconstructed at this server
    /// boundary. This is an internal framing failure, not a client command
    /// error.
    ResponseFraming(String),
    /// The automatic table-less result metadata boundary rejected the query
    /// shape instead of guessing a catalog-backed schema.
    AutomaticResultMetadata(String),
}

/// A local server connection attached to one shared catalog cluster.
#[derive(Debug)]
pub struct Connection {
    session: Session,
    request: DistSqlContext,
    closed: bool,
}

impl Connection {
    /// Creates a connection attached to the supplied shared cluster.
    #[must_use]
    pub fn new(cluster: &Cluster) -> Self {
        Self {
            session: cluster.session(),
            request: DistSqlContext::new(),
            closed: false,
        }
    }

    /// Dispatches one unframed MySQL command payload.
    ///
    /// The packet header/continuation layer is owned by `tidb-protocol`; this
    /// method starts at the command byte just like Go's `clientConn.dispatch`.
    /// Only `COM_QUERY`, `COM_PING`, and `COM_QUIT` are executable today.
    pub fn dispatch(&mut self, payload: &[u8]) -> Result<DispatchResponse, DispatchError> {
        if self.closed {
            return Err(DispatchError::ConnectionClosed);
        }
        let command = decode_command(payload)
            .map_err(|error| DispatchError::MalformedCommand(error.to_string()))?;
        let code = command_code(&command);
        match command {
            Command::Quit => {
                self.closed = true;
                Ok(DispatchResponse::Quit)
            }
            Command::Ping => Ok(DispatchResponse::Ok),
            Command::Query(bytes) => {
                let sql = std::str::from_utf8(&bytes)
                    .map_err(|_| DispatchError::InvalidQueryUtf8)?
                    .to_owned();
                self.request.request.original_sql = sql.clone();
                let outcome = self
                    .session
                    .execute_sql(&sql)
                    .map_err(DispatchError::Execution)?;
                Ok(DispatchResponse::Query {
                    outcome,
                    original_sql: sql,
                })
            }
            Command::InitDb(_)
            | Command::FieldList(_)
            | Command::StmtPrepare(_)
            | Command::StmtExecute(_)
            | Command::StmtClose(_)
            | Command::StmtReset(_)
            | Command::StmtFetch(_)
            | Command::SetOption(_)
            | Command::ResetConnection
            | Command::Unknown { .. } => Err(DispatchError::UnsupportedCommand(code)),
        }
    }

    /// Frames a source-rendered execution error after [`Self::dispatch`]
    /// returned [`DispatchError::Execution`].
    ///
    /// The session contributes only its latest published warning/message and
    /// `ExecSuccess` snapshot; the caller still owns the rendered message and
    /// the negotiated Protocol 4.1 bit. This keeps the Go `dispatch` →
    /// `writeError` ownership split (`pkg/server/conn.go:1338-1345,1725-1768`)
    /// without synthesizing table, column, SQLSTATE, or warning fields.
    pub fn frame_execution_error(
        &self,
        error: &ExecError,
        rendered_message: impl Into<Vec<u8>>,
        protocol_41: bool,
    ) -> Result<FramedResponse, DispatchError> {
        let rendered = self.session.render_exec_error(error, rendered_message);
        frame_rendered_error_response(&rendered, protocol_41)
    }

    /// Dispatches exactly one framed, uncompressed MySQL command.
    ///
    /// The incoming command must begin at packet sequence zero. Query result
    /// metadata remains caller-owned because the current executor does not yet
    /// derive source `ResultField`s automatically; the supplied columns and
    /// status options are passed directly to
    /// [`Session::execute_framed_query_text_result_set`]. Responses begin at
    /// server packet sequence one, as required after a sequence-zero request.
    /// Authentication, TLS, compression, listener lifecycle, and automatic
    /// schema/result-field derivation are intentionally outside this API.
    pub fn dispatch_framed(
        &mut self,
        framed: &[u8],
        columns: &[ColumnInfo],
        options: ResultSetOptions,
    ) -> Result<FramedResponse, DispatchError> {
        if self.closed {
            return Err(DispatchError::ConnectionClosed);
        }
        let command = decode_framed_command(framed)?;
        let code = command_code(&command);
        match command {
            Command::Quit => {
                self.closed = true;
                Ok(FramedResponse::Quit)
            }
            Command::Ping => {
                let payload = encode_ok_packet(&OkPacket {
                    status_flags: options.status_flags,
                    warnings: options.warnings,
                    protocol_41: options.protocol_41,
                    ..OkPacket::default()
                });
                Ok(FramedResponse::Packets(frame_payloads(
                    std::slice::from_ref(&payload),
                    1,
                )?))
            }
            Command::Query(bytes) => {
                std::str::from_utf8(&bytes).map_err(|_| DispatchError::InvalidQueryUtf8)?;
                let encoded = self
                    .session
                    .execute_framed_query_text_result_set(
                        framed,
                        &mut self.request,
                        columns,
                        options,
                    )
                    .map_err(DispatchError::Execution)?;
                Ok(FramedResponse::Packets(reframe_response(&encoded)?))
            }
            Command::InitDb(_)
            | Command::FieldList(_)
            | Command::StmtPrepare(_)
            | Command::StmtExecute(_)
            | Command::StmtClose(_)
            | Command::StmtReset(_)
            | Command::StmtFetch(_)
            | Command::SetOption(_)
            | Command::ResetConnection
            | Command::Unknown { .. } => Err(DispatchError::UnsupportedCommand(code)),
        }
    }

    /// Dispatches one framed command while deriving result columns for a
    /// dependency-closed table-less or single-table catalog-backed `SELECT`.
    ///
    /// This is the first connected consumer of the executor's automatic
    /// result-field adapter. It keeps the existing
    /// [`Self::dispatch_framed`] caller-supplied path intact while making the
    /// safe no-`FROM` and single-table catalog-backed paths executable end to
    /// end. Bounded INNER/CROSS/LEFT/USING bare-wildcard joins are also
    /// resolved from catalog snapshots, preserving null extension and
    /// coalesced field order. Direct columns, aliases, and qualified/bare
    /// wildcards over those joins cross the isolated planner projection
    /// contract; typed expressions and redundant right-side `USING` columns
    /// still require planner typing/FullSchema mappings. RIGHT/NATURAL joins,
    /// set operations, and all other unsupported schema-dependent shapes return
    /// [`DispatchError::AutomaticResultMetadata`] rather than inferring
    /// columns from runtime values.
    pub fn dispatch_framed_auto(
        &mut self,
        framed: &[u8],
        options: ResultSetOptions,
    ) -> Result<FramedResponse, DispatchError> {
        let command = decode_framed_command(framed)?;
        let columns = match command {
            Command::Query(bytes) => {
                let sql =
                    std::str::from_utf8(&bytes).map_err(|_| DispatchError::InvalidQueryUtf8)?;
                self.session
                    .resolve_query_result_columns(sql, Collation::DEFAULT, "")
                    .map_err(|error| DispatchError::AutomaticResultMetadata(error.to_string()))?
            }
            Command::Ping | Command::Quit => Vec::new(),
            _ => Vec::new(),
        };
        self.dispatch_framed(framed, &columns, options)
    }

    /// Returns the current request metadata snapshot.
    #[must_use]
    pub fn request(&self) -> &DistSqlContext {
        &self.request
    }

    /// Returns whether this connection has observed `COM_QUIT`.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }
}

fn command_code(command: &Command) -> u8 {
    match command {
        Command::Quit => tidb_protocol::COM_QUIT,
        Command::InitDb(_) => tidb_protocol::COM_INIT_DB,
        Command::Query(_) => tidb_protocol::COM_QUERY,
        Command::FieldList(_) => tidb_protocol::COM_FIELD_LIST,
        Command::Ping => tidb_protocol::COM_PING,
        Command::StmtPrepare(_) => tidb_protocol::COM_STMT_PREPARE,
        Command::StmtExecute(_) => tidb_protocol::COM_STMT_EXECUTE,
        Command::StmtClose(_) => tidb_protocol::COM_STMT_CLOSE,
        Command::StmtReset(_) => tidb_protocol::COM_STMT_RESET,
        Command::StmtFetch(_) => tidb_protocol::COM_STMT_FETCH,
        Command::SetOption(_) => tidb_protocol::COM_SET_OPTION,
        Command::ResetConnection => tidb_protocol::COM_RESET_CONNECTION,
        Command::Unknown { code, .. } => *code,
    }
}

fn decode_framed_command(framed: &[u8]) -> Result<Command, DispatchError> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    let payload = reader
        .read_packet()
        .map_err(|error| DispatchError::MalformedCommand(error.to_string()))?;
    let cursor = reader.into_inner();
    if cursor.position() != cursor.get_ref().len() as u64 {
        return Err(DispatchError::MalformedCommand(
            "trailing bytes after command packet".to_owned(),
        ));
    }
    decode_command(&payload).map_err(|error| DispatchError::MalformedCommand(error.to_string()))
}

fn reframe_response(encoded: &[u8]) -> Result<Vec<u8>, DispatchError> {
    let mut reader = PacketReader::new(Cursor::new(encoded));
    let mut payloads = Vec::new();
    loop {
        let cursor = reader.get_ref();
        if cursor.position() == cursor.get_ref().len() as u64 {
            break;
        }
        payloads.push(
            reader
                .read_packet()
                .map_err(|error| DispatchError::ResponseFraming(error.to_string()))?,
        );
    }
    frame_payloads(&payloads, 1)
}

fn frame_payloads(payloads: &[Vec<u8>], sequence: u8) -> Result<Vec<u8>, DispatchError> {
    let mut framed = Vec::new();
    let mut writer = PacketWriter::with_sequence(&mut framed, sequence);
    for payload in payloads {
        writer
            .write_packet(payload)
            .map_err(|error| DispatchError::ResponseFraming(error.to_string()))?;
    }
    writer
        .flush()
        .map_err(|error| DispatchError::ResponseFraming(error.to_string()))?;
    Ok(framed)
}

#[cfg(test)]
mod tests {
    use super::{Connection, DispatchError, DispatchResponse};
    use tidb_exec::{Cluster, Outcome};

    #[test]
    fn dispatch_query_connects_command_decode_session_and_request_metadata() {
        let cluster = Cluster::new();
        let mut connection = Connection::new(&cluster);
        let response = connection
            .dispatch(b"\x03select 7\0")
            .expect("COM_QUERY should execute");
        assert_eq!(
            response,
            DispatchResponse::Query {
                outcome: Outcome::Rows(tidb_exec::ResultSet {
                    rows: vec![vec![tidb_datatype::Datum::Int(7)]],
                    ordered: false,
                }),
                original_sql: "select 7".to_owned(),
            }
        );
        assert_eq!(connection.request().request.original_sql, "select 7");
    }

    #[test]
    fn dispatch_supports_ping_and_quit_but_rejects_other_commands() {
        let cluster = Cluster::new();
        let mut connection = Connection::new(&cluster);
        assert_eq!(
            connection.dispatch(&[tidb_protocol::COM_PING]),
            Ok(DispatchResponse::Ok)
        );
        assert_eq!(
            connection.dispatch(&[tidb_protocol::COM_INIT_DB, b't']),
            Err(DispatchError::UnsupportedCommand(
                tidb_protocol::COM_INIT_DB
            ))
        );
        assert_eq!(
            connection.dispatch(&[tidb_protocol::COM_QUIT]),
            Ok(DispatchResponse::Quit)
        );
        assert!(connection.is_closed());
        assert_eq!(
            connection.dispatch(&[tidb_protocol::COM_PING]),
            Err(DispatchError::ConnectionClosed)
        );
    }

    #[test]
    fn dispatch_rejects_malformed_and_non_utf8_queries_before_execution() {
        let cluster = Cluster::new();
        let mut connection = Connection::new(&cluster);
        assert!(matches!(
            connection.dispatch(&[]),
            Err(DispatchError::MalformedCommand(_))
        ));
        assert_eq!(
            connection.dispatch(&[tidb_protocol::COM_QUERY, 0xff]),
            Err(DispatchError::InvalidQueryUtf8)
        );
        assert_eq!(connection.request().request.original_sql, "");
    }
}
