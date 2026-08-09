// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Live `TcpStream` ownership for the bounded MySQL connection lifecycle.

use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use tidb_protocol::result_encoder::ResultEncoder;
use tidb_protocol::{
    decode_command, decode_prepared_statement_close,
    decode_prepared_statement_execute_with_bound_params, decode_prepared_statement_fetch,
    decode_prepared_statement_send_long_data, encode_prepared_statement_prepare_response, Command,
    PacketError, PacketReader, PreparedParameterType, PreparedParameterTypes, PreparedValue,
    DEFAULT_MAX_ALLOWED_PACKET,
};

use crate::auth_exchange::AuthSwitchRequest;
use crate::configured_user_store::{AuthenticationFailure, ConfiguredUserStore};
use crate::connection_resultset::{
    write_connection_binary_result_set_to_sink, write_connection_result_set_to_sink,
};
use crate::connection_writers::{
    access_denied_message, account_locked_message, prepared_parameter_column,
    prepared_statement_id, write_affected_rows_ok, write_eof_or_ok, write_error, write_ok,
    write_packet_to, write_payload, write_query_error, write_query_error_at,
    write_unknown_statement, TcpResultSetSink, WireFraming,
};
use crate::cursor_state::{CursorFetchError, CursorState};
use crate::handshake::{
    negotiate_capabilities, parse_response, parse_response_header, InitialHandshake,
    AUTH_NATIVE_PASSWORD, CLIENT_CONNECT_ATTRS, CLIENT_CONNECT_WITH_DB, CLIENT_PLUGIN_AUTH,
    CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, CLIENT_SSL, DEFAULT_COLLATION_ID,
};
use crate::mysql_tls::{ClientStream, MysqlServerTls};
use crate::native_password::generate_handshake_salt;
use crate::resultset_source::ResultSetSource;
use crate::resultset_writer::ResultSetSink;
use crate::secure_transport::TransportKind;
use crate::sql_node::{
    ConnectionCancellation, ConnectionClose, ConnectionTracker, GeneralExecuteOutcome,
    PreparedStatement, QuerySession, QuerySessionFactory, SessionContext,
};
use crate::wire_status::{WireStatus, SERVER_STATUS_CURSOR_EXISTS, SERVER_STATUS_LAST_ROW_SEND};
use tidb_planner::prepared_dml::PreparedBindValue;
use tidb_planner::transaction_control::classify_transaction_control;
use tidb_session::privilege::plugin_needs_cleartext;

/// Extracts the signed-integer parameters a point read requires, rejecting a
/// string parameter (a point read binds only a clustered integer handle).
fn point_read_integer_parameters(values: Vec<PreparedValue>) -> Result<Vec<i64>, String> {
    values
        .into_iter()
        .map(|value| match value {
            PreparedValue::SignedLongLong(value) => Ok(value),
            // A point read binds a clustered signed handle; every other
            // parameter shape belongs to the general path.
            _ => Err("prepared point read parameter must be an integer".to_owned()),
        })
        .collect()
}

/// Converts decoded execute values into the planner's storage-neutral bind
/// currency, one variant at a time so every wire type reaches the write path
/// as the exact `PreparedBindValue` shape `configured_stored_value` expects:
/// a signed integer stays `Int`, an unsigned `BIGINT UNSIGNED` value stays
/// `UInt` (not truncated through a signed cast), `FLOAT`/`DOUBLE` become
/// `Float` (not stringified — a stringified float would parse as a `DECIMAL`
/// text on the other side, silently rounding through `Decimal::parse_mysql`
/// instead of storing the real IEEE-754 value into a `DOUBLE` column), `NULL`
/// becomes `PreparedBindValue::Null` (not empty bytes — an empty byte string
/// would bind to a nullable `VARCHAR` column as `''`, or refuse an `INT`
/// column as a type mismatch, either way silently losing `NULL`), and
/// `DECIMAL`/temporal parameters carry their text bytes, exactly as a string
/// parameter does, for their target column's own type to parse.
fn write_bind_parameters(values: Vec<PreparedValue>) -> Vec<PreparedBindValue> {
    values
        .into_iter()
        .map(|value| match value {
            PreparedValue::SignedLongLong(value) => PreparedBindValue::Int(value),
            PreparedValue::String(bytes) | PreparedValue::Decimal(bytes) => {
                PreparedBindValue::Bytes(bytes)
            }
            PreparedValue::UnsignedLongLong(value) => PreparedBindValue::UInt(value),
            PreparedValue::Float(value) => PreparedBindValue::Float(f64::from(value)),
            PreparedValue::Double(value) => PreparedBindValue::Float(value),
            PreparedValue::Null => PreparedBindValue::Null,
            PreparedValue::Temporal(text) => PreparedBindValue::Bytes(text.into_bytes()),
        })
        .collect()
}

const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
/// Go's `defaultCapability` (`pkg/server/server.go`) restricted to what this
/// node actually serves.
///
/// `CLIENT_CONNECT_WITH_DB` is load-bearing beyond selecting a schema. A real
/// libmysqlclient sets that bit in its *response* unconditionally, but only
/// writes the database field when the *server* advertised the bit. Omitting it
/// here therefore produced a response whose capability flags promised a field
/// the packet did not contain, so every field after the auth data -- database,
/// auth plugin, connection attributes -- was read one field early.
const SERVER_CAPABILITIES: u32 = CLIENT_PROTOCOL_41
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_DEPRECATE_EOF;
const ER_ACCESS_DENIED_ERROR: u16 = 1045;
const ER_UNKNOWN_COM_ERROR: u16 = 1047;
/// SQLSTATE `08S01` for [`ER_UNKNOWN_COM_ERROR`]. Go resolves every ERR
/// packet's state through `mysql.MySQLState` (`pkg/parser/mysql/state.go`),
/// which maps `ErrUnknownCom` to `08S01`, not the `HY000` default.
const ER_UNKNOWN_COM_ERROR_STATE: [u8; 5] = *b"08S01";
/// Go's `mysql.ErrAccountHasBeenLocked` (`pkg/errno/errcode.go`): the login
/// errno an `ACCOUNT LOCK`'d (or ROLE) account gets, distinct from the
/// generic access-denied a bad password or unknown user gets.
const ER_ACCOUNT_HAS_BEEN_LOCKED: u16 = 3118;
/// Go `errno.ErrSecureTransportRequired`. It carries no SQLSTATE mapping in
/// `pkg/errno`, so it reports the default `HY000`.
const ER_SECURE_TRANSPORT_REQUIRED: u16 = 3159;
const SECURE_TRANSPORT_REQUIRED_MESSAGE: &str =
    "Connections using insecure transport are prohibited while --require_secure_transport=ON.";
/// Go's `mysql.ErUserAccessDeniedForUserAccountBlockedByPasswordLock`: the
/// errno an account auto-locked by `FAILED_LOGIN_ATTEMPTS` gets, distinct
/// again from the manual `ACCOUNT LOCK` above.
const ER_USER_ACCESS_DENIED_BLOCKED_BY_PASSWORD_LOCK: u16 = 3955;
/// Go's `mysql.ErrMustChangePasswordLogin`: the login errno an account with
/// an EXPIRED password gets when the server refuses expired logins.
const ER_MUST_CHANGE_PASSWORD_LOGIN: u16 = 1862;
const ER_PARSE_ERROR: u16 = 1064;
const ER_UNKNOWN_ERROR: u16 = 1105;
const ER_WRONG_ARGUMENTS: u16 = 1210;
pub(crate) const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
pub(crate) const RESULT_BATCH_SIZE: usize = 128;

struct ConnectionPreparedStatement {
    statement: PreparedStatement,
    parameter_types: Option<Vec<PreparedParameterType>>,
    /// An open read-only cursor: the materialized result a cursor-mode
    /// execute stored for later `COM_STMT_FETCH` commands, with the columns
    /// it advertises and the next unread row. Go holds the same thing on the
    /// statement as a row container.
    cursor: Option<CursorState>,
    /// The `COM_STMT_SEND_LONG_DATA` buffer for each parameter, indexed by
    /// parameter ID: Go's `TiDBStatement.boundParams`, allocated
    /// `make([][]byte, paramCount)` at prepare time
    /// (`pkg/server/driver_tidb.go:358`). `None` is Go's nil slice -- "this
    /// parameter was never sent as long data" -- and is what makes an empty
    /// bound buffer distinguishable from an unbound one.
    bound_params: Vec<Option<Vec<u8>>>,
}

impl ConnectionPreparedStatement {
    /// Go `for i := range ts.boundParams { ts.boundParams[i] = nil }`.
    /// The vector keeps its prepare-time length; only the buffers go.
    fn clear_bound_params(&mut self) {
        for slot in &mut self.bound_params {
            *slot = None;
        }
    }
}

/// Why one `COM_STMT_SEND_LONG_DATA` chunk could not be appended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AppendParamError {
    /// No such prepared statement handle on this connection.
    UnknownStatement,
    /// The parameter ID is at or past the statement's marker count.
    ParameterOutOfRange,
    /// The accumulated buffer would exceed the wire payload cap.
    TooLarge,
}

struct PreparedStatementRegistry {
    next_id: Option<u32>,
    statements: HashMap<u32, ConnectionPreparedStatement>,
}

impl Default for PreparedStatementRegistry {
    fn default() -> Self {
        Self {
            next_id: Some(1),
            statements: HashMap::new(),
        }
    }
}

impl PreparedStatementRegistry {
    fn insert(&mut self, statement: PreparedStatement) -> Result<u32, &'static str> {
        let statement_id = self
            .next_id
            .ok_or("prepared statement ID space exhausted")?;
        self.next_id = statement_id.checked_add(1);
        self.statements.insert(
            statement_id,
            ConnectionPreparedStatement {
                bound_params: vec![None; statement.parameter_count()],
                statement,
                parameter_types: None,
                cursor: None,
            },
        );
        Ok(statement_id)
    }

    fn get(&self, statement_id: u32) -> Option<&ConnectionPreparedStatement> {
        self.statements.get(&statement_id)
    }

    fn remember_parameter_types(
        &mut self,
        statement_id: u32,
        parameter_types: &[PreparedParameterType],
    ) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.parameter_types = Some(parameter_types.to_vec());
        }
    }

    fn remove(&mut self, statement_id: u32) -> Option<ConnectionPreparedStatement> {
        self.statements.remove(&statement_id)
    }

    /// Go `stmt.Reset` (`pkg/server/driver_tidb.go:151-160`): returns the
    /// statement to the state it had right after PREPARE. Go nils every
    /// `boundParams[i]`, drops the active cursor, and leaves the statement
    /// itself installed -- and `handleStmtReset` names exactly those two
    /// things it must clear, the open cursor and "the argument sent through
    /// `SEND_LONG_DATA`" (`pkg/server/conn_stmt.go:627-631`). The remembered
    /// parameter-type vector deliberately survives: Go's `TiDBStatement.Reset`
    /// leaves `paramsType` untouched, so a later execute may keep its
    /// new-parameter-bound flag clear.
    fn reset(&mut self, statement_id: u32) -> Result<Option<CursorState>, ()> {
        match self.statements.get_mut(&statement_id) {
            Some(statement) => {
                statement.clear_bound_params();
                Ok(statement.cursor.take())
            }
            None => Err(()),
        }
    }

    /// Go `TiDBStatement.AppendParam` (`pkg/server/driver_tidb.go:104-116`):
    /// appends one `COM_STMT_SEND_LONG_DATA` chunk to a parameter's buffer.
    ///
    /// A parameter ID at or past the prepared marker count is Go's
    /// `ErrWrongArguments("stmt_send_longdata")`. An empty chunk stores an
    /// empty buffer rather than nothing, which is how Go keeps "bound to the
    /// empty string" distinct from "never bound".
    fn append_param(
        &mut self,
        statement_id: u32,
        parameter_id: usize,
        chunk: &[u8],
    ) -> Result<(), AppendParamError> {
        let statement = self
            .statements
            .get_mut(&statement_id)
            .ok_or(AppendParamError::UnknownStatement)?;
        let slot = statement
            .bound_params
            .get_mut(parameter_id)
            .ok_or(AppendParamError::ParameterOutOfRange)?;
        let buffer = slot.get_or_insert_with(Vec::new);
        // Go bounds the accumulated value with `max_allowed_packet`, which
        // this node does not have as a session variable yet (gap #185); the
        // same hardcoded wire cap the packet reader enforces is therefore the
        // bound here, so a client cannot grow one parameter without limit.
        if buffer.len().saturating_add(chunk.len()) > DEFAULT_MAX_ALLOWED_PACKET {
            return Err(AppendParamError::TooLarge);
        }
        buffer.extend_from_slice(chunk);
        Ok(())
    }

    fn bound_params(&self, statement_id: u32) -> &[Option<Vec<u8>>] {
        self.statements
            .get(&statement_id)
            .map_or(&[], |statement| statement.bound_params.as_slice())
    }

    /// Go's `stmt.Reset()` call inside `handleStmtExecute`, run right after
    /// `parseBinaryParams` has read the buffers
    /// (`pkg/server/conn_stmt.go:212-217`): long data is consumed by exactly
    /// one execute and never leaks into the next one.
    fn clear_bound_params(&mut self, statement_id: u32) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.clear_bound_params();
        }
    }

    fn open_cursor(&mut self, statement_id: u32, state: CursorState) -> Option<CursorState> {
        self.statements
            .get_mut(&statement_id)
            .and_then(|statement| statement.cursor.replace(state))
    }

    fn take_cursor(&mut self, statement_id: u32) -> Option<CursorState> {
        self.statements
            .get_mut(&statement_id)
            .and_then(|statement| statement.cursor.take())
    }
}

/// Observable terminal state of one accepted connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionExit {
    /// The client sent `COM_QUIT`.
    Quit,
    /// The peer closed the socket between commands.
    PeerClosed,
    /// Authentication was rejected after an ERR packet was written.
    AuthenticationRejected,
    /// Authentication succeeded but a worker-local query session could not open.
    SessionRejected,
    /// A `KILL` / `KILL CONNECTION` ended the connection (Go closes the
    /// session's socket and the command loop stops).
    Killed,
}

/// MySQL command opcodes observed while serving one connection.
///
/// Command counts advance as soon as dispatch identifies the opcode. Success
/// counts advance only after the complete success response has been written.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionCommandCounts {
    /// Number of `COM_QUERY` commands, including rejected queries.
    pub text_query_commands: u64,
    /// Number of `COM_STMT_PREPARE` commands, including rejected prepares.
    pub stmt_prepare_commands: u64,
    /// Number of complete successful `COM_STMT_PREPARE` responses.
    pub stmt_prepare_successes: u64,
    /// Number of `COM_STMT_EXECUTE` commands, including rejected executes.
    pub stmt_execute_commands: u64,
    /// Number of complete successful `COM_STMT_EXECUTE` responses.
    pub stmt_execute_successes: u64,
    /// Number of `COM_STMT_CLOSE` commands, including malformed closes.
    pub stmt_close_commands: u64,
    /// Number of `COM_STMT_RESET` commands, including unknown handles.
    pub stmt_reset_commands: u64,
    /// Number of `COM_STMT_SEND_LONG_DATA` commands, including the ones whose
    /// handle or parameter ID was rejected. A successful one writes no packet.
    pub stmt_send_long_data_commands: u64,
    /// Number of `COM_STMT_FETCH` commands, including unknown handles.
    pub stmt_fetch_commands: u64,
    /// Number of complete successful `COM_STMT_FETCH` responses.
    pub stmt_fetch_successes: u64,
}

/// Successful lifecycle report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionReport {
    /// Stable server connection ID advertised in the handshake.
    pub connection_id: u64,
    /// Number of successful text-query or prepared-execute commands.
    pub queries: u64,
    /// Exact command opcodes observed on this connection.
    pub commands: ConnectionCommandCounts,
    /// Why the connection stopped.
    pub exit: ConnectionExit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AcceptedConnectionIdentity {
    connection_id: u64,
    peer_addr: SocketAddr,
}

/// Fatal socket/protocol failure that prevents orderly command continuation.
#[derive(Debug)]
pub enum MysqlConnectionError {
    /// Cloning or configuring the accepted TCP stream failed.
    Io(std::io::Error),
    /// MySQL packet framing failed.
    Packet(PacketError),
    /// The checked handshake encoder or parser rejected the peer.
    Handshake(String),
    /// A result failed after response bytes may already have escaped.
    PartialResult(String),
    /// The statement was published and then lost its answer, so whether it
    /// committed is unknown and this connection must end without a verdict.
    ///
    /// Go `pkg/server/conn.go:1288-1291`:
    ///
    /// > } else if terror.ErrResultUndetermined.Equal(err) {
    /// >     logutil.Logger(ctx).Warn("result undetermined, close this connection", zap.Error(err))
    /// >     server_metrics.DisconnectErrorUndetermined.Inc()
    /// >     return
    ///
    /// It `return`s from the command loop *without* writing an ERR packet: no
    /// SQL error code can express "unknown", and a client that receives an
    /// error is entitled to retry — which double-applies if the commit landed.
    /// A closed connection is the one answer the client cannot mistake for a
    /// verdict.
    ResultUndetermined(String),
}

impl fmt::Display for MysqlConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "TCP I/O failed: {error}"),
            Self::Packet(error) => write!(formatter, "MySQL packet failed: {error}"),
            Self::Handshake(message) => formatter.write_str(message),
            Self::PartialResult(message) => {
                write!(formatter, "result failed after bytes escaped: {message}")
            }
            Self::ResultUndetermined(message) => {
                write!(
                    formatter,
                    "result undetermined, close this connection: {message}"
                )
            }
        }
    }
}

impl std::error::Error for MysqlConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Packet(error) => Some(error),
            Self::Handshake(_) | Self::PartialResult(_) | Self::ResultUndetermined(_) => None,
        }
    }
}

impl From<PacketError> for MysqlConnectionError {
    fn from(error: PacketError) -> Self {
        Self::Packet(error)
    }
}

/// Go's `authSha` packet: the one-byte command that introduces a
/// caching_sha2 / sm3 authentication decision, and the `fastAuthFail`
/// decision itself. TiDB caches nothing and serves no RSA public key, so
/// `fastAuthFail` is the ONLY decision it ever sends
/// (`server/conn.go`'s "Currently we always send a FastAuthFail").
const SHA2_FAST_AUTH_COMMAND: u8 = 1;
const SHA2_FAST_AUTH_FAIL: u8 = 4;

/// Serves one accepted socket on a MySQL port that offers no TLS.
///
/// The port advertises no `CLIENT_SSL`, so a client that wants TLS is refused
/// by the negotiation rather than left waiting for an upgrade.
pub fn serve_mysql_connection<F: QuerySessionFactory>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
) -> Result<ConnectionReport, MysqlConnectionError> {
    serve_mysql_connection_with_tls(
        stream,
        peer_addr,
        cancellation,
        factory,
        users,
        tracker,
        max_allowed_packet,
        None,
    )
}

/// Serves one accepted socket through handshake, optional TLS upgrade,
/// authentication, commands, result/error writes, and exactly-once connection
/// cleanup.
///
/// `tls` is both the advertisement and the capability: `CLIENT_SSL` reaches the
/// initial handshake packet exactly when material is present, matching Go's
/// `s.capability |= mysql.ClientSSL` guard in `pkg/server/server.go`.
#[allow(clippy::too_many_arguments)]
pub fn serve_mysql_connection_with_tls<F: QuerySessionFactory>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    tls: Option<&MysqlServerTls>,
) -> Result<ConnectionReport, MysqlConnectionError> {
    let mut lease = tracker.begin();
    eprintln!(
        "{{\"event\":\"connection_begin\",\"connection_id\":{},\"active\":{},\"accepted\":{}}}",
        lease.id(),
        tracker.active(),
        tracker.accepted()
    );
    let shutdown = cancellation.clone();
    let mut commands = ConnectionCommandCounts::default();
    let result = serve_connection_inner(
        stream,
        AcceptedConnectionIdentity {
            connection_id: lease.id(),
            peer_addr,
        },
        cancellation,
        factory,
        users,
        max_allowed_packet,
        tls,
        &mut commands,
    );
    let failed = result.is_err() && !shutdown.is_cancelled();
    if failed {
        lease.mark_failed();
    }
    let connection_id = lease.id();
    drop(lease);
    eprintln!(
        "{{\"event\":\"connection_closed\",\"connection_id\":{connection_id},\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{},\"text_query_commands\":{},\"stmt_prepare_commands\":{},\"stmt_prepare_successes\":{},\"stmt_execute_commands\":{},\"stmt_execute_successes\":{},\"stmt_close_commands\":{}}}",
        tracker.active(),
        tracker.accepted(),
        tracker.completed(),
        tracker.failed(),
        commands.text_query_commands,
        commands.stmt_prepare_commands,
        commands.stmt_prepare_successes,
        commands.stmt_execute_commands,
        commands.stmt_execute_successes,
        commands.stmt_close_commands,
    );
    result
}

#[allow(clippy::too_many_arguments)]
fn serve_connection_inner<F: QuerySessionFactory>(
    stream: TcpStream,
    identity: AcceptedConnectionIdentity,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    max_allowed_packet: usize,
    tls: Option<&MysqlServerTls>,
    commands: &mut ConnectionCommandCounts,
) -> Result<ConnectionReport, MysqlConnectionError> {
    let AcceptedConnectionIdentity {
        connection_id,
        peer_addr,
    } = identity;
    stream.set_nodelay(true).map_err(MysqlConnectionError::Io)?;
    // The KILL path shuts the raw socket down from another thread, so it keeps
    // its own descriptor: a TLS session cannot be cloned, and shutting the
    // descriptor down is what wakes a blocked read either way.
    let stream_for_close = stream.try_clone().map_err(MysqlConnectionError::Io)?;
    // Reader and writer share one connection object, because after a TLS
    // upgrade there is exactly one TLS session and both directions run through
    // it.
    let socket = ClientStream::plain(stream);
    let mut output = socket.clone();
    let server_capabilities = if tls.is_some() {
        SERVER_CAPABILITIES | CLIENT_SSL
    } else {
        SERVER_CAPABILITIES
    };
    let salt = generate_handshake_salt();
    let handshake = InitialHandshake {
        connection_id: u32::try_from(connection_id).unwrap_or(u32::MAX),
        salt: salt.to_vec(),
        capability: server_capabilities,
        collation: DEFAULT_COLLATION_ID,
        // Go `writeInitialHandshake` (`pkg/server/conn.go:496`) hardcodes this
        // one word, and only this one: the handshake precedes any session.
        status_flags: WireStatus::AUTOCOMMIT.bits(),
        server_version: "5.7.25-TiDB-Rust".to_owned(),
        auth_plugin: AUTH_NATIVE_PASSWORD.to_owned(),
    };
    output
        .write_all(
            &handshake
                .encode_packet()
                .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?,
        )
        .map_err(MysqlConnectionError::Io)?;
    output.flush().map_err(MysqlConnectionError::Io)?;

    let mut reader = PacketReader::with_max_allowed_packet(socket.clone(), max_allowed_packet);
    reader.set_sequence(1);
    let mut auth_payload = reader.read_packet()?;
    // Go's `clientConn.handshake`: the common header is parsed first, and a
    // response that set CLIENT_SSL while the server holds a TLS config is a
    // *truncated* SSLRequest -- the socket is upgraded and the client repeats a
    // full HandshakeResponse41 over the encrypted stream, so this connection
    // reads two response packets. Sequence numbering is continuous across the
    // upgrade, which is why every later reply sequence shifts by one.
    let mut reply_sequence = 2_u8;
    if let Some(tls) = tls {
        let wants_tls = parse_response_header(&auth_payload)
            .is_ok_and(|(header, _)| header.capability & CLIENT_SSL != 0);
        if wants_tls {
            socket
                .upgrade_to_tls(tls)
                .map_err(MysqlConnectionError::Io)?;
            auth_payload = reader.read_packet()?;
            reply_sequence = 3;
        }
    }
    let response = match parse_response(&auth_payload) {
        Ok(response) => response,
        Err(_error) => {
            // The handshake response never parsed, so Go's own `cc.user`
            // would still be its zero value: an empty username, exactly
            // like this.
            write_error(
                &mut output,
                reply_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message("", &peer_addr.ip().to_string(), &[]),
                true,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
    };
    // Handshake parsing is byte-authoritative, matching Go strings. The
    // configured account/session owners are UTF-8-native today, so conversion
    // is explicit at that boundary rather than silently replacing bytes in
    // the protocol parser.
    let response_user = response.user.to_string_lossy().into_owned();
    let response_db_name = response.db_name.to_string_lossy().into_owned();
    let capabilities = match negotiate_capabilities(response.capability, server_capabilities) {
        Ok(capabilities) => capabilities,
        Err(_error) => {
            write_error(
                &mut output,
                reply_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response_user, &peer_addr.ip().to_string(), &response.auth),
                response.capability & CLIENT_PROTOCOL_41 != 0,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
    };
    let protocol_41 = capabilities & CLIENT_PROTOCOL_41 != 0;
    if !protocol_41 {
        write_error(
            &mut output,
            reply_sequence,
            ER_ACCESS_DENIED_ERROR,
            *b"28000",
            access_denied_message(&response_user, &peer_addr.ip().to_string(), &response.auth),
            protocol_41,
        )?;
        return Ok(ConnectionReport {
            connection_id,
            queries: 0,
            commands: *commands,
            exit: ConnectionExit::AuthenticationRejected,
        });
    }
    // Go `checkAuthPlugin` (`server/conn.go` line 939): the ACCOUNT's
    // `mysql.user.plugin` -- not the plugin the client offered -- decides
    // what this connection speaks. An account with no plugin (or none at
    // all) is Go's "assuming MySQL Native Password".
    let account_plugin = users
        .auth_plugin_for(&response_user, &peer_addr.ip().to_string())
        .unwrap_or_else(|| AUTH_NATIVE_PASSWORD.to_owned());
    // Go switches when the server's advertised plugin differs from EITHER
    // the account's or the client's. This server always advertises
    // `mysql_native_password`, so that reduces to these two disjuncts.
    let (auth_response, response_sequence) = if capabilities & CLIENT_PLUGIN_AUTH != 0
        && (account_plugin != AUTH_NATIVE_PASSWORD
            || (!response.auth_plugin.is_empty()
                && response.auth_plugin.as_bytes() != AUTH_NATIVE_PASSWORD.as_bytes()))
    {
        let request = AuthSwitchRequest::new(&account_plugin, salt.to_vec())
            .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?;
        write_payload(&mut output, reply_sequence, &request.encode_payload())?;
        reader.set_sequence(reply_sequence + 1);
        (reader.read_packet()?, reply_sequence + 2)
    } else {
        (response.auth, reply_sequence)
    };
    // Go's `authSha`/`authSM3` (`server/conn.go` lines 761 and 799), which
    // are one function twice: TiDB implements NEITHER the cached fast path
    // NOR the RSA public-key exchange, so it unconditionally answers
    // `fastAuthFail` and reads the CLEARTEXT password the client then sends
    // over what it believes is a secure channel. The reply is NUL-trimmed
    // exactly as Go's `bytes.Trim(data, "\x00")` does.
    //
    // An EMPTY response skips the exchange entirely -- Go's own carve-out
    // for issue 40831, because asking a passwordless client for a full
    // authentication confuses it.
    let (auth_response, response_sequence) =
        if plugin_needs_cleartext(&account_plugin) && !auth_response.is_empty() {
            write_payload(
                &mut output,
                response_sequence,
                &[SHA2_FAST_AUTH_COMMAND, SHA2_FAST_AUTH_FAIL],
            )?;
            reader.set_sequence(response_sequence + 1);
            let cleartext = reader.read_packet()?;
            let start = cleartext
                .iter()
                .position(|byte| *byte != 0)
                .unwrap_or(cleartext.len());
            let end = cleartext
                .iter()
                .rposition(|byte| *byte != 0)
                .map_or(start, |last| last + 1);
            (cleartext[start..end].to_vec(), response_sequence + 2)
        } else {
            (auth_response, response_sequence)
        };
    // Go's two TLS rules both live behind this call; what the connection
    // owner knows and the account table does not is whether the socket was
    // upgraded. Every port here is TCP -- this server opens no Unix-domain
    // listener -- so the transport is exactly "TLS or not".
    let auth_result = users.authenticate(
        &response_user,
        &peer_addr.ip().to_string(),
        &salt,
        &auth_response,
        if socket.is_tls() {
            TransportKind::DirectTls
        } else {
            TransportKind::PlainTcp
        },
    );
    let identity = match auth_result {
        Ok(identity) => identity,
        // Go refuses this one BEFORE authenticating, with its own errno,
        // because it is a property of the connection and not of the account.
        Err(AuthenticationFailure::SecureTransportRequired) => {
            write_error(
                &mut output,
                response_sequence,
                ER_SECURE_TRANSPORT_REQUIRED,
                *b"HY000",
                SECURE_TRANSPORT_REQUIRED_MESSAGE,
                protocol_41,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
        Err(AuthenticationFailure::AccountLocked) => {
            write_error(
                &mut output,
                response_sequence,
                ER_ACCOUNT_HAS_BEEN_LOCKED,
                *b"HY000",
                account_locked_message(&response_user, &peer_addr.ip().to_string()),
                protocol_41,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
        // Go's 3955: the account auto-locked itself after too many
        // consecutive wrong passwords, and the message names the lock time
        // remaining.
        Err(AuthenticationFailure::AutoLocked(lockout)) => {
            write_error(
                &mut output,
                response_sequence,
                ER_USER_ACCESS_DENIED_BLOCKED_BY_PASSWORD_LOCK,
                *b"HY000",
                lockout.message(),
                protocol_41,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
        // Go's 1862: the password verified but has expired, and the server
        // is not admitting expired logins into a sandbox session.
        Err(AuthenticationFailure::PasswordExpired) => {
            write_error(
                &mut output,
                response_sequence,
                ER_MUST_CHANGE_PASSWORD_LOGIN,
                *b"HY000",
                tidb_session::privilege::PasswordExpiredLogin.message(),
                protocol_41,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
        Err(AuthenticationFailure::AccessDenied) => {
            write_error(
                &mut output,
                response_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response_user, &peer_addr.ip().to_string(), &auth_response),
                protocol_41,
            )?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
    };
    // The KILL handle is bound to this connection's own socket, so a `KILL`
    // reaching a connection that is idle between commands wakes it up rather
    // than leaving the row in the process list until the client happens to
    // send something.
    let close = ConnectionClose::with_socket(stream_for_close);
    let mut engine = match factory.open_session(SessionContext {
        connection_id,
        peer_addr,
        identity,
        cancellation,
        close: close.clone(),
    }) {
        Ok(session) => session,
        Err(error) => {
            write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::SessionRejected,
            });
        }
    };
    // Go's `openSessionAndDoAuth`: the handshake's initial database is applied
    // before the connection is reported ready, and a schema that does not
    // exist ends the connection with its own errno rather than the OK packet.
    if !response_db_name.is_empty() {
        if let Err(error) = engine.select_database(&response_db_name) {
            write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::SessionRejected,
            });
        }
    }
    write_ok(
        &mut output,
        response_sequence,
        engine.wire_status(),
        engine.warning_count(),
        protocol_41,
    )?;

    // The connection-lifetime half of the framing, and ALL of it: only the
    // capabilities negotiated at handshake live this long. The status word and
    // the warning count are per-statement facts Go re-reads off the session at
    // every `writeOkWith`/`writeEOF`, so neither is cached here -- caching the
    // status is precisely what reported `autocommit` to a client that had an
    // open transaction, and cost it the transaction's writes.
    let framing = WireFraming {
        deprecate_eof: capabilities & CLIENT_DEPRECATE_EOF != 0,
        protocol_41,
    };
    let mut queries = 0_u64;
    let mut prepared = PreparedStatementRegistry::default();
    loop {
        // A `KILL` that arrived while the previous command ran ends the
        // connection here, before it serves another one.
        if close.is_closed() {
            return Ok(ConnectionReport {
                connection_id,
                queries,
                commands: *commands,
                exit: ConnectionExit::Killed,
            });
        }
        reader.set_sequence(0);
        let payload = match reader.read_packet() {
            Ok(payload) => payload,
            // A `KILL` shuts the socket down to wake an idle connection, so
            // the read failure it causes -- including the clean end of stream
            // the shutdown produces -- is that kill, not a peer disconnect.
            Err(_) if close.is_closed() => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
                    commands: *commands,
                    exit: ConnectionExit::Killed,
                });
            }
            Err(PacketError::EndOfStream) => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
                    commands: *commands,
                    exit: ConnectionExit::PeerClosed,
                });
            }
            Err(error) => return Err(error.into()),
        };
        // Go `clientConn.dispatch` calls `initResultEncoder` here, once per
        // COMMAND: `@@character_set_results` can be `SET` between two
        // statements, and the second one has to go out in the new charset.
        // An unregistered name falls back to Go's unset state, which is what
        // `initResultEncoder` does when the read fails.
        let result_encoder =
            ResultEncoder::new(&engine.result_charset()).unwrap_or_else(|_| ResultEncoder::null());
        let command = match decode_command(&payload) {
            Ok(command) => command,
            Err(error) => {
                write_error(
                    &mut output,
                    1,
                    ER_UNKNOWN_COM_ERROR,
                    ER_UNKNOWN_COM_ERROR_STATE,
                    error.to_string(),
                    protocol_41,
                )?;
                continue;
            }
        };
        match command {
            Command::Quit => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
                    commands: *commands,
                    exit: ConnectionExit::Quit,
                });
            }
            Command::Ping => write_ok(
                &mut output,
                1,
                engine.wire_status(),
                engine.warning_count(),
                protocol_41,
            )?,
            Command::Query(bytes) => {
                commands.text_query_commands += 1;
                // `decode_command` has already trimmed exactly one terminal
                // NUL for issue 1989. Embedded and repeated NUL bytes remain
                // parser-visible here.
                let sql = match std::str::from_utf8(&bytes) {
                    Ok(sql) => sql,
                    Err(_) => {
                        write_error(
                            &mut output,
                            1,
                            ER_PARSE_ERROR,
                            *b"42000",
                            "COM_QUERY is not valid UTF-8",
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                // BEGIN/COMMIT/ROLLBACK update the session's transaction state and
                // answer with an OK packet carrying the transaction status, not a
                // result set; every other statement runs as an ordinary query.
                match engine.control_transaction(sql) {
                    Ok(Some(_)) => {
                        // The session has already applied the statement, so its
                        // own status is the answer -- there is no separate
                        // transaction flag for this packet to get wrong.
                        write_ok(
                            &mut output,
                            1,
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                        )?;
                        queries += 1;
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        write_query_error(&mut output, &error, protocol_41)?;
                        continue;
                    }
                }
                // A DML write or DDL answers with an OK packet carrying its
                // affected-row count, as MySQL does on the text protocol;
                // everything else runs as an ordinary result-set query.
                match engine.execute_write(sql) {
                    Ok(Some(outcome)) => {
                        write_affected_rows_ok(
                            &mut output,
                            1,
                            outcome.affected_rows,
                            outcome.last_insert_id,
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                        )?;
                        queries += 1;
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        write_query_error(&mut output, &error, protocol_41)?;
                        continue;
                    }
                }
                let mut result = match engine.execute(sql) {
                    Ok(result) => result,
                    Err(error) => {
                        write_query_error(&mut output, &error, protocol_41)?;
                        continue;
                    }
                };
                let write_result = {
                    let statement_options = framing.result_set(
                        result.wire_status(),
                        result.warning_count(),
                        result_encoder,
                    );
                    let mut sink = TcpResultSetSink::new(&mut output, 1);
                    write_connection_result_set_to_sink(
                        result.source(),
                        &mut sink,
                        statement_options,
                        RESULT_BATCH_SIZE,
                    )
                };
                match write_result {
                    Ok(_) => queries += 1,
                    Err(error) if !error.bytes_escaped => {
                        write_error(
                            &mut output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            error.message,
                            protocol_41,
                        )?;
                    }
                    Err(error) => return Err(MysqlConnectionError::PartialResult(error.message)),
                }
            }
            Command::StmtPrepare(bytes) => {
                commands.stmt_prepare_commands += 1;
                let sql = match std::str::from_utf8(&bytes) {
                    Ok(sql) => sql,
                    Err(_) => {
                        write_error(
                            &mut output,
                            1,
                            ER_PARSE_ERROR,
                            *b"42000",
                            "COM_STMT_PREPARE is not valid UTF-8",
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                // Transaction control is claimed before any planner sees the
                // statement, because a prepared `BEGIN` is a `BEGIN`: its
                // meaning is the connection's transaction, not a plan. Left to
                // the general path it would be *executed* by the prepare-time
                // column probe and then executed again at EXECUTE, and neither
                // run would reach `control_transaction` -- so the connection's
                // transaction never opens and every statement of the
                // transaction reads as an autocommit statement -- a point get
                // among them at `MaxUint64`, seeing whatever is committed at
                // the instant it runs rather than the transaction's snapshot.
                // The predicate
                // is the same one the text arm routes on, so a statement takes
                // the same route whichever protocol carried it.
                let statement = if classify_transaction_control(sql).is_some() {
                    PreparedStatement::TransactionControl(sql.to_owned())
                }
                // A read is admitted first so an existing prepared SELECT
                // keeps its exact error text; only a statement the read path
                // rejects is offered to the write planner.
                else {
                    match engine.prepare_point_read(sql) {
                        Ok(point_read) => PreparedStatement::PointRead(point_read),
                        Err(read_error) => match engine.prepare_write(sql) {
                            Ok(write) => PreparedStatement::Write(write),
                            // Any other statement takes the general path, which
                            // binds its markers and runs it through the session.
                            Err(_) => match engine.prepare_general(sql) {
                                Ok(general) => PreparedStatement::General(general),
                                Err(general_error) => {
                                    // The configured read's own message is the
                                    // more specific one when the general path
                                    // simply has no session behind it.
                                    let reported = if general_error
                                        .message
                                        .contains("does not support general prepared statements")
                                    {
                                        read_error
                                    } else {
                                        general_error
                                    };
                                    write_query_error(&mut output, &reported, protocol_41)?;
                                    continue;
                                }
                            },
                        },
                    }
                };
                let result_columns = statement.result_columns().to_vec();
                let parameter_count = statement.parameter_count();
                let statement_id = match prepared.insert(statement) {
                    Ok(statement_id) => statement_id,
                    Err(message) => {
                        write_error(
                            &mut output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            message,
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                let parameter_columns = vec![prepared_parameter_column(); parameter_count];
                // Go `conn_stmt.go:111`/`:129` frames the prepare metadata
                // with `cc.writeEOF(ctx, cc.ctx.Status())` -- the live word,
                // like every other EOF.
                let packets = match encode_prepared_statement_prepare_response(
                    statement_id,
                    &parameter_columns,
                    &result_columns,
                    framing.result_set(engine.wire_status(), 0, result_encoder),
                ) {
                    Ok(packets) => packets,
                    Err(error) => {
                        drop(prepared.remove(statement_id));
                        write_error(
                            &mut output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            error.to_string(),
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                let mut sink = TcpResultSetSink::new(&mut output, 1);
                for packet in packets {
                    sink.write_payload(&packet)
                        .map_err(|error| MysqlConnectionError::PartialResult(error.message))?;
                }
                commands.stmt_prepare_successes += 1;
            }
            Command::StmtExecute(bytes) => {
                commands.stmt_execute_commands += 1;
                let statement_id = match prepared_statement_id(&bytes) {
                    Ok(statement_id) => statement_id,
                    Err(message) => {
                        write_error(
                            &mut output,
                            1,
                            ER_WRONG_ARGUMENTS,
                            *b"HY000",
                            message,
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                let Some(statement) = prepared.get(statement_id) else {
                    write_unknown_statement(
                        &mut output,
                        statement_id,
                        "stmt_execute",
                        protocol_41,
                    )?;
                    continue;
                };
                let prepared_statement = statement.statement.clone();
                let previous_types = statement.parameter_types.clone();
                // The marker count is per statement: a point read owns one, a
                // write owns one per bound column plus its handle.
                let parameter_count = prepared_statement.parameter_count();
                // Go reads `stmt.BoundParams()` into `parseBinaryParams` and
                // then calls `stmt.Reset()` unconditionally -- on the decode
                // error path too (`pkg/server/conn_stmt.go:212-217`), so a
                // rejected execute still consumes the long data.
                let bound_params = prepared.bound_params(statement_id).to_vec();
                prepared.clear_bound_params(statement_id);
                // Go calls `stmt.Reset()` after parsing every execute packet,
                // successful or not. The retained cursor therefore closes
                // before a replacement execution starts, and a malformed
                // replacement cannot leave the old cursor fetchable.
                drop(prepared.take_cursor(statement_id));
                let execute = match decode_prepared_statement_execute_with_bound_params(
                    &bytes,
                    parameter_count,
                    previous_types.as_deref(),
                    &bound_params,
                ) {
                    Ok(execute) => execute,
                    Err(error) => {
                        write_error(
                            &mut output,
                            1,
                            ER_WRONG_ARGUMENTS,
                            *b"HY000",
                            error.to_string(),
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                if execute.statement_id != statement_id {
                    write_error(
                        &mut output,
                        1,
                        ER_WRONG_ARGUMENTS,
                        *b"HY000",
                        "prepared statement ID changed during decode",
                        protocol_41,
                    )?;
                    continue;
                }
                if let PreparedParameterTypes::New(types) = &execute.parameter_types {
                    prepared.remember_parameter_types(statement_id, types);
                }
                let values = execute.values;
                match prepared_statement {
                    // The same two lines the text arm runs, so the transaction
                    // a prepared BEGIN opens, and the status flag the client
                    // reads back, are the text protocol's own.
                    PreparedStatement::TransactionControl(sql) => {
                        match engine.control_transaction(&sql) {
                            Ok(Some(_)) => {
                                write_ok(
                                    &mut output,
                                    1,
                                    engine.wire_status(),
                                    engine.warning_count(),
                                    protocol_41,
                                )?;
                                queries += 1;
                                commands.stmt_execute_successes += 1;
                            }
                            // A statement PREPARE classified as transaction
                            // control that the session then declines is a
                            // disagreement between the two, not a client
                            // error: report it rather than answering OK.
                            Ok(None) => write_error(
                                &mut output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                "prepared transaction control was not applied",
                                protocol_41,
                            )?,
                            Err(error) => write_query_error(&mut output, &error, protocol_41)?,
                        }
                    }
                    PreparedStatement::PointRead(point_read) => {
                        // A point read binds a signed-integer clustered handle; a
                        // string parameter has no place there.
                        let parameters = match point_read_integer_parameters(values) {
                            Ok(parameters) => parameters,
                            Err(message) => {
                                write_error(
                                    &mut output,
                                    1,
                                    ER_UNKNOWN_ERROR,
                                    *b"HY000",
                                    message,
                                    protocol_41,
                                )?;
                                continue;
                            }
                        };
                        let mut result =
                            match engine.execute_prepared_point_read(&point_read, &parameters) {
                                Ok(result) => result,
                                Err(error) => {
                                    write_query_error(&mut output, &error, protocol_41)?;
                                    continue;
                                }
                            };
                        let write_result = {
                            let statement_options = framing.result_set(
                                result.wire_status(),
                                result.warning_count(),
                                result_encoder,
                            );
                            let mut sink = TcpResultSetSink::new(&mut output, 1);
                            write_connection_binary_result_set_to_sink(
                                result.source(),
                                &mut sink,
                                statement_options,
                                RESULT_BATCH_SIZE,
                            )
                        };
                        match write_result {
                            Ok(_) => {
                                queries += 1;
                                commands.stmt_execute_successes += 1;
                            }
                            Err(error) if !error.bytes_escaped => {
                                write_error(
                                    &mut output,
                                    1,
                                    ER_UNKNOWN_ERROR,
                                    *b"HY000",
                                    error.message,
                                    protocol_41,
                                )?;
                            }
                            Err(error) => {
                                return Err(MysqlConnectionError::PartialResult(error.message))
                            }
                        }
                    }
                    PreparedStatement::General(general) => {
                        // Go's read-only cursor: the execute materializes the
                        // rows, holds them on the statement, and answers with
                        // only the column definitions plus an EOF whose
                        // status advertises the open cursor; the rows travel
                        // later through COM_STMT_FETCH.
                        if execute.cursor_flags & tidb_protocol::CURSOR_TYPE_READ_ONLY != 0 {
                            let mut write_outcome = None;
                            match engine.execute_general(&general, &values) {
                                Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                                    let warnings = result.warning_count();
                                    let status = result.wire_status();
                                    let authority = match result.take_cursor_materialization() {
                                        Some(authority) => authority,
                                        None => {
                                            let _ = result.source().close();
                                            drop(result);
                                            write_error(
                                                &mut output,
                                                1,
                                                ER_UNKNOWN_ERROR,
                                                *b"HY000",
                                                "prepared cursor result is missing its materialization authority",
                                                protocol_41,
                                            )?;
                                            continue;
                                        }
                                    };
                                    let cursor =
                                        match CursorState::materialize(&mut result, authority) {
                                            Ok(cursor) => cursor,
                                            Err(error) => {
                                                drop(result);
                                                write_query_error(
                                                    &mut output,
                                                    &error,
                                                    protocol_41,
                                                )?;
                                                continue;
                                            }
                                        };
                                    let columns = cursor.columns().to_vec();
                                    drop(result);
                                    let cursor_options = framing.result_set(
                                        status.with(SERVER_STATUS_CURSOR_EXISTS),
                                        warnings,
                                        result_encoder,
                                    );
                                    let mut stream = match tidb_protocol::BinaryResultSetStream::new(
                                        columns.clone(),
                                        cursor_options,
                                    ) {
                                        Ok(stream) => stream,
                                        Err(error) => {
                                            write_error(
                                                &mut output,
                                                1,
                                                ER_UNKNOWN_ERROR,
                                                *b"HY000",
                                                error.to_string(),
                                                protocol_41,
                                            )?;
                                            continue;
                                        }
                                    };
                                    let mut sequence = 1;
                                    let metadata = match stream.metadata_packets() {
                                        Ok(metadata) => metadata,
                                        Err(error) => {
                                            write_error(
                                                &mut output,
                                                1,
                                                ER_UNKNOWN_ERROR,
                                                *b"HY000",
                                                error.to_string(),
                                                protocol_41,
                                            )?;
                                            continue;
                                        }
                                    };
                                    for packet in metadata {
                                        write_packet_to(&mut output, sequence, &packet)?;
                                        sequence += 1;
                                    }
                                    // The deprecate-EOF mode still terminates
                                    // the metadata with an OK-as-EOF here,
                                    // because no row packets follow.
                                    write_eof_or_ok(&mut output, sequence, cursor_options)?;
                                    drop(prepared.open_cursor(statement_id, cursor));
                                    queries += 1;
                                    commands.stmt_execute_successes += 1;
                                }
                                // Go clears the cursor bit when the statement
                                // produced no result set and answers a plain
                                // OK. The answer is written after the match
                                // because its warning count is read off the
                                // session, whose borrow the result arm holds
                                // for as long as the match scrutinee lives.
                                Ok(GeneralExecuteOutcome::Write(outcome)) => {
                                    write_outcome = Some(outcome);
                                }
                                Err(error) => {
                                    write_query_error(&mut output, &error, protocol_41)?;
                                }
                            }
                            if let Some(outcome) = write_outcome {
                                write_affected_rows_ok(
                                    &mut output,
                                    1,
                                    outcome.affected_rows,
                                    outcome.last_insert_id,
                                    engine.wire_status(),
                                    engine.warning_count(),
                                    protocol_41,
                                )?;
                                queries += 1;
                                commands.stmt_execute_successes += 1;
                            }
                            continue;
                        }
                        let mut write_outcome = None;
                        match engine.execute_general(&general, &values) {
                            Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                                let write_result = {
                                    let statement_options = framing.result_set(
                                        result.wire_status(),
                                        result.warning_count(),
                                        result_encoder,
                                    );
                                    let mut sink = TcpResultSetSink::new(&mut output, 1);
                                    write_connection_binary_result_set_to_sink(
                                        result.source(),
                                        &mut sink,
                                        statement_options,
                                        RESULT_BATCH_SIZE,
                                    )
                                };
                                match write_result {
                                    Ok(_) => {
                                        queries += 1;
                                        commands.stmt_execute_successes += 1;
                                    }
                                    Err(error) if !error.bytes_escaped => {
                                        write_error(
                                            &mut output,
                                            1,
                                            ER_UNKNOWN_ERROR,
                                            *b"HY000",
                                            error.message,
                                            protocol_41,
                                        )?;
                                    }
                                    Err(error) => {
                                        return Err(MysqlConnectionError::PartialResult(
                                            error.message,
                                        ))
                                    }
                                }
                            }
                            // Answered after the match: see the cursor arm
                            // above for why the session cannot be read while
                            // the scrutinee is alive.
                            Ok(GeneralExecuteOutcome::Write(outcome)) => {
                                write_outcome = Some(outcome);
                            }
                            Err(error) => {
                                write_query_error(&mut output, &error, protocol_41)?;
                            }
                        }
                        if let Some(outcome) = write_outcome {
                            write_affected_rows_ok(
                                &mut output,
                                1,
                                outcome.affected_rows,
                                outcome.last_insert_id,
                                engine.wire_status(),
                                engine.warning_count(),
                                protocol_41,
                            )?;
                            queries += 1;
                            commands.stmt_execute_successes += 1;
                        }
                    }
                    PreparedStatement::Write(write) => {
                        // A write answers with one OK packet and never a result
                        // set. Affected rows reach the client only after the
                        // transaction committed determinately; every other
                        // terminal state arrived here as an error.
                        match engine.execute_prepared_write(&write, &write_bind_parameters(values))
                        {
                            Ok(outcome) => {
                                write_affected_rows_ok(
                                    &mut output,
                                    1,
                                    outcome.affected_rows,
                                    outcome.last_insert_id,
                                    engine.wire_status(),
                                    engine.warning_count(),
                                    protocol_41,
                                )?;
                                queries += 1;
                                commands.stmt_execute_successes += 1;
                            }
                            Err(error) => {
                                write_query_error(&mut output, &error, protocol_41)?;
                            }
                        }
                    }
                }
            }
            Command::StmtSendLongData(bytes) => {
                commands.stmt_send_long_data_commands += 1;
                // Go's `handleStmtSendLongData` returns nil on success, and a
                // nil return from `clientConn.dispatch` writes NO packet at
                // all (`pkg/server/conn.go:1578-1579`): the client is not
                // reading, so any reply here would be consumed as the answer
                // to the NEXT command. Its error returns, by contrast, DO
                // reach the wire -- `dispatch`'s caller ends the failed
                // command with `cc.writeError(ctx, err)`
                // (`pkg/server/conn.go:1338`) -- so the two error shapes below
                // are packets exactly as they are in Go.
                match decode_prepared_statement_send_long_data(&bytes) {
                    Ok(long_data) => {
                        let statement_id = long_data.statement_id;
                        match prepared.append_param(
                            statement_id,
                            usize::from(long_data.parameter_id),
                            &long_data.chunk,
                        ) {
                            Ok(()) => {}
                            Err(AppendParamError::UnknownStatement) => {
                                write_unknown_statement(
                                    &mut output,
                                    statement_id,
                                    "stmt_send_longdata",
                                    protocol_41,
                                )?;
                            }
                            // Go `AppendParam`'s own
                            // `ErrWrongArguments("stmt_send_longdata")`.
                            Err(AppendParamError::ParameterOutOfRange) => write_error(
                                &mut output,
                                1,
                                ER_WRONG_ARGUMENTS,
                                *b"HY000",
                                "Incorrect arguments to stmt_send_longdata",
                                protocol_41,
                            )?,
                            Err(AppendParamError::TooLarge) => write_error(
                                &mut output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                "COM_STMT_SEND_LONG_DATA exceeds the maximum packet size",
                                protocol_41,
                            )?,
                        }
                    }
                    Err(error) => write_error(
                        &mut output,
                        1,
                        ER_WRONG_ARGUMENTS,
                        *b"HY000",
                        error.to_string(),
                        protocol_41,
                    )?,
                }
            }
            Command::StmtClose(bytes) => {
                commands.stmt_close_commands += 1;
                if let Ok(statement_id) = decode_prepared_statement_close(&bytes) {
                    drop(prepared.remove(statement_id));
                }
            }
            Command::StmtReset(bytes) => {
                commands.stmt_reset_commands += 1;
                match decode_prepared_statement_close(&bytes) {
                    // The payload is the same four-byte statement id the
                    // close command carries.
                    Ok(statement_id) => match prepared.reset(statement_id) {
                        Ok(cursor) => {
                            drop(cursor);
                            // COM_STMT_RESET runs no statement, so like Go's
                            // `writeOK` here it reports the buffer as it stands.
                            write_affected_rows_ok(
                                &mut output,
                                1,
                                0,
                                0,
                                engine.wire_status(),
                                engine.warning_count(),
                                protocol_41,
                            )?;
                        }
                        Err(()) => write_unknown_statement(
                            &mut output,
                            statement_id,
                            "stmt_reset",
                            protocol_41,
                        )?,
                    },
                    Err(error) => {
                        write_error(
                            &mut output,
                            1,
                            ER_WRONG_ARGUMENTS,
                            *b"HY000",
                            error.to_string(),
                            protocol_41,
                        )?;
                    }
                }
            }
            Command::StmtFetch(bytes) => {
                commands.stmt_fetch_commands += 1;
                let (statement_id, fetch_size) = match decode_prepared_statement_fetch(&bytes) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        write_error(
                            &mut output,
                            1,
                            ER_WRONG_ARGUMENTS,
                            *b"HY000",
                            error.to_string(),
                            protocol_41,
                        )?;
                        continue;
                    }
                };
                if prepared.get(statement_id).is_none() {
                    write_unknown_statement(&mut output, statement_id, "stmt_fetch", protocol_41)?;
                    continue;
                }
                let Some(mut cursor) = prepared.take_cursor(statement_id) else {
                    // Go `ErrSpCursorNotOpen` (1326).
                    write_error(
                        &mut output,
                        1,
                        1326,
                        *b"24000",
                        "Cursor is not open",
                        protocol_41,
                    )?;
                    continue;
                };
                // Go sends up to fetch_size binary rows and an EOF; when the
                // iterator is exhausted the EOF drops the cursor bit, sets
                // ServerStatusLastRowSend, and the statement resets.
                let (row_count, exhausted) = cursor.fetch_plan(fetch_size);
                let status = if exhausted {
                    engine
                        .wire_status()
                        .with(SERVER_STATUS_LAST_ROW_SEND)
                        .without(SERVER_STATUS_CURSOR_EXISTS)
                } else {
                    engine.wire_status().with(SERVER_STATUS_CURSOR_EXISTS)
                };
                match cursor.write_fetch(
                    &mut output,
                    row_count,
                    // Go clears the statement warning buffer before FETCH,
                    // then writes the live transaction status.
                    framing.result_set(status, 0, result_encoder),
                ) {
                    Ok(()) => {
                        if !exhausted {
                            drop(prepared.open_cursor(statement_id, cursor));
                        }
                        commands.stmt_fetch_successes += 1;
                    }
                    Err(CursorFetchError::Protocol { message, sequence }) => {
                        write_error(
                            &mut output,
                            sequence,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            message,
                            protocol_41,
                        )?;
                    }
                    Err(CursorFetchError::Transport(error)) => return Err(error),
                }
            }
            // The `mysql` client implements `USE db` as COM_INIT_DB, not as a
            // query, so this is the command an interactive `USE` arrives on.
            Command::InitDb(name) => {
                let name = String::from_utf8_lossy(&name).into_owned();
                match engine.select_database(&name) {
                    Ok(()) => write_ok(
                        &mut output,
                        1,
                        engine.wire_status(),
                        engine.warning_count(),
                        protocol_41,
                    )?,
                    Err(error) => write_query_error(&mut output, &error, protocol_41)?,
                }
            }
            Command::FieldList(_)
            | Command::SetOption(_)
            | Command::ResetConnection
            | Command::Unknown { .. } => write_error(
                &mut output,
                1,
                ER_UNKNOWN_COM_ERROR,
                ER_UNKNOWN_COM_ERROR_STATE,
                "command is not supported by the read-only Rust SQL node",
                protocol_41,
            )?,
        }
    }
}

/// Wire-level proof that a real `COM_STMT_EXECUTE` binary payload — not a
/// hand-built [`PreparedBindValue`] — reaches the configured write path
/// correctly typed for every widened scalar family (`2d10dcd232`'s second
/// documented gap: this exact seam, `decode_prepared_statement_execute` ->
/// `write_bind_parameters` -> `tidb-planner` bind -> `tidb-exec` plan, had
/// never been exercised end to end). No cluster is involved: `plan_insert`/
/// `plan_update` are the same pure, storage-free planning step production
/// runs before ever opening a real transaction.
#[cfg(test)]
mod prepared_execute_wire_tests {
    use tidb_exec::real_tikv_dml::{plan_insert, plan_update, ConfiguredWritePlan};
    use tidb_planner::configured_catalog::ConfiguredCatalog;
    use tidb_planner::prepared_dml::{lower_prepared_write, ConfiguredPreparedWrite};
    use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
    use tidb_protocol::decode_prepared_statement_execute;

    use super::write_bind_parameters;

    /// Builds a real `COM_STMT_EXECUTE` payload binding `params` as
    /// `(type_code, flag, value_bytes)` triples, or `None` for an explicit
    /// SQL `NULL` (a set bitmap bit with no value bytes, exactly as the wire
    /// format requires).
    fn execute_packet(params: &[Option<(u8, u8, Vec<u8>)>]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&7_u32.to_le_bytes()); // statement ID
        packet.push(0); // no cursor
        packet.extend_from_slice(&1_u32.to_le_bytes()); // iteration count
        let mut null_bitmap = vec![0u8; params.len().div_ceil(8)];
        for (index, param) in params.iter().enumerate() {
            if param.is_none() {
                null_bitmap[index / 8] |= 1 << (index % 8);
            }
        }
        packet.extend_from_slice(&null_bitmap);
        packet.push(1); // new parameter types follow
        for param in params {
            let (type_code, flag) = param
                .as_ref()
                .map_or((0x08, 0), |(type_code, flag, _)| (*type_code, *flag));
            packet.push(type_code);
            packet.push(flag);
        }
        for (_, _, value_bytes) in params.iter().flatten() {
            packet.extend_from_slice(value_bytes);
        }
        packet
    }

    /// Decodes `params` through the real binary-protocol decoder, then
    /// through this module's own wire-to-planner conversion — the identical
    /// two calls production makes for `Command::StmtExecute`.
    fn decode_and_bind(
        params: &[Option<(u8, u8, Vec<u8>)>],
    ) -> Vec<tidb_planner::prepared_dml::PreparedBindValue> {
        let decoded =
            decode_prepared_statement_execute(&execute_packet(params), params.len(), None)
                .expect("a well-formed execute packet decodes");
        write_bind_parameters(decoded.values)
    }

    fn widened_table(column: ConfiguredColumn) -> ConfiguredTable {
        ConfiguredTable::new(
            "wire",
            "t",
            300,
            [ConfiguredColumn::clustered_primary_key("id", 1), column],
        )
    }

    #[test]
    fn an_unsigned_bigint_wire_parameter_binds_as_uint_not_a_truncated_signed_cast() {
        // MYSQL_TYPE_LONGLONG with the unsigned flag set, carrying u64::MAX:
        // truncating through `as i64` (the bug this proof exists to catch)
        // would silently store -1 instead of refusing or keeping the value.
        let table = widened_table(ConfiguredColumn::stored_unsigned_bigint_not_null("v", 2));
        let catalog = ConfiguredCatalog::new([table.clone()]).expect("catalog must validate");
        let bound = lower_prepared_write(
            &tidb_parser::parse("INSERT INTO wire.t (id, v) VALUES (?, ?)")
                .expect("SQL must parse"),
            &catalog,
        )
        .expect("prepared write must lower")
        .bind(&decode_and_bind(&[
            Some((0x08, 0, 1_i64.to_le_bytes().to_vec())),
            Some((0x08, 0x80, u64::MAX.to_le_bytes().to_vec())),
        ]))
        .expect("bind must succeed");
        let ConfiguredPreparedWrite::InsertRows { table, rows } = bound else {
            panic!("expected an INSERT command");
        };
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_insert(&table, &rows, 0).expect("insert must plan")
        else {
            panic!("an INSERT always publishes");
        };
        assert_eq!(
            tidb_tablecodec::decode_table_row_to_map(
                mutations[0].value(),
                &std::collections::BTreeMap::from([(
                    2,
                    table.columns()[1].scalar_type().chunk_field_type()
                )]),
                None
            )
            .expect("row must decode")
            .remove(&2),
            Some(tidb_datatype::Datum::UInt(u64::MAX))
        );
    }

    #[test]
    fn a_double_wire_parameter_binds_as_the_real_ieee754_value_not_a_stringified_decimal() {
        let table = widened_table(ConfiguredColumn::stored_double_not_null("v", 2));
        let catalog = ConfiguredCatalog::new([table.clone()]).expect("catalog must validate");
        let bound = lower_prepared_write(
            &tidb_parser::parse("INSERT INTO wire.t (id, v) VALUES (?, ?)")
                .expect("SQL must parse"),
            &catalog,
        )
        .expect("prepared write must lower")
        .bind(&decode_and_bind(&[
            Some((0x08, 0, 1_i64.to_le_bytes().to_vec())),
            Some((0x05, 0, 2.5_f64.to_bits().to_le_bytes().to_vec())),
        ]))
        .expect("bind must succeed");
        let ConfiguredPreparedWrite::InsertRows { table, rows } = bound else {
            panic!("expected an INSERT command");
        };
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_insert(&table, &rows, 0).expect("insert must plan")
        else {
            panic!("an INSERT always publishes");
        };
        assert_eq!(
            tidb_tablecodec::decode_table_row_to_map(
                mutations[0].value(),
                &std::collections::BTreeMap::from([(
                    2,
                    table.columns()[1].scalar_type().chunk_field_type()
                )]),
                None
            )
            .expect("row must decode")
            .remove(&2),
            Some(tidb_datatype::Datum::Real(2.5))
        );
    }

    #[test]
    fn a_wire_null_bitmap_bit_binds_as_sql_null_into_a_prepared_update() {
        // The bug this proof exists to catch: mapping a wire NULL to empty
        // bytes would bind `''`/a type mismatch instead of NULL.
        let table =
            widened_table(ConfiguredColumn::stored_varchar_not_null("v", 2, 8, false).nullable());
        let catalog = ConfiguredCatalog::new([table.clone()]).expect("catalog must validate");

        // Seed the row with a non-NULL value through the same wire path.
        let seed = lower_prepared_write(
            &tidb_parser::parse("INSERT INTO wire.t (id, v) VALUES (?, ?)")
                .expect("SQL must parse"),
            &catalog,
        )
        .expect("prepared write must lower")
        .bind(&decode_and_bind(&[
            Some((0x08, 0, 1_i64.to_le_bytes().to_vec())),
            Some((0x0f, 0, {
                let mut bytes = vec![5u8];
                bytes.extend_from_slice(b"hello");
                bytes
            })),
        ]))
        .expect("bind must succeed");
        let ConfiguredPreparedWrite::InsertRows {
            table: seeded_table,
            rows,
        } = seed
        else {
            panic!("expected an INSERT command");
        };
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_insert(&seeded_table, &rows, 0).expect("seed insert must plan")
        else {
            panic!("an INSERT always publishes");
        };
        let stored = mutations[0].value().to_vec();

        // A prepared UPDATE binding a wire NULL parameter.
        let bound = lower_prepared_write(
            &tidb_parser::parse("UPDATE wire.t SET v = ? WHERE id = ?").expect("SQL must parse"),
            &catalog,
        )
        .expect("prepared write must lower")
        .bind(&decode_and_bind(&[
            None,
            Some((0x08, 0, 1_i64.to_le_bytes().to_vec())),
        ]))
        .expect("bind must succeed");
        let ConfiguredPreparedWrite::UpdatePoint {
            handle,
            column_index,
            assignment,
            ..
        } = bound
        else {
            panic!("expected an UPDATE command");
        };
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_update(&table, handle, column_index, assignment, Some(&stored), 0)
                .expect("update must plan")
        else {
            panic!("a changed row publishes");
        };
        assert_eq!(
            tidb_tablecodec::decode_table_row_to_map(
                mutations[0].value(),
                &std::collections::BTreeMap::from([(
                    2,
                    table.columns()[1].scalar_type().chunk_field_type()
                )]),
                None
            )
            .expect("row must decode")
            .remove(&2),
            Some(tidb_datatype::Datum::Null)
        );
    }
}

#[cfg(test)]
mod undetermined_tests {
    use crate::sql_node::{SqlQueryError, RESULT_UNDETERMINED_MESSAGE};

    /// Go `pkg/server/conn.go:1288-1291` returns from the command loop for an
    /// undetermined result, writing no ERR packet: the client must not be able
    /// to read the failure as a verdict and retry a commit that may have
    /// landed.
    #[test]
    fn an_undetermined_result_is_never_answered_with_an_err_packet() {
        let undetermined = SqlQueryError::result_undetermined();
        assert!(undetermined.is_result_undetermined());
        assert_eq!(undetermined.message, RESULT_UNDETERMINED_MESSAGE);

        // Every ordinary failure still gets its ERR packet; only this one does
        // not, and the refusal lives in the single shared writer so no answer
        // path can forget it.
        let ordinary = SqlQueryError::unknown("configured write did not commit: RolledBack");
        assert!(!ordinary.is_result_undetermined());
        let same_code_different_text = SqlQueryError::unknown("execution result undetermine");
        assert!(!same_code_different_text.is_result_undetermined());
        let different_code = SqlQueryError::new(1062, *b"23000", RESULT_UNDETERMINED_MESSAGE);
        assert!(!different_code.is_result_undetermined());
    }
}
