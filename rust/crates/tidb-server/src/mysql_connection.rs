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

use tidb_protocol::{
    decode_command, decode_prepared_statement_close, decode_prepared_statement_execute,
    decode_prepared_statement_fetch, encode_error_packet, encode_ok_packet,
    encode_prepared_statement_prepare_response, ColumnInfo, Command, ErrorPacket, OkPacket,
    PacketError, PacketReader, PacketWriter, PreparedParameterType, PreparedParameterTypes,
    PreparedValue, ResultSetOptions, BINARY_DEFAULT_COLLATION_ID, MYSQL_TYPE_LONGLONG,
};

use crate::auth_exchange::AuthSwitchRequest;
use crate::configured_user_store::{AuthenticationFailure, ConfiguredUserStore};
use crate::connection_resultset::{
    write_connection_binary_result_set_to_sink, write_connection_result_set_to_sink,
};
use crate::handshake::{
    negotiate_capabilities, parse_response, InitialHandshake, AUTH_NATIVE_PASSWORD,
    CLIENT_CONNECT_ATTRS, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
    DEFAULT_COLLATION_ID, SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS,
};
use crate::native_password::generate_handshake_salt;
use crate::resultset_source::ResultSetSource;
use crate::resultset_writer::{ResultSetSink, SinkWriteError};
use crate::sql_node::{
    ConnectionCancellation, ConnectionClose, ConnectionTracker, GeneralExecuteOutcome,
    PreparedStatement, QuerySession, QuerySessionFactory, SessionContext, SqlQueryError,
};
use tidb_planner::prepared_dml::PreparedBindValue;

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
const SERVER_CAPABILITIES: u32 = CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_DEPRECATE_EOF;
const ER_ACCESS_DENIED_ERROR: u16 = 1045;
const ER_UNKNOWN_COM_ERROR: u16 = 1047;
/// Go's `mysql.ErrAccountHasBeenLocked` (`pkg/errno/errcode.go`): the login
/// errno an `ACCOUNT LOCK`'d (or ROLE) account gets, distinct from the
/// generic access-denied a bad password or unknown user gets.
const ER_ACCOUNT_HAS_BEEN_LOCKED: u16 = 3118;
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
const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
const RESULT_BATCH_SIZE: usize = 128;

#[derive(Clone, Debug)]
struct ConnectionPreparedStatement {
    statement: PreparedStatement,
    parameter_types: Option<Vec<PreparedParameterType>>,
    /// An open read-only cursor: the materialized result a cursor-mode
    /// execute stored for later `COM_STMT_FETCH` commands, with the columns
    /// it advertises and the next unread row. Go holds the same thing on the
    /// statement as a row container.
    cursor: Option<CursorState>,
}

#[derive(Clone, Debug)]
struct CursorState {
    columns: Vec<tidb_protocol::ColumnInfo>,
    rows: Vec<Vec<tidb_datatype::Datum>>,
    next_row: usize,
}

#[derive(Debug)]
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

    fn remove(&mut self, statement_id: u32) {
        self.statements.remove(&statement_id);
    }

    /// Go `stmt.Reset`: returns the statement to the state it had right after
    /// PREPARE. The cursor and the `SEND_LONG_DATA` buffers Go also clears do
    /// not exist here, so the remembered parameter-type vector -- the only
    /// state an execute leaves behind -- is what a reset drops.
    fn reset(&mut self, statement_id: u32) -> bool {
        match self.statements.get_mut(&statement_id) {
            Some(statement) => {
                statement.parameter_types = None;
                // Go's stmt.Reset closes the open cursor too.
                statement.cursor = None;
                true
            }
            None => false,
        }
    }

    fn open_cursor(&mut self, statement_id: u32, state: CursorState) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.cursor = Some(state);
        }
    }

    fn cursor_mut(&mut self, statement_id: u32) -> Option<&mut CursorState> {
        self.statements
            .get_mut(&statement_id)
            .and_then(|statement| statement.cursor.as_mut())
    }

    fn close_cursor(&mut self, statement_id: u32) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.cursor = None;
        }
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
        }
    }
}

impl std::error::Error for MysqlConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Packet(error) => Some(error),
            Self::Handshake(_) | Self::PartialResult(_) => None,
        }
    }
}

impl From<PacketError> for MysqlConnectionError {
    fn from(error: PacketError) -> Self {
        Self::Packet(error)
    }
}

/// Serves one accepted socket through handshake, authentication, commands,
/// result/error writes, and exactly-once connection cleanup.
pub fn serve_mysql_connection<F: QuerySessionFactory>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
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

fn serve_connection_inner<F: QuerySessionFactory>(
    stream: TcpStream,
    identity: AcceptedConnectionIdentity,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    max_allowed_packet: usize,
    commands: &mut ConnectionCommandCounts,
) -> Result<ConnectionReport, MysqlConnectionError> {
    let AcceptedConnectionIdentity {
        connection_id,
        peer_addr,
    } = identity;
    stream.set_nodelay(true).map_err(MysqlConnectionError::Io)?;
    let mut output = stream.try_clone().map_err(MysqlConnectionError::Io)?;
    let stream_for_close = stream.try_clone().map_err(MysqlConnectionError::Io)?;
    let salt = generate_handshake_salt();
    let handshake = InitialHandshake {
        connection_id: u32::try_from(connection_id).unwrap_or(u32::MAX),
        salt: salt.to_vec(),
        capability: SERVER_CAPABILITIES,
        collation: DEFAULT_COLLATION_ID,
        status_flags: SERVER_STATUS_AUTOCOMMIT,
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

    let mut reader = PacketReader::with_max_allowed_packet(stream, max_allowed_packet);
    reader.set_sequence(1);
    let auth_payload = reader.read_packet()?;
    let response = match parse_response(&auth_payload) {
        Ok(response) => response,
        Err(_error) => {
            // The handshake response never parsed, so Go's own `cc.user`
            // would still be its zero value: an empty username, exactly
            // like this.
            write_error(
                &mut output,
                2,
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
    let capabilities = match negotiate_capabilities(response.capability, SERVER_CAPABILITIES) {
        Ok(capabilities) => capabilities,
        Err(_error) => {
            write_error(
                &mut output,
                2,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response.user, &peer_addr.ip().to_string(), &response.auth),
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
            2,
            ER_ACCESS_DENIED_ERROR,
            *b"28000",
            access_denied_message(&response.user, &peer_addr.ip().to_string(), &response.auth),
            protocol_41,
        )?;
        return Ok(ConnectionReport {
            connection_id,
            queries: 0,
            commands: *commands,
            exit: ConnectionExit::AuthenticationRejected,
        });
    }
    let (auth_response, response_sequence) = if capabilities & CLIENT_PLUGIN_AUTH != 0
        && !response.auth_plugin.is_empty()
        && response.auth_plugin != AUTH_NATIVE_PASSWORD
    {
        let request = AuthSwitchRequest::new(AUTH_NATIVE_PASSWORD, salt.to_vec())
            .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?;
        write_payload(&mut output, 2, &request.encode_payload())?;
        reader.set_sequence(3);
        (reader.read_packet()?, 4)
    } else {
        (response.auth, 2)
    };
    let auth_result = users.authenticate_native(
        &response.user,
        &peer_addr.ip().to_string(),
        &salt,
        &auth_response,
    );
    let identity = match auth_result {
        Ok(identity) => identity,
        Err(AuthenticationFailure::AccountLocked) => {
            write_error(
                &mut output,
                response_sequence,
                ER_ACCOUNT_HAS_BEEN_LOCKED,
                *b"HY000",
                account_locked_message(&response.user, &peer_addr.ip().to_string()),
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
                access_denied_message(&response.user, &peer_addr.ip().to_string(), &auth_response),
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
    write_ok(&mut output, response_sequence, protocol_41)?;

    let options = ResultSetOptions {
        status_flags: SERVER_STATUS_AUTOCOMMIT,
        warnings: 0,
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
        let command = match decode_command(&payload) {
            Ok(command) => command,
            Err(error) => {
                write_error(
                    &mut output,
                    1,
                    ER_UNKNOWN_COM_ERROR,
                    *b"HY000",
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
            Command::Ping => write_ok(&mut output, 1, protocol_41)?,
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
                    Ok(Some(in_transaction)) => {
                        write_transaction_control_ok(&mut output, 1, in_transaction, protocol_41)?;
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
                    let mut sink = TcpResultSetSink::new(&mut output, 1);
                    write_connection_result_set_to_sink(
                        result.source(),
                        &mut sink,
                        options,
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
                // A read is admitted first so an existing prepared SELECT
                // keeps its exact error text; only a statement the read path
                // rejects is offered to the write planner.
                let statement = match engine.prepare_point_read(sql) {
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
                let packets = match encode_prepared_statement_prepare_response(
                    statement_id,
                    &parameter_columns,
                    &result_columns,
                    options,
                ) {
                    Ok(packets) => packets,
                    Err(error) => {
                        prepared.remove(statement_id);
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
                    write_unknown_statement(&mut output, statement_id, protocol_41)?;
                    continue;
                };
                let prepared_statement = statement.statement.clone();
                let previous_types = statement.parameter_types.clone();
                // The marker count is per statement: a point read owns one, a
                // write owns one per bound column plus its handle.
                let parameter_count = prepared_statement.parameter_count();
                let execute = match decode_prepared_statement_execute(
                    &bytes,
                    parameter_count,
                    previous_types.as_deref(),
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
                            let mut sink = TcpResultSetSink::new(&mut output, 1);
                            write_connection_binary_result_set_to_sink(
                                result.source(),
                                &mut sink,
                                options,
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
                            match engine.execute_general(&general, &values) {
                                Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                                    let columns = match result.source().columns() {
                                        Ok(columns) => columns,
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
                                    let rows = match drain_result_rows(&mut result) {
                                        Ok(rows) => rows,
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
                                    drop(result);
                                    let cursor_options = ResultSetOptions {
                                        status_flags: options.status_flags
                                            | SERVER_STATUS_CURSOR_EXISTS,
                                        ..options
                                    };
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
                                    prepared.open_cursor(
                                        statement_id,
                                        CursorState {
                                            columns,
                                            rows,
                                            next_row: 0,
                                        },
                                    );
                                    queries += 1;
                                    commands.stmt_execute_successes += 1;
                                }
                                Ok(GeneralExecuteOutcome::Write(outcome)) => {
                                    // Go clears the cursor bit when the
                                    // statement produced no result set and
                                    // answers a plain OK.
                                    write_affected_rows_ok(
                                        &mut output,
                                        1,
                                        outcome.affected_rows,
                                        outcome.last_insert_id,
                                        protocol_41,
                                    )?;
                                    queries += 1;
                                    commands.stmt_execute_successes += 1;
                                }
                                Err(error) => {
                                    write_query_error(&mut output, &error, protocol_41)?;
                                }
                            }
                            continue;
                        }
                        match engine.execute_general(&general, &values) {
                            Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                                let write_result = {
                                    let mut sink = TcpResultSetSink::new(&mut output, 1);
                                    write_connection_binary_result_set_to_sink(
                                        result.source(),
                                        &mut sink,
                                        options,
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
                            Ok(GeneralExecuteOutcome::Write(outcome)) => {
                                write_affected_rows_ok(
                                    &mut output,
                                    1,
                                    outcome.affected_rows,
                                    outcome.last_insert_id,
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
            Command::StmtClose(bytes) => {
                commands.stmt_close_commands += 1;
                if let Ok(statement_id) = decode_prepared_statement_close(&bytes) {
                    prepared.remove(statement_id);
                }
            }
            Command::StmtReset(bytes) => {
                commands.stmt_reset_commands += 1;
                match decode_prepared_statement_close(&bytes) {
                    // The payload is the same four-byte statement id the
                    // close command carries.
                    Ok(statement_id) if prepared.reset(statement_id) => {
                        write_affected_rows_ok(&mut output, 1, 0, 0, protocol_41)?;
                    }
                    Ok(statement_id) => {
                        write_unknown_statement(&mut output, statement_id, protocol_41)?;
                    }
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
                    write_unknown_statement(&mut output, statement_id, protocol_41)?;
                    continue;
                }
                let Some(cursor) = prepared.cursor_mut(statement_id) else {
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
                let end = cursor
                    .next_row
                    .saturating_add(fetch_size as usize)
                    .min(cursor.rows.len());
                let batch: Vec<Vec<tidb_datatype::Datum>> =
                    cursor.rows[cursor.next_row..end].to_vec();
                cursor.next_row = end;
                let exhausted = end >= cursor.rows.len();
                let columns = cursor.columns.clone();
                let status = if exhausted {
                    (options.status_flags | SERVER_STATUS_LAST_ROW_SEND)
                        & !SERVER_STATUS_CURSOR_EXISTS
                } else {
                    options.status_flags | SERVER_STATUS_CURSOR_EXISTS
                };
                match write_cursor_fetch_batch(
                    &mut output,
                    &columns,
                    &batch,
                    ResultSetOptions {
                        status_flags: status,
                        ..options
                    },
                ) {
                    Ok(()) => {
                        if exhausted {
                            prepared.close_cursor(statement_id);
                        }
                        commands.stmt_fetch_successes += 1;
                    }
                    Err(message) => {
                        write_error(
                            &mut output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            message,
                            protocol_41,
                        )?;
                    }
                }
            }
            Command::InitDb(_)
            | Command::FieldList(_)
            | Command::SetOption(_)
            | Command::ResetConnection
            | Command::Unknown { .. } => write_error(
                &mut output,
                1,
                ER_UNKNOWN_COM_ERROR,
                *b"HY000",
                "command is not supported by the read-only Rust SQL node",
                protocol_41,
            )?,
        }
    }
}

/// Writes the OK packet that answers a successful prepared write.
fn write_affected_rows_ok(
    output: &mut TcpStream,
    sequence: u8,
    affected_rows: u64,
    last_insert_id: u64,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let payload = encode_ok_packet(&OkPacket {
        affected_rows,
        last_insert_id,
        status_flags: SERVER_STATUS_AUTOCOMMIT,
        protocol_41,
        ..OkPacket::default()
    });
    write_payload(output, sequence, &payload)
}

/// Writes the OK packet answering a `BEGIN`/`COMMIT`/`ROLLBACK`, advertising the
/// resulting transaction status so the client tracks whether one is open.
fn write_transaction_control_ok(
    output: &mut TcpStream,
    sequence: u8,
    in_transaction: bool,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let mut status_flags = SERVER_STATUS_AUTOCOMMIT;
    if in_transaction {
        status_flags |= SERVER_STATUS_IN_TRANS;
    }
    let payload = encode_ok_packet(&OkPacket {
        affected_rows: 0,
        status_flags,
        protocol_41,
        ..OkPacket::default()
    });
    write_payload(output, sequence, &payload)
}

fn prepared_statement_id(payload: &[u8]) -> Result<u32, &'static str> {
    let bytes = payload.get(..4).ok_or("truncated prepared statement ID")?;
    let statement_id = u32::from_le_bytes(bytes.try_into().expect("four-byte statement ID"));
    if statement_id == 0 {
        return Err("prepared statement ID must be nonzero");
    }
    Ok(statement_id)
}

fn prepared_parameter_column() -> ColumnInfo {
    ColumnInfo {
        schema: String::new(),
        table: String::new(),
        org_table: String::new(),
        name: "?".to_owned(),
        org_name: String::new(),
        column_length: 20,
        charset: BINARY_DEFAULT_COLLATION_ID,
        flag: 0,
        decimal: 0,
        type_code: MYSQL_TYPE_LONGLONG,
        default_value: None,
    }
}

/// MySQL `SERVER_STATUS_CURSOR_EXISTS` (0x0040): a read-only cursor is open
/// on the statement, and rows arrive through `COM_STMT_FETCH`.
const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
/// MySQL `SERVER_STATUS_LAST_ROW_SEND` (0x0080): the fetch that carried this
/// flag exhausted the cursor.
const SERVER_STATUS_LAST_ROW_SEND: u16 = 0x0080;

/// Materializes every remaining row of a general execute's result, which the
/// cursor holds for later fetches -- Go's eager cursor fetch fills a row
/// container the same way.
fn drain_result_rows(
    result: &mut crate::sql_node::QueryResult<'_>,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
    let mut rows = Vec::new();
    loop {
        let batch = result.source().next_batch(RESULT_BATCH_SIZE.max(1))?;
        if batch.is_empty() {
            result.source().finish()?;
            return Ok(rows);
        }
        rows.extend(batch);
    }
}

/// Writes one packet with an explicit sequence number.
fn write_packet_to(
    output: &mut TcpStream,
    sequence: u8,
    payload: &[u8],
) -> Result<(), MysqlConnectionError> {
    let mut writer = PacketWriter::with_sequence(output, sequence);
    writer
        .write_packet(payload)
        .and_then(|()| writer.flush())
        .map_err(|error| MysqlConnectionError::PartialResult(error.to_string()))
}

/// Writes the terminal EOF (or its deprecate-EOF OK form) carrying the given
/// options' status flags.
fn write_eof_or_ok(
    output: &mut TcpStream,
    sequence: u8,
    options: ResultSetOptions,
) -> Result<(), MysqlConnectionError> {
    let payload = tidb_protocol::encode_eof_packet(&tidb_protocol::EofPacket {
        warnings: options.warnings,
        status_flags: options.status_flags,
        deprecate_eof: options.deprecate_eof,
        protocol_41: options.protocol_41,
        info: Vec::new(),
    });
    write_packet_to(output, sequence, &payload)
}

/// Writes one `COM_STMT_FETCH` answer: up to the requested number of binary
/// rows and the EOF whose status says whether the cursor survives.
fn write_cursor_fetch_batch(
    output: &mut TcpStream,
    columns: &[tidb_protocol::ColumnInfo],
    rows: &[Vec<tidb_datatype::Datum>],
    options: ResultSetOptions,
) -> Result<(), String> {
    let mut stream = tidb_protocol::BinaryResultSetStream::new(columns.to_vec(), options)
        .map_err(|error| error.to_string())?;
    // The fetch answer has no metadata section: the client learned the
    // columns at execute time. The stream still has to pass its own state
    // machine, so the metadata packets are built and discarded.
    let _ = stream
        .metadata_packets()
        .map_err(|error| error.to_string())?;
    let mut sequence = 1;
    for row in rows {
        let cells: Vec<tidb_protocol::BinaryResultCell> = row
            .iter()
            .zip(columns)
            .map(|(datum, column)| {
                crate::connection_resultset::datum_to_binary_cell(datum.clone(), column.type_code)
                    .ok_or_else(|| {
                        format!(
                            "cursor row datum does not match column type {}",
                            column.type_code
                        )
                    })
            })
            .collect::<Result<_, _>>()?;
        let packet = stream
            .row_packet(&cells)
            .map_err(|error| error.to_string())?;
        write_packet_to(output, sequence, &packet).map_err(|error| error.to_string())?;
        sequence += 1;
    }
    write_eof_or_ok(output, sequence, options).map_err(|error| error.to_string())
}

fn write_unknown_statement(
    output: &mut TcpStream,
    statement_id: u32,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    write_error(
        output,
        1,
        ER_UNKNOWN_STMT_HANDLER,
        *b"HY000",
        format!("Unknown prepared statement handler ({statement_id}) given to COM_STMT_EXECUTE"),
        protocol_41,
    )
}

fn write_ok(
    output: &mut TcpStream,
    sequence: u8,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let payload = encode_ok_packet(&OkPacket {
        status_flags: SERVER_STATUS_AUTOCOMMIT,
        protocol_41,
        ..OkPacket::default()
    });
    write_payload(output, sequence, &payload)
}

fn write_query_error(
    output: &mut TcpStream,
    error: &SqlQueryError,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    write_query_error_at(output, 1, error, protocol_41)
}

fn write_query_error_at(
    output: &mut TcpStream,
    sequence: u8,
    error: &SqlQueryError,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    write_error(
        output,
        sequence,
        error.code,
        error.state,
        error.message.as_bytes(),
        protocol_41,
    )
}

/// Go's `errno.ErrAccessDenied` template, `pkg/parser/mysql/errname.go`:
/// `"Access denied for user '%-.48s'@'%-.64s' (using password: %s)"`.
/// `using_password` is `"YES"`/`"NO"` and is `"NO"` ONLY when the client sent
/// zero auth-response bytes (`pkg/server/conn.go`'s `hasPassword` — the same
/// rule at every one of Go's call sites, `pkg/server/conn.go`,
/// `pkg/privilege/privileges/privileges.go`, `pkg/session/session.go`: it
/// reflects what the CLIENT sent, not whether the account itself has a
/// password configured).
fn access_denied_message(user: &str, host: &str, auth_response: &[u8]) -> String {
    let using_password = if auth_response.is_empty() {
        "NO"
    } else {
        "YES"
    };
    format!("Access denied for user '{user}'@'{host}' (using password: {using_password})")
}

/// Go's `mysql.ErrAccountHasBeenLocked` template
/// (`pkg/errno/errname.go`): `"Access denied for user '%s'@'%s'. Account is
/// locked."`.
fn account_locked_message(user: &str, host: &str) -> String {
    format!("Access denied for user '{user}'@'{host}'. Account is locked.")
}

fn write_error(
    output: &mut TcpStream,
    sequence: u8,
    code: u16,
    state: [u8; 5],
    message: impl AsRef<[u8]>,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let payload = encode_error_packet(&ErrorPacket::new(
        code,
        state,
        message.as_ref(),
        protocol_41,
    ));
    write_payload(output, sequence, &payload)
}

fn write_payload(
    output: &mut TcpStream,
    sequence: u8,
    payload: &[u8],
) -> Result<(), MysqlConnectionError> {
    let mut writer = PacketWriter::with_sequence(output, sequence);
    writer.write_packet(payload)?;
    writer.flush()?;
    Ok(())
}

struct TcpResultSetSink<'a> {
    output: &'a mut TcpStream,
    sequence: u8,
    packets: usize,
}

impl<'a> TcpResultSetSink<'a> {
    const fn new(output: &'a mut TcpStream, sequence: u8) -> Self {
        Self {
            output,
            sequence,
            packets: 0,
        }
    }
}

impl ResultSetSink for TcpResultSetSink<'_> {
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError> {
        let mut writer = PacketWriter::with_sequence(&mut *self.output, self.sequence);
        let result = writer.write_packet(payload).and_then(|()| writer.flush());
        self.sequence = writer.sequence();
        result.map_err(|error| SinkWriteError {
            message: error.to_string(),
            bytes_escaped: true,
        })?;
        self.packets += 1;
        Ok(())
    }

    fn packets_written(&self) -> usize {
        self.packets
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
