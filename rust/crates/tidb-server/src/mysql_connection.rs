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
    encode_error_packet, encode_ok_packet, encode_prepared_statement_prepare_response, ColumnInfo,
    Command, ErrorPacket, OkPacket, PacketError, PacketReader, PacketWriter, PreparedParameterType,
    PreparedParameterTypes, PreparedValue, ResultSetOptions, BINARY_DEFAULT_COLLATION_ID,
    MYSQL_TYPE_LONGLONG,
};

use crate::auth_exchange::AuthSwitchRequest;
use crate::configured_user_store::ConfiguredUserStore;
use crate::connection_resultset::{
    write_connection_binary_result_set_to_sink, write_connection_result_set_to_sink,
};
use crate::handshake::{
    negotiate_capabilities, parse_response, InitialHandshake, AUTH_NATIVE_PASSWORD,
    CLIENT_CONNECT_ATTRS, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
    DEFAULT_COLLATION_ID, SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS,
};
use crate::native_password::generate_handshake_salt;
use crate::resultset_writer::{ResultSetSink, SinkWriteError};
use crate::sql_node::{
    ConnectionCancellation, ConnectionTracker, GeneralExecuteOutcome, PreparedStatement,
    QuerySession, QuerySessionFactory, SessionContext, SqlQueryError,
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
/// currency: an integer stays an integer, a string becomes raw bytes.
fn write_bind_parameters(values: Vec<PreparedValue>) -> Vec<PreparedBindValue> {
    values
        .into_iter()
        .map(|value| match value {
            PreparedValue::SignedLongLong(value) => PreparedBindValue::Int(value),
            PreparedValue::String(bytes) | PreparedValue::Decimal(bytes) => {
                PreparedBindValue::Bytes(bytes)
            }
            // The configured write path binds only the two shapes its
            // template models; the general path carries the rest.
            PreparedValue::UnsignedLongLong(value) => PreparedBindValue::Int(value as i64),
            PreparedValue::Float(value) => PreparedBindValue::Bytes(value.to_string().into_bytes()),
            PreparedValue::Double(value) => {
                PreparedBindValue::Bytes(value.to_string().into_bytes())
            }
            PreparedValue::Null => PreparedBindValue::Bytes(Vec::new()),
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
const ER_PARSE_ERROR: u16 = 1064;
const ER_UNKNOWN_ERROR: u16 = 1105;
const ER_WRONG_ARGUMENTS: u16 = 1210;
const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
const RESULT_BATCH_SIZE: usize = 128;

#[derive(Clone, Debug)]
struct ConnectionPreparedStatement {
    statement: PreparedStatement,
    parameter_types: Option<Vec<PreparedParameterType>>,
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
            write_error(
                &mut output,
                2,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                "Access denied",
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
                "Access denied",
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
            "Access denied",
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
    let identity = users.authenticate_native(
        &response.user,
        &peer_addr.ip().to_string(),
        &salt,
        &auth_response,
    );
    let Some(identity) = identity else {
        write_error(
            &mut output,
            response_sequence,
            ER_ACCESS_DENIED_ERROR,
            *b"28000",
            "Access denied",
            protocol_41,
        )?;
        return Ok(ConnectionReport {
            connection_id,
            queries: 0,
            commands: *commands,
            exit: ConnectionExit::AuthenticationRejected,
        });
    };
    let mut engine = match factory.open_session(SessionContext {
        connection_id,
        peer_addr,
        identity,
        cancellation,
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
        reader.set_sequence(0);
        let payload = match reader.read_packet() {
            Ok(payload) => payload,
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
            Command::InitDb(_)
            | Command::FieldList(_)
            | Command::StmtReset(_)
            | Command::StmtFetch(_)
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
