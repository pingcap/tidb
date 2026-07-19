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

use std::fmt;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use tidb_protocol::{
    decode_command, encode_error_packet, encode_ok_packet, Command, ErrorPacket, OkPacket,
    PacketError, PacketReader, PacketWriter, ResultSetOptions,
};

use crate::auth_exchange::AuthSwitchRequest;
use crate::configured_user_store::ConfiguredUserStore;
use crate::connection_resultset::write_connection_result_set_to_sink;
use crate::handshake::{
    negotiate_capabilities, parse_response, InitialHandshake, AUTH_NATIVE_PASSWORD,
    CLIENT_CONNECT_ATTRS, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
    DEFAULT_COLLATION_ID, SERVER_STATUS_AUTOCOMMIT,
};
use crate::native_password::generate_handshake_salt;
use crate::resultset_writer::{ResultSetSink, SinkWriteError};
use crate::sql_node::{
    ConnectionTracker, QuerySession, QuerySessionFactory, SessionContext, SqlQueryError,
};

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
const RESULT_BATCH_SIZE: usize = 128;

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

/// Successful lifecycle report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionReport {
    /// Stable server connection ID advertised in the handshake.
    pub connection_id: u64,
    /// Number of admitted `COM_QUERY` commands.
    pub queries: u64,
    /// Why the connection stopped.
    pub exit: ConnectionExit,
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
    let result = serve_connection_inner(
        stream,
        peer_addr,
        factory,
        users,
        lease.id(),
        max_allowed_packet,
    );
    let failed = result.is_err();
    if failed {
        lease.mark_failed();
    }
    let connection_id = lease.id();
    drop(lease);
    eprintln!(
        "{{\"event\":\"connection_closed\",\"connection_id\":{connection_id},\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{}}}",
        tracker.active(),
        tracker.accepted(),
        tracker.completed(),
        tracker.failed()
    );
    result
}

fn serve_connection_inner<F: QuerySessionFactory>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    factory: &F,
    users: &ConfiguredUserStore,
    connection_id: u64,
    max_allowed_packet: usize,
) -> Result<ConnectionReport, MysqlConnectionError> {
    stream.set_nodelay(true).map_err(MysqlConnectionError::Io)?;
    let mut output = stream.try_clone().map_err(MysqlConnectionError::Io)?;
    let salt = generate_handshake_salt()
        .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?;
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
            exit: ConnectionExit::AuthenticationRejected,
        });
    };
    let mut engine = match factory.open_session(SessionContext {
        connection_id,
        peer_addr,
        identity,
    }) {
        Ok(session) => session,
        Err(error) => {
            write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
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
    loop {
        reader.set_sequence(0);
        let payload = match reader.read_packet() {
            Ok(payload) => payload,
            Err(PacketError::EndOfStream) => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
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
                    exit: ConnectionExit::Quit,
                });
            }
            Command::Ping => write_ok(&mut output, 1, protocol_41)?,
            Command::Query(bytes) => {
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
            Command::InitDb(_)
            | Command::FieldList(_)
            | Command::StmtPrepare(_)
            | Command::StmtExecute(_)
            | Command::StmtClose(_)
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
