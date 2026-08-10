// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! How a served answer reaches the client socket.
//!
//! Every OK, EOF, ERR and result-set packet a connection writes leaves through
//! this module, so the framing rules — sequence numbering, the `protocol_41`
//! split, and the fact that a status word is only ever read from live session
//! facts (see [`WireStatus`]) — live in exactly one place.

use std::io::Write;
use tidb_protocol::result_encoder::ResultEncoder;

use tidb_protocol::{
    encode_error_packet, encode_ok_packet, ColumnInfo, ErrorPacket, OkPacket, PacketIoWriter,
    PacketWriter, ResultSetOptions, BINARY_DEFAULT_COLLATION_ID, MYSQL_TYPE_LONGLONG,
};

use crate::mysql_connection::MysqlConnectionError;
use crate::mysql_connection::ER_UNKNOWN_STMT_HANDLER;
use crate::mysql_tls::ClientStream;
use crate::resultset_writer::{ResultSetSink, SinkWriteError};
use crate::sql_node::SqlQueryError;
use crate::wire_status::WireStatus;

/// Writes the OK packet that answers a successful prepared write.
pub(crate) fn write_affected_rows_ok<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    affected_rows: u64,
    last_insert_id: u64,
    status: WireStatus,
    warnings: u16,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let payload = encode_ok_packet(&OkPacket {
        affected_rows,
        last_insert_id,
        status_flags: status.bits(),
        warnings,
        protocol_41,
        ..OkPacket::default()
    });
    write_payload(output, sequence, &payload)
}

pub(crate) fn prepared_statement_id(payload: &[u8]) -> Result<u32, &'static str> {
    let bytes = payload.get(..4).ok_or("truncated prepared statement ID")?;
    let statement_id = u32::from_le_bytes(bytes.try_into().expect("four-byte statement ID"));
    if statement_id == 0 {
        return Err("prepared statement ID must be nonzero");
    }
    Ok(statement_id)
}

pub(crate) fn prepared_parameter_column() -> ColumnInfo {
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

/// Writes one packet with an explicit sequence number.
pub(crate) fn write_packet_to<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    payload: &[u8],
) -> Result<(), MysqlConnectionError> {
    output
        .write_packet(sequence, payload)
        .map(|_| ())
        .map_err(|error| MysqlConnectionError::PartialResult(error.to_string()))
}

/// Writes the terminal EOF (or its deprecate-EOF OK form) carrying the given
/// options' status flags.
pub(crate) fn write_eof_or_ok<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
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

pub(crate) fn write_unknown_statement<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    statement_id: u32,
    command: &str,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    // Go names the command with the same short token it passes to
    // `mysql.NewErr(mysql.ErrUnknownStmtHandler, strconv.Itoa(stmtID), ...)`:
    // `stmt_execute`, `stmt_reset`, `stmt_fetch`, `stmt_send_longdata`.
    write_error(
        output,
        1,
        ER_UNKNOWN_STMT_HANDLER,
        *b"HY000",
        format!("Unknown prepared statement handler ({statement_id}) given to {command}"),
        protocol_41,
    )
}

/// Writes the bare OK packet Go's `writeOK` answers `COM_PING`, `COM_INIT_DB`,
/// the post-authentication handshake, and `BEGIN`/`COMMIT`/`ROLLBACK` with.
///
/// Go's `writeOK` is exactly `writeOkWith(ctx, mysql.OKHeader, true,
/// cc.ctx.Status())` (`pkg/server/conn.go:1685`): one live status read, no
/// per-command variant. Transaction control needs no writer of its own for the
/// same reason -- the session has already applied the statement by the time
/// this runs, so its status IS the transaction state, and there is no second
/// flag to keep in step with it.
///
/// None of these commands runs a statement, so none resets the warning buffer
/// and each reports the count the preceding statement left -- which is what
/// `ctx.WarningCount()` reads at that moment too.
pub(crate) fn write_ok<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    status: WireStatus,
    warnings: u16,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    let payload = encode_ok_packet(&OkPacket {
        status_flags: status.bits(),
        warnings,
        protocol_41,
        ..OkPacket::default()
    });
    write_payload(output, sequence, &payload)
}

/// The only connection-lifetime part of an OK/EOF packet: what the client
/// negotiated at handshake.
///
/// Every [`ResultSetOptions`] this connection builds is built here, and this is
/// the one place a `status_flags` field is filled -- from a [`WireStatus`],
/// which cannot be a literal. That is the whole point: a fourth hardcoded
/// `SERVER_STATUS_AUTOCOMMIT` has nowhere to go.
#[derive(Clone, Copy)]
pub(crate) struct WireFraming {
    pub(crate) deprecate_eof: bool,
    pub(crate) protocol_41: bool,
}

impl WireFraming {
    /// `encoder` is Go's `clientConn.rsEncoder`: the connection's
    /// `@@character_set_results` policy, which `initResultEncoder` refreshes
    /// once per COMMAND rather than caching for the connection -- the
    /// variable can be `SET` between two statements, and the second one must
    /// go out in the new charset.
    pub(crate) fn result_set(
        self,
        status: WireStatus,
        warnings: u16,
        encoder: ResultEncoder,
    ) -> ResultSetOptions {
        ResultSetOptions {
            status_flags: status.bits(),
            warnings,
            deprecate_eof: self.deprecate_eof,
            protocol_41: self.protocol_41,
            result_encoder: encoder,
        }
    }
}

pub(crate) fn write_query_error<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    error: &SqlQueryError,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    write_query_error_at(output, 1, error, protocol_41)
}

pub(crate) fn write_query_error_at<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    error: &SqlQueryError,
    protocol_41: bool,
) -> Result<(), MysqlConnectionError> {
    // The undetermined verdict is refused here rather than at each call site,
    // so no answer path can write it by omission. Go `pkg/server/conn.go:1288`
    // reaches the same place by testing the error once, in the command loop,
    // for every command alike.
    if error.is_result_undetermined() {
        return Err(MysqlConnectionError::ResultUndetermined(
            error.message.clone(),
        ));
    }
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
pub(crate) fn access_denied_message(user: &str, host: &str, auth_response: &[u8]) -> String {
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
pub(crate) fn account_locked_message(user: &str, host: &str) -> String {
    format!("Access denied for user '{user}'@'{host}'. Account is locked.")
}

pub(crate) fn write_error<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
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

pub(crate) fn write_payload<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    payload: &[u8],
) -> Result<(), MysqlConnectionError> {
    output.write_packet(sequence, payload).map(|_| ())
}

/// One connection-owned logical-packet output.
///
/// Authentication uses the raw stream. After the authentication OK, the same
/// response encoders use [`PacketIoWriter`] through this boundary, so packet
/// framing and negotiated compression cannot diverge by command type.
pub(crate) trait ConnectionPacketOutput {
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) -> Result<u8, MysqlConnectionError>;
}

impl ConnectionPacketOutput for ClientStream {
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) -> Result<u8, MysqlConnectionError> {
        let mut writer = PacketWriter::with_sequence(self, sequence);
        writer.write_packet(payload)?;
        writer.flush()?;
        Ok(writer.sequence())
    }
}

impl<W: Write> ConnectionPacketOutput for PacketIoWriter<W> {
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) -> Result<u8, MysqlConnectionError> {
        self.set_sequence(sequence);
        self.write_packet(payload)?;
        let next_sequence = self.sequence();
        self.flush()?;
        Ok(next_sequence)
    }
}

pub(crate) struct TcpResultSetSink<'a, O: ConnectionPacketOutput + ?Sized> {
    output: &'a mut O,
    sequence: u8,
    packets: usize,
}

impl<'a, O: ConnectionPacketOutput + ?Sized> TcpResultSetSink<'a, O> {
    pub(crate) const fn new(output: &'a mut O, sequence: u8) -> Self {
        Self {
            output,
            sequence,
            packets: 0,
        }
    }
}

impl<O: ConnectionPacketOutput + ?Sized> ResultSetSink for TcpResultSetSink<'_, O> {
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError> {
        self.sequence = self
            .output
            .write_packet(self.sequence, payload)
            .map_err(|error| SinkWriteError {
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
