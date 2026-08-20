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

//! Source-shaped MySQL command decoding.
//!
//! This leaf mirrors the command byte split in TiDB's `clientConn.dispatch`.
//! It deliberately stops before authentication, session lifecycle, prepared
//! statement state, and response writing. Keeping the raw command payload
//! lets those owners make their own charset and capability decisions instead
//! of making the protocol crate guess at SQL or client state.

/// MySQL command byte for closing a connection.
pub const COM_QUIT: u8 = 0x01;
/// MySQL command byte for selecting the default database.
pub const COM_INIT_DB: u8 = 0x02;
/// MySQL command byte for a text query.
pub const COM_QUERY: u8 = 0x03;
/// MySQL command byte for a metadata request.
pub const COM_FIELD_LIST: u8 = 0x04;
/// MySQL command byte for the server statistics line (`mysqladmin status`).
pub const COM_STATISTICS: u8 = 0x09;
/// MySQL command byte for a server ping.
pub const COM_PING: u8 = 0x0e;
/// MySQL command byte for a prepared statement.
pub const COM_STMT_PREPARE: u8 = 0x16;
/// MySQL command byte for executing a prepared statement.
pub const COM_STMT_EXECUTE: u8 = 0x17;
/// MySQL command byte for appending a chunk to a prepared statement parameter.
pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
/// MySQL command byte for closing a prepared statement.
pub const COM_STMT_CLOSE: u8 = 0x19;
/// MySQL command byte for resetting a prepared statement.
pub const COM_STMT_RESET: u8 = 0x1a;
/// MySQL command byte for fetching a cursor result.
pub const COM_STMT_FETCH: u8 = 0x1c;
/// MySQL command byte for changing connection options.
pub const COM_SET_OPTION: u8 = 0x1b;
/// MySQL command byte for resetting a connection.
pub const COM_RESET_CONNECTION: u8 = 0x1f;

/// A decoded command and its source-owned raw payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    /// Close the connection; the payload is ignored by TiDB.
    Quit,
    /// Change the default database.
    InitDb(Vec<u8>),
    /// Execute a text query. One trailing NUL is accepted by TiDB's server.
    Query(Vec<u8>),
    /// Request table-field metadata.
    FieldList(Vec<u8>),
    /// Ping the server.
    Ping,
    /// `COM_STATISTICS`: the one-line server summary `mysqladmin status`
    /// prints. Carries no payload.
    Statistics,
    /// Prepare a statement from its raw SQL payload.
    StmtPrepare(Vec<u8>),
    /// Execute a prepared statement with its binary payload.
    StmtExecute(Vec<u8>),
    /// Append a chunk to one prepared statement parameter. Go answers this
    /// command with NO packet at all on success (`clientConn.dispatch`
    /// returns the handler's nil, `pkg/server/conn.go:1578-1579`).
    StmtSendLongData(Vec<u8>),
    /// Close a prepared statement.
    StmtClose(Vec<u8>),
    /// Reset a prepared statement.
    StmtReset(Vec<u8>),
    /// Fetch rows from a prepared cursor.
    StmtFetch(Vec<u8>),
    /// Set client connection options.
    SetOption(Vec<u8>),
    /// Reset all per-connection state.
    ResetConnection,
    /// A command byte not yet owned by a Rust server leaf.
    Unknown {
        /// The command byte that has no Rust owner yet.
        code: u8,
        /// The raw bytes following the command byte.
        payload: Vec<u8>,
    },
}

/// Why a command payload could not be decoded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandError {
    /// The packet contained no command byte.
    EmptyPayload,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPayload => formatter.write_str("empty MySQL command payload"),
        }
    }
}

impl std::error::Error for CommandError {}

/// Splits a MySQL command payload exactly once, preserving all bytes after
/// the command byte except the one trailing NUL accepted for `COM_QUERY`.
pub fn decode_command(payload: &[u8]) -> Result<Command, CommandError> {
    let Some((&code, command_payload)) = payload.split_first() else {
        return Err(CommandError::EmptyPayload);
    };
    // Go's `clientConn.dispatch` trims exactly one trailing NUL for BOTH
    // COM_QUERY (issue 1989) and COM_STMT_PREPARE (issue 39132); clients that
    // send a NUL-terminated statement must reach the parser identically on
    // either command (`pkg/server/conn.go:1543-1546,1571-1574`).
    let command_payload = match code {
        COM_QUERY | COM_STMT_PREPARE => command_payload
            .strip_suffix(&[0])
            .unwrap_or(command_payload),
        _ => command_payload,
    };
    Ok(match code {
        COM_QUIT => Command::Quit,
        COM_INIT_DB => Command::InitDb(command_payload.to_vec()),
        COM_QUERY => Command::Query(command_payload.to_vec()),
        COM_FIELD_LIST => Command::FieldList(command_payload.to_vec()),
        COM_PING => Command::Ping,
        COM_STATISTICS => Command::Statistics,
        COM_STMT_PREPARE => Command::StmtPrepare(command_payload.to_vec()),
        COM_STMT_EXECUTE => Command::StmtExecute(command_payload.to_vec()),
        COM_STMT_SEND_LONG_DATA => Command::StmtSendLongData(command_payload.to_vec()),
        COM_STMT_CLOSE => Command::StmtClose(command_payload.to_vec()),
        COM_STMT_RESET => Command::StmtReset(command_payload.to_vec()),
        COM_STMT_FETCH => Command::StmtFetch(command_payload.to_vec()),
        COM_SET_OPTION => Command::SetOption(command_payload.to_vec()),
        COM_RESET_CONNECTION => Command::ResetConnection,
        code => Command::Unknown {
            code,
            payload: command_payload.to_vec(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{decode_command, Command, CommandError};

    #[test]
    fn dispatch_command_vectors_preserve_source_payloads() {
        assert_eq!(
            decode_command(&[0x03, b's', b'e', b'l', 0]),
            Ok(Command::Query(b"sel".to_vec()))
        );
        assert_eq!(
            decode_command(&[0x03, b's', 0, 0]),
            Ok(Command::Query(b"s\0".to_vec()))
        );
        assert_eq!(
            decode_command(&[0x02, b't', b'e', b's', b't']),
            Ok(Command::InitDb(b"test".to_vec()))
        );
        // Issue 39132: COM_STMT_PREPARE shares COM_QUERY's trailing-NUL trim.
        assert_eq!(
            decode_command(&[0x16, b's', b'e', b'l', 0]),
            Ok(Command::StmtPrepare(b"sel".to_vec()))
        );
        // A NUL is only trimmed for the two commands Go trims it for.
        assert_eq!(
            decode_command(&[0x02, b'd', b'b', 0]),
            Ok(Command::InitDb(b"db\0".to_vec()))
        );
        assert_eq!(decode_command(&[0x0e]), Ok(Command::Ping));
        assert_eq!(decode_command(&[0x09]), Ok(Command::Statistics));
        assert_eq!(decode_command(&[0x01]), Ok(Command::Quit));
        assert_eq!(
            decode_command(&[0xfa, 1, 2]),
            Ok(Command::Unknown {
                code: 0xfa,
                payload: vec![1, 2],
            })
        );
    }

    /// `COM_STATISTICS` is what `mysqladmin status` sends, and it decodes to
    /// its own command rather than falling into the unknown-command arm that
    /// answered "command is not supported by the read-only Rust SQL node".
    #[test]
    fn com_statistics_decodes_as_its_own_command() {
        assert_eq!(
            decode_command(&[crate::command::COM_STATISTICS]),
            Ok(Command::Statistics)
        );
        // It carries no payload; a stray one is ignored, as Go ignores it.
        assert_eq!(
            decode_command(&[crate::command::COM_STATISTICS, b'x']),
            Ok(Command::Statistics)
        );
        // The neighbouring bytes keep their own meanings.
        assert_eq!(
            decode_command(&[crate::command::COM_PING]),
            Ok(Command::Ping)
        );
    }

    #[test]
    fn empty_dispatch_payload_is_rejected_before_command_routing() {
        assert_eq!(decode_command(&[]), Err(CommandError::EmptyPayload));
    }
}
