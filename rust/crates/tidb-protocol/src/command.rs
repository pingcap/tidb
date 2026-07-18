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
/// MySQL command byte for a server ping.
pub const COM_PING: u8 = 0x0e;
/// MySQL command byte for a prepared statement.
pub const COM_STMT_PREPARE: u8 = 0x16;
/// MySQL command byte for executing a prepared statement.
pub const COM_STMT_EXECUTE: u8 = 0x17;
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
    /// Prepare a statement from its raw SQL payload.
    StmtPrepare(Vec<u8>),
    /// Execute a prepared statement with its binary payload.
    StmtExecute(Vec<u8>),
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
    let command_payload = match code {
        COM_QUERY => command_payload
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
        COM_STMT_PREPARE => Command::StmtPrepare(command_payload.to_vec()),
        COM_STMT_EXECUTE => Command::StmtExecute(command_payload.to_vec()),
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
        assert_eq!(decode_command(&[0x0e]), Ok(Command::Ping));
        assert_eq!(decode_command(&[0x01]), Ok(Command::Quit));
        assert_eq!(
            decode_command(&[0xfa, 1, 2]),
            Ok(Command::Unknown {
                code: 0xfa,
                payload: vec![1, 2],
            })
        );
    }

    #[test]
    fn empty_dispatch_payload_is_rejected_before_command_routing() {
        assert_eq!(decode_command(&[]), Err(CommandError::EmptyPayload));
    }
}
