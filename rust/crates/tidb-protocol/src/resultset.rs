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

use crate::{append_length_encoded_int, encode_text_row, ColumnInfo};

/// MySQL's OK packet header.
pub const OK_HEADER: u8 = 0x00;

/// MySQL's EOF packet header.
pub const EOF_HEADER: u8 = 0xfe;

/// The source-owned fields emitted by TiDB's `writeOK` helper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkPacket {
    /// Number of rows affected by the statement.
    pub affected_rows: u64,
    /// Statement-generated auto-increment identifier.
    pub last_insert_id: u64,
    /// MySQL server status flags.
    pub status_flags: u16,
    /// Number of warnings attached to the statement.
    pub warnings: u16,
    /// Optional informational text, encoded as a length-encoded string.
    pub info: Vec<u8>,
    /// Whether the peer negotiated `CLIENT_PROTOCOL_41`.
    pub protocol_41: bool,
}

impl Default for OkPacket {
    fn default() -> Self {
        Self {
            affected_rows: 0,
            last_insert_id: 0,
            status_flags: 0,
            warnings: 0,
            info: Vec::new(),
            protocol_41: true,
        }
    }
}

/// The source-owned fields emitted by TiDB's `writeEOF` helper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EofPacket {
    /// Number of warnings attached to the result set.
    pub warnings: u16,
    /// MySQL server status flags.
    pub status_flags: u16,
    /// Whether the peer negotiated `CLIENT_DEPRECATE_EOF`.
    pub deprecate_eof: bool,
    /// Whether the peer negotiated `CLIENT_PROTOCOL_41`.
    pub protocol_41: bool,
    /// Informational text used by the OK-shaped EOF variant.
    pub info: Vec<u8>,
}

impl Default for EofPacket {
    fn default() -> Self {
        Self {
            warnings: 0,
            status_flags: 0,
            deprecate_eof: false,
            protocol_41: true,
            info: Vec::new(),
        }
    }
}

/// Controls the metadata and terminal packets around a text result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultSetOptions {
    /// MySQL server status flags for metadata and terminal EOF packets.
    pub status_flags: u16,
    /// Number of warnings emitted with metadata and terminal EOF packets.
    pub warnings: u16,
    /// Whether the peer negotiated `CLIENT_DEPRECATE_EOF`.
    pub deprecate_eof: bool,
    /// Whether the peer negotiated `CLIENT_PROTOCOL_41`.
    pub protocol_41: bool,
}

impl Default for ResultSetOptions {
    fn default() -> Self {
        Self {
            status_flags: 0,
            warnings: 0,
            deprecate_eof: false,
            protocol_41: true,
        }
    }
}

/// Errors caused by a row that cannot match the result-set metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultSetError {
    /// A text row has a different number of values than the column metadata.
    RowColumnCount {
        /// Zero-based row index.
        row: usize,
        /// Number of columns advertised by the result set.
        expected: usize,
        /// Number of values supplied by the row.
        actual: usize,
    },
}

impl std::fmt::Display for ResultSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowColumnCount {
                row,
                expected,
                actual,
            } => write!(
                formatter,
                "result row {row} has {actual} values, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for ResultSetError {}

/// Encodes the payload of an OK packet using TiDB's `writeOkWith` ordering.
pub fn encode_ok_packet(packet: &OkPacket) -> Vec<u8> {
    encode_ok_like_packet(OK_HEADER, packet)
}

/// Encodes the payload of an EOF packet using TiDB's `writeEOF` behavior.
///
/// With `deprecate_eof`, TiDB emits an OK-shaped packet with an EOF header and
/// zero affected rows/last-insert-id fields. Otherwise it emits the compact
/// legacy EOF form, including warning/status fields only under protocol 4.1.
pub fn encode_eof_packet(packet: &EofPacket) -> Vec<u8> {
    if packet.deprecate_eof {
        return encode_ok_like_packet(
            EOF_HEADER,
            &OkPacket {
                affected_rows: 0,
                last_insert_id: 0,
                status_flags: packet.status_flags,
                warnings: packet.warnings,
                info: packet.info.clone(),
                protocol_41: packet.protocol_41,
            },
        );
    }

    let mut encoded = vec![EOF_HEADER];
    if packet.protocol_41 {
        encoded.extend_from_slice(&packet.warnings.to_le_bytes());
        encoded.extend_from_slice(&packet.status_flags.to_le_bytes());
    }
    encoded
}

/// Encodes a complete text-protocol result set as logical packet payloads.
///
/// The sequence mirrors `clientConn.writeChunks`: column count, one packet per
/// column, an old-client metadata EOF, each text row, and a terminal EOF. The
/// caller owns packet framing/sequence numbers and can pass each returned
/// payload to [`crate::PacketWriter`]. Typed Datum formatting, charset
/// conversion, and result-set iteration remain outside this protocol leaf.
pub fn encode_text_result_set(
    columns: &[ColumnInfo],
    rows: &[Vec<Option<Vec<u8>>>],
    options: ResultSetOptions,
) -> Result<Vec<Vec<u8>>, ResultSetError> {
    let mut packets = Vec::with_capacity(2 + columns.len() + rows.len());

    let mut column_count = Vec::new();
    append_length_encoded_int(&mut column_count, columns.len() as u64);
    packets.push(column_count);

    for column in columns {
        let mut metadata = Vec::new();
        column.dump(&mut metadata);
        packets.push(metadata);
    }

    let eof = EofPacket {
        warnings: options.warnings,
        status_flags: options.status_flags,
        deprecate_eof: options.deprecate_eof,
        protocol_41: options.protocol_41,
        info: Vec::new(),
    };
    if !options.deprecate_eof {
        packets.push(encode_eof_packet(&eof));
    }

    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(ResultSetError::RowColumnCount {
                row: row_index,
                expected: columns.len(),
                actual: row.len(),
            });
        }
        let values = row.iter().map(|value| value.as_deref()).collect::<Vec<_>>();
        packets.push(encode_text_row(&values));
    }

    packets.push(encode_eof_packet(&eof));
    Ok(packets)
}

fn encode_ok_like_packet(header: u8, packet: &OkPacket) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.push(header);
    append_length_encoded_int(&mut encoded, packet.affected_rows);
    append_length_encoded_int(&mut encoded, packet.last_insert_id);
    if packet.protocol_41 {
        encoded.extend_from_slice(&packet.status_flags.to_le_bytes());
        encoded.extend_from_slice(&packet.warnings.to_le_bytes());
    }
    if !packet.info.is_empty() {
        append_length_encoded_int(&mut encoded, packet.info.len() as u64);
        encoded.extend_from_slice(&packet.info);
    }
    encoded
}
