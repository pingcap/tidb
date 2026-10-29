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

//! Source-shaped MySQL ERR packet payloads.
//!
//! This leaf mirrors `clientConn.writeError` in `pkg/server/conn.go`: the
//! caller supplies the already-resolved MySQL error code, SQLSTATE bytes, and
//! message bytes, and this module emits the ERR header and fields in the
//! source order. Error conversion, errno lookup, packet framing/flush,
//! connection state, and capability negotiation remain outside this module.

/// MySQL's ERR packet header.
pub const ERR_HEADER: u8 = 0xff;

/// The source-owned fields emitted by TiDB's `writeError` helper.
///
/// `state` and `message` are bytes rather than Rust strings so callers do not
/// lose source data through an implicit UTF-8 conversion. TiDB currently
/// supplies a five-byte SQLSTATE, but the Go writer appends the state as-is;
/// this struct intentionally preserves that behavior for custom errors too.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorPacket {
    /// MySQL error number, encoded little-endian after [`ERR_HEADER`].
    pub code: u16,
    /// SQLSTATE bytes emitted after `#` for protocol 4.1 clients.
    pub state: Vec<u8>,
    /// Error message bytes emitted after the optional SQLSTATE field.
    pub message: Vec<u8>,
    /// Whether the peer negotiated `CLIENT_PROTOCOL_41`.
    pub protocol_41: bool,
}

impl ErrorPacket {
    /// Creates a source-shaped ERR packet from owned or borrowed byte inputs.
    pub fn new(
        code: u16,
        state: impl Into<Vec<u8>>,
        message: impl Into<Vec<u8>>,
        protocol_41: bool,
    ) -> Self {
        Self {
            code,
            state: state.into(),
            message: message.into(),
            protocol_41,
        }
    }
}

/// Encodes an ERR packet payload using TiDB's `writeError` ordering.
///
/// The returned bytes are the logical payload only. The caller owns the
/// uncompressed packet header, sequence number, write, and flush. For
/// protocol-4.1 clients the SQLSTATE marker `#` and the supplied state bytes
/// are included; legacy clients receive only the header, code, and message.
pub fn encode_error_packet(packet: &ErrorPacket) -> Vec<u8> {
    let state_len = if packet.protocol_41 {
        1 + packet.state.len()
    } else {
        0
    };
    let mut encoded = Vec::with_capacity(3 + state_len + packet.message.len());
    encoded.push(ERR_HEADER);
    encoded.extend_from_slice(&packet.code.to_le_bytes());
    if packet.protocol_41 {
        encoded.push(b'#');
        encoded.extend_from_slice(&packet.state);
    }
    encoded.extend_from_slice(&packet.message);
    encoded
}
