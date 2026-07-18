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

//! Source-shaped authentication-plugin exchange bytes.
//!
//! The Go server exposes a deliberately small [`conn.AuthConn`] boundary to
//! authentication plugins: a plugin can write an AuthMoreData packet, read a
//! client packet, and flush the transport.  The server also constructs an
//! AuthSwitchRequest when the identity's configured plugin differs from the
//! client's advertised plugin.  This module owns only those wire envelopes.
//! It does not perform password verification, read the user table, establish
//! TLS, or claim that a client is authenticated.

use std::{fmt, io::Cursor};

use tidb_protocol::{PacketError, PacketReader, PacketWriter};

/// MySQL's AuthSwitchRequest packet header.
pub const AUTH_SWITCH_REQUEST: u8 = 0xfe;
/// AuthMoreData packets begin with this marker before plugin-owned bytes.
pub const AUTH_MORE_DATA_PREFIX: u8 = 0x01;

/// The payload sent when the server asks a client to use another plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthSwitchRequest {
    /// Client-side plugin name advertised in the switch request.
    pub client_plugin: String,
    /// Plugin-specific challenge bytes, without the protocol terminator.
    pub auth_data: Vec<u8>,
}

impl AuthSwitchRequest {
    /// Builds a switch request from the plugin name and challenge bytes.
    ///
    /// Go's `authSwitchRequest` appends a NUL after both the plugin name and
    /// challenge.  NUL in the name is rejected because it would change the
    /// protocol field boundary; challenge bytes remain opaque to this leaf.
    pub fn new(
        client_plugin: impl Into<String>,
        auth_data: impl Into<Vec<u8>>,
    ) -> Result<Self, AuthExchangeError> {
        let client_plugin = client_plugin.into();
        if client_plugin.as_bytes().contains(&0) {
            return Err(AuthExchangeError::EmbeddedNul("client auth plugin"));
        }
        Ok(Self {
            client_plugin,
            auth_data: auth_data.into(),
        })
    }

    /// Encodes the unframed AuthSwitchRequest payload.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(1 + self.client_plugin.len() + 1 + self.auth_data.len() + 1);
        payload.push(AUTH_SWITCH_REQUEST);
        payload.extend_from_slice(self.client_plugin.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&self.auth_data);
        payload.push(0);
        payload
    }

    /// Encodes the request as one uncompressed packet at `sequence`.
    pub fn encode_packet(&self, sequence: u8) -> Result<Vec<u8>, AuthExchangeError> {
        frame_payload(&self.encode_payload(), sequence)
    }

    /// Parses a source-shaped AuthSwitchRequest payload.
    ///
    /// The trailing NUL is removed from the challenge when present, matching
    /// the bytes appended by Go's request builder.  The returned challenge is
    /// still opaque and must be passed to the selected auth implementation.
    pub fn parse_payload(payload: &[u8]) -> Result<Self, AuthExchangeError> {
        if payload.first().copied() != Some(AUTH_SWITCH_REQUEST) {
            return Err(AuthExchangeError::UnexpectedHeader {
                expected: AUTH_SWITCH_REQUEST,
                received: payload.first().copied(),
            });
        }
        let plugin_end = payload[1..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| offset + 1)
            .ok_or(AuthExchangeError::Malformed(
                "auth switch plugin is not NUL terminated",
            ))?;
        let client_plugin = String::from_utf8_lossy(&payload[1..plugin_end]).into_owned();
        let challenge = &payload[plugin_end + 1..];
        if challenge.last().copied() != Some(0) {
            return Err(AuthExchangeError::Malformed(
                "auth switch challenge is not NUL terminated",
            ));
        }
        Self::new(client_plugin, challenge[..challenge.len() - 1].to_vec())
    }
}

/// An AuthMoreData envelope emitted by an authentication plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthMoreData {
    /// Opaque plugin-owned bytes after the protocol marker.
    pub data: Vec<u8>,
}

impl AuthMoreData {
    /// Constructs an AuthMoreData envelope without interpreting its bytes.
    #[must_use]
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self { data: data.into() }
    }

    /// Encodes the unframed AuthMoreData payload.
    #[must_use]
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.data.len() + 1);
        payload.push(AUTH_MORE_DATA_PREFIX);
        payload.extend_from_slice(&self.data);
        payload
    }

    /// Encodes the envelope as one uncompressed packet at `sequence`.
    pub fn encode_packet(&self, sequence: u8) -> Result<Vec<u8>, AuthExchangeError> {
        frame_payload(&self.encode_payload(), sequence)
    }

    /// Parses an AuthMoreData payload and preserves all plugin bytes.
    pub fn parse_payload(payload: &[u8]) -> Result<Self, AuthExchangeError> {
        if payload.first().copied() != Some(AUTH_MORE_DATA_PREFIX) {
            return Err(AuthExchangeError::UnexpectedHeader {
                expected: AUTH_MORE_DATA_PREFIX,
                received: payload.first().copied(),
            });
        }
        Ok(Self::new(payload[1..].to_vec()))
    }
}

/// The unframed response read from a client after a plugin exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthClientResponse {
    /// Opaque password/token response bytes from the client.
    pub bytes: Vec<u8>,
}

impl AuthClientResponse {
    /// Retains client bytes without hashing, comparing, or authenticating them.
    #[must_use]
    pub fn from_payload(payload: &[u8]) -> Self {
        Self {
            bytes: payload.to_vec(),
        }
    }
}

/// Errors raised while constructing or parsing authentication envelopes.
#[derive(Debug)]
pub enum AuthExchangeError {
    /// A NUL byte would terminate a delimited plugin name early.
    EmbeddedNul(&'static str),
    /// A payload has the wrong protocol marker.
    UnexpectedHeader {
        /// Header required by this envelope.
        expected: u8,
        /// Header observed in the payload, or `None` for an empty payload.
        received: Option<u8>,
    },
    /// A required NUL-terminated field is missing.
    Malformed(&'static str),
    /// Packet framing failed while encoding an envelope.
    Packet(PacketError),
}

impl PartialEq for AuthExchangeError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::EmbeddedNul(left), Self::EmbeddedNul(right)) => left == right,
            (
                Self::UnexpectedHeader {
                    expected: left_expected,
                    received: left_received,
                },
                Self::UnexpectedHeader {
                    expected: right_expected,
                    received: right_received,
                },
            ) => left_expected == right_expected && left_received == right_received,
            (Self::Malformed(left), Self::Malformed(right)) => left == right,
            (Self::Packet(left), Self::Packet(right)) => left.to_string() == right.to_string(),
            _ => false,
        }
    }
}

impl Eq for AuthExchangeError {}

impl fmt::Display for AuthExchangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmbeddedNul(field) => write!(formatter, "{field} contains an embedded NUL"),
            Self::UnexpectedHeader { expected, received } => {
                write!(
                    formatter,
                    "expected auth header 0x{expected:02x}, got {received:?}"
                )
            }
            Self::Malformed(message) => formatter.write_str(message),
            Self::Packet(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for AuthExchangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Packet(error) => Some(error),
            Self::EmbeddedNul(_) | Self::UnexpectedHeader { .. } | Self::Malformed(_) => None,
        }
    }
}

fn frame_payload(payload: &[u8], sequence: u8) -> Result<Vec<u8>, AuthExchangeError> {
    let mut framed = Vec::with_capacity(payload.len() + 4);
    let mut writer = PacketWriter::with_sequence(&mut framed, sequence);
    writer
        .write_packet(payload)
        .map_err(AuthExchangeError::Packet)?;
    writer.flush().map_err(AuthExchangeError::Packet)?;
    Ok(framed)
}

/// Decodes one framed client auth response while retaining the packet boundary.
pub fn decode_client_packet(
    framed: &[u8],
    sequence: u8,
) -> Result<AuthClientResponse, AuthExchangeError> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(sequence);
    let payload = reader.read_packet().map_err(AuthExchangeError::Packet)?;
    if reader.get_ref().position() != reader.get_ref().get_ref().len() as u64 {
        return Err(AuthExchangeError::Malformed(
            "auth response contains trailing packet bytes",
        ));
    }
    Ok(AuthClientResponse::from_payload(&payload))
}
