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

//! Source-shaped MySQL connection-phase handshake primitives.
//!
//! This module deliberately stops at packet construction and parsing.  TLS,
//! authentication, compression, and session creation are owned by the server
//! connection state machine.  Keeping those boundaries explicit is important:
//! a parsed auth response is not an authenticated session.

use std::{collections::HashMap, fmt};

use tidb_protocol::{PacketError, PacketWriter};

/// MySQL `CLIENT_CONNECT_WITH_DB`.
pub const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
/// MySQL `CLIENT_PROTOCOL_41`.
pub const CLIENT_PROTOCOL_41: u32 = 1 << 9;
/// MySQL `CLIENT_SSL`.
pub const CLIENT_SSL: u32 = 1 << 11;
/// MySQL `CLIENT_SECURE_CONNECTION`.
pub const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
/// MySQL `CLIENT_PLUGIN_AUTH`.
pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
/// MySQL `CLIENT_CONNECT_ATTRS`.
pub const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
/// MySQL `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA`.
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
/// MySQL `CLIENT_ZSTD_COMPRESSION_ALGORITHM`.
pub const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 1 << 26;

/// MySQL's native password plugin name used when an old client does not
/// advertise `CLIENT_PLUGIN_AUTH`.
pub const AUTH_NATIVE_PASSWORD: &str = "mysql_native_password";

/// TiDB's default metadata collation (`utf8mb4_bin`).
pub const DEFAULT_COLLATION_ID: u8 = 46;
/// `SERVER_STATUS_IN_TRANS`: an explicit transaction is open, set on the OK
/// packet for `BEGIN`/`START TRANSACTION` and cleared on `COMMIT`/`ROLLBACK`.
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
/// The status flag set in the initial TiDB handshake.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
/// The protocol version emitted by TiDB's initial handshake.
pub const PROTOCOL_VERSION_10: u8 = 10;
/// The maximum connection-attribute payload accepted by TiDB's parser.
pub const MAX_CONNECT_ATTRS_SIZE: usize = 1 << 20;

/// The fields written by Go's `clientConn.writeInitialHandshake`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialHandshake {
    /// Server-side connection identifier.
    pub connection_id: u32,
    /// Authentication salt.  TiDB emits a 20-byte salt, but accepts any
    /// source-compatible length from eight through 254 bytes here.
    pub salt: Vec<u8>,
    /// Server capability flags.
    pub capability: u32,
    /// Server collation.  Zero selects [`DEFAULT_COLLATION_ID`].
    pub collation: u8,
    /// Server status flags.
    pub status_flags: u16,
    /// NUL-terminated server version string.
    pub server_version: String,
    /// NUL-terminated authentication plugin name.
    pub auth_plugin: String,
}

impl InitialHandshake {
    /// Encodes the unframed handshake payload.
    pub fn encode_payload(&self) -> Result<Vec<u8>, HandshakeError> {
        validate_initial_handshake(self)?;

        let mut data = Vec::with_capacity(64 + self.server_version.len() + self.salt.len());
        data.push(PROTOCOL_VERSION_10);
        data.extend_from_slice(self.server_version.as_bytes());
        data.push(0);
        data.extend_from_slice(&self.connection_id.to_le_bytes());
        data.extend_from_slice(&self.salt[..8]);
        data.push(0);
        data.extend_from_slice(&(self.capability as u16).to_le_bytes());
        data.push(if self.collation == 0 {
            DEFAULT_COLLATION_ID
        } else {
            self.collation
        });
        data.extend_from_slice(&self.status_flags.to_le_bytes());
        data.extend_from_slice(&((self.capability >> 16) as u16).to_le_bytes());
        data.push((self.salt.len() + 1) as u8);
        data.extend_from_slice(&[0; 10]);
        data.extend_from_slice(&self.salt[8..]);
        data.push(0);
        data.extend_from_slice(self.auth_plugin.as_bytes());
        data.push(0);
        Ok(data)
    }

    /// Encodes the handshake as one sequence-zero uncompressed MySQL packet.
    pub fn encode_packet(&self) -> Result<Vec<u8>, HandshakeError> {
        let payload = self.encode_payload()?;
        let mut packet = Vec::with_capacity(payload.len() + 4);
        let mut writer = PacketWriter::new(&mut packet);
        writer
            .write_packet(&payload)
            .map_err(HandshakeError::Packet)?;
        writer.flush().map_err(HandshakeError::Packet)?;
        Ok(packet)
    }
}

fn validate_initial_handshake(handshake: &InitialHandshake) -> Result<(), HandshakeError> {
    if handshake.salt.len() < 8 || handshake.salt.len() > 254 {
        return Err(HandshakeError::InvalidSaltLength(handshake.salt.len()));
    }
    if handshake.server_version.as_bytes().contains(&0) {
        return Err(HandshakeError::EmbeddedNul("server version"));
    }
    if handshake.auth_plugin.as_bytes().contains(&0) {
        return Err(HandshakeError::EmbeddedNul("auth plugin"));
    }
    Ok(())
}

/// Parsed common fields from a HandshakeResponse41 or SSLRequest packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeResponseHeader {
    /// Client capability flags.
    pub capability: u32,
    /// Client requested collation.
    pub collation: u8,
}

/// Parsed fields from a HandshakeResponse41 packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeResponse {
    /// Client connection attributes.  Duplicate keys follow Go map semantics:
    /// the last value wins.
    pub attrs: HashMap<String, String>,
    /// Client user name.
    pub user: String,
    /// Optional initial database name.
    pub db_name: String,
    /// Client-selected authentication plugin.
    pub auth_plugin: String,
    /// Authentication response bytes.  This is not an authentication result.
    pub auth: Vec<u8>,
    /// Requested zstd level when the corresponding capability is set.
    pub zstd_level: u8,
    /// Client capability flags.
    pub capability: u32,
    /// Client requested collation.
    pub collation: u8,
}

/// The explicit phase of the source-shaped connection handshake.
///
/// This state machine stops before password verification and session
/// creation.  That boundary is intentional: the Go connection owner may
/// still need to upgrade the transport, resolve the user's configured plugin,
/// or run an external authentication implementation after this parser has
/// preserved the wire bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthHandshakePhase {
    /// The server has sent its initial handshake and is waiting for a client
    /// response (or an SSLRequest followed by that response).
    AwaitingResponse,
    /// The client sent an SSLRequest.  The transport owner must complete the
    /// TLS upgrade before another response packet is accepted.
    TlsRequested {
        /// Capability flags after the server/client intersection.
        negotiated_capability: u32,
        /// Client collation from the SSLRequest header.
        collation: u8,
    },
    /// A complete HandshakeResponse41 was parsed and is waiting for the
    /// identity/authentication owner.  No password has been verified.
    AuthenticationPending(Box<AuthHandshakeRequest>),
}

/// A parsed HandshakeResponse41 retained by [`AuthHandshake`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthHandshakeRequest {
    /// Parsed response fields, including the exact authentication bytes.
    pub response: HandshakeResponse,
    /// Client/server capability intersection.
    pub negotiated_capability: u32,
    /// Exact unframed packet payload received from the client.
    pub raw_packet: Vec<u8>,
    /// Plugin advertised by the server's initial handshake.
    pub server_auth_plugin: String,
}

/// The next source-shaped action for the authentication owner.
///
/// The action only describes protocol negotiation.  It never compares a
/// password, reads user storage, performs TLS, or claims an authenticated
/// session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthPluginAction {
    /// Client and server selected the same plugin and the caller can pass the
    /// preserved response bytes to that plugin.
    UseClientPlugin,
    /// The user/identity owner requires a different plugin and the protocol
    /// supports an auth-switch request for it.
    RequestSwitch {
        /// Authentication plugin requested by the identity owner.
        plugin: String,
    },
    /// A legacy client omitted `CLIENT_PLUGIN_AUTH`; TiDB falls back to the
    /// native password plugin when that is the required user plugin.
    NativePasswordFallback,
    /// A legacy client cannot express the configured non-native plugin.
    RejectLegacyClient {
        /// Configured non-native plugin unavailable to this legacy client.
        required_plugin: String,
    },
    /// Plugin selection requires the identity store, which this leaf does not
    /// own.  The client/server names and raw auth bytes remain available in
    /// [`AuthHandshakeRequest`].
    DeferToIdentityPlugin,
}

/// Owns the dependency-closed connection-phase transition up to authentication.
///
/// Go's `clientConn.readOptionalSSLRequestAndHandshakeResponse` first parses a
/// common capability header, optionally hands an SSLRequest to the transport,
/// then parses the remaining response and checks the configured auth plugin
/// (`pkg/server/conn.go:593-714,721-755,939-1040`).  This Rust owner makes those
/// transitions explicit while leaving TLS, user lookup, auth-switch writes,
/// and password verification to later owners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthHandshake {
    server_capability: u32,
    server_auth_plugin: String,
    phase: AuthHandshakePhase,
}

impl AuthHandshake {
    /// Starts a handshake after the initial server packet has been emitted.
    #[must_use]
    pub fn new(server_capability: u32, server_auth_plugin: impl Into<String>) -> Self {
        Self {
            server_capability,
            server_auth_plugin: server_auth_plugin.into(),
            phase: AuthHandshakePhase::AwaitingResponse,
        }
    }

    /// Returns the current explicit handshake phase.
    #[must_use]
    pub const fn phase(&self) -> &AuthHandshakePhase {
        &self.phase
    }

    /// Parses one unframed client packet and advances the handshake.
    ///
    /// A 32-byte SSLRequest is returned as [`AuthHandshakePacket::TlsRequest`]
    /// rather than being passed to the response-body parser.  The caller must
    /// invoke [`Self::tls_established`] after the transport upgrade, then send
    /// the full HandshakeResponse41 packet through this method.
    pub fn receive_packet(&mut self, data: &[u8]) -> Result<AuthHandshakePacket, HandshakeError> {
        if !matches!(&self.phase, AuthHandshakePhase::AwaitingResponse) {
            return Err(HandshakeError::InvalidState(
                "a client response is not accepted in the current handshake phase",
            ));
        }

        let (header, offset) = parse_response_header(data)?;
        let negotiated_capability =
            negotiate_capabilities(header.capability, self.server_capability)?;
        if header.capability & CLIENT_SSL != 0
            && self.server_capability & CLIENT_SSL != 0
            && data.len() == offset
        {
            self.phase = AuthHandshakePhase::TlsRequested {
                negotiated_capability,
                collation: header.collation,
            };
            return Ok(AuthHandshakePacket::TlsRequest {
                negotiated_capability,
                collation: header.collation,
                raw_packet: data.to_vec(),
            });
        }

        let response = parse_response_body(header, data, offset)?;
        let request = AuthHandshakeRequest {
            response,
            negotiated_capability,
            raw_packet: data.to_vec(),
            server_auth_plugin: self.server_auth_plugin.clone(),
        };
        self.phase = AuthHandshakePhase::AuthenticationPending(Box::new(request.clone()));
        Ok(AuthHandshakePacket::Authentication(request))
    }

    /// Completes the transport-owned TLS transition after an SSLRequest.
    pub fn tls_established(&mut self) -> Result<(), HandshakeError> {
        if !matches!(&self.phase, AuthHandshakePhase::TlsRequested { .. }) {
            return Err(HandshakeError::InvalidState(
                "TLS is not pending for this handshake",
            ));
        }
        self.phase = AuthHandshakePhase::AwaitingResponse;
        Ok(())
    }

    /// Classifies plugin negotiation for the parsed response.
    ///
    /// `expected_plugin` is supplied by the future identity/user-store owner;
    /// passing `None` deliberately defers mismatches instead of guessing a
    /// configured plugin.  The returned action does not mutate the state or
    /// perform an auth-switch write.
    pub fn auth_plugin_action(
        &self,
        expected_plugin: Option<&str>,
    ) -> Result<AuthPluginAction, HandshakeError> {
        let AuthHandshakePhase::AuthenticationPending(request) = &self.phase else {
            return Err(HandshakeError::InvalidState(
                "auth plugin selection requires a parsed handshake response",
            ));
        };
        if request.negotiated_capability & CLIENT_PLUGIN_AUTH == 0 {
            return Ok(match expected_plugin.filter(|plugin| !plugin.is_empty()) {
                None | Some(AUTH_NATIVE_PASSWORD) => AuthPluginAction::NativePasswordFallback,
                Some(plugin) => AuthPluginAction::RejectLegacyClient {
                    required_plugin: plugin.to_owned(),
                },
            });
        }

        let client_plugin = request.response.auth_plugin.as_str();
        let server_plugin = request.server_auth_plugin.as_str();
        let Some(expected_plugin) = expected_plugin.filter(|plugin| !plugin.is_empty()) else {
            return if !client_plugin.is_empty() && client_plugin == server_plugin {
                Ok(AuthPluginAction::UseClientPlugin)
            } else {
                Ok(AuthPluginAction::DeferToIdentityPlugin)
            };
        };

        if expected_plugin == client_plugin && expected_plugin == server_plugin {
            Ok(AuthPluginAction::UseClientPlugin)
        } else {
            Ok(AuthPluginAction::RequestSwitch {
                plugin: expected_plugin.to_owned(),
            })
        }
    }
}

/// The packet-level result of [`AuthHandshake::receive_packet`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthHandshakePacket {
    /// A complete SSLRequest that requires a transport-owned TLS upgrade.
    TlsRequest {
        /// Capability flags after the server/client intersection.
        negotiated_capability: u32,
        /// Client collation from the request header.
        collation: u8,
        /// Exact unframed SSLRequest payload.
        raw_packet: Vec<u8>,
    },
    /// A complete HandshakeResponse41 ready for identity/auth plugin policy.
    Authentication(AuthHandshakeRequest),
}

impl HandshakeResponse {
    fn from_header(header: HandshakeResponseHeader) -> Self {
        Self {
            attrs: HashMap::new(),
            user: String::new(),
            db_name: String::new(),
            auth_plugin: String::new(),
            auth: Vec::new(),
            zstd_level: 0,
            capability: header.capability,
            collation: header.collation,
        }
    }
}

/// Parses the 32-byte common HandshakeResponse41/SSLRequest header.
pub fn parse_response_header(
    data: &[u8],
) -> Result<(HandshakeResponseHeader, usize), HandshakeError> {
    if data.len() < 4 + 4 + 1 + 23 {
        return Err(HandshakeError::Malformed(
            "handshake response header is truncated".to_owned(),
        ));
    }
    let capability = u32::from_le_bytes(data[..4].try_into().expect("checked length"));
    Ok((
        HandshakeResponseHeader {
            capability,
            // Bytes 4..8 are max packet size and are intentionally ignored,
            // matching Go's HandshakeResponseHeader.
            collation: data[8],
        },
        32,
    ))
}

/// Parses a complete HandshakeResponse41 packet.
pub fn parse_response(data: &[u8]) -> Result<HandshakeResponse, HandshakeError> {
    let (header, offset) = parse_response_header(data)?;
    parse_response_body(header, data, offset)
}

/// Parses the variable body after [`parse_response_header`].
pub fn parse_response_body(
    header: HandshakeResponseHeader,
    data: &[u8],
    mut offset: usize,
) -> Result<HandshakeResponse, HandshakeError> {
    let mut response = HandshakeResponse::from_header(header.clone());

    let (user, next) = read_nul_string(data, offset, "user")?;
    response.user = user;
    offset = next;

    if header.capability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let (encoded, consumed) = read_lenenc(data, offset)?;
        offset += consumed;
        match encoded {
            None => response.auth.clear(),
            Some(_) if data[offset - consumed] == 1 => {
                // Go accepts a client that sets this capability but sends the
                // one-byte "no auth data" marker followed by a filler byte.
                if offset >= data.len() {
                    return Err(HandshakeError::Malformed(
                        "length-encoded auth marker is truncated".to_owned(),
                    ));
                }
                offset += 1;
            }
            Some(auth_len) => {
                let auth_len = usize::try_from(auth_len).map_err(|_| {
                    HandshakeError::Malformed("auth data length overflows usize".to_owned())
                })?;
                let end = checked_end(offset, auth_len, data.len(), "auth data")?;
                response.auth = data[offset..end].to_vec();
                offset = end;
            }
        }
    } else if header.capability & CLIENT_SECURE_CONNECTION != 0 {
        let auth_len = *data.get(offset).ok_or_else(|| {
            HandshakeError::Malformed("secure auth length is truncated".to_owned())
        })? as usize;
        offset += 1;
        let end = checked_end(offset, auth_len, data.len(), "auth data")?;
        response.auth = data[offset..end].to_vec();
        offset = end;
    } else {
        let (auth, next) = read_nul_bytes(data, offset, "auth data")?;
        response.auth = auth.to_vec();
        offset = next;
    }

    if header.capability & CLIENT_CONNECT_WITH_DB != 0 && offset < data.len() {
        let (db_name, next) = read_nul_string(data, offset, "database")?;
        response.db_name = db_name;
        offset = next;
    }

    if header.capability & CLIENT_PLUGIN_AUTH != 0 {
        let (plugin, next) = read_nul_string(data, offset, "auth plugin")?;
        response.auth_plugin = plugin;
        offset = next;
    }

    if header.capability & CLIENT_CONNECT_ATTRS != 0 {
        if offset == data.len() {
            // Go deliberately treats absent optional attributes as harmless.
            return Ok(response);
        }
        let (attrs_len, consumed) = read_lenenc(data, offset)?;
        offset += consumed;
        let attrs_len = attrs_len.unwrap_or(0).try_into().map_err(|_| {
            HandshakeError::Malformed("connection attributes length overflows usize".to_owned())
        })?;
        if attrs_len > MAX_CONNECT_ATTRS_SIZE {
            return Err(HandshakeError::Malformed(
                "connection attributes exceed the 1 MiB hard limit".to_owned(),
            ));
        }
        let end = checked_end(offset, attrs_len, data.len(), "connection attributes")?;
        response.attrs = parse_attrs(&data[offset..end])?;
        offset = end;
    }

    if header.capability & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
        response.zstd_level = *data.get(offset).ok_or_else(|| {
            HandshakeError::Malformed("zstd compression level is truncated".to_owned())
        })?;
    }

    Ok(response)
}

/// Intersects client capabilities with the server's advertised capabilities.
///
/// `CLIENT_PROTOCOL_41` is required by TiDB's connection path; authentication
/// and TLS policy are intentionally not performed by this helper.
pub fn negotiate_capabilities(client: u32, server: u32) -> Result<u32, HandshakeError> {
    if client & CLIENT_PROTOCOL_41 == 0 {
        return Err(HandshakeError::MissingCapability(CLIENT_PROTOCOL_41));
    }
    Ok(client & server)
}

fn parse_attrs(data: &[u8]) -> Result<HashMap<String, String>, HandshakeError> {
    let mut attrs = HashMap::new();
    let mut offset = 0;
    while offset < data.len() {
        let (key, key_len) = read_lenenc_bytes(data, offset, "attribute key")?;
        offset += key_len;
        let (value, value_len) = read_lenenc_bytes(data, offset, "attribute value")?;
        offset += value_len;
        attrs.insert(lossy(key), lossy(value));
    }
    Ok(attrs)
}

fn read_lenenc_bytes<'a>(
    data: &'a [u8],
    offset: usize,
    field: &'static str,
) -> Result<(&'a [u8], usize), HandshakeError> {
    let (length, consumed) = read_lenenc(data, offset)?;
    let length = length.ok_or_else(|| HandshakeError::Malformed(format!("{field} is NULL")))?;
    let start = offset + consumed;
    let length = usize::try_from(length)
        .map_err(|_| HandshakeError::Malformed(format!("{field} length overflows usize")))?;
    let end = checked_end(start, length, data.len(), field)?;
    Ok((&data[start..end], consumed + length))
}

fn read_lenenc(data: &[u8], offset: usize) -> Result<(Option<u64>, usize), HandshakeError> {
    let first = *data.get(offset).ok_or_else(|| {
        HandshakeError::Malformed("length-encoded integer is truncated".to_owned())
    })?;
    match first {
        0xfb => Ok((None, 1)),
        0xfc => {
            let bytes = data.get(offset + 1..offset + 3).ok_or_else(|| {
                HandshakeError::Malformed("two-byte length-encoded integer is truncated".to_owned())
            })?;
            Ok((
                Some(u64::from(u16::from_le_bytes(
                    bytes.try_into().expect("checked length"),
                ))),
                3,
            ))
        }
        0xfd => {
            let bytes = data.get(offset + 1..offset + 4).ok_or_else(|| {
                HandshakeError::Malformed(
                    "three-byte length-encoded integer is truncated".to_owned(),
                )
            })?;
            Ok((
                Some(
                    u64::from(bytes[0]) | (u64::from(bytes[1]) << 8) | (u64::from(bytes[2]) << 16),
                ),
                4,
            ))
        }
        0xfe => {
            let bytes = data.get(offset + 1..offset + 9).ok_or_else(|| {
                HandshakeError::Malformed(
                    "eight-byte length-encoded integer is truncated".to_owned(),
                )
            })?;
            Ok((
                Some(u64::from_le_bytes(
                    bytes.try_into().expect("checked length"),
                )),
                9,
            ))
        }
        0xff => Err(HandshakeError::Malformed(
            "invalid length-encoded integer marker".to_owned(),
        )),
        value => Ok((Some(u64::from(value)), 1)),
    }
}

fn read_nul_string(
    data: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<(String, usize), HandshakeError> {
    let (bytes, next) = read_nul_bytes(data, offset, field)?;
    Ok((lossy(bytes), next))
}

fn read_nul_bytes<'a>(
    data: &'a [u8],
    offset: usize,
    field: &'static str,
) -> Result<(&'a [u8], usize), HandshakeError> {
    let tail = data
        .get(offset..)
        .ok_or_else(|| HandshakeError::Malformed(format!("{field} is truncated")))?;
    let end = tail
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| HandshakeError::Malformed(format!("{field} is not NUL terminated")))?;
    Ok((&tail[..end], offset + end + 1))
}

fn checked_end(
    offset: usize,
    length: usize,
    data_len: usize,
    field: &'static str,
) -> Result<usize, HandshakeError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| HandshakeError::Malformed(format!("{field} length overflows")))?;
    if end > data_len {
        return Err(HandshakeError::Malformed(format!("{field} is truncated")));
    }
    Ok(end)
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Errors returned by handshake encoding and parsing.
#[derive(Debug)]
pub enum HandshakeError {
    /// A packet is structurally malformed or truncated.
    Malformed(String),
    /// The auth salt is outside the one-byte protocol length range.
    InvalidSaltLength(usize),
    /// A NUL byte would terminate an initial-handshake string early.
    EmbeddedNul(&'static str),
    /// The client omitted a capability required by TiDB's protocol path.
    MissingCapability(u32),
    /// A packet or transition was attempted in the wrong connection phase.
    InvalidState(&'static str),
    /// Packet framing failed while encoding the initial handshake.
    Packet(PacketError),
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(message) => formatter.write_str(message),
            Self::InvalidSaltLength(length) => {
                write!(
                    formatter,
                    "auth salt length {length} must be between 8 and 254"
                )
            }
            Self::EmbeddedNul(field) => write!(formatter, "{field} contains an embedded NUL"),
            Self::MissingCapability(capability) => {
                write!(
                    formatter,
                    "client is missing required capability 0x{capability:08x}"
                )
            }
            Self::InvalidState(message) => formatter.write_str(message),
            Self::Packet(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HandshakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Packet(error) => Some(error),
            Self::Malformed(_)
            | Self::InvalidSaltLength(_)
            | Self::EmbeddedNul(_)
            | Self::MissingCapability(_)
            | Self::InvalidState(_) => None,
        }
    }
}
