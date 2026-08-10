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

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    sync::atomic::{AtomicI64, Ordering},
};

use tidb_protocol::{PacketError, PacketWriter};

use crate::handshake_response::{HandshakeResponse41, WireString};

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
/// The status flag set in the initial TiDB handshake, which Go hardcodes there
/// too (`pkg/server/conn.go:496`) because the handshake precedes any session.
/// Every LATER packet's status is the session's own -- see
/// [`crate::wire_status::WireStatus`], which owns the rest of the bits.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
/// The protocol version emitted by TiDB's initial handshake.
pub const PROTOCOL_VERSION_10: u8 = 10;
/// The maximum connection-attribute payload accepted by TiDB's parser.
pub const MAX_CONNECT_ATTRS_SIZE: usize = 1 << 20;
/// Go's default aggregate connection-attribute byte limit.
pub const DEFAULT_CONNECT_ATTRS_SIZE: i64 = 4096;
const AUTO_CONNECT_ATTRS_SIZE: i64 = 65_536;

/// Process-wide connection-attribute policy and status counters.
///
/// Go stores these values in `vardef.ConnectAttrsSize`,
/// `ConnectAttrsLongestSeen`, and `ConnectAttrsLost`. Keeping the three
/// atomics together also permits isolated state in boundary tests without
/// mutating process-global values.
#[derive(Debug)]
pub struct ConnectionAttrsState {
    limit: AtomicI64,
    longest_seen: AtomicI64,
    lost: AtomicI64,
}

impl ConnectionAttrsState {
    /// Creates an isolated state with the supplied Go policy value.
    #[must_use]
    pub const fn new(limit: i64) -> Self {
        Self {
            limit: AtomicI64::new(limit),
            longest_seen: AtomicI64::new(0),
            lost: AtomicI64::new(0),
        }
    }

    /// Changes the aggregate-byte limit for subsequent responses.
    pub fn set_limit(&self, limit: i64) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    /// Returns the configured policy value before `-1` normalization.
    #[must_use]
    pub fn limit(&self) -> i64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Returns the largest aggregate below 64 KiB observed so far.
    #[must_use]
    pub fn longest_seen(&self) -> i64 {
        self.longest_seen.load(Ordering::Relaxed)
    }

    /// Returns the number of responses whose attributes were truncated.
    #[must_use]
    pub fn lost(&self) -> i64 {
        self.lost.load(Ordering::Relaxed)
    }
}

static CONNECTION_ATTRS_STATE: ConnectionAttrsState =
    ConnectionAttrsState::new(DEFAULT_CONNECT_ATTRS_SIZE);

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
    /// The client sent an SSLRequest.  The transport owner
    /// ([`crate::mysql_tls::ClientStream::upgrade_to_tls`]) must complete the
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
    pub response: HandshakeResponse41,
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
        Ok(AuthHandshakePacket::Authentication(Box::new(request)))
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

        let client_plugin = request.response.auth_plugin.as_utf8().unwrap_or("");
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
    Authentication(Box<AuthHandshakeRequest>),
}

/// Parses the 32-byte common HandshakeResponse41/SSLRequest header.
pub fn parse_response_header(
    data: &[u8],
) -> Result<(HandshakeResponseHeader, usize), HandshakeError> {
    let mut response = HandshakeResponse41::default();
    let offset = parse_response_header_into(&mut response, data)?;
    Ok((
        HandshakeResponseHeader {
            capability: response.capability,
            collation: response.collation,
        },
        offset,
    ))
}

/// Mutates the common fields exactly as Go's `HandshakeResponseHeader` does.
pub fn parse_response_header_into(
    response: &mut HandshakeResponse41,
    data: &[u8],
) -> Result<usize, HandshakeError> {
    if data.len() < 4 + 4 + 1 + 23 {
        return Err(HandshakeError::Malformed(
            "handshake response header is truncated".to_owned(),
        ));
    }
    response.capability = u32::from_le_bytes(data[..4].try_into().expect("checked length"));
    // Bytes 4..8 are max packet size and are intentionally ignored, matching
    // Go's HandshakeResponseHeader.
    response.collation = data[8];
    Ok(32)
}

/// Parses a complete HandshakeResponse41 packet.
pub fn parse_response(data: &[u8]) -> Result<HandshakeResponse41, HandshakeError> {
    parse_response_with_attrs_state(data, &CONNECTION_ATTRS_STATE)
}

/// Parses a response with an explicit connection-attribute policy/metrics
/// owner. This is the source-shaped seam for Go's three `vardef` atomics.
pub fn parse_response_with_attrs_state(
    data: &[u8],
    attrs_state: &ConnectionAttrsState,
) -> Result<HandshakeResponse41, HandshakeError> {
    let (header, offset) = parse_response_header(data)?;
    parse_response_body_with_attrs_state(header, data, offset, attrs_state)
}

/// Parses the variable body after [`parse_response_header`].
pub fn parse_response_body(
    header: HandshakeResponseHeader,
    data: &[u8],
    offset: usize,
) -> Result<HandshakeResponse41, HandshakeError> {
    parse_response_body_with_attrs_state(header, data, offset, &CONNECTION_ATTRS_STATE)
}

/// Parses a variable response body with an explicit source policy owner.
pub fn parse_response_body_with_attrs_state(
    header: HandshakeResponseHeader,
    data: &[u8],
    offset: usize,
    attrs_state: &ConnectionAttrsState,
) -> Result<HandshakeResponse41, HandshakeError> {
    let mut response = HandshakeResponse41 {
        capability: header.capability,
        collation: header.collation,
        ..HandshakeResponse41::default()
    };
    parse_response_body_into_with_attrs_state(&mut response, data, offset, attrs_state)?;
    Ok(response)
}

/// Mutates an existing response in Go field order and preserves mutations
/// made before a later malformed field.
pub fn parse_response_body_into_with_attrs_state(
    response: &mut HandshakeResponse41,
    data: &[u8],
    mut offset: usize,
    attrs_state: &ConnectionAttrsState,
) -> Result<(), HandshakeError> {
    let (user, next) = read_nul_bytes(data, offset, "user")?;
    response.user = WireString::from_bytes(user);
    offset = next;

    if response.capability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let marker = *data.get(offset).ok_or_else(|| {
            HandshakeError::Malformed("length-encoded auth marker is truncated".to_owned())
        })?;
        if marker == 1 {
            // MySQL 5.7 can set the capability but send this two-byte no-auth
            // shape. Go advances by two without inspecting the filler byte.
            offset = offset.checked_add(2).ok_or_else(|| {
                HandshakeError::Malformed("auth marker offset overflows".to_owned())
            })?;
        } else {
            let (encoded, consumed) = read_lenenc(data, offset)?;
            offset += consumed;
            if let Some(auth_len) = encoded {
                let auth_len = usize::try_from(auth_len).map_err(|_| {
                    HandshakeError::Malformed("auth data length overflows usize".to_owned())
                })?;
                let end = checked_end(offset, auth_len, data.len(), "auth data")?;
                response.auth = data[offset..end].to_vec();
                offset = end;
            }
        }
    } else if response.capability & CLIENT_SECURE_CONNECTION != 0 {
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

    if response.capability & CLIENT_CONNECT_WITH_DB != 0 {
        let tail = data
            .get(offset..)
            .ok_or_else(|| HandshakeError::Malformed("database is truncated".to_owned()))?;
        if !tail.is_empty() {
            let (db_name, next) = read_nul_bytes(data, offset, "database")?;
            response.db_name = WireString::from_bytes(db_name);
            offset = next;
        }
    }

    if response.capability & CLIENT_PLUGIN_AUTH != 0 {
        let tail = data
            .get(offset..)
            .ok_or_else(|| HandshakeError::Malformed("auth plugin is truncated".to_owned()))?;
        if let Some(plugin_len) = tail.iter().position(|byte| *byte == 0) {
            if plugin_len > 0 {
                response.auth_plugin = WireString::from_bytes(&tail[..plugin_len]);
            }
            offset += plugin_len + 1;
        }
    }

    if response.capability & CLIENT_CONNECT_ATTRS != 0 {
        let tail = data.get(offset..).ok_or_else(|| {
            HandshakeError::Malformed("connection attributes are truncated".to_owned())
        })?;
        if tail.is_empty() {
            // Go deliberately treats absent optional attributes as harmless.
            return Ok(());
        }
        let (attrs_len, consumed) = read_lenenc(data, offset)?;
        offset += consumed;
        if let Some(attrs_len) = attrs_len {
            let attrs_len = attrs_len.try_into().map_err(|_| {
                HandshakeError::Malformed("connection attributes length overflows usize".to_owned())
            })?;
            if attrs_len > MAX_CONNECT_ATTRS_SIZE {
                return Err(HandshakeError::ConnectionAttrsTooLarge);
            }
            let end = checked_end(offset, attrs_len, data.len(), "connection attributes")?;
            match parse_attrs(&data[offset..end], attrs_state) {
                Ok((attrs, raw_attrs, warnings)) => {
                    response.attrs = attrs;
                    response.raw_attrs = raw_attrs;
                    response.attr_warnings = warnings;
                }
                Err(_) => {
                    // Go logs this decode failure and accepts the handshake.
                    // It also returns immediately, so zstd parsing is skipped.
                    return Ok(());
                }
            }
            offset = end;
        }
    }

    if response.capability & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
        response.zstd_level = i32::from(*data.get(offset).ok_or_else(|| {
            HandshakeError::Malformed("zstd compression level is truncated".to_owned())
        })?);
    }

    Ok(())
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

#[derive(Debug)]
pub(crate) struct DecodedConnAttrs {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    total_size: i64,
    has_deprecated_underscore_attr: bool,
}

type ParsedConnAttrs = (
    HashMap<String, String>,
    HashMap<Vec<u8>, Vec<u8>>,
    Vec<String>,
);

pub(crate) fn parse_attrs(
    data: &[u8],
    state: &ConnectionAttrsState,
) -> Result<ParsedConnAttrs, HandshakeError> {
    if state.limit() == 0 {
        return Ok((HashMap::new(), HashMap::new(), Vec::new()));
    }
    let decoded = decode_conn_attrs(data)?;
    Ok(apply_conn_attrs_policy_and_metrics(decoded, state))
}

pub(crate) fn decode_conn_attrs(data: &[u8]) -> Result<DecodedConnAttrs, HandshakeError> {
    let mut decoded = DecodedConnAttrs {
        items: Vec::new(),
        total_size: 0,
        has_deprecated_underscore_attr: false,
    };
    let mut offset = 0;
    while offset < data.len() {
        let (key, key_len) = read_lenenc_bytes(data, offset, "attribute key")?;
        offset += key_len;
        let (value, value_len) = read_lenenc_bytes(data, offset, "attribute value")?;
        offset += value_len;
        decoded.total_size += i64::try_from(key.len() + value.len()).unwrap_or(i64::MAX);
        if !decoded.has_deprecated_underscore_attr
            && key.starts_with(b"_")
            && !matches!(
                key,
                b"_client_name" | b"_client_version" | b"_os" | b"_pid" | b"_platform"
            )
        {
            decoded.has_deprecated_underscore_attr = true;
        }
        decoded.items.push((key.to_vec(), value.to_vec()));
    }
    Ok(decoded)
}

pub(crate) fn apply_conn_attrs_policy_and_metrics(
    decoded: DecodedConnAttrs,
    state: &ConnectionAttrsState,
) -> ParsedConnAttrs {
    let effective_limit = normalize_connect_attrs_limit(state.limit());
    let mut attrs = HashMap::new();
    let mut raw_attrs = HashMap::new();
    let mut total_size = 0_i64;
    let mut accepted_size = 0_i64;
    let mut truncated = false;

    for (key, value) in decoded.items {
        let pair_size = i64::try_from(key.len() + value.len()).unwrap_or(i64::MAX);
        total_size = total_size.saturating_add(pair_size);
        if total_size > effective_limit {
            if !truncated {
                truncated = true;
                state.lost.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        if !truncated {
            attrs.insert(lossy(&key), lossy(&value));
            raw_attrs.insert(key, value);
            accepted_size = accepted_size.saturating_add(pair_size);
        }
    }

    update_connect_attrs_longest_seen(decoded.total_size, state);

    let mut warnings = Vec::with_capacity(2);
    if decoded.has_deprecated_underscore_attr {
        warnings.push(
            "custom connection attributes with leading underscore are deprecated and will be rejected in a future release"
                .to_owned(),
        );
    }
    if truncated {
        let truncated_bytes = decoded.total_size.saturating_sub(accepted_size);
        let value = truncated_bytes.to_string();
        attrs.insert("_truncated".to_owned(), value.clone());
        raw_attrs.insert(b"_truncated".to_vec(), value.into_bytes());
        warnings.push(format!(
            "session connection attributes truncated: total size {} bytes exceeds performance_schema_session_connect_attrs_size ({}), {} bytes were discarded",
            decoded.total_size, effective_limit, truncated_bytes
        ));
    }
    (attrs, raw_attrs, warnings)
}

pub(crate) fn normalize_connect_attrs_limit(limit: i64) -> i64 {
    if limit < 0 {
        AUTO_CONNECT_ATTRS_SIZE
    } else {
        limit
    }
}

pub(crate) fn update_connect_attrs_longest_seen(total_size: i64, state: &ConnectionAttrsState) {
    if total_size >= AUTO_CONNECT_ATTRS_SIZE {
        return;
    }
    loop {
        let old = state.longest_seen.load(Ordering::Relaxed);
        if total_size <= old {
            break;
        }
        if state
            .longest_seen
            .compare_exchange_weak(old, total_size, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
}

fn read_lenenc_bytes<'a>(
    data: &'a [u8],
    offset: usize,
    field: &'static str,
) -> Result<(&'a [u8], usize), HandshakeError> {
    let (length, consumed) = read_lenenc(data, offset)?;
    let length = length.unwrap_or(0);
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
        value => Ok((Some(u64::from(value)), 1)),
    }
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
    /// The outer connection-attribute frame exceeds TiDB's hard 1 MiB limit.
    ConnectionAttrsTooLarge,
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

impl HandshakeError {
    /// Returns the error text Go exposes through `clientConn.writeError`.
    ///
    /// Parser bounds failures all collapse to the plain
    /// `mysql.ErrMalformPacket`; the connection-attribute hard limit is its
    /// own plain error and keeps the source message.
    #[must_use]
    pub(crate) fn client_error_message(&self) -> Cow<'_, str> {
        match self {
            Self::Malformed(_) => Cow::Borrowed(tidb_error::mysql::ERR_MALFORM_PACKET),
            _ => Cow::Owned(self.to_string()),
        }
    }
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(message) => formatter.write_str(message),
            Self::ConnectionAttrsTooLarge => formatter.write_str(
                "connection refused: session connection attributes exceed the 1 MiB hard limit",
            ),
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
            | Self::ConnectionAttrsTooLarge
            | Self::InvalidSaltLength(_)
            | Self::EmbeddedNul(_)
            | Self::MissingCapability(_)
            | Self::InvalidState(_) => None,
        }
    }
}
