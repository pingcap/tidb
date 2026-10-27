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
    sync::atomic::Ordering,
};

use tidb_protocol::{PacketError, PacketWriter};

use crate::handshake_response::{HandshakeResponse41, WireString};

/// MySQL `CLIENT_CONNECT_WITH_DB`.
pub const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
/// MySQL `CLIENT_FOUND_ROWS` (UPDATE reports matched rather than changed rows).
pub const CLIENT_FOUND_ROWS: u32 = 1 << 1;
/// MySQL `CLIENT_TRANSACTIONS` (the client understands transaction commands).
pub const CLIENT_TRANSACTIONS: u32 = 1 << 13;
/// MySQL `CLIENT_COMPRESS` (zlib compressed command packets).
pub const CLIENT_COMPRESS: u32 = 1 << 5;
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
const AUTO_CONNECT_ATTRS_SIZE: i64 = 65_536;

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
    let (header, offset) = parse_response_header(data)?;
    parse_response_body(header, data, offset)
}

/// Parses the variable body after [`parse_response_header`].
pub fn parse_response_body(
    header: HandshakeResponseHeader,
    data: &[u8],
    offset: usize,
) -> Result<HandshakeResponse41, HandshakeError> {
    let mut response = HandshakeResponse41 {
        capability: header.capability,
        collation: header.collation,
        ..HandshakeResponse41::default()
    };
    parse_response_body_into(&mut response, data, offset)?;
    Ok(response)
}

/// Mutates an existing response in Go field order and preserves mutations
/// made before a later malformed field.
pub fn parse_response_body_into(
    response: &mut HandshakeResponse41,
    data: &[u8],
    mut offset: usize,
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
            match parse_attrs(&data[offset..end]) {
                Ok((attrs, warnings)) => {
                    response.attrs = attrs;
                    if !warnings.is_empty() {
                        tidb_log::debug(&warnings.join("; "), &[]);
                    }
                }
                Err(error) => {
                    // Go logs this decode failure and accepts the handshake.
                    // It also returns immediately, so zstd parsing is skipped.
                    tidb_log::warn(&format!("parse attrs failed: {error}"), &[]);
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

type ParsedConnAttrs = (HashMap<WireString, WireString>, Vec<String>);

pub(crate) fn parse_attrs(data: &[u8]) -> Result<ParsedConnAttrs, HandshakeError> {
    if tidb_vardef::CONNECT_ATTRS_SIZE.load(Ordering::Relaxed) == 0 {
        return Ok((HashMap::new(), Vec::new()));
    }
    let decoded = decode_conn_attrs(data)?;
    Ok(apply_conn_attrs_policy_and_metrics(
        decoded,
        tidb_vardef::CONNECT_ATTRS_SIZE.load(Ordering::Relaxed),
    ))
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
    limit: i64,
) -> ParsedConnAttrs {
    let effective_limit = normalize_connect_attrs_limit(limit);
    let mut attrs = HashMap::new();
    let mut total_size = 0_i64;
    let mut accepted_size = 0_i64;
    let mut truncated = false;

    for (key, value) in decoded.items {
        let pair_size = i64::try_from(key.len() + value.len()).unwrap_or(i64::MAX);
        total_size = total_size.saturating_add(pair_size);
        if total_size > effective_limit {
            if !truncated {
                truncated = true;
                tidb_vardef::CONNECT_ATTRS_LOST.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        if !truncated {
            attrs.insert(WireString::from_bytes(key), WireString::from_bytes(value));
            accepted_size = accepted_size.saturating_add(pair_size);
        }
    }

    update_connect_attrs_longest_seen(decoded.total_size);

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
        attrs.insert(
            WireString::from("_truncated"),
            WireString::from(value.clone()),
        );
        warnings.push(format!(
            "session connection attributes truncated: total size {} bytes exceeds performance_schema_session_connect_attrs_size ({}), {} bytes were discarded",
            decoded.total_size, effective_limit, truncated_bytes
        ));
    }
    (attrs, warnings)
}

pub(crate) fn normalize_connect_attrs_limit(limit: i64) -> i64 {
    if limit < 0 {
        AUTO_CONNECT_ATTRS_SIZE
    } else {
        limit
    }
}

pub(crate) fn update_connect_attrs_longest_seen(total_size: i64) {
    // Update LongestSeen only for normal-sized payloads (< 64 KiB).
    if total_size >= AUTO_CONNECT_ATTRS_SIZE {
        return;
    }
    let longest_seen = &tidb_vardef::CONNECT_ATTRS_LONGEST_SEEN;
    loop {
        let old = longest_seen.load(Ordering::Relaxed);
        if total_size <= old {
            break;
        }
        if longest_seen
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
