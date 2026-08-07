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

//! Stable Rust-native authority for `pkg/server/internal/handshake`.

use std::borrow::Cow;
use std::collections::HashMap;

/// A MySQL protocol string whose authority is the exact wire bytes.
///
/// Go's `string([]byte)` preserves arbitrary bytes. Using Rust `String` at the
/// packet boundary used to replace malformed UTF-8 with U+FFFD and could make
/// two distinct identities compare equal. Text-only consumers must now choose
/// explicitly between strict UTF-8 and a lossy diagnostic rendering.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct WireString(Vec<u8>);

impl WireString {
    /// Owns the exact bytes from a protocol field.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the exact protocol bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns strict UTF-8 when the field is textual in Rust's domain.
    #[must_use]
    pub fn as_utf8(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// Returns a display-only rendering without changing the authoritative
    /// bytes.
    #[must_use]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }

    /// Reports whether the wire field has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Test/support convenience matching `String::push_str` while preserving
    /// the byte owner.
    pub fn push_str(&mut self, value: &str) {
        self.0.extend_from_slice(value.as_bytes());
    }
}

impl From<&str> for WireString {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

impl From<String> for WireString {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl PartialEq<str> for WireString {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<&str> for WireString {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

/// Rust-native authority for Go's complete `handshake.Response41` contract.
///
/// [`Default`] preserves the useful semantics of Go's zero value: collection
/// reads are empty, owned strings and authentication bytes have length zero,
/// and every numeric field is zero. `HashMap` is the native mutable-map owner;
/// unlike Go's nil map it can be populated without a separate allocation
/// state, while preserving zero-value lookup and length behavior.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HandshakeResponse41 {
    /// Client connection attributes. Duplicate keys follow Go map semantics:
    /// the last value wins.
    pub attrs: HashMap<String, String>,
    /// Byte-exact connection attributes. This is the authority when a key or
    /// value is not UTF-8; `attrs` is the lossy compatibility view used by
    /// existing text-only consumers.
    pub raw_attrs: HashMap<Vec<u8>, Vec<u8>>,
    /// Client user name.
    pub user: WireString,
    /// Optional initial database name.
    pub db_name: WireString,
    /// Client-selected authentication plugin.
    pub auth_plugin: WireString,
    /// Authentication response bytes. This is not an authentication result.
    pub auth: Vec<u8>,
    /// Requested zstd level when the corresponding capability is set.
    ///
    /// The wire field is one byte, but the zstd encoder API and negotiated
    /// compression owner both use `i32`; normalizing once during parsing keeps
    /// the shared response contract native to all downstream consumers.
    pub zstd_level: i32,
    /// Client capability flags.
    pub capability: u32,
    /// Client requested collation.
    pub collation: u8,
    /// Source-generated connection-attribute warnings. Go writes these to its
    /// logger; Rust retains them on the parsed response so no diagnostic is
    /// silently discarded before the connection owner chooses a log sink.
    pub attr_warnings: Vec<String>,
}
