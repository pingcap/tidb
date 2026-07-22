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

use std::collections::HashMap;

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
    /// Client user name.
    pub user: String,
    /// Optional initial database name.
    pub db_name: String,
    /// Client-selected authentication plugin.
    pub auth_plugin: String,
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
}
