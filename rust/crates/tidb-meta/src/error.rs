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

//! Decode failures for the catalog codec.

use std::fmt;

/// Go's `structure.ErrInvalidHashKeyFlag` / `meta.ErrInvalidString` surface,
/// narrowed to the decode failures this crate can actually produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaError {
    /// The key does not start with the `m` meta namespace prefix.
    NotMetaKey,
    /// The mem-comparable bytes/uint framing inside the key is malformed.
    MalformedKey,
    /// The type byte did not match the expected structure flag.
    ///
    /// Go: `ErrInvalidHashKeyFlag`, "invalid encoded ... key flag %c".
    UnexpectedTypeFlag(u8),
    /// A field name did not carry the expected `<prefix>:<id>` shape.
    ///
    /// Go: `ErrInvalidString`, "fail to parse ...".
    InvalidFieldKey,
    /// A scalar value was not the decimal ASCII integer Go writes.
    ///
    /// Go: `strconv.ParseInt` failing inside `TxStructure.GetInt64`.
    InvalidIntValue,
    /// A catalog JSON value did not parse into its model struct.
    InvalidJson(String),
}

impl fmt::Display for MetaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotMetaKey => formatter.write_str("invalid encoded meta key prefix"),
            Self::MalformedKey => formatter.write_str("malformed encoded meta key"),
            Self::UnexpectedTypeFlag(flag) => {
                write!(formatter, "invalid encoded key flag {}", *flag as char)
            }
            Self::InvalidFieldKey => formatter.write_str("fail to parse meta field key"),
            Self::InvalidIntValue => formatter.write_str("invalid meta integer value"),
            Self::InvalidJson(message) => write!(formatter, "invalid meta JSON value: {message}"),
        }
    }
}

impl std::error::Error for MetaError {}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, MetaError>;
